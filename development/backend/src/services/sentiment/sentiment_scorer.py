# sentiment.py
from functools import lru_cache
import json
import os
import warnings

import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

__all__ = ["score_texts", "load_scores"]

MODEL_ID = "StephanAkkerman/FinTwitBERT-sentiment"
BATCH_SIZE = 32
MAX_LENGTH = 256


@lru_cache(maxsize=1)
def _load_model():
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_ID)
    model.to(device).eval()

    # Prefer the model's own label mapping; fall back to the documented
    # order (0 = neutral, 1 = bullish, 2 = bearish).
    labels = {i: str(l).lower() for i, l in model.config.id2label.items()}
    by_name = {name: i for i, name in labels.items()}
    bullish_idx = by_name.get("bullish", 1)
    bearish_idx = by_name.get("bearish", 2)

    return tokenizer, model, device, bullish_idx, bearish_idx


def _clean(text):
    """Coerce any input item to a string (None/NaN -> '')."""
    if text is None or (isinstance(text, float) and text != text):
        return ""
    return text if isinstance(text, str) else str(text)


def _repair_tail(filename):
    """Truncate a trailing partial line left behind by a crash.

    Every record is written as one complete line ending in '\\n', so any
    bytes after the last newline are an incomplete record.
    """
    if not os.path.exists(filename):
        return
    with open(filename, "rb+") as f:
        data = f.read()
        keep = data.rfind(b"\n") + 1  # 0 if there is no newline at all
        if keep != len(data):
            f.truncate(keep)


def _score_batch(batch, tokenizer, model, device, bullish_idx, bearish_idx):
    inputs = tokenizer(
        batch,
        return_tensors="pt",
        padding=True,
        truncation=True,
        max_length=MAX_LENGTH,
    )
    inputs = {k: v.to(device) for k, v in inputs.items()}

    with torch.inference_mode():
        probs = model(**inputs).logits.softmax(dim=-1).cpu()

    results = []
    for p in probs:
        net = p[bullish_idx].item() - p[bearish_idx].item()  # -1 .. 1
        results.append(round(5.5 + 4.5 * net, 2))             # 1 .. 10
    return results


def score_texts(texts, filename, batch_size=BATCH_SIZE, resume=True):
    """Score each string from 1 (most negative) to 10 (most positive) and
    append the results to `filename` after each completed batch.

    Format is JSON Lines: one {"text": ..., "score": ...} object per line.
    Use `load_scores(filename)` to read it back as a dict.

    Args:
        texts: list of strings (None/NaN are treated as "").
        filename: path of the output file.
        batch_size: texts per forward pass (auto-halved on GPU OOM).
        resume: if True (default), keep existing results in the file and
            only score texts that aren't in it yet. If False, the file is
            overwritten.

    Returns:
        None
    """
    if isinstance(texts, str):
        raise TypeError("Expected a list of strings, not a single string.")
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1.")

    # Load the model first so a load failure can't touch the output file.
    tokenizer, model, device, bullish_idx, bearish_idx = _load_model()

    # Deduplicate while preserving order.
    unique = list(dict.fromkeys(_clean(t) for t in texts))

    os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)

    if resume:
        _repair_tail(filename)
        done = load_scores(filename) if os.path.exists(filename) else {}
        mode = "a"
    else:
        done = {}
        mode = "w"

    todo = [t for t in unique if t not in done]

    with open(filename, mode, encoding="utf-8") as f:
        pos = 0
        while pos < len(todo):
            batch = todo[pos:pos + batch_size]

            try:
                scores = _score_batch(batch, tokenizer, model, device,
                                      bullish_idx, bearish_idx)
            except torch.cuda.OutOfMemoryError:
                if device.type == "cuda":
                    torch.cuda.empty_cache()
                if batch_size == 1:
                    raise
                batch_size = max(1, batch_size // 2)
                warnings.warn(f"GPU out of memory; retrying with "
                              f"batch_size={batch_size}.")
                continue  # retry the same position with a smaller batch

            # One write call per batch, then force it to disk.
            lines = "".join(
                json.dumps({"text": t, "score": s}, ensure_ascii=False) + "\n"
                for t, s in zip(batch, scores)
            )
            f.write(lines)
            f.flush()
            os.fsync(f.fileno())

            pos += len(batch)


def load_scores(filename):
    """Read a file written by score_texts back into a {text: score} dict.

    Malformed lines (e.g. a partial final line from a crash) are skipped
    with a warning instead of raising.
    """
    scores = {}
    skipped = 0
    with open(filename, encoding="utf-8", errors="replace") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                scores[record["text"]] = record["score"]
            except (json.JSONDecodeError, KeyError, TypeError):
                skipped += 1
    if skipped:
        warnings.warn(f"Skipped {skipped} malformed line(s) in {filename}.")
    return scores
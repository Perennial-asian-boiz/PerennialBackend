# sentiment_scorer.py
from functools import lru_cache
import json
import os
import warnings

import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

__all__ = ["score_texts", "score_items", "load_scores", "load_score_records"]

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
    neutral_idx = by_name.get("neutral", 0)

    return tokenizer, model, device, bullish_idx, bearish_idx, neutral_idx


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


def _forward(args, tokenizer, model, device):
    inputs = tokenizer(
        *args,
        return_tensors="pt",
        padding=True,
        truncation=True,          # pairs: trims the longer sequence first
        max_length=MAX_LENGTH,
    )
    inputs = {k: v.to(device) for k, v in inputs.items()}
    with torch.inference_mode():
        return model(**inputs).logits.softmax(dim=-1).cpu()


def _batch_probs(texts, contexts, tokenizer, model, device):
    """Class probabilities for each text, in input order.

    Items with a non-empty context are encoded as a (context, text) pair so the
    model sees the parent comment / post title first and the text being scored
    last; items without context are encoded alone.
    """
    out = [None] * len(texts)
    single = [i for i, c in enumerate(contexts) if not c]
    paired = [i for i, c in enumerate(contexts) if c]
    if single:
        probs = _forward(([texts[i] for i in single],), tokenizer, model, device)
        for i, p in zip(single, probs):
            out[i] = p
    if paired:
        probs = _forward(([contexts[i] for i in paired], [texts[i] for i in paired]),
                         tokenizer, model, device)
        for i, p in zip(paired, probs):
            out[i] = p
    return out


def _summarize(p, bullish_idx, bearish_idx, neutral_idx):
    pb, pr, pn = p[bullish_idx].item(), p[bearish_idx].item(), p[neutral_idx].item()
    net = pb - pr                                   # -1 .. 1
    return round(5.5 + 4.5 * net, 2), pb, pr, pn    # score 1 .. 10


def _handle_oom(device, batch_size):
    if device.type == "cuda":
        torch.cuda.empty_cache()
    if batch_size == 1:
        raise
    new_size = max(1, batch_size // 2)
    warnings.warn(f"GPU out of memory; retrying with batch_size={new_size}.")
    return new_size


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
        The filename.

    For per-item metadata, context and class probabilities, use score_items().
    """
    if isinstance(texts, str):
        raise TypeError("Expected a list of strings, not a single string.")
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1.")

    # Load the model first so a load failure can't touch the output file.
    tokenizer, model, device, bullish_idx, bearish_idx, neutral_idx = _load_model()

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
                probs = _batch_probs(batch, [""] * len(batch), tokenizer, model, device)
            except torch.cuda.OutOfMemoryError:
                batch_size = _handle_oom(device, batch_size)
                continue  # retry the same position with a smaller batch

            scores = [_summarize(p, bullish_idx, bearish_idx, neutral_idx)[0]
                      for p in probs]

            # One write call per batch, then force it to disk.
            lines = "".join(
                json.dumps({"text": t, "score": s}, ensure_ascii=False) + "\n"
                for t, s in zip(batch, scores)
            )
            f.write(lines)
            f.flush()
            os.fsync(f.fileno())

            pos += len(batch)

    return filename


def score_items(items, filename, batch_size=BATCH_SIZE, resume=True):
    """Score items keyed by an id, optionally with context, and keep the
    class probabilities.

    Each item is a dict:
        {"id": <unique key>, "text": <text to score>, "context": <optional>}
    `context` (e.g. the parent comment) is shown to the model before `text`
    as a sentence pair; it is trimmed first if the input is too long.

    Unlike score_texts(), results are keyed by `id` rather than by text, so
    identical texts under different ids are kept as separate records.

    Output is JSON Lines, one record per item, appended after every batch:
        {"id": ..., "score": 1..10, "bullish": p, "bearish": p, "neutral": p}
    Read it back with load_score_records(filename).

    Texts are scored shortest-first to minimise padding; the file order
    therefore differs from the input order (use the id).

    Returns:
        The filename.
    """
    if isinstance(items, (str, dict)):
        raise TypeError("Expected a list of dicts.")
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1.")

    tokenizer, model, device, bullish_idx, bearish_idx, neutral_idx = _load_model()

    unique = {}
    for it in items:
        key = str(it["id"])
        if key not in unique:
            unique[key] = (_clean(it.get("text")), _clean(it.get("context")))

    os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)

    if resume:
        _repair_tail(filename)
        done = load_score_records(filename) if os.path.exists(filename) else {}
        mode = "a"
    else:
        done = {}
        mode = "w"

    todo = [(k, t, c) for k, (t, c) in unique.items() if k not in done]
    todo.sort(key=lambda x: len(x[1]) + len(x[2]))

    with open(filename, mode, encoding="utf-8") as f:
        pos = 0
        while pos < len(todo):
            batch = todo[pos:pos + batch_size]

            try:
                probs = _batch_probs([b[1] for b in batch], [b[2] for b in batch],
                                     tokenizer, model, device)
            except torch.cuda.OutOfMemoryError:
                batch_size = _handle_oom(device, batch_size)
                continue

            lines = []
            for (key, _t, _c), p in zip(batch, probs):
                score, pb, pr, pn = _summarize(p, bullish_idx, bearish_idx, neutral_idx)
                lines.append(json.dumps(
                    {"id": key, "score": score, "bullish": round(pb, 4),
                     "bearish": round(pr, 4), "neutral": round(pn, 4)}) + "\n")
            f.write("".join(lines))
            f.flush()
            os.fsync(f.fileno())

            pos += len(batch)

    return filename


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


def load_score_records(filename):
    """Read a file written by score_items back into {id: record}.

    Malformed lines are skipped with a warning instead of raising.
    """
    records = {}
    skipped = 0
    with open(filename, encoding="utf-8", errors="replace") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                records[str(record["id"])] = record
            except (json.JSONDecodeError, KeyError, TypeError):
                skipped += 1
    if skipped:
        warnings.warn(f"Skipped {skipped} malformed line(s) in {filename}.")
    return records
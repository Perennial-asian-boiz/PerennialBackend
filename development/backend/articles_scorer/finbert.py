"""Sentiment of news articles with FinBERT (ProsusAI/finbert).

Quick use:
    from finbert_articles import download_model, score_articles
    download_model()                        # once; later runs use the local copy
    results = score_articles([article_1, article_2, ...])
    results[i]                              # result for article i, same order as the input

Each result:
    {"label": "positive" | "negative" | "neutral",
     "sentiment": positive - negative,      # -1 (most negative) .. 1 (most positive)
     "positive": p, "negative": p, "neutral": p,
     "chunks": n}                           # pieces a long article was split into

Long articles are never cut off: anything longer than FinBERT's 512-token window
is split into overlapping chunks, every chunk is scored, and the chunk
probabilities are averaged (weighted by chunk length) into one result. Chunks of
all articles are scored together in batches.

Command line:
    python finbert_articles.py article1.txt article2.txt ...
    python finbert_articles.py --json articles.json      # a JSON list of strings
Prints one JSON result per line, in input order.

Environment variables:
    FINBERT_MODEL_DIR   where the model is downloaded to (default: Hugging Face cache)
"""
from functools import lru_cache
import json
import os
import warnings

import torch
from huggingface_hub import snapshot_download
from transformers import AutoModelForSequenceClassification, AutoTokenizer

__all__ = ["download_model", "load_model", "score_articles"]

MODEL_ID = "ProsusAI/finbert"
MODEL_DIR = os.environ.get("FINBERT_MODEL_DIR") or None
BATCH_SIZE = 16          # chunks per forward pass
MAX_LENGTH = 512         # tokens per chunk, including special tokens (BERT's limit)
STRIDE = 64              # tokens shared by neighbouring chunks of one article
LABELS = ("positive", "negative", "neutral")


# ---- model ---------------------------------------------------------------------------------
def download_model(model_id=MODEL_ID, cache_dir=MODEL_DIR):
    """Download the model files (if not already present) and return the local path.

    Safe to call every run: files already downloaded are not fetched again.
    """
    if os.path.isdir(model_id):             # already a local folder
        return model_id
    return snapshot_download(
        model_id, cache_dir=cache_dir,
        allow_patterns=["*.json", "*.txt", "*.safetensors", "*.bin"],
    )


@lru_cache(maxsize=1)
def load_model(model_id=MODEL_ID):
    """Download (if needed) and load the model once per process.

    Returns (tokenizer, model, device, label_index) where label_index maps
    "positive" / "negative" / "neutral" to the model's output columns.
    """
    path = download_model(model_id)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    tokenizer = AutoTokenizer.from_pretrained(path)
    model = AutoModelForSequenceClassification.from_pretrained(path)
    model.to(device).eval()

    label_index = {str(l).strip().lower(): int(i) for i, l in model.config.id2label.items()}
    if set(label_index) != set(LABELS):
        # FinBERT's documented order: 0 = positive, 1 = negative, 2 = neutral.
        label_index = {name: i for i, name in enumerate(LABELS)}
    return tokenizer, model, device, label_index


# ---- chunking and inference ----------------------------------------------------------------
def _clean(text):
    """Coerce any input item to a string (None/NaN -> '')."""
    if text is None or (isinstance(text, float) and text != text):
        return ""
    return text if isinstance(text, str) else str(text)


def _chunk(texts, tokenizer):
    """Split every text into MAX_LENGTH windows overlapping by STRIDE tokens.

    Returns (chunks, owners): chunks[k] is a list of input ids and owners[k] is
    the index of the text it came from.
    """
    enc = tokenizer(texts, truncation=True, max_length=MAX_LENGTH, stride=STRIDE,
                    return_overflowing_tokens=True)
    return enc["input_ids"], list(enc["overflow_to_sample_mapping"])


def _forward(chunks, tokenizer, model, device):
    batch = tokenizer.pad({"input_ids": chunks}, return_tensors="pt")
    batch = {k: v.to(device) for k, v in batch.items()}
    with torch.inference_mode():
        return model(**batch).logits.float().softmax(dim=-1).cpu()


def _handle_oom(device, batch_size):
    if device.type == "cuda":
        torch.cuda.empty_cache()
    if batch_size == 1:
        raise
    new_size = max(1, batch_size // 2)
    warnings.warn(f"GPU out of memory; retrying with batch_size={new_size}.")
    return new_size


def _probs(texts, batch_size):
    """Class probabilities per text, in input order, plus the chunk count per text.

    All chunks are scored shortest-first (less padding); each text's chunks are
    then averaged, weighted by length.
    """
    tokenizer, model, device, _ = load_model()
    chunks, owners = _chunk(texts, tokenizer)
    order = sorted(range(len(chunks)), key=lambda k: len(chunks[k]))

    sums = [None] * len(texts)
    weights = [0] * len(texts)
    n_chunks = [0] * len(texts)
    pos = 0
    while pos < len(order):
        idx = order[pos:pos + batch_size]
        try:
            probs = _forward([chunks[k] for k in idx], tokenizer, model, device)
        except torch.cuda.OutOfMemoryError:
            batch_size = _handle_oom(device, batch_size)
            continue  # retry the same position with a smaller batch
        for k, p in zip(idx, probs):
            i, w = owners[k], len(chunks[k])
            sums[i] = p * w if sums[i] is None else sums[i] + p * w
            weights[i] += w
            n_chunks[i] += 1
        pos += len(idx)

    return [s / w for s, w in zip(sums, weights)], n_chunks


# ---- public API ----------------------------------------------------------------------------
def score_articles(texts, batch_size=BATCH_SIZE):
    """Score articles of any length and return one result per article, in input order.

    Args:
        texts: list of strings (None/NaN are treated as ""). To include a
            headline, pass f"{title}. {body}".
        batch_size: chunks per forward pass (auto-halved on GPU OOM).

    Returns:
        A list where results[i] belongs to texts[i] (see the module docstring).
    """
    if isinstance(texts, str):
        raise TypeError("Expected a list of strings, not a single string.")
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1.")
    texts = [_clean(t) for t in texts]
    if not texts:
        return []

    label_index = load_model()[3]
    probs, n_chunks = _probs(texts, batch_size)
    results = []
    for p, n in zip(probs, n_chunks):
        pr = {name: p[label_index[name]].item() for name in LABELS}
        results.append({
            "label": max(pr, key=pr.get),
            "sentiment": round(pr["positive"] - pr["negative"], 4),
            **{name: round(v, 4) for name, v in pr.items()},
            "chunks": n,
        })
    return results


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Score news articles with FinBERT.")
    ap.add_argument("files", nargs="*", help="text files, one article each")
    ap.add_argument("--json", help="JSON file holding a list of article strings")
    ap.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    args = ap.parse_args()

    articles = []
    if args.json:
        with open(args.json, encoding="utf-8") as f:
            articles += json.load(f)
    for path in args.files:
        with open(path, encoding="utf-8") as f:
            articles.append(f.read())
    if not articles:
        ap.error("give article files or --json")

    for result in score_articles(articles, args.batch_size):
        print(json.dumps(result))

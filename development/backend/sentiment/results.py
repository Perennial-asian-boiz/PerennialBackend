"""Hand a source script's per-ticker results to sentiment_manager without a file.

A script prints its report as ONE line on stdout, prefixed with MARKER:
    @@SENTIMENT_RESULTS@@ [{"ticker": "NVDA", "average_sentiment": 0.1378,
                             "post_count": 2, "posts": {"<id>": 0.4222, ...}}, ...]
Scores are in [-1, 1]. sentiment_manager reads that line from the script's output
(it is not shown on the console) and writes the posts to the database. Run on its
own, a script just prints the line, so nothing is written to disk.
"""

import json
import sys

MARKER = "@@SENTIMENT_RESULTS@@ "


def emit_results(report):
    """Print the report as one marker line. ensure_ascii keeps the line free of raw
    carriage returns / newlines, so it can't be split on its way to the manager."""
    sys.stdout.write(MARKER + json.dumps(report, ensure_ascii=True) + "\n")
    sys.stdout.flush()


def parse_results_line(text):
    """The report in a marker line, or None if `text` is not one (or is malformed)."""
    if not text.startswith(MARKER):
        return None
    try:
        report = json.loads(text[len(MARKER) :])
    except ValueError:
        return None
    return report if isinstance(report, list) else None

"""Decide whether a theme's coverage on one day is a spike worth showing as an event."""
from dataclasses import dataclass
from datetime import date, timedelta
from statistics import median
from typing import Mapping

BASELINE_DAYS = 28
MIN_BASELINE_DAYS = 14
SPIKE_RATIO = 2.0
# Placeholder until real daily counts are reviewed; keeps tiny themes from spiking on noise.
MIN_ARTICLES = 20


@dataclass(frozen=True)
class SpikeCheck:
    article_count: int
    baseline_median: float
    ratio: float | None
    is_spike: bool


def check_spike(
    counts: Mapping[date, int],
    day: date,
    *,
    baseline_days: int = BASELINE_DAYS,
    min_baseline_days: int = MIN_BASELINE_DAYS,
    ratio: float = SPIKE_RATIO,
    min_articles: int = MIN_ARTICLES,
) -> SpikeCheck | None:
    """Spike when the day's count is at least `ratio` x the trailing median and at least `min_articles`.

    Returns None when the day has no data or the baseline is too thin to judge.
    Days missing from the timeline inside the window count as zero, but the
    window must contain at least `min_baseline_days` observed days.
    """
    if day not in counts:
        return None
    window = [day - timedelta(days=n) for n in range(1, baseline_days + 1)]
    if sum(d in counts for d in window) < min_baseline_days:
        return None
    baseline = float(median(counts.get(d, 0) for d in window))
    count = counts[day]
    return SpikeCheck(
        article_count=count,
        baseline_median=baseline,
        ratio=count / baseline if baseline > 0 else None,
        is_spike=count >= min_articles and count >= ratio * baseline,
    )

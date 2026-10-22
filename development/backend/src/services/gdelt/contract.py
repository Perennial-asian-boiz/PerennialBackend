"""GDELT through the ingestion contract (docs/work-items/gdelt-contract-design-note.md).

Two collected sources and one derivation, each an ordinary ingestion run with an
immutable batch:

* gdelt_theme_volume: every watched theme's daily volume and tone over the
  trailing window up to the target day (the most recent complete UTC day).
  Days missing from GDELT's own timeline are coverage gaps, recorded in the
  batch envelope, never zero rows.
* gdelt_theme_articles: up to EVIDENCE_PER_DAY evidence articles per theme for
  the target day, from the theme map's evidence outlets.
* gdelt_spike_results (collection_mode 'derived'): the spike rule applied to one
  recorded gdelt_theme_volume batch, named in run_dependencies.

Any failed theme fails the whole collected batch (G1, provisional): readers
keep the last good batch. Diagnostics hold theme codes and fixed codes only.
"""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from statistics import median
from typing import Any, Callable, Mapping

from pydantic import ValidationError
from sqlalchemy.engine import Engine
from src.db.queries import batch_rows, latest_successful_run
from src.ingestion.diagnostics import failure_code
from src.ingestion.importer import CollectionOutcome, ImportResult, import_collection
from src.ingestion.schemas import GdeltThemeArticle
from src.pipeline.lifecycle import begin_run, heartbeat_worker, source_lock
from src.services.gdelt.client import DocApiClient, GdeltError
from src.services.gdelt.themes import ThemeMap

logger = logging.getLogger(__name__)

VOLUME, ARTICLES, SPIKES = "gdelt_theme_volume", "gdelt_theme_articles", "gdelt_spike_results"
WINDOW_DAYS = 90
EVIDENCE_PER_DAY = 5
UNIT_KIND = "GDELT themes"


@dataclass(frozen=True)
class SpikeRule:
    """spike-v2: missing baseline days are unknown, never zero (settled answer 4)."""

    version: str = "spike-v2"
    ratio: float = 2.0
    baseline_days: int = 28
    min_baseline_days: int = 14
    min_articles: int = 20

    def envelope(self) -> dict[str, Any]:
        return {
            "rule_version": self.version,
            "spike_ratio": self.ratio,
            "baseline_days": self.baseline_days,
            "min_baseline_days": self.min_baseline_days,
            "min_articles": self.min_articles,
        }

    def check(self, counts: Mapping[date, int], day: date) -> dict[str, Any] | None:
        """One theme's target day; None when GDELT reported no count for it."""
        if day not in counts:
            return None
        window = [day - timedelta(days=n) for n in range(1, self.baseline_days + 1)]
        observed = [counts[d] for d in window if d in counts]
        count = counts[day]
        result: dict[str, Any] = {
            "article_count": count,
            "observed_baseline_days": len(observed),
            "baseline_median": None,
            "ratio": None,
            "is_spike": None,
            "rule_version": self.version,
        }
        if len(observed) < self.min_baseline_days:
            return result  # too thin to judge: unknown, not "no spike"
        baseline = float(median(observed))
        result["baseline_median"] = round(baseline, 2)
        result["ratio"] = round(count / baseline, 2) if baseline > 0 else None
        result["is_spike"] = count >= self.min_articles and count >= self.ratio * baseline
        return result


def target_day(now: datetime) -> date:
    return now.astimezone(timezone.utc).date() - timedelta(days=1)


@dataclass
class _Themes:
    planned: int
    failures: list[dict[str, str]] = field(default_factory=list)

    def fail(self, theme: str, exc: GdeltError) -> None:
        self.failures.append({"unit": theme, "code": failure_code(str(exc))})

    def failed_outcome(self, source: str) -> CollectionOutcome | None:
        if not self.failures:
            return None
        completed = self.planned - len(self.failures)
        return CollectionOutcome.failure(
            source,
            "live",
            "partial_collection" if completed else "collection_failed",
            diagnostics={
                "unit_kind": UNIT_KIND,
                "planned": self.planned,
                "attempted": self.planned,
                "completed": completed,
                "failed": len(self.failures),
                "failures": self.failures,
            },
        )


def collect_volume(
    client: DocApiClient, theme_map: ThemeMap, now: datetime
) -> CollectionOutcome:
    day = target_day(now)
    first = day - timedelta(days=WINDOW_DAYS - 1)
    tracker = _Themes(len(theme_map.themes))
    records: list[dict[str, Any]] = []
    missing: list[str] = []
    for theme in theme_map.themes:
        try:
            volume = client.volume(theme)
            tone = client.tone(theme)
        except GdeltError as exc:
            tracker.fail(theme, exc)
            continue
        days = sorted(d for d in volume if first <= d <= day)
        for d in days:
            records.append(
                {
                    "theme": theme,
                    "day": d.isoformat(),
                    "article_count": volume[d].article_count,
                    "total_articles": volume[d].total_articles,
                    "avg_tone": tone.get(d),
                }
            )
        # Every day of the window GDELT did not report is a gap, including a
        # sparse start and an empty timeline (checker GD-1).
        seen = set(days)
        missing.extend(
            f"{theme}:{first + timedelta(days=n)}"
            for n in range(WINDOW_DAYS)
            if first + timedelta(days=n) not in seen
        )
    failed = tracker.failed_outcome(VOLUME)
    if failed is not None:
        return failed
    if theme_map.themes and not records:
        # No theme has any volume in the whole window: a provider-side outage,
        # not a quiet day (checker GD-2). Keep the last good batch.
        return CollectionOutcome.failure(VOLUME, "live", "empty_gdelt_timeline")
    return CollectionOutcome.success(
        VOLUME,
        "live",
        records,
        source_as_of=day,
        coverage={"complete": True, "scope": "window"},
        envelope={
            "source": "gdelt_doc_api",
            "target_day": day.isoformat(),
            "window_days": WINDOW_DAYS,
            "themes": len(theme_map.themes),
            "missing_days": missing,
            "missing_days_count": len(missing),
        },
    )


def collect_articles(
    client: DocApiClient, theme_map: ThemeMap, now: datetime
) -> CollectionOutcome:
    day = target_day(now)
    tracker = _Themes(len(theme_map.themes))
    records: list[dict[str, Any]] = []
    skipped = 0
    for theme in theme_map.themes:
        try:
            articles = client.articles(theme, day)
        except GdeltError as exc:
            tracker.fail(theme, exc)
            continue
        seen: set[str] = set()
        kept = 0
        for a in articles:
            if kept == EVIDENCE_PER_DAY:
                break
            if not theme_map.is_evidence_domain(a.domain) or a.url in seen:
                continue
            raw = {
                "theme": theme,
                "day": day.isoformat(),
                "url": a.url,
                "title": a.title,
                "domain": a.domain,
                "seen_at": a.seen_at,
            }
            try:  # evidence is optional: an unusable link is skipped, not fatal
                GdeltThemeArticle.model_validate(raw)
            except ValidationError:
                skipped += 1
                continue
            seen.add(a.url)
            records.append(raw)
            kept += 1
    failed = tracker.failed_outcome(ARTICLES)
    if failed is not None:
        return failed
    return CollectionOutcome.success(
        ARTICLES,
        "live",
        records,
        source_as_of=day,
        coverage={"complete": True, "scope": "window"},
        envelope={
            "source": "gdelt_doc_api",
            "target_day": day.isoformat(),
            "themes": len(theme_map.themes),
            "skipped_articles": skipped,
        },
    )


def derive_spikes(engine: Engine, rule: SpikeRule = SpikeRule()) -> ImportResult:
    """Apply the spike rule to the latest good volume batch, as a derived run."""
    with source_lock(engine, SPIKES):
        with engine.connect() as conn:
            latest = latest_successful_run(conn, VOLUME)
        dependencies = {VOLUME: latest.batch_id} if latest is not None else {}
        run_id = begin_run(engine, SPIKES, "derived", dependencies=dependencies)
        with heartbeat_worker(engine, run_id):
            if latest is None or latest.source_as_of is None:
                outcome = CollectionOutcome.failure(SPIKES, "derived", "missing_upstream")
                return import_collection(engine, outcome, run_id=run_id)
            day = latest.source_as_of
            with engine.connect() as conn:
                rows = batch_rows(conn, VOLUME, latest.batch_id)
            by_theme: dict[str, dict[date, int]] = {}
            for row in rows:
                by_theme.setdefault(row["theme"], {})[row["day"]] = row["article_count"]
            records = []
            for theme in sorted(by_theme):
                checked = rule.check(by_theme[theme], day)
                if checked is not None:
                    records.append({"theme": theme, "day": day.isoformat(), **checked})
            outcome = CollectionOutcome.success(
                SPIKES,
                "derived",
                records,
                source_as_of=day,
                coverage={"complete": True, "scope": "derivation"},
                envelope={**rule.envelope(), "input_batch_id": latest.batch_id},
            )
            return import_collection(engine, outcome, run_id=run_id)


@dataclass
class ContractRun:
    """What one scheduled GDELT run produced, for the job's exit code and logs."""

    day: date
    results: dict[str, ImportResult] = field(default_factory=dict)
    events: list[str] = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return len(self.results) == 3 and all(r.succeeded for r in self.results.values())


def run_contract(
    engine: Engine,
    client: DocApiClient,
    theme_map: ThemeMap,
    now: datetime,
    collect: Callable[..., ImportResult] | None = None,
) -> ContractRun:
    """Collect both GDELT sources, then derive spikes from the latest volume batch.

    The derivation runs even when today's volume collection failed: it then
    re-derives from the last good batch (an identical result reuses its batch).
    """
    from src.pipeline.runner import collect_source

    collect = collect_source if collect is None else collect
    run = ContractRun(day=target_day(now))
    run.results[VOLUME] = collect(
        engine, VOLUME, collector=lambda: collect_volume(client, theme_map, now)
    )
    run.results[ARTICLES] = collect(
        engine, ARTICLES, collector=lambda: collect_articles(client, theme_map, now)
    )
    run.results[SPIKES] = derive_spikes(engine)
    spikes = run.results[SPIKES]
    if spikes.succeeded and spikes.batch_id is not None:
        with engine.connect() as conn:
            run.events = sorted(
                r["theme"]
                for r in batch_rows(conn, SPIKES, spikes.batch_id)
                if r["is_spike"] and r["day"] == run.day
            )
    for source, result in run.results.items():
        logger.info(
            "gdelt source=%s status=%s code=%s run_id=%s",
            source,
            result.status,
            result.error_code,
            result.run_id,
        )
    return run

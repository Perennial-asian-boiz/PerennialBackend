"""
PURPOSE:
    Reads ticker lists from fmp.py and ark.py outputs,
    then checks each ticker for SEC Form 4 insider buying activity.

    BUCKET 1 — "Popular & Stable"
        Tickers from trades_congress.json (fmp.py output)
        Checks if insiders confirm what Congress is buying

    BUCKET 2 — "Affordable & Growing"
        Tickers from ark_holdings.json (ark.py output)
        Checks if insiders are buying ARK's growth picks

    NOTE on current market:
        If no insider buys are found AND the run succeeded, that is real
        market data — insiders may be selling after a run-up or receiving
        grants. ARK tickers still feed consensus.py as the Affordable &
        Growing candidate list regardless of insider activity.
        Insider buying just adds an extra confidence boost.

        Zero buys from a FAILED run is not market data — it means we never
        got an answer. Check coverage.run_status before concluding anything
        from an absence.

SOURCE:
    SecuritiesDB — Free, no key needed
    https://securitiesdb.com/api/v1/stocks/{ticker}/insider-activity
    Returns SEC Form 4 filings from EDGAR, refreshed daily

SCHEDULE:
    Runs daily at 9:05am (after fmp.py and ark.py)
    via scheduler/cron.py

OUTPUT:
    database/local_data/trades_insider.json

    Every run records what it established, so a collection failure is never
    mistaken for an absence of insider buying:

        data_as_of            when the data in this file was collected
        coverage              what that data establishes, incl. which
                              tickers we never got an answer for
        last_attempt          the most recent run, succeeded or not

    A `failed` run does NOT overwrite the file. The previous data is kept
    and the failed attempt is stamped into `last_attempt`, so bad or empty
    responses can never replace data a working run already collected.
"""

import requests
import json
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import Counter, defaultdict
from dotenv import load_dotenv
# ── shared path resolution (see paths.py) ──
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(next(
    p for p in _Path(__file__).resolve().parents if p.name == "consensus_watchlist"
)))
from paths import ENV_PATH, LOCAL_DATA_DIR, local_data_dir


# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────

load_dotenv(dotenv_path=ENV_PATH)

SECURITIES_URL      = "https://securitiesdb.com/api/v1/stocks"
LOOKBACK_DAYS       = 180    # 6 months
MIN_BUY_VALUE       = 10_000 # $10K minimum conviction filter
MAX_CONGRESS_TICKERS = 50
REQUEST_DELAY       = 0.5    # seconds between API calls

# Collection outcomes. These travel in the output file, not just the console,
# so consensus.py can tell "checked, no buys" from "never found out".
STATUS_SUCCESS   = "success"    # provider answered, transactions parsed (may be empty)
STATUS_NOT_FOUND = "not_found"  # provider has no record of this ticker
STATUS_FAILED    = "failed"     # request or parse did not complete — result unknown
STATUS_PARTIAL   = "partial"    # run level only: some tickers failed

# A run that answered less than this fraction of its tickers is treated as
# failed: too little was established to be worth replacing good data with.
MIN_USABLE_SUCCESS_RATE = 0.5
MAX_LISTED_FAILED_TICKERS = 25  # keep the output file readable on an outage

# Why a request failed. A fixed vocabulary rather than exception class names,
# so these stay stable and greppable as the code changes.
ERR_RATE_LIMITED     = "rate_limited"      # 429 — provider throttled us
ERR_TIMEOUT          = "timeout"           # no response within REQUEST_TIMEOUT
ERR_CONNECTION       = "connection_error"  # DNS / refused / network down
ERR_HTTP             = "http_error"        # other non-200
ERR_BAD_RESPONSE     = "bad_response"      # answered 200, shape unusable
ERR_REQUEST          = "request_error"     # anything unclassified
ERR_INPUT_MISSING    = "input_missing"     # upstream fetcher never ran
ERR_INPUT_UNREADABLE = "input_unreadable"  # upstream output corrupt

# What a dev should DO about each code — the field worth reading first:
#   transient — provider or network problem; retrying later should work
#   provider  — provider rejected or errored on us; check status / API changes
#   contract  — provider answered but the shape broke our parser; FIX CODE
#   pipeline  — an earlier stage of our own pipeline produced no input
ERROR_CATEGORY = {
    ERR_RATE_LIMITED:     "transient",
    ERR_TIMEOUT:          "transient",
    ERR_CONNECTION:       "transient",
    ERR_HTTP:             "provider",
    ERR_BAD_RESPONSE:     "contract",
    ERR_REQUEST:          "unknown",
    ERR_INPUT_MISSING:    "pipeline",
    ERR_INPUT_UNREADABLE: "pipeline",
}
REQUEST_TIMEOUT = 15  # seconds


# ─────────────────────────────────────────────────────────
# LOAD — Bucket 1: Congress tickers from fmp.py
# ─────────────────────────────────────────────────────────

def load_congress_tickers():
    """
    Reads trades_congress.json produced by fmp.py.
    Returns (tickers, error) — error is None on a clean read. An empty list
    with no error means fmp.py genuinely recorded no purchases.
    """
    input_file = LOCAL_DATA_DIR / "trades_congress.json"

    if not input_file.exists():
        print("[insider] ❌ trades_congress.json not found — run fmp.py first")
        return [], ERR_INPUT_MISSING

    try:
        with open(input_file, "r") as f:
            data = json.load(f)

        trades    = data.get("trades", [])
        purchases = [
            t["ticker"] for t in trades
            if "purchase" in t.get("trade_type", "").lower()
            and t.get("ticker")
        ]

        ticker_counts = Counter(purchases).most_common(MAX_CONGRESS_TICKERS)
        tickers       = [t for t, _ in ticker_counts]
        print(f"[insider] Loaded {len(tickers)} Congress tickers (Bucket 1)")
        return tickers, None

    except Exception as e:
        print(f"[insider] ❌ Failed to load Congress tickers: {e}")
        return [], ERR_INPUT_UNREADABLE


# ─────────────────────────────────────────────────────────
# LOAD — Bucket 2: ARK tickers from ark.py
# ─────────────────────────────────────────────────────────

def load_ark_tickers():
    """
    Reads ark_holdings.json produced by ark.py.
    Returns (tickers, error) for Bucket 2 (Affordable & Growing).
    Prioritizes multi-fund tickers (appear in 2+ ARK funds)
    as they represent stronger ARK conviction.
    """
    input_file = LOCAL_DATA_DIR / "ark_holdings.json"

    if not input_file.exists():
        print("[insider] ❌ ark_holdings.json not found — run ark.py first")
        return [], ERR_INPUT_MISSING

    try:
        with open(input_file, "r") as f:
            data = json.load(f)

        holdings = data.get("holdings", [])

        # Sort: multi-fund first, then single fund
        multi  = [h["ticker"] for h in holdings if h["fund_count"] > 1]
        single = [h["ticker"] for h in holdings if h["fund_count"] == 1]
        tickers = multi + single

        print(f"[insider] Loaded {len(tickers)} ARK tickers "
              f"({len(multi)} multi-fund, {len(single)} single-fund) "
              f"(Bucket 2)")
        return tickers, None

    except Exception as e:
        print(f"[insider] ❌ Failed to load ARK tickers: {e}")
        return [], ERR_INPUT_UNREADABLE


# ─────────────────────────────────────────────────────────
# FETCH — Insider activity for one ticker
# ─────────────────────────────────────────────────────────

def fetch_insider_activity(ticker: str):
    """
    Fetches insider transaction data from SecuritiesDB.
    Returns {"status": ..., "data": ..., "error_code": ..., "error_detail": ...}.

    A 404 is an answer (the provider has no record), so it is reported
    separately from a failure, where we learned nothing.
    """
    def failure(code, detail):
        return {"status": STATUS_FAILED, "data": None,
                "error_code": code, "error_detail": detail}

    try:
        resp = requests.get(
            f"{SECURITIES_URL}/{ticker}/insider-activity",
            timeout=REQUEST_TIMEOUT
        )

        if resp.status_code == 404:
            return {"status": STATUS_NOT_FOUND, "data": None,
                    "error_code": None, "error_detail": None}
        if resp.status_code == 429:
            print(f"[insider] ⚠️  {ticker} rate limited — waiting 5s")
            time.sleep(5)
            return failure(ERR_RATE_LIMITED, "HTTP 429 from provider")
        if resp.status_code != 200:
            return failure(ERR_HTTP, f"HTTP {resp.status_code} from provider")

        try:
            payload = resp.json()
        except Exception as e:
            # Answered 200 but the body is not JSON — provider-side change or
            # an error page. Not retryable; needs a look.
            return failure(ERR_BAD_RESPONSE,
                           f"200 but body is not JSON ({type(e).__name__})")

        return {"status": STATUS_SUCCESS, "data": payload,
                "error_code": None, "error_detail": None}

    except requests.exceptions.Timeout:
        return failure(ERR_TIMEOUT, f"no response in {REQUEST_TIMEOUT}s")
    except requests.exceptions.ConnectionError as e:
        return failure(ERR_CONNECTION, f"{type(e).__name__}: {e}"[:200])
    except Exception as e:
        return failure(ERR_REQUEST, f"{type(e).__name__}: {e}"[:200])


# ─────────────────────────────────────────────────────────
# PARSE — Extract meaningful insider buys
# ─────────────────────────────────────────────────────────

def parse_insider_buys(ticker: str, raw: dict, bucket: str):
    """
    Extracts open market PURCHASE transactions only.

    SecuritiesDB transaction types:
        "Purchase" → open market buy with real money ✅ KEEP
        "Sale"     → insider selling                 ❌ SKIP
        "Grant"    → compensation grant, value=0     ❌ SKIP
        "Other"    → misc, usually value=0           ❌ SKIP

    Additional filters:
        value >= MIN_BUY_VALUE  → conviction buy only
        date >= cutoff          → within lookback window

    Returns (buys, error_code, error_detail). An unparseable response is a
    failure, not an empty result — we cannot claim this ticker had no
    insider buying. It is reported as `bad_response` (category "contract")
    because the provider answered and our parser is what broke.
    """
    buys = []

    try:
        insider_data = raw.get("data", {})
        transactions = insider_data.get(
            "insider_transactions", {}
        ).get("recent", [])

        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
        ).date()

        for txn in transactions:
            # Only open market purchases
            if txn.get("type") != "Purchase":
                continue

            # Date within lookback window
            date_str = txn.get("date", "")
            try:
                txn_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                if txn_date < cutoff:
                    continue
            except ValueError:
                continue

            # Must be real money — filters grants and option exercises
            value = txn.get("value", 0) or 0
            if value < MIN_BUY_VALUE:
                continue

            buys.append({
                "ticker":           ticker,
                "insider_name":     txn.get("insider", "").strip(),
                "transaction_type": "Purchase",
                "value":            value,
                "shares":           txn.get("shares", 0),
                "transaction_date": date_str,
                "bucket":           bucket,
                "data_source":      "securitiesdb",
                "fetched_at":       datetime.now(timezone.utc).isoformat(),
            })

    except Exception as e:
        return [], ERR_BAD_RESPONSE, f"{type(e).__name__}: {e}"[:200]

    return buys, None, None


# ─────────────────────────────────────────────────────────
# SCAN — Check a list of tickers for insider buys
# ─────────────────────────────────────────────────────────

def scan_tickers(tickers: list, bucket: str, label: str):
    """
    Loops through tickers checking each for insider buying.
    Returns (all_buys, outcomes) where outcomes carries one status record
    per ticker so a coverage gap is never mistaken for an absence of buying.
    """
    all_buys  = []
    outcomes  = []
    found     = 0
    no_buys   = 0
    missing   = 0
    failed    = 0

    print(f"\n[insider] {label}")
    print(f"[insider] Checking {len(tickers)} tickers...")
    print(f"[insider] Est. time: ~{len(tickers) * REQUEST_DELAY:.0f}s\n")

    for i, ticker in enumerate(tickers, 1):
        result = fetch_insider_activity(ticker)
        status = result["status"]
        code   = result["error_code"]
        detail = result["error_detail"]
        buys   = []

        if status == STATUS_SUCCESS:
            buys, parse_code, parse_detail = parse_insider_buys(
                ticker, result["data"], bucket
            )
            if parse_code:
                status, code, detail, buys = STATUS_FAILED, parse_code, parse_detail, []

        if status == STATUS_FAILED:
            failed += 1
            print(f"  ⚠️  {ticker:<8} UNKNOWN — {code} "
                  f"[{ERROR_CATEGORY.get(code, 'unknown')}]: {detail} "
                  f"[{i}/{len(tickers)}]")
        elif status == STATUS_NOT_FOUND:
            missing += 1
        elif buys:
            all_buys.extend(buys)
            found += 1
            total = sum(b["value"] for b in buys)
            print(f"  ✅ {ticker:<8} {len(buys)} buy(s) | "
                  f"${total:,.0f} total [{i}/{len(tickers)}]")
        else:
            no_buys += 1

        outcomes.append({
            "ticker":       ticker,
            "bucket":       bucket,
            "status":       status,
            "buy_count":    len(buys),
            "error_code":   code,
            "error_detail": detail,
            "checked_at":   datetime.now(timezone.utc).isoformat(),
        })

        time.sleep(REQUEST_DELAY)

    print(f"\n[insider] Done — {found} with buys, {no_buys} checked with none, "
          f"{missing} not on provider, {failed} unknown (failed)")
    return all_buys, outcomes


# ─────────────────────────────────────────────────────────
# ANALYZE
# ─────────────────────────────────────────────────────────

def analyze_insider_buys(all_buys: list):
    """
    Groups buys by ticker and calculates signal metrics.
    Multiple insiders buying same stock = cluster buy signal.
    """
    ticker_summary = defaultdict(lambda: {
        "ticker":            "",
        "bucket":            "",
        "insider_buy_count": 0,
        "total_buy_value":   0,
        "most_recent_buy":   "",
        "insiders":          [],
    })

    for buy in all_buys:
        ticker = buy["ticker"]
        s      = ticker_summary[ticker]
        s["ticker"]            = ticker
        s["bucket"]            = buy["bucket"]
        s["insider_buy_count"] += 1
        s["total_buy_value"]   += buy["value"]
        s["insiders"].append(buy["insider_name"])

        if not s["most_recent_buy"] or \
           buy["transaction_date"] > s["most_recent_buy"]:
            s["most_recent_buy"] = buy["transaction_date"]

    return sorted(
        ticker_summary.values(),
        key=lambda x: x["insider_buy_count"],
        reverse=True
    )


# ─────────────────────────────────────────────────────────
# SAVE
# ─────────────────────────────────────────────────────────

def build_coverage(outcomes: list, inputs: dict):
    """
    Summarizes what this run actually established.

    run_status is what downstream should branch on:
        success  — every ticker we set out to check returned an answer
        partial  — some tickers failed, so absences are not conclusive
        failed   — too little was established to use: nothing was checked,
                   or fewer than MIN_USABLE_SUCCESS_RATE answered
    """
    checked    = len(outcomes)
    failed     = sum(1 for o in outcomes if o["status"] == STATUS_FAILED)
    not_found  = sum(1 for o in outcomes if o["status"] == STATUS_NOT_FOUND)
    succeeded  = sum(1 for o in outcomes if o["status"] == STATUS_SUCCESS)
    with_buys  = sum(1 for o in outcomes if o["buy_count"] > 0)
    bad_inputs = [name for name, i in inputs.items() if i.get("error_code")]

    answered = succeeded + not_found
    success_rate = (answered / checked) if checked else 0.0
    errors = summarize_errors(outcomes)

    if checked == 0 or success_rate < MIN_USABLE_SUCCESS_RATE:
        run_status = STATUS_FAILED
    elif failed or bad_inputs:
        run_status = STATUS_PARTIAL
    else:
        run_status = STATUS_SUCCESS

    return {
        "run_status":           run_status,
        "success_rate":         round(success_rate, 3),
        "tickers_checked":      checked,
        "succeeded":            succeeded,
        "with_buys":            with_buys,
        "confirmed_no_buys":    succeeded - with_buys,
        "not_found":            not_found,
        "failed":               failed,
        # consensus.py evaluates every Congress purchase ticker, but we only
        # scan the top MAX_CONGRESS_TICKERS. Without this list it cannot tell
        # "scanned, no buys" from "never scanned".
        "scanned_tickers":      [o["ticker"] for o in outcomes],
        # Of those, the ones whose result is unknown. A scanned ticker that is
        # absent from both this list and transactions genuinely had no
        # qualifying purchases.
        "failed_tickers":       errors["failed_tickers"],
        # Why those failed, grouped by cause, each with the category that
        # says what to do about it. This is the dev-facing diagnostic.
        "errors_by_code":       errors["by_code"],
        "inputs":               inputs,
        "degraded_inputs":      bad_inputs,
    }


def summarize_errors(outcomes: list):
    """
    Groups failures by error code so a dev can tell WHY a run degraded
    without reading console scrollback: which cause, what to do about it
    (category), how many tickers, which ones, and one real message.

    Also returns the full failed-ticker list — correctness depends on it,
    since consensus.py treats a scanned ticker absent from it as genuinely
    having had no buys.
    """
    groups = {}
    tickers = []

    for o in outcomes:
        if o["status"] != STATUS_FAILED:
            continue

        code = o.get("error_code") or ERR_REQUEST
        g = groups.setdefault(code, {
            "category":      ERROR_CATEGORY.get(code, "unknown"),
            "count":         0,
            "tickers":       [],
            "sample_detail": o.get("error_detail"),
        })
        g["count"] += 1
        g["tickers"].append(o["ticker"])
        tickers.append(o["ticker"])

    return {"by_code": groups, "failed_tickers": tickers}


def build_attempt(coverage: dict, outcomes: list, retained: bool):
    """
    One record of 'we tried to collect, here is what happened' — the
    human-readable diagnostic, so the ticker list is truncated here. The
    authoritative full list lives in coverage.failed_tickers.
    """
    errors = summarize_errors(outcomes)
    errors.pop("failed_tickers")  # authoritative copy lives in coverage
    for group in errors["by_code"].values():
        listed = group["tickers"][:MAX_LISTED_FAILED_TICKERS]
        if len(group["tickers"]) > len(listed):
            group["tickers_truncated"] = len(group["tickers"]) - len(listed)
        group["tickers"] = listed

    return {
        "fetcher":                "insider.py",
        "attempted_at":           datetime.now(timezone.utc).isoformat(),
        "run_status":             coverage["run_status"],
        "tickers_checked":        coverage["tickers_checked"],
        "succeeded":              coverage["succeeded"],
        "failed":                 coverage["failed"],
        "degraded_inputs":        coverage["degraded_inputs"],
        "errors":                 errors,
        "retained_previous_data": retained,
    }


def _write_json(path, payload):
    """Atomic write — a crash mid-save must not leave a truncated file."""
    tmp = Path(f"{path}.tmp")
    with open(tmp, "w") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, path)


def retain_previous_data(filename, attempt: dict):
    """
    A failed run establishes nothing, so it must not overwrite data a
    previous run did establish. Keep the existing payload and stamp the
    failed attempt onto it.

    Returns True if existing data was preserved.
    """
    previous = None
    if Path(filename).exists():
        try:
            with open(filename, "r") as f:
                previous = json.load(f)
        except Exception as e:
            print(f"[insider] ⚠️  Existing {Path(filename).name} is unreadable "
                  f"({type(e).__name__}) — nothing to preserve")

    if previous is None:
        # No good data to protect. Write a data-less record so consensus.py
        # sees an explicit failure rather than a missing file.
        attempt["retained_previous_data"] = False
        _write_json(filename, {
            "source":        "SecuritiesDB (SEC Form 4)",
            "data_as_of":    None,
            "coverage":      {"run_status": STATUS_FAILED, "tickers_checked": 0},
            "last_attempt":  attempt,
            "transactions":  [],
        })
        print(f"[insider] 💾 No prior data to keep — recorded failed attempt "
              f"→ {filename}")
        return False

    previous["last_attempt"] = attempt
    _write_json(filename, previous)
    print(f"[insider] 🛡️  Collection failed — kept existing data from "
          f"{previous.get('data_as_of') or 'an earlier run'} "
          f"({len(previous.get('transactions', []))} buys) and recorded the "
          f"failed attempt")
    return True


def save_to_json(all_buys: list, summary: list, outcomes: list,
                 inputs: dict, filename=None):
    """
    Writes trades_insider.json — but only when this run actually collected
    something. On a failed run the previous file is preserved and the
    attempt is recorded in `last_attempt`.
    """
    if filename is None:
        output_dir = LOCAL_DATA_DIR
        output_dir.mkdir(parents=True, exist_ok=True)
        filename   = output_dir / "trades_insider.json"

    coverage = build_coverage(outcomes, inputs)

    if coverage["run_status"] == STATUS_FAILED:
        retain_previous_data(filename, build_attempt(coverage, outcomes, True))
        return

    popular = [s for s in summary if s["bucket"] == "popular_stable"]
    growth  = [s for s in summary if s["bucket"] == "affordable_growing"]

    now = datetime.now(timezone.utc).isoformat()

    output = {
        "source":            "SecuritiesDB (SEC Form 4)",
        # When the data IN this file was collected. Survives a later failed
        # run, unlike last_attempt.attempted_at.
        "data_as_of":        now,
        "fetched_at":        now,
        "lookback_days":     LOOKBACK_DAYS,
        "min_buy_value":     MIN_BUY_VALUE,
        "total_buys":        len(all_buys),
        "tickers_with_buys": len(summary),
        "note": (
            "insider.py is a SCORER not a gate. "
            "Zero buys is valid — tickers still enter consensus.py "
            "from fmp.py and ark.py. Insider buying only adds "
            "bonus points to the confidence score in consensus.py. "
            "Read coverage.run_status before treating an absent ticker "
            "as 'no insider buying': on 'partial' or 'failed' the "
            "absence may just mean we never got an answer."
        ),

        "coverage":      coverage,
        "last_attempt":  build_attempt(coverage, outcomes, retained=False),

        "popular_stable": {
            "count":   len(popular),
            "tickers": popular,
        },
        "affordable_growing": {
            "count":   len(growth),
            "tickers": growth,
        },
        "transactions": all_buys,
    }

    _write_json(filename, output)

    print(f"[insider] 💾 Saved {len(all_buys)} insider buys "
          f"(run_status={coverage['run_status']}) → {filename}")


# ─────────────────────────────────────────────────────────
# PRINT
# ─────────────────────────────────────────────────────────

def print_summary(summary: list, all_buys: list, coverage: dict):
    popular = [s for s in summary if s["bucket"] == "popular_stable"]
    growth  = [s for s in summary if s["bucket"] == "affordable_growing"]

    print("\n" + "="*60)
    print("  PERENNIAL — Insider Buys Summary")
    print("="*60)
    print(f"  Run status             : {coverage['run_status'].upper()}")
    print(f"  Tickers checked        : {coverage['tickers_checked']}")
    print(f"  Confirmed no buys      : {coverage['confirmed_no_buys']}")
    print(f"  Not on provider        : {coverage['not_found']}")
    print(f"  Unknown (failed)       : {coverage['failed']}")
    print(f"  Total insider buys     : {len(all_buys)}")
    print(f"  Popular & Stable hits  : {len(popular)}")
    print(f"  Affordable & Growing   : {len(growth)}")
    print(f"  Lookback window        : {LOOKBACK_DAYS} days")
    print(f"  Min buy value          : ${MIN_BUY_VALUE:,}")

    if coverage["degraded_inputs"]:
        print(f"  ⚠️  Degraded inputs     : "
              f"{', '.join(coverage['degraded_inputs'])}")

    by_code = coverage.get("errors_by_code") or {}
    if by_code:
        print("\n  ❌ Why requests failed:")
        print("  " + "-" * 40)
        for code, g in sorted(by_code.items(), key=lambda kv: -kv[1]["count"]):
            print(f"  {code:<18} {g['count']:>3}x  [{g['category']}]")
            print(f"     e.g. {g['tickers'][0]}: {g['sample_detail']}")
        if any(g["category"] == "contract" for g in by_code.values()):
            print("\n     ⚠️  'contract' means SecuritiesDB answered but the")
            print("        response shape broke our parser — needs a code fix,")
            print("        not a retry.")

    if popular:
        print("\n  📊 Popular & Stable — Congress + Insider overlap:")
        print("  " + "-"*40)
        for s in popular[:10]:
            bar = "█" * min(s["insider_buy_count"], 10)
            print(f"  {s['ticker']:<8} {bar:<12} "
                  f"{s['insider_buy_count']} insider(s) | "
                  f"${s['total_buy_value']:>12,.0f} | "
                  f"last: {s['most_recent_buy']}")

    if growth:
        print("\n  🚀 Affordable & Growing — ARK + Insider signal:")
        print("  " + "-"*40)
        for s in growth[:10]:
            bar = "█" * min(s["insider_buy_count"], 10)
            print(f"  {s['ticker']:<8} {bar:<12} "
                  f"{s['insider_buy_count']} insider(s) | "
                  f"${s['total_buy_value']:>12,.0f} | "
                  f"last: {s['most_recent_buy']}")

    if not popular and not growth:
        if coverage["run_status"] == STATUS_SUCCESS:
            print("\n  ℹ️  No insider buys found — this is fine.")
            print("     Every ticker returned an answer; none had")
            print("     qualifying purchases. insider.py is a scorer,")
            print("     not a gate. Congress + ARK tickers still enter")
            print("     consensus.py. Zero buys = no bonus, not no output.")
        else:
            print("\n  ⚠️  No insider buys recorded, but this run did not")
            print("     complete cleanly — do NOT read this as 'no insider")
            print("     buying'. See coverage.run_status in the output file.")


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────

def run():
    print("[insider] Starting insider trades fetch...")
    print("[insider] Reads from: trades_congress.json + ark_holdings.json")
    print("[insider] Checks via: SecuritiesDB (SEC Form 4)\n")

    all_buys = []
    outcomes = []
    inputs   = {}

    # ── BUCKET 1: Congress tickers ───────────────────────
    congress_tickers, congress_error = load_congress_tickers()
    inputs["trades_congress.json"] = {
        "tickers_loaded": len(congress_tickers),
        "error_code":     congress_error,
        "category":       ERROR_CATEGORY.get(congress_error),
    }
    if congress_tickers:
        stable_buys, stable_outcomes = scan_tickers(
            congress_tickers,
            bucket="popular_stable",
            label="BUCKET 1 — Popular & Stable"
        )
        all_buys.extend(stable_buys)
        outcomes.extend(stable_outcomes)

    # ── BUCKET 2: ARK tickers ────────────────────────────
    ark_tickers, ark_error = load_ark_tickers()
    inputs["ark_holdings.json"] = {
        "tickers_loaded": len(ark_tickers),
        "error_code":     ark_error,
        "category":       ERROR_CATEGORY.get(ark_error),
    }
    if ark_tickers:
        congress_set  = set(congress_tickers)
        unique_growth = [t for t in ark_tickers if t not in congress_set]
        print(f"[insider] {len(unique_growth)} unique ARK tickers "
              f"(excluding {len(ark_tickers) - len(unique_growth)} "
              f"already in Bucket 1)")

        growth_buys, growth_outcomes = scan_tickers(
            unique_growth,
            bucket="affordable_growing",
            label="BUCKET 2 — Affordable & Growing"
        )
        all_buys.extend(growth_buys)
        outcomes.extend(growth_outcomes)

    # ── Analyze + Print + Save ───────────────────────────
    summary  = analyze_insider_buys(all_buys)
    coverage = build_coverage(outcomes, inputs)
    print_summary(summary, all_buys, coverage)
    save_to_json(all_buys, summary, outcomes, inputs)

    if coverage["run_status"] == STATUS_FAILED:
        print("[insider] ❌ Done, but nothing was established this run.")
    elif coverage["run_status"] == STATUS_PARTIAL:
        print(f"[insider] ⚠️  Done with gaps — {coverage['failed']} ticker(s) "
              f"unknown.")
    else:
        print("[insider] ✅ Done.")

    return all_buys


if __name__ == "__main__":
    run()
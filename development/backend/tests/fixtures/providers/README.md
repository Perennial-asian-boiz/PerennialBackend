# Synthetic provider contracts

These payloads are hand-authored, fictitious examples, not captured responses.
FMP fields follow `_FMP_KEYS` / `_string_fields_ok`, ARK follows `_ark_fund_ok`,
and Nasdaq follows `_short_interest_kind` in `src/ingestion/collectors.py`.
Finnhub follows `parse_finnhub_buys` in the insider fetcher, anticipating PR #18.
No SecuritiesDB or retired Senate Stock Watcher contract is introduced.

From the repository root:

```
.venv/bin/python -m pytest development/backend/tests/test_provider_contracts.py -q
.venv/bin/python development/backend/tests/provider_contracts.py BASELINE.json CANDIDATE.json
```

The CLI compares local payloads and emits a JSON array of path/kind/expected/
observed differences. Exit 0 means matching observed shapes, 1 means drift or
insufficient item evidence, and 2 means unreadable/invalid input. Null is a type;
missing fields are optional when absent in some objects. All list items are
aggregated. Array order, row counts and scalar values do not affect shape.
Integer and number are distinguished deliberately. An empty candidate list
reports `unobserved` for its previously observed items, not deleted keys.
Paths escape `~`, `/`, `*` as `~0`, `~1`, `~2`; `/*` denotes array items.

Observed synthetic shapes are not exhaustive API schemas. Changes need review,
not automatic baseline replacement. Reports omit values but retain keys; do not
assume arbitrary provider keys are safe. Real captures require approved
scrubbing and auditor review. No raw responses should become CI artifacts.

D3 (staging credentials) blocks real capture and weekly live monitoring/issue
creation. `provider-contracts.yml` is manual-only and runs synthetic tests only;
it has no provider calls, secret access, issue-write permission or schedule.
Enabling live monitoring is a later reviewed change after D3, including approved
capture/scrubbing and safe issue contents. Manual success does not prove live
provider compatibility.

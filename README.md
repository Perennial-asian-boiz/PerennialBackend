# PerennialBackend

Backend for Project Perennial — CSULB Senior Project 2026, team Asian Boiz.

This repo replaces `haohnguyen94-droid/project-perennial`, where five branches
ran for three months off a single README-only `main` and never merged. Work is
being ported branch by branch onto a trunk that is settled first.

## Status

| Landed | Component | From |
|---|---|---|
| ✅ | Consensus watchlist pipeline (Phase B) | `hong-working` |
| ✅ | PostgreSQL snapshots, migrations, publication pipeline and tests (deployment pending) | `database-ingestion-pipeline` |
| ⬜ | YouTube / web-search ingestion | `cohen-working` |
| ⬜ | Cloudflare worker | `cohen-cloudflare` |

## Layout

```
development/
  backend/
    .env.example          # copy to .env — the only credentials file read
    src/services/consensus_watchlist/
      paths.py            # repo-root, .env and data-dir resolution
      consensus.py        # aggregates + ranks the four signals
      fetchers/           # fmp, ark, insider, short_interest
      scheduler/cron.py   # legacy JSON pipeline (--test makes live calls)
  database/local_data/    # fetcher JSON output, gitignored
  database/               # PostgreSQL Compose, Alembic migrations — see database/README.md
  backend/src/db/, backend/src/ingestion/  # schema, importer, live collectors, CLI
  backend/src/pipeline/   # database orchestration, replay, health, production scheduler
requirements.txt
```

## Quick start

```bash
pip install -r requirements.txt
cp development/backend/.env.example development/backend/.env   # add your keys
python3 development/backend/src/services/consensus_watchlist/scheduler/cron.py --test
```

See [`consensus_watchlist/README.md`](development/backend/src/services/consensus_watchlist/README.md)
for legacy JSON commands. For the database workflow, follow
[`database/README.md`](development/database/README.md) to migrate PostgreSQL,
then run `python -m src.ingestion.cli run-pipeline` from `development/backend`.
It publishes a verified watchlist without source JSON files. Read the
[operations runbook](docs/technical-specs/OPERATIONS.md) before deployment.

### Sprint 1 account API

The `feature/backend-auth-api` branch adds the first user-facing API layer on
top of the existing PostgreSQL database. It includes account creation, emailed
OTP verification, login sessions, onboarding preferences, notification
settings, password changes, logout/session revocation, and a personal
watchlist. The API is started from `development/backend` after applying the
migrations:

```bash
cd development/database && alembic upgrade head
cd ../backend
uvicorn src.api.app:app --reload --port 8000
```

The API loads `development/backend/.env`; exported environment variables take
precedence. Resend is the default email provider. Signup, password login, and
resend send a six-digit OTP to the user's email. They return
`development_code: null` even during local development when Resend is enabled.
No session is created until the code is verified. Missing email configuration
or a provider failure returns 503 without committing the pending account/code
changes. Resend accepting a message does not guarantee inbox delivery; check
the Resend dashboard for delivery or bounce status.

#### Account API endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/auth/signup` | Create an account and issue a verification code |
| `POST` | `/auth/verify-otp` | Verify the account and create a session |
| `POST` | `/auth/resend-otp` | Email a replacement verification code |
| `POST` | `/auth/login` | Check credentials and email an OTP; no session yet |
| `POST` | `/auth/logout` | Revoke all active sessions for the account |
| `GET/PUT` | `/me/preferences` | Read or save sectors and risk level |
| `GET/PUT` | `/me/notifications` | Read or save notification toggles |
| `PUT` | `/me/password` | Change the password and revoke sessions |
| `GET/POST/DELETE` | `/me/watchlist` | Read or update the account watchlist |

#### Resend email setup

Auth architecture is still pending alignment with
[issue #3 (Supabase Auth/JWT)](https://github.com/Perennial-asian-boiz/PerennialBackend/issues/3)
and the `main` branch's schema review plan. This branch's current opaque
sessions are a temporary integration, not an implementation of Supabase Auth.
The adapter below supports the current account endpoints. After migrating to
Supabase Auth, configure Resend as
[Supabase's SMTP provider](https://resend.com/docs/send-with-supabase-smtp)
and retire the custom identity/code/session path; do not treat the two token
formats as interchangeable. This change does not migrate any shared database.

Email delivery lives in `development/backend/src/api/mail.py` and uses Resend's
HTTP API with the existing `requests` dependency. The frontend continues using
the same account endpoints; no provider key belongs in `VITE_*` variables.

1. Create a [Resend account](https://resend.com) owned by the team and a
   sending API key.
2. For a first test without a domain, use
   `EMAIL_FROM=Perennial <onboarding@resend.dev>` and register in Perennial
   using **the same email address as your Resend account**. Resend's test sender
   cannot send to arbitrary users.
3. For other users, [verify a domain you control](https://resend.com/docs/dashboard/domains/introduction)
   and change `EMAIL_FROM` to an address on that domain. A Gmail address cannot
   be used as a verified Resend sender.
4. Put these values in the deployment secret store or the untracked
   `development/backend/.env`, then restart the backend:

```dotenv
EMAIL_PROVIDER=resend
RESEND_API_KEY=replace_with_secret
EMAIL_FROM=Perennial <noreply@verified-domain.example>
```

The sender domain must be verified with Resend. Never commit `RESEND_API_KEY`,
`.env`, or a real sender credential.

OTP codes expire after **15 minutes**, are stored as salted hashes, and can be
used once. **Five failed attempts** lock a code. Requests for another code
(including password login) are limited to **one every 30 seconds** and
**five per hour per account**. These are application limits, independent of
the provider's own quotas. A replacement invalidates the previous code only
after the provider accepts the new email. Delivery failures retain the prior
code. Timeouts are not retried automatically because Resend may already have
accepted the message; a late email from a failed request may contain an invalid
code, so request a new one after a timeout.

For offline UI work only, explicitly set `EMAIL_PROVIDER=development` and
`APP_ENV=development`. This mode sends no email and returns `development_code`
for the existing development UI. It is rejected in production or staging and
is never an automatic fallback when Resend fails.

The frontend can use `http://127.0.0.1:8000` as `VITE_API_URL`. The initial
endpoint contract is documented by the OpenAPI page at
`http://127.0.0.1:8000/docs` once the server is running.

#### Dashboard data API

`GET /api/watchlist` exposes the current production publication in the format
used by the frontend dashboard:

```json
{
  "schema_version": "1.0",
  "generated_at": "2026-10-14T00:00:00+00:00",
  "affordable_growing": [
    {
      "name": "PLTR",
      "ticker": "PLTR",
      "sector": "Unclassified",
      "price": null,
      "change_percent": null,
      "confidence_score": null,
      "sentiment": "Positive",
      "reason": "Published growth signals: Ark."
    }
  ],
  "popular_stable": []
}
```

The adapter already accepts `name`, `sector`, `price`, `change_percent`, and
`confidence_score` when the publication pipeline provides them. The current
ranking publication only contains tickers, market caps, and raw signals, so
missing display fields remain explicit fallbacks until company metadata is
added to the ingestion pipeline. This keeps the API stable while that work is
completed.

For the temporary frontend-to-backend demo, the API defaults to the copied
fixture data during development. Set `DASHBOARD_DATA_MODE=publication` to use
the real current publication instead. Keep `DASHBOARD_DATA_MODE=publication`
in production.

## Conventions for the branches still to land

- **One `.gitignore`, one `requirements.txt`, one `.env.example`** — at the
  paths above. Three competing copies of each is what made the old repo
  unmergeable.
- **Never hardcode a path depth.** Import `paths.py` rather than counting
  `parents[N]` or matching on the repo's directory name.
- **Credentials live in `development/backend/.env`.** Nothing else is loaded.

### Backend quality checks

Use Python 3.12 (`.python-version`) and PostgreSQL 17. Install
`requirements-dev.txt` into `.venv`, then run `python -m ruff check`,
`python -m ruff format --check`, and `python -m mypy` from the repository root.
For the mandatory synthetic PostgreSQL suite, migration round-trip/drift check,
dependency audit, and protection activation, see
[Phase 0 quality gates](docs/technical-specs/OPERATIONS.md#phase-0-quality-gates).
Historical independent reviews are preserved in [docs/reviews](docs/reviews/README.md).

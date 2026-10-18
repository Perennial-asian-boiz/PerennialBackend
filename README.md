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

### Sprint 1: Supabase Auth + FastAPI

The `codex/supabase-auth-resend` branch replaces the temporary custom
password/OTP/session implementation with Supabase Auth. It follows
[issue #3](https://github.com/Perennial-asian-boiz/PerennialBackend/issues/3).
Review this branch together with the matching frontend branch before merging;
it does not change `main` or the team's company-data collectors.

- **Supabase Auth** owns accounts, password hashing, email OTPs and JWT refresh.
- **FastAPI** validates each JWT with this project's Supabase Auth `/user`
  endpoint, then checks the app's OTP-approved session record.
- **PostgreSQL on Supabase** stores per-user preferences, watchlists and settings
  in the separate `app` schema. Row-level security isolates identity UUIDs.
- **Resend SMTP** delivers Supabase's email templates. No second mail sender,
  custom password hash, fake login, displayed development OTP or admin key in
  the browser remains.

Company information is **still temporary fixture data** in development.
Authentication and account data are not fixtures. Real company integration is
a separate team task; leave `DASHBOARD_DATA_MODE=publication` in production.

#### Current setup and remaining owner actions

The team's development project is `perennial-dev` (project reference
`ljgndlvelidwqqodlpvj`). Database connectivity and the additive account
migration have been verified there. Runtime credentials are private and must
be shared through a secret manager, not committed or posted to Discord.
Invite teammates to the Supabase organization instead of sharing your login.

**Resend SMTP and the email-template settings still require dashboard setup
and an inbox test.** Passing offline tests does not prove email delivery.
Production hosting, HTTPS, secret rotation, monitoring and team merge review
are separate deployment work, not completed by this branch.

#### Clone and run

Use Python 3.12 (the CI version) and the matching branch in both repositories:

```bash
git clone --branch codex/supabase-auth-resend https://github.com/Perennial-asian-boiz/PerennialBackend.git
git clone --branch codex/supabase-auth-resend https://github.com/Perennial-asian-boiz/PerennialFrontEnd.git
cd PerennialBackend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cp development/backend/.env.example development/backend/.env
```

Fill the ignored backend `.env` privately. Exported environment settings win.

| Setting | Purpose |
|---|---|
| `SUPABASE_URL` | Project HTTPS URL |
| `SUPABASE_PUBLISHABLE_KEY` | Project publishable key; not a service-role key |
| `DATABASE_URL` | Administrative/migration connection; session pooler + TLS |
| `SUPABASE_DB_PASSWORD` | Raw migration password, if the URL contains literal `[YOUR-PASSWORD]` |
| `API_DATABASE_URL` | Separate restricted FastAPI login; encoded password |
| `CORS_ORIGINS` | Exact frontend origins |
| `APP_ENV` | `development` locally; `production` when deployed |

Do not run the API with migration-owner credentials in a deployment.
An administrator applies migrations, then provisions the `perennial_api`
NOLOGIN group with `development/database/ops/provision_api_role.py` using
`OPS_DATABASE_URL`. Give a dedicated LOGIN role membership in that group;
it must be NOSUPERUSER, NOCREATEDB, NOCREATEROLE and NOBYPASSRLS. Use that
login in `API_DATABASE_URL`. Its only pipeline permissions are SELECT on
`publications` and `current_publication`; it cannot alter company data.
Reapply API grants after future migrations. Importer/reader groups must not
receive access to `app`.

```bash
cd development/database
../../.venv/bin/alembic upgrade head
cd ../backend
../../.venv/bin/uvicorn src.api.app:app --reload --port 8000
```

In the frontend repo, copy `.env.example` to `.env`, keep
`VITE_API_URL=http://127.0.0.1:8000`, then run `npm ci` and `npm run dev`.
Open the frontend at port 5173, not the API port. API reference: port 8000
`/docs`. No Supabase secret, database password or Resend key goes in `VITE_*`.

#### Supabase + Resend dashboard checklist

1. Keep the Data API disabled: this application reads/writes through FastAPI.
   Supabase Auth continues to work without PostgREST. Do not expose `app`
   or `auth` through the Data API.
2. Enable email/password authentication and **Confirm email**. Set minimum
   password length to at least 8, **Email OTP Expiration = 900 seconds** and
   **OTP length = 6**. The API fails closed if signup immediately returns a session.
3. Set Site URL to the frontend URL and allow the exact local/deployed redirect
   URLs. For local development use Site URL `http://127.0.0.1:5173` and allow
   `http://127.0.0.1:5173/auth/confirm`. Set `AUTH_EMAIL_REDIRECT_URL` to that
   callback in the backend. The old default `localhost:3000` is not this app.
4. In Authentication's email/SMTP settings, enable custom SMTP:
   host `smtp.resend.com`, port `465`, username `resend`, password =
   Resend sending API key, sender name `Perennial`.
5. Use a sender on a verified domain for arbitrary users. Without a domain,
   `onboarding@resend.dev` can test **only the Resend account's own email**.
   Supabase's default SMTP is also restricted and is not the production setup.
6. **Confirm signup** uses `{{ .ConfirmationURL }}`: registration sends a link,
   not a six-digit signup code. **Magic Link** (login) and **Reset Password**
   use `{{ .Token }}` for the existing six-digit login/recovery screens.
   Supabase's dashboard currently requires custom SMTP to edit these templates;
   without it the default login/recovery emails may still contain links. Do not
   claim email OTP delivery is ready until SMTP/templates and inbox tests pass.
7. Test signup, existing-user login, resend, recovery and delivery/bounce status
   in both Supabase Auth logs and the Resend dashboard. Never post API keys,
   SMTP passwords, DB URLs or OTPs in Discord.

Official setup:
[Resend SMTP](https://resend.com/docs/send-with-supabase-smtp),
[Supabase email OTP](https://supabase.com/docs/guides/auth/auth-email-passwordless).

#### Login and data contract

Signup/login/recovery first return `challenge_id`, `flow` and
`resend_after_seconds`; they never return a usable session. Signup also returns
`verification_method=email_link`. Show **Account created** immediately with
only **Back to sign in**; the user confirms their email independently later.
Do not route registration to the six-digit verification screen.

The emailed signup link is consumed by Supabase and returns to `/auth/confirm`.
The frontend removes its token fragment immediately, validates the result via
`/auth/confirm-email`, and returns to Sign in with a confirmation notice. The
confirmation JWT is never stored as an app session. Confirmation alone grants
no Dashboard access. Invalid, expired or reused links show a safe retry message.

For login/recovery send `email + challenge_id + code` to verify. Only successful OTP verification
approves the provider's `session_id` and returns access/refresh tokens.
A directly obtained Supabase password or passwordless token cannot access
Perennial data without this approval. Email OTP here is an application
verification gate, **not Supabase native MFA/AAL2**.

Existing accounts with `onboarding_completed=true` go straight to Dashboard
after OTP. New users go to preferences; saving or explicitly skipping completes
onboarding. Updates keep their existing saved preferences until submitted.
Recovery-approved sessions can only reset a password, not read app data.

| Method | Endpoint | Purpose |
|---|---|---|
| POST | `/auth/signup` | Register with Supabase and start signup challenge |
| POST | `/auth/confirm-email` | Validate email link, sync profile, return to Sign in; no app session |
| POST | `/auth/resend-confirmation` | Resend a signup confirmation link |
| POST | `/auth/login` | Validate password, send email OTP, no session yet |
| POST | `/auth/forgot-password` | Start recovery; generic reply for unknown emails |
| POST | `/auth/resend-otp` | Resend within the existing challenge |
| POST | `/auth/verify-otp` | Verify code and approve session |
| POST | `/auth/refresh` | Refresh only an approved, unrevoked session |
| POST | `/auth/reset-password` | Reset with a recovery-only session |
| POST | `/auth/logout` | Revoke all of this user's app sessions |
| GET | `/me` | Verified profile + onboarding status |
| GET/PUT | `/me/preferences` | Selected sectors and risk level |
| GET/PUT | `/me/notifications` | Notification toggles |
| PUT | `/me/password` | Validate old password, change and revoke sessions |
| GET/POST/DELETE | `/me/watchlist` | Personal ticker list |

Access tokens are checked with the issuer on every request; failures do not
fall back to accepting decoded claims. The app checks issuer, audience,
expiry, role, confirmed identity and UUID session ID after issuer validation.
Logout/password reset/change revoke app approvals immediately, including
refresh. The browser keeps only tokens and the pending challenge in tab-scoped
`sessionStorage`; account data is stored in PostgreSQL, not localStorage.

Codes/challenges expire after 15 minutes, lock after five wrong attempts and
are single-use. Resend waits 60 seconds; five emails per hour per email address
are allowed. Resending does not reset failed attempts. Supabase and Resend may
apply stricter limits. Provider failures do not become successful login or
consume an invalid-code attempt. Only the challenge token's SHA-256 digest is
stored by FastAPI; Supabase owns password/OTP storage.

#### Storage and compatibility

The new `app` tables are `profiles`, `preferences`, `notifications`,
`watchlist`, `auth_challenges` and `verified_sessions`. User-data RLS is
forced, with transaction-local identity context set only after authentication.
Backend-only challenge/approval tables have no browser grants.

Signup checks both confirmed and unconfirmed emails against the authoritative
Supabase database and returns 409 for an existing email (including differences
in case/outer whitespace). It does not overwrite an account or its password.
`app.email_is_registered(text)` is a fixed-search-path SECURITY DEFINER
function that returns only a boolean. Only the API group can execute it; the
API still cannot SELECT `auth.users`, hashes or identity lists. Supabase's
sanitized duplicate-signup response is also handled rather than displayed as
new-account success. Explicit 409 errors intentionally reveal that an email
exists, as requested by the product; production also needs IP/bot rate limits.

Auth identities are saved at signup in `auth.users`. App profiles are synced
after a verified confirmation link or first verified login, not before email
ownership is established. Preferences/settings/watchlists remain per-user.

Legacy custom-auth tables/migration files are retained for history/recovery,
but are no longer read by the API. Old opaque tokens are invalid. Existing
legacy users need to register with Supabase or follow a separately reviewed
account/data migration; this branch does not copy passwords or silently
reassign old user data. Do not delete historical migrations or legacy shared
data during teammate cleanup. Supabase Auth users can be inspected under
Authentication → Users; app data is under schema `app` in the Table Editor
or SQL Editor, not `public`.

#### Verification

Install development requirements. Database tests create/drop only randomly
named local test databases; **never set TEST_DATABASE_URL to Supabase**.

```bash
TEST_DATABASE_URL=postgresql+psycopg://perennial:perennial@127.0.0.1:5433/perennial_test python -m pytest development/backend/tests -q
python -m ruff check
python -m mypy
python -m pip_audit -r requirements-dev.txt --progress-spinner off
```

Offline provider mocks test the real ASGI routes against isolated PostgreSQL.
Coverage includes OTP approval, expired/reused challenges, resend/attempt
limits, two-account isolation, non-owner RLS, onboarding, password recovery,
session revocation and sanitized errors. The local restore-drill test also
requires PostgreSQL 17 `pg_dump`/`pg_restore` on PATH (installed in CI).
Migration round-trip and model drift must pass before applying a migration.

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

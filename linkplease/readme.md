# LinkPlease Tech Intern Assignment

A small, reliable webhook-to-DM pipeline for LinkPlease's "comment `PRICE` → get DM'd" flow, built against the PseudoGram mock API. FastAPI for the web layer, Postgres for state, a single background worker for everything that talks to PseudoGram.

**Deployed at:** `https://linkplease-assigment.onrender.com`

## How it's put together

Two processes, one database, started together by `run.py`:

- **Web process** (`app.main:app`, uvicorn) — owns `POST /webhook`, `POST /rules`, `GET /stats`. It never calls PseudoGram itself. All it does is verify the signature, upsert some Postgres rows, and return `200`. That's deliberate: the assignment gives `/webhook` a 5-second budget, and the only slow, flaky thing in this system is the PseudoGram API — so it's kept out of the request path entirely.
- **Worker process** (`app.worker.dm_worker`) — a single polling loop that does the actual sending, status-checking, retrying, and backoff. It's the only thing that calls `POST /v1/dm/send` and `GET /v1/dm/{dm_id}`.

They talk to each other only through Postgres, via a `dm_jobs` table that acts as a queue with a status column (`queued → sending → waiting → delivered/failed`).

### Tables

| Table | Purpose |
|---|---|
| `rules` | keyword → dm_message, created via `POST /rules` |
| `events` | one row per `event_id` seen — this *is* the webhook dedup, enforced by a primary key + `ON CONFLICT DO NOTHING`, not an in-memory set |
| `comments` | one row per `comment_id`, tracks `state` (`active`/`deleted`) so a `comment.deleted` event can be checked against, regardless of arrival order |
| `dm_jobs` | one row per (user, rule) that should get a DM — `UNIQUE(user_id, rule_id)` is the actual dedup mechanism for "never DM the same user twice for the same rule" |
| `blocked_duplicates` | a log row every time the `dm_jobs` unique constraint blocks an insert — this is what `duplicates_blocked` in `/stats` counts |

Both dedup guarantees (event-level and DM-level) are enforced by Postgres unique constraints and `INSERT ... ON CONFLICT`, not by a "check then insert" in application code — so two copies of the same event landing within milliseconds of each other can't both slip through.

### Webhook processing (`app/services/webhook_service.py`)

Per event, in order:

1. Insert into `events` on `event_id`. If the insert reports zero rows affected, it's a redelivery — stop here, nothing else happens.
2. If it's `comment.deleted`, upsert the `comments` row to `state="deleted"` and stop. This works whether or not the `comment.created` for that comment has arrived yet.
3. Otherwise, upsert the `comments` row as `active` (upsert is a no-op if a `deleted` tombstone already exists — see below).
4. Re-read the comment row from the DB and check its `state`. If it's `deleted`, skip — this is what makes "delete arrives before create" safe: the tombstone written in step 2 survives the no-op insert in step 3.
5. Match the comment text against every rule (case-insensitive substring). For each match, try to insert a `dm_jobs` row. If the unique constraint blocks it, log a `blocked_duplicates` row instead.
6. Commit once at the end, so a crash mid-processing rolls the whole event back — a redelivery after a crash is processed clean, not half-applied.

### DM sending & retries (`app/worker/dm_worker.py`)

The worker loop alternates between two checks each iteration (so a burst on one side can't starve the other):

- **New jobs** (`status="queued"` and due): call `POST /v1/dm/send` with `Idempotency-Key: dm-job-{id}` (stable per row, so a retried send after a lost response reuses the same PseudoGram-side DM instead of creating a second one). `202` with `status="queued"` → move to `waiting` and schedule a reconciliation check. `5xx` / network errors → exponential backoff (`2^attempts`, capped at 60s) up to 5 attempts, then `failed`. `429` → sleep for the server's `Retry-After`. `400` → `failed` immediately, no retry.
- **Waiting jobs** (`status="waiting"`, past their next check time): call `GET /v1/dm/{dm_id}`. `delivered` → done. `failed` → re-queued with backoff (this is the Part C reconciliation case — PseudoGram accepted it and failed it later). Still `queued` → check again later.

Row selection uses `SELECT ... FOR UPDATE SKIP LOCKED`, so this is safe to run as more than one worker process if it's ever scaled out, even though only one runs today.

### `GET /stats`

Computed live from the `dm_jobs` / `blocked_duplicates` tables, not from counters kept in memory — a restart doesn't lose the numbers.

```
sent               = count(dm_jobs.status = delivered)
failed             = count(dm_jobs.status = failed)
queued             = count(dm_jobs.status in (queued, waiting))
duplicates_blocked = count(blocked_duplicates)
```

### Signature verification

`X-PseudoGram-Signature: sha256=<hmac>` is checked with `hmac.compare_digest` against the raw request body (read as raw `bytes`, not the re-serialized parsed model, so re-encoding can't change the signature under it). `POST /webhook/sign` is a dev-only helper that computes a valid signature for a given body using the same secret/logic, for testing with curl/Postman — it should be removed or disabled before this is treated as production.

## Endpoints

Matches the assignment contract exactly:

- `POST /webhook` — receives PseudoGram events, returns `200` fast
- `POST /rules` → `201` with `{rule_id, keyword, dm_message}`
- `GET /stats` → `{sent, failed, queued, duplicates_blocked}`

## Running locally

```bash
cd linkplease
pip install -r requirements.txt
```

Set these (e.g. in `linkplease/.env`):

```
DATABASE_URL=postgresql+psycopg://user:pass@host/db
PSEUDOGRAM_BASE_URL=https://pseudogram-api.onrender.com
PSEUDOGRAM_API_KEY=your_key
```

Create the tables (there's no migration tool wired up — this is a straight `create_all`, so re-run it after any model change):

```bash
python -m app.init_db
```

Run both the API and the worker together:

```bash
python run.py
```

## Testing against PseudoGram

- `generate_signature.py` — prints a valid signature for a hardcoded sample payload, for manual curl testing.
- `load_test.py` — fires 500 synthetic `comment.created` events at the deployed `/webhook` over 10 seconds, matching the Part C load shape. Point it at PseudoGram's own `POST /v1/simulate/start` for a real end-to-end run (it exercises the actual event stream, including redeliveries and out-of-order delivery) and check the result against `GET /v1/simulate/{run_id}/truth`.
- `tests/` currently has empty stub files (`test_dedup.py`, `test_rules.py`, `test_webhook.py`) — no automated test suite exists yet; all verification so far has been manual, against the simulate endpoint.

## Known limitations

See `FAILURES.md` for the specific ways this system can still lose a DM, send a duplicate, or misreport a number.
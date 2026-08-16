# Known failure modes

Honest list of the ways this system can still lose a DM, send a duplicate, or report a wrong number, and the conditions under which each happens.

## 1. A job stuck in "sending" survives no restart

`process_job()` sets a `dm_jobs` row to `status="sending"` and commits *before* calling `POST /v1/dm/send`. If the worker process is killed or restarted (a Render deploy, an OOM, a crash) in the window between that commit and the HTTP call returning, the row is left at `status="sending"` forever. Neither `get_next_job()` (only selects `queued`) nor `get_waiting_job()` (only selects `waiting`) will ever pick it up again, and `/stats` doesn't count `sending` rows in `queued` either — so that DM disappears from every code path and every number the grader checks. The web process and worker are started together by `run.py` and live in the same dyno, so any restart of one is a restart of both, which makes this more likely to matter than it would if they were isolated.

## 2. `comment.deleted` arriving after the DM job already exists doesn't cancel it

The tombstone check only protects the window before a `dm_jobs` row is created: if `comment.deleted` arrives while a comment has no matching rule yet, or before `comment.created` at all, it's handled correctly. But if `comment.created` is processed first, a `dm_jobs` row is created, and *then* `comment.deleted` arrives before the worker has sent it, nothing re-checks the comment's state before sending. The DM goes out for a comment that's already deleted. I haven't added a check in `process_job()` (or a cancellation step in the delete handler) for this ordering.

## 3. Worker doesn't proactively stay under PseudoGram's rate limit

The worker loop sends jobs back-to-back with no pacing — it only sleeps when the queue is empty. It backs off correctly on `429` using the `Retry-After` header, so nothing gets lost, but during a burst (the 500-event / 10s load test is exactly this case) it will hit `429` repeatedly on the way to settling into the 10-req/60s limit rather than staying under it the whole time. This satisfies "nothing lost" but not "rate limit never breached" from the Part C stretch goal.

## 4. `/stats` has a small blind spot during an in-flight send

`queued` in `/stats` counts rows with `status in (queued, waiting)`. A row that's currently `sending` — i.e. the HTTP call to PseudoGram is in flight — is counted in none of `sent`, `failed`, or `queued` for that instant. With one worker processing jobs one at a time this window is short (one HTTP round-trip), but if `/stats` is polled at exactly the wrong moment, the four numbers won't sum to the total row count in `dm_jobs`.

## 5. `/webhook` does several sequential DB round-trips per event, all inline

Each webhook call does roughly five to six sequential statements against Postgres (insert event, upsert comment, re-select comment, select all rules, insert job, possibly insert a blocked-duplicate row) before returning `200`. None of it calls PseudoGram, so it's normally fast, but it's all synchronous inside the request — there's no background task or queue in front of it. Under the 500-events/10s load test this has held up in testing, but a slow or cold-started Postgres connection (e.g. a free-tier instance waking up) would push individual webhook calls toward the 5-second contract limit, since there's nothing backgrounding the DB work itself.
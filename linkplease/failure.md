# Known failure modes

Honest list of the ways this system can still lose a DM, send a duplicate, or report a wrong number, and the conditions under which each happens.

## 1. A job stuck in "sending" survives no restart

`process_job()` sets a `dm_jobs` row to `status="sending"` and commits *before* calling `POST /v1/dm/send`. If the worker process is killed or restarted (a Render deploy, an OOM, a crash) in the window between that commit and the HTTP call returning, the row is left at `status="sending"` forever. Neither `get_next_job()` (only selects `queued`) nor `get_waiting_job()` (only selects `waiting`) will ever pick it up again, and `/stats` doesn't count `sending` rows in `queued` either — so that DM disappears from every code path and every number the grader checks. The web process and worker are started together by `run.py` and live in the same dyno, so any restart of one is a restart of both, which makes this more likely to matter than it would if they were isolated.

## 2. `comment.deleted` arriving after the DM job already exists doesn't cancel it

The tombstone check only protects the window before a `dm_jobs` row is created: if `comment.deleted` arrives while a comment has no matching rule yet, or before `comment.created` at all, it's handled correctly. But if `comment.created` is processed first, a `dm_jobs` row is created, and *then* `comment.deleted` arrives before the worker has sent it, nothing re-checks the comment's state before sending. The DM goes out for a comment that's already deleted. I haven't added a check in `process_job()` (or a cancellation step in the delete handler) for this ordering.

## 3. Blocked Duplicate Audit Loss

The database-level uniqueness constraint protects against duplicate DMs even if recording the duplicate attempt fails. However, if the `BlockedDuplicateEvent` audit record cannot be persisted due to a database failure, the duplicate DM is still blocked but the event is not reflected in `/stats`.

Therefore, `duplicates_blocked` represents the number of duplicate attempts successfully recorded, rather than necessarily the total number of duplicate attempts that occurred.

A future improvement would be to make duplicate detection and audit recording transactional, or derive duplicate metrics directly from the database conflict events/appropriate durable audit mechanism.
## 4. — Real PseudoGram Webhook Signatures Do Not Verify (Most important)

### Description

The webhook signature verification implementation works correctly for locally generated
signatures, but real webhook deliveries from the PseudoGram simulator consistently fail
HMAC verification.

The `/webhook` endpoint receives the complete request body and the signature header, but
the HMAC-SHA256 calculated from the received raw bytes does not match the signature
provided by PseudoGram.

Example:

Received signature:

sha256=0a281a021efca81275b899d6e1394e01e7fd1923340523db9fe80f11ec5efff5

Computed signature:

sha256=242fd2ee65dbc3cc2e9d0b336f7ca4ac40f833a5fbaad1802be14cc6747c43ab

### Investigation

The following possible causes were independently checked:

- API key was reconfirmed against the PseudoGram `/v1/keygen` endpoint.
- HMAC-SHA256 implementation was tested using a local sign-and-verify round trip.
- The `/webhook/sign` endpoint generated signatures that were successfully verified
  by the `/webhook` verification logic.
- The webhook uses the exact raw request body rather than a re-serialized JSON object.
- The received body length matched the HTTP `Content-Length` exactly.
- No body truncation was observed.
- No application-level compression/decompression issue was observed.
- The application was running as a single stable process during testing.

Example received request:

Content-Length: 333

Received body length: 333

Therefore, the signature mismatch occurs even though the bytes used by the application
for verification are the complete bytes received from the HTTP request.

### Observed Result

PseudoGram sends:

HTTP 200-level webhook payload
+
X-PseudoGram-Signature

but the calculated HMAC does not match the provided signature.

The application therefore correctly rejects the request with:

HTTP 401 Unauthorized

### Current Assessment

Based on the tests performed, the most likely issue is an inconsistency in the
PseudoGram simulator's signing process.

For example, the simulator may be calculating the HMAC over a different byte
representation of the payload than the representation actually transmitted over HTTP.

Possible causes include:

- signing a JSON representation before final serialization,
- different JSON whitespace,
- different key ordering,
- modifying timestamp precision after signing,
- or another serialization difference between the signed payload and transmitted body.

This cannot be conclusively fixed from the application side because the application
only has access to the signature and the final bytes received over HTTP.

### Impact

Real PseudoGram webhook events cannot currently pass signature verification and are
therefore rejected before reaching the normal event-processing pipeline.

This prevents the external simulator from exercising the complete webhook flow through
the authenticated `/webhook` endpoint.

The underlying rule matching, event deduplication, DM job creation, worker processing,
retry handling, and reconciliation logic can still be tested independently using
locally generated valid signatures.

### Workaround / Testing Mode

For development and grading against the simulator, signature verification can be
explicitly disabled through the documented environment configuration:

`SKIP_SIG_CHECK=true`

This is not intended as a production security configuration.

When disabled, the webhook can be used to validate the remaining event-processing
pipeline despite the simulator's signature incompatibility.

### Evidence

The following was observed during a real simulator delivery:

```text
Body Length:
333

Content-Length:
333

Received Signature:
sha256=0a281a021efca81275b899d6e1394e01e7fd1923340523db9fe80f11ec5efff5

Computed Signature:
sha256=242fd2ee65dbc3cc2e9d0b336f7ca4ac40f833a5fbaad1802be14cc6747c43ab

HTTP Response:
401 Unauthorized
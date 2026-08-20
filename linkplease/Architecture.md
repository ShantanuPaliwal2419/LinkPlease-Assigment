# Architecture

## Overview

This project is a FastAPI-based Instagram comment-to-DM automation system.

The system receives comment events from the PseudoGram API, matches comments against user-defined rules, creates durable DM jobs in PostgreSQL, and processes those jobs asynchronously through a background worker.

The main design goals are:

- Never silently lose a DM job
- Prevent duplicate DMs for the same user and rule
- Handle duplicate webhook events
- Handle out-of-order events
- Respect the PseudoGram API rate limit
- Retry transient failures
- Reconcile accepted DMs to verify actual delivery
- Keep webhook processing fast and independent from DM delivery

---

# High-Level Architecture

```text
                         PseudoGram API
                              |
                              |
                    POST /webhook
                              |
                              v
                    +-------------------+
                    |      FastAPI      |
                    |   Webhook API     |
                    +-------------------+
                              |
                              |
                    HMAC Signature Check
                              |
                              v
                    +-------------------+
                    | Webhook Processor |
                    +-------------------+
                       |             |
                       |             |
                Store Event       Match Rules
                       |             |
                       v             v
                 +-----------------------+
                 |      PostgreSQL       |
                 |        Neon           |
                 |                       |
                 |  Events               |
                 |  Comments             |
                 |  Rules                |
                 |  DM Jobs              |
                 |  Duplicate Events    |
                 +-----------------------+
                              |
                              |
                       Durable DM Queue
                              |
                              v
                    +-------------------+
                    |   Async Worker    |
                    |    dm_worker.py   |
                    +-------------------+
                              |
                       Controlled pacing
                          ~6.5 seconds
                              |
                              v
                    +-------------------+
                    |   PseudoGram API  |
                    |     DM Send       |
                    +-------------------+
                              |
                              v
                       DM accepted
                              |
                              v
                    +-------------------+
                    |    Reconciliation |
                    |       Worker      |
                    +-------------------+
                              |
                              v
                    delivered / failed
# Main Components

## 1. FastAPI Application

FastAPI provides the HTTP API for the application.

### Main Endpoints

POST /rules

POST /webhook

POST /webhook/sign

GET  /stats

----------

## 2. Rules API

### `POST /rules`

Creates a DM automation rule.

Example request:

{

  "keyword": "PRICE",

  "dm_message": "Here's the price list: ..."

}

The rule is stored in PostgreSQL.

A rule contains:

rule_id

keyword

dm_message

The rule is later used by the webhook processor to determine whether a comment should create a DM job.

----------

# 3. Webhook Processing

The `/webhook` endpoint receives events from PseudoGram.

The webhook handler performs the minimum required processing needed to safely persist the event and create any required DM jobs.

The general flow is:

Webhook Request

      |

      v

Verify Signature

      |

      v

Deduplicate event_id

      |

      v

Store Event

      |

      v

Handle Comment

      |

      v

Match Rules

      |

      v

Create DM Job

      |

      v

Commit Transaction

      |

      v

Return Response

The actual DM is not sent directly inside the webhook request.

Instead, a `DMJob` is stored in PostgreSQL and processed by the background worker.

This keeps webhook processing separate from slow or unreliable external API calls.

----------

# 4. Webhook Signature Verification

PseudoGram sends a signature in:

X-PseudoGram-Signature

The signature is based on:

HMAC-SHA256

using the API key as the secret.

The signature is calculated against the raw request body.

Conceptually:

Raw Request Body

       |

       v

HMAC-SHA256

       |

       v

API Key as Secret

       |

       v

Expected Signature

       |

       v

Compare with X-PseudoGram-Signature

The application also provides:

POST /webhook/sign

for generating a signature for testing purposes.

----------

# 5. Event Deduplication

PseudoGram may deliver the same webhook event more than once.

Therefore, `event_id` is treated as the idempotency key for webhook events.

The `Event` table has a unique constraint on:

event_id

The application uses an atomic database operation equivalent to:

INSERT ... ON CONFLICT DO NOTHING

If the event already exists, the new delivery is considered a duplicate.

Example:

Event 1

event_id = evt_123

       |

       v

Stored

  

Event 2

event_id = evt_123

       |

       v

Duplicate

       |

       v

Ignored

This prevents duplicate webhook deliveries from triggering duplicate processing.

----------

# 6. Comment Storage

Comments are stored separately from webhook events.

A comment is identified using:

comment_id

The comment stores information such as:

comment_id

user_id

post_id

text

state

Possible states include:

active

deleted

The database keeps the comment record even after deletion so that it can act as a tombstone.

----------

# 7. Rule Matching

When a `comment.created` event arrives, the comment text is matched against the configured rules.

Matching is case-insensitive.

For example:

PRICE

can match:

price

PRICE

Price

pricing

prices

priced

The rule matching logic also supports matching the keyword anywhere inside the comment text.

Example:

"Can you send me the PRICE?"

matches:

PRICE

----------

# 8. DM Job Creation

When a comment matches a rule, the application creates a `DMJob`.

Example:

DMJob

--------------------------------

user_id      = usr_123

rule_id      = rule_1

comment_id   = cmt_123

message      = "Here's the price list..."

status       = queued

attempts     = 0

The job is persisted in PostgreSQL.

PostgreSQL therefore acts as the durable queue for DM work.

The webhook does not need to wait for the DM API to complete.

----------

# 9. Per-User / Per-Rule Deduplication

The assignment requires:

> The same user never gets DMed twice for the same rule.

The system enforces this at the database level.

The uniqueness is based on:

user_id

rule_id

Therefore:

User A + Rule 1

can only create one DM job.

If the same user comments multiple times and continues matching the same rule:

User A + Rule 1

User A + Rule 1

User A + Rule 1

only the first DM job is created.

The later attempts are blocked and recorded in:

BlockedDuplicateEvent

This database-level constraint is important because checking only in application code could still allow duplicates during concurrent requests.

----------

# 10. Background Worker

The DM worker runs independently from the FastAPI request lifecycle.

The worker is implemented in:

dm_worker.py

The worker continuously checks PostgreSQL for eligible jobs.

It handles:

queued

waiting

jobs.

----------

# 11. Queued Job Processing

For a new queued job, the worker:

1. Find an eligible job

2. Lock the job

3. Change status to sending

4. Increment attempt counter

5. Commit the state

6. Send DM to PseudoGram

7. Process the response

Conceptually:

PostgreSQL

    |

    v

queued

    |

    v

sending

    |

    v

PseudoGram

    |

    v

Response

----------

# 12. Database Row Locking

The worker uses database row locking with:

SELECT ... FOR UPDATE SKIP LOCKED

This prevents multiple workers from claiming the same job at the same time.

For example:

             PostgreSQL

                  |

        +---------+---------+

        |         |         |

        v         v         v

      Job 1     Job 2     Job 3

        |         |         |

        v         v         v

    Worker 1  Worker 2  Worker 3

Instead of:

Worker 1 ---> Job 1

Worker 2 ---> Job 1

multiple workers can safely claim different jobs.

----------

# 13. Rate Limit Handling

The PseudoGram DM API allows:

10 requests per rolling 60 seconds

During testing, I initially experimented with higher concurrency.

However, higher concurrency caused more requests to hit the API rate limit and increased the failure rate.

Because of this, the current architecture intentionally prioritizes reliability over maximum throughput.

The worker uses controlled pacing of approximately:

6.5 seconds between DM sends

The flow is:

Send Job 1

    |

    v

Wait ~6.5 seconds

    |

    v

Send Job 2

    |

    v

Wait ~6.5 seconds

    |

    v

Send Job 3

    |

    v

...

This keeps the outgoing request rate safely below the API limit.

### Tradeoff

The advantage is:

Lower rate-limit risk

More predictable delivery

The disadvantage is:

Lower throughput

Longer queue-draining time

This was a deliberate reliability-over-throughput tradeoff.

----------

# 14. Durable Queue Design

The incoming webhook traffic and outgoing DM traffic are intentionally separated.

For example:

500 incoming events

        |

        v

     FastAPI

        |

        v

   PostgreSQL

        |

        v

   500 DM Jobs

        |

        v

 Controlled Worker

        |

        v

PseudoGram DM API

The system does not attempt to send 500 DMs immediately.

Instead, the webhook can quickly persist incoming work while the worker gradually processes the jobs.

This protects the external API from a burst of outgoing requests.

----------

# 15. Retry Handling

Transient failures are retried.

Examples include:

HTTP 500

Network exceptions

Temporary reconciliation failures

Normal job failures use exponential backoff.

The retry delay is approximately:

Attempt 1 -> 2 seconds

Attempt 2 -> 4 seconds

Attempt 3 -> 8 seconds

Attempt 4 -> 16 seconds

...

Retries are limited using:

MAX_ATTEMPTS = 5

After the maximum number of attempts is reached:

status = failed

This prevents permanently failing jobs from retrying forever.

----------

# 16. HTTP 429 Handling

HTTP `429` means that the PseudoGram rate limit was exceeded.

The API provides a:

Retry-After

header.

The worker uses that value to schedule the job for a later retry.

Example:

PseudoGram

    |

    v

HTTP 429

    |

    v

Read Retry-After

    |

    v

status = queued

    |

    v

next_attempt_at = current_time + retry_after

    |

    v

Retry later

This allows the worker to respect the server-provided retry timing.

----------

# 17. HTTP 500 Handling

A `500` response is considered transient.

The job is sent through the normal retry mechanism.

Example:

DM Request

    |

    v

HTTP 500

    |

    v

Retry

    |

    v

Success

If the job continues failing until the maximum number of attempts is reached:

status = failed

----------

# 18. HTTP 400 Handling

HTTP `400` indicates that the request is invalid.

Retrying the same malformed request will not fix the problem.

Therefore:

HTTP 400

    |

    v

Permanent Failure

    |

    v

status = failed

No normal retry is performed.

----------

# 19. Idempotent DM Sending

Every DM job uses an idempotency key:

dm-job-{job.id}

Example:

dm-job-123

If the worker retries the same logical job, it reuses the same idempotency key.

This gives the external API a way to recognize repeated requests for the same logical DM.

The goal is to prevent a network timeout or retry from accidentally creating multiple DMs.

----------

# 20. DM Delivery Reconciliation

A `202 Accepted` response from PseudoGram does not mean the DM was delivered.

It only means that PseudoGram accepted the request.

Example:

{

  "dm_id": "dm_123",

  "status": "queued"

}

When this happens, the job becomes:

waiting

The worker later calls:

GET /v1/dm/{dm_id}

to determine the actual delivery status.

Possible statuses:

queued

delivered

failed

----------

# 21. Successful Reconciliation

DM accepted

    |

    v

status = waiting

    |

    v

Check DM status

    |

    v

delivered

    |

    v

DMJob = delivered

The `/stats` endpoint then counts this job as sent.

----------

# 22. Failed Reconciliation

DM accepted

    |

    v

status = waiting

    |

    v

Check DM status

    |

    v

failed

    |

    v

Retry

    |

    +----> delivered

    |

    +----> failed after MAX_ATTEMPTS

This prevents an accepted-but-eventually-failed DM from being incorrectly counted as delivered.

----------

# 23. Comment Deletion

The system handles:

comment.deleted

events.

When a comment deletion event is received, the corresponding comment is marked:

state = deleted

The comment is not physically deleted from the database.

This creates a tombstone.

Example:

comment.created

      |

      v

Comment = active

      |

      v

comment.deleted

      |

      v

Comment = deleted

If another event for the same comment arrives later, the tombstone prevents the deleted comment from being processed as an active comment.

----------

# 24. Handling Out-of-Order Events

Webhook events are not guaranteed to arrive in order.

For example:

comment.deleted

       |

       v

comment.created

can arrive in that order.

The comment state is therefore persisted in the database.

The processor checks the current comment state before processing it.

If the comment is already marked:

deleted

it is not treated as an active comment.

This makes the system more resilient to out-of-order delivery.

----------

# 25. `/stats`

The `/stats` endpoint calculates live values from PostgreSQL.

Example:

{

  "sent": 83,

  "failed": 11,

  "queued": 0,

  "duplicates_blocked": 83

}

### Sent

Count of:

DMJob.status = delivered

### Failed

Count of:

DMJob.status = failed

### Queued

Count of jobs with:

queued

waiting

### Duplicates Blocked

Count of:

BlockedDuplicateEvent

----------

# 26. Complete Successful Flow

                  PseudoGram

                       |

                       |

                 POST /webhook

                       |

                       v

              +----------------+

              |    FastAPI     |

              +----------------+

                       |

                       v

              Verify Signature

                       |

                       v

              Deduplicate Event

                       |

                       v

                Store Event

                       |

                       v

               Match Rules

                       |

                       v

          Check User + Rule Uniqueness

                       |

                       v

                 Create Job

                       |

                       v

                PostgreSQL

                       |

                       |

                 DM Job Queue

                       |

                       v

              Background Worker

                       |

                       v

              Claim DM Job

                       |

                       v

             Controlled Pacing

                       |

                       v

             PseudoGram DM API

                       |

                       v

                  202 Queued

                       |

                       v

                 waiting

                       |

                       v

              Delivery Check

                       |

                 +-----+-----+

                 |           |

                 v           v

             delivered     failed

                 |           |

                 v           v

              success      retry

----------

# 27. 500 Event Load

The assignment can send:

500 events

within approximately 10 seconds

The architecture intentionally separates ingestion from delivery.

The expected flow is:

500 Events

    |

    v

FastAPI Webhook

    |

    v

PostgreSQL

    |

    v

DM Jobs

    |

    v

Worker

    |

    | controlled rate

    v

PseudoGram

The important point is:

> The system does not need to send 500 DMs within 10 seconds.

It needs to safely accept and persist the incoming work without silently losing jobs, while respecting the external API's rate limit during delivery.

----------

# 28. Why PostgreSQL Is Used as the Queue

For this assignment, PostgreSQL acts as both:

Application Database

        +

Durable Job Queue

This was a deliberate simplicity tradeoff.

I chose not to introduce a separate queueing system for the current implementation.

### Advantages

-   Jobs survive worker restarts
-   Job state is durable
-   Retry timing can be stored using `next_attempt_at`
-   Database transactions can atomically create jobs
-   Unique constraints provide strong duplicate protection
-   Row locking allows safe job claiming
-   No additional queue infrastructure is required

### Disadvantages

PostgreSQL is not a specialized high-throughput message broker.

For a much larger production system, a dedicated queue could be considered.

Possible options include:

Redis Streams

RabbitMQ

Kafka

Amazon SQS

The choice would depend on the required throughput and delivery guarantees.

----------

# 29. Current Architecture Tradeoff

The current architecture intentionally favors:

Reliability

    >

Maximum Throughput

The webhook layer is designed to accept incoming events quickly.

The worker layer deliberately sends DMs at a controlled rate.

Therefore:

Incoming Event Rate

        >

DM Delivery Rate

is expected when the system receives a large burst.

The database acts as the buffer between these two rates.

----------

# 30. Future Architecture

With additional development time, I would introduce a distributed rate limiter and multiple workers.

A possible future architecture would be:

                       PostgreSQL

                           |

                       DM Jobs

                           |

            +--------------+--------------+

            |              |              |

            v              v              v

        Worker 1       Worker 2       Worker 3

            |              |              |

            +--------------+--------------+

                           |

                           v

                  Redis Rate Limiter

                           |

                           v

                    PseudoGram API

A Redis token-bucket or similar distributed rate-limiting mechanism could coordinate multiple workers.

This would allow higher concurrency while still respecting the global API limit.

----------

# 31. Future Improvements

With another week of development, I would focus on:

1.  Distributed rate limiting using Redis
2.  Multiple worker processes
3.  Better queue monitoring
4.  Queue latency metrics
5.  Retry metrics
6.  Delivery latency metrics
7.  More extensive load testing
8.  Worker crash/restart testing
9.  Improved reconciliation backoff
10.  Better operational logging and observability

The goal would be to increase throughput without sacrificing correctness or rate-limit safety.

----------

# Design Philosophy

The core design principle is:

Persist first.

Process asynchronously.

Make operations idempotent.

Retry transient failures.

Verify actual delivery.

Respect external rate limits.

The system intentionally separates:

Webhook Ingestion

        |

        v

Durable Database

        |

        v

Background Processing

        |

        v

External API

        |

        v

Delivery Reconciliation

This prevents the reliability of the webhook endpoint from depending directly on the availability or speed of the external DM API.                    
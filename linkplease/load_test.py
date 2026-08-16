import asyncio
import hashlib
import hmac
import json
import time

import httpx

from app.config import settings


WEBHOOK_URL = "https://linkplease-assigment.onrender.com/webhook"

TOTAL_EVENTS = 500
DURATION_SECONDS = 10


def create_event(i: int):
    return {
        "event_id": f"load_evt_{i}",
        "event_type": "comment.created",
        "sent_at": "2026-08-16T10:20:00.000Z",
        "data": {
            "comment_id": f"load_comment_{i}",
            "post_id": f"load_post_{i % 10}",
            "text": "PRICE please",
            "created_at": "2026-08-16T10:19:59.000Z",
            "from": {
                "user_id": f"load_user_{i}",
                "username": f"loaduser{i}",
            },
        },
    }


def create_signature(body: bytes) -> str:
    signature = hmac.new(
        settings.pseudogram_api_key.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()

    return f"sha256={signature}"


async def send_event(
    client: httpx.AsyncClient,
    event: dict,
):
    body = json.dumps(
        event,
        separators=(",", ":"),
    ).encode()

    signature = create_signature(body)

    try:
        response = await client.post(
            WEBHOOK_URL,
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-PseudoGram-Signature": signature,
            },
        )

        print(
            f"Event {event['event_id']}: "
            f"{response.status_code}"
        )

        return response.status_code

    except Exception as e:
        print(
            f"Request failed: "
            f"{type(e).__name__}: {repr(e)}"
        )

        return None


async def main():
    print(
        f"Starting load test: "
        f"{TOTAL_EVENTS} events / "
        f"{DURATION_SECONDS} seconds"
    )

    start = time.monotonic()

    limits = httpx.Limits(
        max_connections=5,
        max_keepalive_connections=5,
    )

    semaphore = asyncio.Semaphore(5)

    async with httpx.AsyncClient(
        timeout=30,
        limits=limits,
    ) as client:

        async def bounded_send(event):
            async with semaphore:
                return await send_event(client, event)

        tasks = []

        interval = DURATION_SECONDS / TOTAL_EVENTS

        for i in range(TOTAL_EVENTS):
            event = create_event(i)

            tasks.append(
                asyncio.create_task(
                    bounded_send(event)
                )
            )

            await asyncio.sleep(interval)

        results = await asyncio.gather(*tasks)

    elapsed = time.monotonic() - start

    success = results.count(200)
    failed = len(results) - success

    print()
    print("=" * 50)
    print("LOAD TEST RESULT")
    print("=" * 50)
    print(f"Events sent:     {TOTAL_EVENTS}")
    print(f"HTTP 200:        {success}")
    print(f"HTTP failures:   {failed}")
    print(f"Elapsed time:    {elapsed:.2f}s")
    print("=" * 50)
    
if __name__ == "__main__":
     asyncio.run(main())
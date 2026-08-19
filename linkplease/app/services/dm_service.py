from typing import Optional
import httpx

from app.config import settings


async def send_dm_async(
    recipient_user_id: str,
    message: str,
    comment_id: str,
    idempotency_key: str,
    client: Optional[httpx.AsyncClient] = None,
) -> httpx.Response:
    url = f"{settings.pseudogram_base_url}/v1/dm/send"

    headers = {
        "X-API-Key": settings.pseudogram_api_key,
        "Idempotency-Key": idempotency_key,
    }

    payload = {
        "recipient_user_id": recipient_user_id,
        "message": message,
        "comment_id": comment_id,
    }

    if client is not None:
        response = await client.post(
            url,
            json=payload,
            headers=headers,
            timeout=10,
        )
    else:
        async with httpx.AsyncClient() as async_client:
            response = await async_client.post(
                url,
                json=payload,
                headers=headers,
                timeout=10,
            )

    print("PseudoGram status:", response.status_code)
    print("PseudoGram body:", response.text)

    return response


async def get_dm_status_async(
    dm_id: str,
    client: Optional[httpx.AsyncClient] = None,
) -> httpx.Response:
    url = f"{settings.pseudogram_base_url}/v1/dm/{dm_id}"

    headers = {
        "X-API-Key": settings.pseudogram_api_key,
    }

    if client is not None:
        response = await client.get(
            url,
            headers=headers,
            timeout=10,
        )
    else:
        async with httpx.AsyncClient() as async_client:
            response = await async_client.get(
                url,
                headers=headers,
                timeout=10,
            )

    print("Reconciliation status:", response.status_code)
    print("Reconciliation body:", response.text)

    return response


# Aliases for backward compatibility
send_dm = send_dm_async
get_dm_status = get_dm_status_async
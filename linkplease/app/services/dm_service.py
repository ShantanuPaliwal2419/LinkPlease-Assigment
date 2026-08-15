import httpx

from app.config import settings


def send_dm(
    recipient_user_id: str,
    message: str,
    comment_id: str,
    idempotency_key: str,
):
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

    response = httpx.post(
        url,
        json=payload,
        headers=headers,
        timeout=10,
    )

    print("PseudoGram status:", response.status_code)
    print("PseudoGram body:", response.text)

    return response
import hashlib
import hmac
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("PSEUDOGRAM_API_KEY")

if not API_KEY:
    raise ValueError("PSEUDOGRAM_API_KEY is not set in .env")

body = b'''{
  "event_id": "evt_signature_test_001",
  "event_type": "comment.created",
  "sent_at": "2026-08-16T10:20:00.000Z",
  "data": {
    "comment_id": "cmt_signature_test_001",
    "post_id": "post_test_001",
    "text": "PRICE please",
    "created_at": "2026-08-16T10:19:59.000Z",
    "from": {
      "user_id": "usr_signature_test_001",
      "username": "testuser"
    }
  }
}'''

signature = hmac.new(
    API_KEY.encode(),
    body,
    hashlib.sha256
).hexdigest()

print("X-PseudoGram-Signature:")
print("sha256=" + signature)
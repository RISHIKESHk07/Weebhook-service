import requests, hmac, hashlib, json

subscription_id = "503cb341-7f53-4906-abd4-db95fff5d3f3"
secret = b"mytestsecret123"  # Must match what's in DB

payload = {
    "event": "order.created",
    "data": {
        "order_id": 101,
        "amount": 299
    }
}

raw = json.dumps(payload)
signature = hmac.new(secret, raw.encode(), hashlib.sha256).hexdigest()

res = requests.post(
    f"http://localhost:8000/ingest/{subscription_id}",
    data=raw,
    headers={
        "Content-Type": "application/json",
        "x-signature": signature
    }
)

print(res.status_code, res.text)
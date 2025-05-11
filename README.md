# Webhook Delivery & Subscription Service Documentation

## 🚀 Overview

This Flask-based microservice provides a complete solution for managing webhook subscriptions, ingesting events, queuing background jobs, and tracking delivery status. It leverages PostgreSQL for persistence, Redis for caching and queuing, and supports secure webhook communication through HMAC signatures.

---

## ⚖️ Tech Stack

| Component        | Technology Used           | Reason                                            |
| ---------------- | ------------------------- | ------------------------------------------------- |
| Backend          | Flask                     | Lightweight, easy to use for APIs                 |
| Database         | PostgreSQL                | Reliable relational storage                       |
| Caching/Queue    | Redis                     | Fast in-memory store and pub/sub capabilities     |
| Background Jobs  | Python threading & Queues | Simple multi-threaded processing                  |
| ORM              | SQLAlchemy                | Pythonic DB layer with migration support          |
| Migrations       | Flask-Migrate             | Easy DB schema evolution                          |
| Containerization | Docker + Docker Compose   | Environment consistency and service orchestration |

---

## 🔧 Setup Instructions

* Start services using Docker Compose:

```bash
docker-compose up --build
```

* Make sure the `Redis`, `PostgreSQL`, and `Flask` services are connected through the Compose network.

---

## 🔍 API Endpoints

### 1. `POST /add_subscription`

Create a new webhook subscription.

**Request JSON:**

```json
{
  "target_url": "http://example.com/webhook",
  "event_type": "order_created",
  "secret_key": "mysecretkey"
}
```

**Response:**

```json
{ "id": "<subscription_id>" }
```

---

### 2. `GET /subscriptions`

Retrieve all active subscriptions.

**Response:**

```json
[
  {"id": "...", "url": "...", "event": "..."}
]
```

---

### 3. `PUT /update_subscription/<subscription_id>`

Update a subscription's details.

**Request JSON:** (Any field can be optional)

```json
{
  "target_url": "http://newurl.com",
  "event_type": "order_shipped",
  "secret_key": "newkey",
  "is_active": true
}
```

**Response:**

```json
{"message": "Subscription updated successfully"}
```

---

### 4. `DELETE /delete_subscription/<subscription_id>`

Delete a subscription.

**Response:**

```json
{"message": "Subscription deleted successfully"}
```

---

### 5. `POST /ingest/<subscription_id>`

Ingest an event to trigger the webhook flow.

**Headers:**

* `x-signature`: HMAC-SHA256 of the payload using the subscription's `secret_key`

**Payload:**

```json
{
  "event": "order_created",
  "data": { ... }
}
```

**Response:**

```json
{"message": "Request accepted for processing"}
```

---

### 6. `GET /subscription_deliveries/<subscription_id>`

Get the 20 most recent delivery attempts for a subscription.

**Response:**

```json
[
  {
    "event_id": "...",
    "attempt": 1,
    "status": "success",
    "http_code": 200,
    "Error_details": null,
    "timestamp": "2025-05-10T13:22:33"
  }
]
```

---

### 7. `GET /delivery_status/<event_id>`

Get delivery status for a specific event.

**Response:**

```json
[
  {
    "attempt": 1,
    "status": "success",
    "http_code": 200,
    "Error_details": null,
    "timestamp": "2025-05-10T13:22:33"
  }
]
```

---

### 8. `POST /webhook`

Testing endpoint to simulate a webhook receiver.

**Response:**

```json
{"messgae":"Post reached to target url"}
```

---

## 🪨 Security

* HMAC-based request validation using a shared `secret_key`.
* Signatures are compared using `hmac.compare_digest` to prevent timing attacks.

---

## 📈 Performance & Caching

* Subscriptions are cached in Redis for fast access.
* Cached entries are keyed as `subscription:<id>`.
* Python `pickle` is used to serialize/deserialize objects.

---

## ⚖️ Background Processing

* A thread is launched on app startup to process the `event_queue`.
* Events are delivered asynchronously to the respective target URLs.
* Retry and delivery tracking is managed via the `WebhookDelivery` model.

---

# trievo-queue Microservice

`trievo-queue` is a high-performance, real-time patient queue microservice built with **Python 3.11+**, **FastAPI**, **Redis Sorted Sets**, **APScheduler**, and **Pydantic**. It maintains priority-ordered queues, guarantees arrival-time fairness to prevent patient starvation, periodically evaluates waiting cases for excessive delay via an automated escalation monitor, and dispatches decoupled alerts for Critical cases.

---

## Table of Contents
- [Purpose](#purpose)
- [Architecture & Design](#architecture--design)
- [Queue Scoring Formula](#queue-scoring-formula)
- [Stale-Case Escalation Monitor](#stale-case-escalation-monitor)
- [Queue Partitioning](#queue-partitioning)
- [Decoupled Alert Dispatcher](#decoupled-alert-dispatcher)
- [Audit Logging](#audit-logging)
- [Event Contracts](#event-contracts)
- [API Endpoints & Examples](#api-endpoints--examples)
- [Environment Variables](#environment-variables)
- [Redis Requirements](#redis-requirements)
- [Local Development & Setup](#local-development--setup)
- [Running Tests](#running-tests)
- [Docker Deployment](#docker-deployment)
- [Design Decisions & Future Improvements](#design-decisions--future-improvements)

---

## Purpose

Emergency departments and specialized triage clinics face two critical operational risks:
1. **Starvation over time**: A Non-Urgent patient waiting hours could be pushed back indefinitely behind a continuous stream of newer arrivals.
2. **Clinical Staleness**: While waiting in the queue, a patient's underlying condition can deteriorate. Cases must be periodically checked for excessive waiting time and flagged/escalated to alert medical staff for timely re-evaluation.

`trievo-queue` solves both problems deterministically by combining strict priority tiers, arrival-time fairness points, automated APScheduler escalation monitoring, and real-time event streaming.

---

## Architecture & Design

### Directory Structure

```
trievo-queue/
├── app/
│   ├── main.py                     # FastAPI application setup, lifecycle, and healthcheck
│   ├── api/
│   │   └── v1/
│   │       └── queue_routes.py     # REST API (reorder, ack, query, audit) & WebSockets
│   ├── queue/
│   │   ├── priority_scorer.py      # Deterministic priority weight + arrival fairness math
│   │   ├── queue_store.py          # Redis Sorted Sets & Hash operations, partitioning
│   │   └── escalation_monitor.py   # APScheduler periodic scanner for stale cases
│   ├── alerts/
│   │   └── alert_dispatcher.py     # Decoupled notification channels (In-app, Redis, etc.)
│   ├── events/
│   │   ├── subscriber.py           # Background subscriber for trievo.verify.completed
│   │   └── publisher.py            # Publisher for trievo.queue.updated and alerts
│   ├── core/
│   │   └── config.py               # Pydantic Settings loaded from .env
│   ├── models/
│   │   ├── events.py               # Schemas for verify, updated, and alert events
│   │   ├── queue.py                # Schemas for queue items, reorder, and ack
│   │   └── audit.py                # Audit log schema
│   ├── websocket/
│   │   └── connection_manager.py   # Live streaming to dashboard WebSocket clients
│   └── tests/
│       ├── conftest.py             # Pytest fixtures and FakeRedis test harnesses
│       ├── test_priority_scorer.py # Tier math, fairness capping, wait estimation
│       ├── test_queue_store.py     # Redis sorted set ranks, partitioning, reorder, ack
│       ├── test_escalation_monitor.py # Periodic scan, threshold escalation, cooldown
│       ├── test_events_and_alerts.py  # Critical cases, invalid payloads, publications
│       ├── test_api_routes.py      # Full REST API integration tests
│       └── test_spec_requirements.py  # 14 explicit specification compliance tests
├── Dockerfile                      # Production container image
├── requirements.txt                # Production and test dependencies
├── .env.example                    # Comprehensive configuration documentation
└── README.md                       # Complete documentation
```

### Event & Data Flow

```
[Upstream Verifier]
         │  Publishes
         ▼
[trievo.verify.completed]
         │
         ▼
[EventSubscriber] (Validates with Pydantic)
         │
         ▼
[PriorityScorer] (Score = Tier Weight + Capped Fairness Bonus)
         │
         ▼
[QueueStore] (Redis Sorted Set: ZADD queue:{partition} score case_id)
         │
   ┌─────┴────────────────────────┐
   ▼                              ▼
[trievo.queue.updated]      [trievo.queue.alert] (If CRITICAL)
   │                              │
   ▼                              ▼
[Dashboard / WebSockets]    [AlertDispatcher] (In-App, Logging, Redis, etc.)
                                  ▲
                                  │ (Raises alert on severe stale delay)
                       [EscalationMonitor] (APScheduler)
                       Periodically scans active partitions
```

---

## Queue Scoring Formula

The queue score is deterministic and calculated as:

$$\text{QueueScore} = \text{TierBaseWeight} + \text{FairnessComponent} + \text{EscalationBonus} + \text{ManualOffset}$$

### 1. Tier Base Weights
Higher score = Higher urgency / Earlier queue position.
- **CRITICAL (P1)**: `1,000,000.0`
- **URGENT (P2)**: `500,000.0`
- **SEMI_URGENT (P3)**: `200,000.0`
- **NON_URGENT (P4)**: `50,000.0`

### 2. Arrival-Time Fairness Component
$$\text{wait\_minutes} = \max(0.0, \frac{\text{current\_timestamp} - \text{arrival\_timestamp}}{60.0})$$
$$\text{raw\_fairness} = \text{wait\_minutes} \times \text{FAIRNESS\_WEIGHT\_PER\_MINUTE (default 10.0)}$$
$$\text{fairness\_component} = \min(\text{MAX\_FAIRNESS\_BONUS (default 40,000.0)}, \text{raw\_fairness}) + \text{micro\_arrival\_tie\_breaker}$$

**Crucial Tier Protection Guarantee**:
The maximum fairness bonus is strictly capped at `40,000.0`, which is substantially smaller than the minimum separation between any adjacent priority tiers ($200,000 - 50,000 = 150,000$). Therefore, a non-critical case **can NEVER surpass a genuinely critical or higher-tier case** solely through waiting time. However, within the same tier, an earlier arrival will strictly have a higher score and be served ahead of later arrivals.

### 3. Escalation Bonus
$$\text{escalation\_bonus} = \text{escalation\_level} \times \text{ESCALATION\_BOOST\_SCORE (default 25,000.0)}$$
When cases exceed stale thresholds, their escalation level increments, elevating their position and flagging staff attention.

### 4. Manual Offset
Provides a direct score adjustment when clinical staff manually reorders the queue.

---

## Stale-Case Escalation Monitor

The Escalation Monitor runs on a periodic background schedule using **APScheduler** (`AsyncIOScheduler`):

1. **Periodic Scan**: Runs every `QUEUE_ESCALATION_INTERVAL_SECONDS` (default: 30s).
2. **Threshold Checks**: Checks elapsed wait time against configured tier-specific thresholds:
   - `CRITICAL`: 10 minutes (`STALE_THRESHOLD_CRITICAL_MINUTES`)
   - `URGENT`: 30 minutes (`STALE_THRESHOLD_URGENT_MINUTES`)
   - `SEMI_URGENT`: 60 minutes (`STALE_THRESHOLD_SEMI_URGENT_MINUTES`)
   - `NON_URGENT`: 120 minutes (`STALE_THRESHOLD_NON_URGENT_MINUTES`)
   - Fallback general threshold: 45 minutes (`QUEUE_STALE_THRESHOLD_MINUTES`)
3. **Escalation Action**:
   - Increments `escalation_level` (1, 2, ... up to `MAX_ESCALATION_LEVEL`).
   - Marks `is_stale = True`.
   - Promotes effective priority tier (e.g. `NON_URGENT` $\rightarrow$ `SEMI_URGENT`).
   - Recalculates queue score and updates Redis Sorted Set.
   - Logs an audit entry with `staff_id="SYSTEM"`.
   - Publishes `trievo.queue.updated` (action: `ESCALATION`).
4. **Alert Generation**:
   - If `escalation_level >= ALERT_TRIGGER_ESCALATION_LEVEL` (default: 1), generates and publishes `trievo.queue.alert` with severity `ESCALATION`.
5. **Anti-Loop Prevention**:
   - Caps escalation at `MAX_ESCALATION_LEVEL` (default: 3) to prevent repeated runaway loops.
   - Applies an escalation cooldown period so a case cannot be escalated multiple times in the same check window.

---

## Queue Partitioning

Different clinical domains maintain independent operational queues while sharing the same underlying infrastructure. Partitioning is implemented using Redis key naming:

- Emergency Acuity Queue: `queue:ed`
- Tuberculosis GeneXpert Queue: `queue:tb:genexpert`
- Dengue Monitoring Queue: `queue:dengue:monitoring`
- Radiology / Imaging Queue: `queue:radiology`

Active partitions are tracked in `queue:partitions:active`. Adding a new queue requires zero architectural changes—simply supply the department or niche in the event payload or API request.

---

## Decoupled Alert Dispatcher

Queue logic is completely decoupled from alert delivery channels. The `AlertDispatcher` uses an extensible channel abstraction (`AlertChannel`):

```python
class AlertChannel(ABC):
    @property
    @abstractmethod
    def channel_name(self) -> str: ...

    @abstractmethod
    async def send_alert(self, alert: QueueAlertEvent) -> bool: ...
```

Registered default channels:
- `RedisPubSubAlertChannel`: Publishes to Redis channel `trievo.queue.alert`.
- `InAppDashboardAlertChannel`: Real-time in-app buffering for connected dashboard WebSocket clients.
- `LoggingAlertChannel`: Structured JSON alert logging for system observability.

*Future channels (SMS, PagerDuty, WhatsApp, Ward Display) can be added simply by subclassing `AlertChannel` and calling `dispatcher.register_channel()`, with zero modifications to queue or scoring logic.*

---

## Audit Logging

Every manual intervention and system escalation is recorded with complete auditability:

**Fields Recorded:**
- `audit_id`: Unique UUID
- `event_type`: `REORDER`, `ACK`, `ESCALATION`, `INSERT`, `REMOVE`
- `case_id`: Identifier of patient case
- `partition`: Queue partition
- `staff_id`: User identifier or `SYSTEM`
- `old_state`: Previous state snapshot (`position`, `priority`, `score`, `status`)
- `new_state`: Updated state snapshot
- `reason`: Clinical justification or escalation trigger note
- `timestamp`: UTC ISO 8601 timestamp

Stored in Redis:
- Global audit log: `queue:audit:log`
- Case-specific audit trail: `queue:audit:case:{case_id}`

---

## Event Contracts

### 1. Ingest Event: `trievo.verify.completed`
```json
{
  "case_id": "case-94182",
  "priority": "CRITICAL",
  "arrival_timestamp": 1727181015.0,
  "department": "ed",
  "patient_id": "patient-771",
  "metadata": {
    "chief_complaint": "Acute respiratory distress",
    "spO2": 82
  }
}
```

### 2. Output Event: `trievo.queue.updated`
```json
{
  "event_type": "trievo.queue.updated",
  "timestamp": "2026-09-24T12:00:00Z",
  "partition": "ed",
  "case_id": "case-94182",
  "patient_id": "patient-771",
  "action": "INSERT",
  "queue_position": 1,
  "estimated_wait_time_minutes": 0.0,
  "score": 1000000.5,
  "priority": "CRITICAL",
  "effective_priority": "CRITICAL",
  "is_stale": false,
  "escalation_level": 0,
  "total_in_queue": 8,
  "summary": "Case case-94182 (CRITICAL) added at position #1"
}
```

### 3. Output Event: `trievo.queue.alert`
```json
{
  "event_type": "trievo.queue.alert",
  "alert_id": "f5e921d7-2c93-4a11-8be9-e0b4c6e91f1a",
  "timestamp": "2026-09-24T12:00:00Z",
  "partition": "ed",
  "case_id": "case-94182",
  "patient_id": "patient-771",
  "severity": "CRITICAL",
  "reason": "CRITICAL case arrived in partition 'ed'. Requires immediate attention.",
  "queue_position": 1,
  "wait_time_minutes": 0.0,
  "escalation_level": 0,
  "status": "ACTIVE",
  "metadata": {
    "source": "trievo.verify.completed"
  }
}
```

---

## API Endpoints & Examples

### `POST /api/v1/queue/reorder`
Manual staff queue intervention.

**Request:**
```bash
curl -X POST http://localhost:8000/api/v1/queue/reorder \
  -H "Content-Type: application/json" \
  -d '{
    "case_id": "case-94182",
    "staff_id": "dr_chen",
    "reason": "Clinical deterioration: oxygen saturation dropped to 82%",
    "new_priority": "CRITICAL"
  }'
```

**Response:**
```json
{
  "status": "SUCCESS",
  "case_id": "case-94182",
  "partition": "ed",
  "old_position": 4,
  "new_position": 1,
  "old_priority": "URGENT",
  "new_priority": "CRITICAL",
  "old_score": 500300.0,
  "new_score": 1000000.0,
  "staff_id": "dr_chen",
  "reason": "Clinical deterioration: oxygen saturation dropped to 82%",
  "audit_id": "b0d62fe5-5b4c-4f76-928d-1be1c905b4e8",
  "timestamp": "2026-09-24T12:05:00Z"
}
```

---

### `POST /api/v1/queue/ack`
Acknowledge an alert or clinical handover.

**Request:**
```bash
curl -X POST http://localhost:8000/api/v1/queue/ack \
  -H "Content-Type: application/json" \
  -d '{
    "case_id": "case-94182",
    "staff_id": "nurse_patel",
    "alert_id": "f5e921d7-2c93-4a11-8be9-e0b4c6e91f1a",
    "note": "Patient assigned to Resuscitation Bay 1",
    "action_taken": "BED_ASSIGNED"
  }'
```

**Response:**
```json
{
  "status": "SUCCESS",
  "case_id": "case-94182",
  "alert_id": "f5e921d7-2c93-4a11-8be9-e0b4c6e91f1a",
  "staff_id": "nurse_patel",
  "timestamp": "2026-09-24T12:06:00Z",
  "audit_id": "c1a9e320-410a-4298-b80c-255d64b1ea82",
  "message": "Acknowledgement logged and processed successfully"
}
```

---

### Additional Query & Dashboard Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Service health status, Redis connectivity, active partition list |
| `GET` | `/api/v1/queue?partition=ed&offset=0&limit=100` | Fetch live ordered queue with positions & wait times |
| `GET` | `/api/v1/queue/case/{case_id}` | Fetch individual case live position, wait time, and details |
| `GET` | `/api/v1/queue/partitions` | List all active queue partitions |
| `GET` | `/api/v1/queue/audit?case_id={case_id}` | Query audit log history |
| `POST`| `/api/v1/queue/events/verify-completed` | Direct HTTP event ingestion (for test/webhook integration) |
| `WS`  | `/api/v1/queue/ws/{partition}` | Real-time WebSocket queue stream for frontend dashboard |

---

## Environment Variables

All operational thresholds are configurable without modifying code:

| Variable | Default | Description |
|---|---|---|
| `APP_ENV` | `development` | Environment name |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `USE_FAKE_REDIS_FALLBACK` | `true` | Allows in-memory FakeRedis fallback if Redis is unreachable locally |
| `QUEUE_DEFAULT_PARTITION` | `ed` | Default department partition |
| `TIER_CRITICAL_WEIGHT` | `1000000.0` | Base score for Critical (P1) |
| `TIER_URGENT_WEIGHT` | `500000.0` | Base score for Urgent (P2) |
| `TIER_SEMI_URGENT_WEIGHT` | `200000.0` | Base score for Semi-Urgent (P3) |
| `TIER_NON_URGENT_WEIGHT` | `50000.0` | Base score for Non-Urgent (P4) |
| `FAIRNESS_WEIGHT_PER_MINUTE` | `10.0` | Points added per minute of wait time |
| `MAX_FAIRNESS_BONUS` | `40000.0` | Maximum arrival fairness bonus cap |
| `DEFAULT_WAIT_TIME_MINUTES` | `15.0` | Average consultation minutes per patient |
| `QUEUE_ESCALATION_INTERVAL_SECONDS` | `30` | Escalation monitor scan interval in seconds |
| `QUEUE_STALE_THRESHOLD_MINUTES` | `45.0` | Default fallback stale threshold in minutes |
| `STALE_THRESHOLD_CRITICAL_MINUTES` | `10.0` | Stale threshold for Critical cases |
| `STALE_THRESHOLD_URGENT_MINUTES` | `30.0` | Stale threshold for Urgent cases |
| `STALE_THRESHOLD_SEMI_URGENT_MINUTES` | `60.0` | Stale threshold for Semi-Urgent cases |
| `STALE_THRESHOLD_NON_URGENT_MINUTES` | `120.0` | Stale threshold for Non-Urgent cases |
| `MAX_ESCALATION_LEVEL` | `3` | Maximum escalation increments permitted |
| `ESCALATION_BOOST_SCORE` | `25000.0` | Score boost applied per escalation level |
| `ALERT_TRIGGER_ESCALATION_LEVEL` | `1` | Escalation level that triggers `trievo.queue.alert` |
| `CRITICAL_ALERT_ENABLED` | `true` | Enable/disable alert publishing |
| `CHANNEL_VERIFY_COMPLETED` | `trievo.verify.completed` | Input Redis Pub/Sub topic |
| `CHANNEL_QUEUE_UPDATED` | `trievo.queue.updated` | Output Redis Pub/Sub topic |
| `CHANNEL_QUEUE_ALERT` | `trievo.queue.alert` | Alert Redis Pub/Sub topic |

---

## Redis Requirements

1. **Redis 5.0+** with support for Sorted Sets (`ZADD`, `ZRANGE`, `ZREVRANGE`, `ZREVRANK`, `ZCARD`), Hashes (`HSET`, `HGET`), Sets (`SADD`, `SMEMBERS`), Lists (`LPUSH`, `LRANGE`), and Pub/Sub.
2. In-memory FakeRedis fallback is supported for local offline development and testing.

---

## Local Development & Setup

### 1. Prerequisites
- Python 3.11+
- Redis (optional for local testing; FakeRedis fallback is built-in)

### 2. Setup
```bash
# Clone or navigate to the directory
cd trievo-queue

# Create virtual environment
python -m venv venv
# Windows:
.\venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create .env
cp .env.example .env
```

### 3. Run Locally
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```
Interactive API docs are available at `http://localhost:8000/docs`.

---

## Running Tests

The test suite covers all 14 specification requirements:

```bash
# Run all unit and integration tests
pytest -v

# Run with coverage report
pytest --cov=app -v
```

All 34 test cases run deterministically without requiring an external Redis server.

---

## Docker Deployment

Build and run using Docker:

```bash
# Build image
docker build -t trievo-queue:latest .

# Run with Redis container
docker network create trievo-net

docker run -d --name redis --network trievo-net -p 6379:6379 redis:7-alpine

docker run -d --name trievo-queue \
  --network trievo-net \
  -p 8000:8000 \
  -e REDIS_URL=redis://redis:6379/0 \
  trievo-queue:latest
```

---

## Design Decisions & Future Improvements

1. **Redis Sorted Sets for Score Ordering**:
   Sorted sets provide logarithmic insertion and rank lookup ($O(\log N)$). Because highest score corresponds to highest urgency, `ZREVRANGE` and `ZREVRANK` are used to compute positions where position 1 is the most urgent patient.
2. **Fairness Cap Strict Invariant**:
   `MAX_FAIRNESS_BONUS` ($40,000$) is strictly smaller than the minimum tier gap ($150,000$). This mathematically prevents low-acuity cases from ever bypassing higher-acuity arrivals through waiting alone, maintaining clinical safety.
3. **Escalation Monitor Cooldown & Bounds**:
   To avoid infinite escalation cascades, cases are bounded by `MAX_ESCALATION_LEVEL` and an interval cooldown check.
4. **Audit Log Persistence**:
   Currently, audit logs are saved to Redis lists (`queue:audit:log` and `queue:audit:case:{case_id}`) with capped retention. In enterprise production, the `append_audit_log` method can be swapped to write to a persistent relational database (PostgreSQL with SQLAlchemy) or MongoDB without changing the rest of the application.
5. **Decoupled Alert Dispatcher**:
   The `AlertDispatcher` uses the Strategy/Observer pattern. Adding external notification channels (e.g., Twilio SMS, Webhooks, or Ward Display boards) requires only adding an `AlertChannel` implementation without altering queue logic.

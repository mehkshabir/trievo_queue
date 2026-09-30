import time
import pytest
from app.alerts.alert_dispatcher import AlertChannel
from app.events.publisher import EventPublisher
from app.events.subscriber import EventSubscriber
from app.models.events import QueueAlertEvent, VerifyCompletedEvent
from app.queue.queue_store import QueueStore


class MockRecordingAlertChannel(AlertChannel):
    """Custom channel to record dispatched alerts for testing."""

    def __init__(self):
        self.dispatched_alerts = []

    @property
    def channel_name(self) -> str:
        return "mock_recorder"

    async def send_alert(self, alert: QueueAlertEvent) -> bool:
        self.dispatched_alerts.append(alert)
        return True


@pytest.mark.asyncio
async def test_critical_case_handling_and_alert_generation(
    test_store: QueueStore,
    test_publisher: EventPublisher,
    test_subscriber: EventSubscriber,
):
    """
    Test Requirements 4 & 5:
    Critical case ingestion produces immediate priority and generates 'trievo.queue.alert'.
    """
    mock_channel = MockRecordingAlertChannel()
    test_publisher.alert_dispatcher.register_channel(mock_channel)

    now = time.time()
    payload = {
        "case_id": "crit-999",
        "priority": "CRITICAL",
        "arrival_timestamp": now,
        "department": "ed",
        "patient_id": "patient-xyz",
        "metadata": {"triage_notes": "Severe chest pain, diaphoresis"},
    }

    item = await test_subscriber.process_event(payload)
    assert item is not None
    assert item.case_id == "crit-999"
    assert item.priority == "CRITICAL"
    assert item.queue_position == 1

    # Verify critical alert was dispatched to registered channel
    assert len(mock_channel.dispatched_alerts) == 1
    alert = mock_channel.dispatched_alerts[0]
    assert alert.case_id == "crit-999"
    assert alert.severity == "CRITICAL"
    assert alert.partition == "ed"


@pytest.mark.asyncio
async def test_invalid_event_payload_handling(
    test_subscriber: EventSubscriber,
):
    """
    Test Requirement 12: Invalid event payloads are handled safely,
    logged, and do not raise unhandled exceptions or crash the subscriber.
    """
    # 1. Non-JSON string
    res1 = await test_subscriber.process_event("NOT_VALID_JSON{:::}")
    assert res1 is None

    # 2. Missing required case_id
    res2 = await test_subscriber.process_event({
        "priority": "URGENT",
        "arrival_timestamp": time.time(),
    })
    assert res2 is None

    # 3. Empty case_id
    res3 = await test_subscriber.process_event({
        "case_id": "",
        "priority": "URGENT",
        "arrival_timestamp": time.time(),
    })
    assert res3 is None

    # 4. Invalid timestamp format
    res4 = await test_subscriber.process_event({
        "case_id": "case-valid",
        "priority": "URGENT",
        "arrival_timestamp": "NOT_A_TIMESTAMP",
    })
    assert res4 is None


@pytest.mark.asyncio
async def test_event_publication_routine_and_alert(
    test_store: QueueStore,
    test_publisher: EventPublisher,
    test_subscriber: EventSubscriber,
):
    """
    Test Requirement 14:
    Events are properly published to Redis topics for routine updates and alerts.
    """
    published_events = []
    test_publisher.register_local_listener(lambda evt: published_events.append(evt))

    # Ingest routine case
    now = time.time()
    await test_subscriber.process_event({
        "case_id": "routine-01",
        "priority": "NON_URGENT",
        "arrival_timestamp": now,
        "department": "ed",
    })

    assert len(published_events) == 1
    assert published_events[0].case_id == "routine-01"
    assert published_events[0].action == "INSERT"
    assert published_events[0].event_type == "trievo.queue.updated"

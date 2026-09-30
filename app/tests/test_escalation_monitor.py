import time
import pytest
from app.events.publisher import EventPublisher
from app.queue.escalation_monitor import EscalationMonitor
from app.queue.queue_store import QueueStore


@pytest.mark.asyncio
async def test_stale_case_detection_and_escalation(
    test_store: QueueStore,
    test_publisher: EventPublisher,
    test_settings,
):
    """
    Test Requirements 6 & 7: Stale cases exceeding wait thresholds
    are automatically escalated, flagged as stale, and score increased.
    """
    monitor = EscalationMonitor(queue_store=test_store, event_publisher=test_publisher, settings=test_settings)
    now = time.time()

    # Case 1: Fresh case (1 minute ago) - should NOT escalate
    test_store.add_or_update_case("ed", "fresh-case", "NON_URGENT", now - 60)

    # Case 2: Stale case (waiting 150 minutes, NON_URGENT threshold is 120 mins) - SHOULD escalate
    test_store.add_or_update_case("ed", "stale-case", "NON_URGENT", now - (150 * 60))

    initial_item = test_store.get_case("stale-case")
    assert initial_item is not None
    assert initial_item.escalation_level == 0
    assert initial_item.is_stale is False
    initial_score = initial_item.score

    # Run escalation scan
    escalated = await monitor.check_partition("ed")
    assert len(escalated) == 1
    assert escalated[0].case_id == "stale-case"

    # Verify updated case attributes
    updated_item = test_store.get_case("stale-case")
    assert updated_item is not None
    assert updated_item.escalation_level == 1
    assert updated_item.is_stale is True
    assert updated_item.score > initial_score
    # Effective priority promoted from NON_URGENT to SEMI_URGENT
    assert updated_item.effective_priority == "SEMI_URGENT"

    # Fresh case was untouched
    fresh_item = test_store.get_case("fresh-case")
    assert fresh_item.escalation_level == 0
    assert fresh_item.is_stale is False


@pytest.mark.asyncio
async def test_duplicate_escalation_prevention(
    test_store: QueueStore,
    test_publisher: EventPublisher,
    test_settings,
):
    """
    Test Requirement 8: Cases that reach MAX_ESCALATION_LEVEL (3)
    must NOT be repeatedly escalated indefinitely.
    """
    monitor = EscalationMonitor(queue_store=test_store, event_publisher=test_publisher, settings=test_settings)
    now = time.time()

    # Create case that has already reached max escalation level (3)
    test_store.add_or_update_case(
        partition="ed",
        case_id="max-escalated-case",
        priority="NON_URGENT",
        arrival_timestamp=now - (500 * 60),
        escalation_level=3,
        is_stale=True,
    )

    # Run escalation check
    escalated = await monitor.check_partition("ed")
    # Must NOT escalate again
    assert len(escalated) == 0

    item = test_store.get_case("max-escalated-case")
    assert item.escalation_level == 3

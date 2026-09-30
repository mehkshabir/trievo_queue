import time
import pytest
from app.models.audit import AuditLogEntry
from app.queue.queue_store import QueueStore, QueueStoreError


def test_add_and_retrieve_queue_case(test_store: QueueStore):
    """Test adding a case and retrieving ordered queue from Redis."""
    now = time.time()
    item1, is_new1 = test_store.add_or_update_case(
        partition="ed",
        case_id="case-101",
        priority="NON_URGENT",
        arrival_timestamp=now - 600,
    )
    assert is_new1 is True
    assert item1.case_id == "case-101"
    assert item1.queue_position == 1

    # Add a higher priority case
    item2, is_new2 = test_store.add_or_update_case(
        partition="ed",
        case_id="case-102",
        priority="CRITICAL",
        arrival_timestamp=now - 60,
    )
    assert is_new2 is True

    # Retrieve full queue
    queue = test_store.get_queue(partition="ed")
    assert len(queue) == 2
    # Critical must be position 1
    assert queue[0].case_id == "case-102"
    assert queue[0].queue_position == 1
    assert queue[0].priority == "CRITICAL"
    # Non-urgent must be position 2
    assert queue[1].case_id == "case-101"
    assert queue[1].queue_position == 2


def test_queue_partitioning_isolation(test_store: QueueStore):
    """Test Requirement 13: Separate partitions operate independently."""
    now = time.time()
    # Add to ED queue
    test_store.add_or_update_case("ed", "ed-01", "CRITICAL", now)
    # Add to TB GeneXpert queue
    test_store.add_or_update_case("tb:genexpert", "tb-01", "URGENT", now)
    # Add to Dengue monitoring queue
    test_store.add_or_update_case("dengue:monitoring", "dengue-01", "NON_URGENT", now)

    ed_queue = test_store.get_queue("ed")
    tb_queue = test_store.get_queue("tb:genexpert")
    dengue_queue = test_store.get_queue("dengue:monitoring")

    assert len(ed_queue) == 1
    assert ed_queue[0].case_id == "ed-01"

    assert len(tb_queue) == 1
    assert tb_queue[0].case_id == "tb-01"

    assert len(dengue_queue) == 1
    assert dengue_queue[0].case_id == "dengue-01"

    partitions = test_store.get_active_partitions()
    assert "ed" in partitions
    assert "tb:genexpert" in partitions
    assert "dengue:monitoring" in partitions


def test_manual_reorder_and_audit(test_store: QueueStore):
    """Test Requirement 9 & 10: Manual reordering updates position and records audit log."""
    now = time.time()
    test_store.add_or_update_case("ed", "case-A", "URGENT", now - 300)
    test_store.add_or_update_case("ed", "case-B", "NON_URGENT", now - 600)

    # Reorder case-B to higher priority
    updated_item, old_pos, old_prio, old_score = test_store.reorder_case(
        case_id="case-B",
        staff_id="dr_smith",
        reason="Clinical deterioration during triage wait",
        new_priority="CRITICAL",
    )

    assert updated_item.effective_priority == "CRITICAL"
    assert updated_item.queue_position == 1

    # Record and verify audit log
    audit_entry = AuditLogEntry(
        event_type="REORDER",
        case_id="case-B",
        partition="ed",
        staff_id="dr_smith",
        old_state={"position": old_pos, "priority": old_prio},
        new_state={"position": updated_item.queue_position, "priority": updated_item.effective_priority},
        reason="Clinical deterioration during triage wait",
    )
    test_store.append_audit_log(audit_entry)

    logs = test_store.get_audit_logs(case_id="case-B")
    assert len(logs) >= 1
    assert logs[0].case_id == "case-B"
    assert logs[0].staff_id == "dr_smith"
    assert logs[0].event_type == "REORDER"


def test_alert_acknowledgement(test_store: QueueStore):
    """Test Requirement 11: Alert acknowledgement updates case status and stores notes."""
    now = time.time()
    test_store.add_or_update_case("ed", "case-crit", "CRITICAL", now)

    ack_item = test_store.acknowledge_case(
        case_id="case-crit",
        staff_id="nurse_kelly",
        alert_id="alert-12345",
        note="Bed assigned in Resus Bay 2",
        action_taken="BED_ASSIGNED",
    )

    assert ack_item.status == "BED_ASSIGNED"
    retrieved = test_store.get_case("case-crit")
    assert retrieved is not None
    assert retrieved.status == "BED_ASSIGNED"
    assert retrieved.acknowledged_by == "nurse_kelly"

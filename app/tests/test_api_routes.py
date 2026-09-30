import time
from fastapi.testclient import TestClient
from app.queue.queue_store import QueueStore


def test_health_check_endpoint(client: TestClient):
    """Test health check returns 200 OK and healthy status."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "HEALTHY"
    assert data["redis_connected"] is True


def test_direct_event_ingest_and_get_queue(client: TestClient):
    """Test ingesting verified cases via HTTP and querying live queue."""
    now = time.time()
    payload = {
        "case_id": "api-case-1",
        "priority": "URGENT",
        "arrival_timestamp": now,
        "department": "ed",
        "patient_id": "pt-01",
    }
    # Ingest event
    res = client.post("/api/v1/queue/events/verify-completed", json=payload)
    assert res.status_code == 201
    item = res.json()
    assert item["case_id"] == "api-case-1"
    assert item["queue_position"] == 1
    assert item["priority"] == "URGENT"

    # Query queue
    q_res = client.get("/api/v1/queue?partition=ed")
    assert q_res.status_code == 200
    q_data = q_res.json()
    assert q_data["partition"] == "ed"
    assert q_data["total"] == 1
    assert len(q_data["items"]) == 1
    assert q_data["items"][0]["case_id"] == "api-case-1"


def test_api_reorder_and_audit(client: TestClient):
    """Test POST /api/v1/queue/reorder endpoint and audit log recording."""
    now = time.time()
    # Add two cases
    client.post(
        "/api/v1/queue/events/verify-completed",
        json={"case_id": "c1", "priority": "URGENT", "arrival_timestamp": now - 100, "department": "ed"},
    )
    client.post(
        "/api/v1/queue/events/verify-completed",
        json={"case_id": "c2", "priority": "NON_URGENT", "arrival_timestamp": now - 200, "department": "ed"},
    )

    # Reorder c2 to CRITICAL
    reorder_payload = {
        "case_id": "c2",
        "staff_id": "dr_watson",
        "reason": "Sudden anaphylactic reaction",
        "new_priority": "CRITICAL",
    }
    reorder_res = client.post("/api/v1/queue/reorder", json=reorder_payload)
    assert reorder_res.status_code == 200
    r_data = reorder_res.json()
    assert r_data["status"] == "SUCCESS"
    assert r_data["case_id"] == "c2"
    assert r_data["new_priority"] == "CRITICAL"
    assert r_data["new_position"] == 1

    # Check audit log endpoint
    audit_res = client.get("/api/v1/queue/audit?case_id=c2")
    assert audit_res.status_code == 200
    a_data = audit_res.json()
    assert a_data["total"] >= 1
    assert a_data["entries"][0]["event_type"] == "REORDER"
    assert a_data["entries"][0]["staff_id"] == "dr_watson"


def test_api_acknowledge(client: TestClient):
    """Test POST /api/v1/queue/ack endpoint."""
    now = time.time()
    client.post(
        "/api/v1/queue/events/verify-completed",
        json={"case_id": "ack-case-1", "priority": "CRITICAL", "arrival_timestamp": now, "department": "ed"},
    )

    ack_payload = {
        "case_id": "ack-case-1",
        "staff_id": "nurse_joy",
        "alert_id": "alert-99",
        "note": "Patient transferred to ICU Bed 4",
        "action_taken": "TRANSFERRED_TO_ICU",
    }
    res = client.post("/api/v1/queue/ack", json=ack_payload)
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "SUCCESS"
    assert data["case_id"] == "ack-case-1"

    # Verify case details
    c_res = client.get("/api/v1/queue/case/ack-case-1")
    assert c_res.status_code == 200
    case_data = c_res.json()
    assert case_data["status"] == "TRANSFERRED_TO_ICU"
    assert case_data["acknowledged_by"] == "nurse_joy"


def test_get_partitions_list(client: TestClient):
    """Test GET /api/v1/queue/partitions endpoint."""
    now = time.time()
    client.post(
        "/api/v1/queue/events/verify-completed",
        json={"case_id": "tb-case-1", "priority": "URGENT", "arrival_timestamp": now, "niche": "tb:genexpert"},
    )
    res = client.get("/api/v1/queue/partitions")
    assert res.status_code == 200
    parts = res.json()
    assert "tb:genexpert" in parts

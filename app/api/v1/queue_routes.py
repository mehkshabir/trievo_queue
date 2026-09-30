import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect, status

from app.core.config import Settings, get_settings
from app.events.publisher import EventPublisher
from app.events.subscriber import EventSubscriber
from app.models.audit import AuditLogEntry, AuditLogListResponse
from app.models.events import QueueUpdatedEvent, VerifyCompletedEvent
from app.models.queue import (
    AckRequest,
    AckResponse,
    QueueItem,
    QueueListResponse,
    ReorderRequest,
    ReorderResponse,
)
from app.queue.queue_store import QueueStore, QueueStoreError
from app.websocket.connection_manager import WebSocketManager, get_ws_manager

logger = logging.getLogger("trievo.api.queue")

router = APIRouter(prefix="/api/v1/queue", tags=["Queue Operations"])


# Dependencies
def get_store(settings: Settings = Depends(get_settings)) -> QueueStore:
    return QueueStore(settings=settings)


def get_publisher(store: QueueStore = Depends(get_store)) -> EventPublisher:
    return EventPublisher(redis_client=store.client, settings=store.settings)


# -----------------------------------------------------------------------------
# POST /api/v1/queue/reorder (Manual Staff Queue Intervention)
# -----------------------------------------------------------------------------
@router.post(
    "/reorder",
    response_model=ReorderResponse,
    status_code=status.HTTP_200_OK,
    summary="Manual staff queue reorder / priority override",
    description="Allows authorized staff to manually adjust priority, position, or score for a patient.",
)
async def reorder_queue_case(
    request: ReorderRequest,
    store: QueueStore = Depends(get_store),
    publisher: EventPublisher = Depends(get_publisher),
):
    try:
        updated_item, old_position, old_priority, old_score = store.reorder_case(
            case_id=request.case_id,
            staff_id=request.staff_id,
            reason=request.reason,
            partition=request.partition,
            new_priority=request.new_priority,
            target_position=request.target_position,
            score_offset=request.score_offset,
        )

        audit_id = str(uuid.uuid4())
        audit_entry = AuditLogEntry(
            audit_id=audit_id,
            event_type="REORDER",
            case_id=request.case_id,
            partition=updated_item.partition,
            staff_id=request.staff_id,
            old_state={
                "position": old_position,
                "priority": old_priority,
                "score": old_score,
            },
            new_state={
                "position": updated_item.queue_position,
                "priority": updated_item.effective_priority,
                "score": updated_item.score,
            },
            reason=request.reason,
        )
        store.append_audit_log(audit_entry)

        # Publish trievo.queue.updated
        total_in_queue = store.get_total_in_queue(updated_item.partition)
        update_event = QueueUpdatedEvent(
            partition=updated_item.partition,
            case_id=updated_item.case_id,
            patient_id=updated_item.patient_id,
            action="REORDER",
            queue_position=updated_item.queue_position,
            estimated_wait_time_minutes=updated_item.estimated_wait_time_minutes,
            score=updated_item.score,
            priority=updated_item.priority,
            effective_priority=updated_item.effective_priority,
            is_stale=updated_item.is_stale,
            escalation_level=updated_item.escalation_level,
            total_in_queue=total_in_queue,
            summary=f"Case {request.case_id} reordered by {request.staff_id}: {request.reason}",
        )
        await publisher.publish_queue_updated(update_event)

        return ReorderResponse(
            status="SUCCESS",
            case_id=updated_item.case_id,
            partition=updated_item.partition,
            old_position=old_position,
            new_position=updated_item.queue_position,
            old_priority=old_priority,
            new_priority=updated_item.effective_priority,
            old_score=old_score,
            new_score=updated_item.score,
            staff_id=request.staff_id,
            reason=request.reason,
            audit_id=audit_id,
        )
    except QueueStoreError as q_err:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(q_err))
    except Exception as exc:
        logger.error("Failed to reorder case %s: %s", request.case_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Reorder failed: {str(exc)}",
        )


# -----------------------------------------------------------------------------
# POST /api/v1/queue/ack (Acknowledge Alert / Queue Intervention)
# -----------------------------------------------------------------------------
@router.post(
    "/ack",
    response_model=AckResponse,
    status_code=status.HTTP_200_OK,
    summary="Acknowledge queue alert or patient intervention",
    description="Records staff acknowledgement of a critical case, escalation alert, or clinical handover.",
)
async def acknowledge_case(
    request: AckRequest,
    store: QueueStore = Depends(get_store),
    publisher: EventPublisher = Depends(get_publisher),
):
    try:
        updated_item = store.acknowledge_case(
            case_id=request.case_id,
            staff_id=request.staff_id,
            alert_id=request.alert_id,
            note=request.note,
            action_taken=request.action_taken or "ACKNOWLEDGED",
        )

        audit_id = str(uuid.uuid4())
        audit_entry = AuditLogEntry(
            audit_id=audit_id,
            event_type="ACK",
            case_id=request.case_id,
            partition=updated_item.partition,
            staff_id=request.staff_id,
            old_state={"status": "WAITING"},
            new_state={"status": updated_item.status, "alert_id": request.alert_id},
            reason=request.note or "Alert acknowledged by staff",
        )
        store.append_audit_log(audit_entry)

        # Publish trievo.queue.updated
        total = store.get_total_in_queue(updated_item.partition)
        update_event = QueueUpdatedEvent(
            partition=updated_item.partition,
            case_id=updated_item.case_id,
            patient_id=updated_item.patient_id,
            action="ACK",
            queue_position=updated_item.queue_position,
            estimated_wait_time_minutes=updated_item.estimated_wait_time_minutes,
            score=updated_item.score,
            priority=updated_item.priority,
            effective_priority=updated_item.effective_priority,
            is_stale=updated_item.is_stale,
            escalation_level=updated_item.escalation_level,
            total_in_queue=total,
            summary=f"Case {request.case_id} acknowledged by {request.staff_id}",
        )
        await publisher.publish_queue_updated(update_event)

        return AckResponse(
            status="SUCCESS",
            case_id=request.case_id,
            alert_id=request.alert_id,
            staff_id=request.staff_id,
            audit_id=audit_id,
            message="Acknowledgement logged and processed successfully",
        )
    except QueueStoreError as q_err:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(q_err))
    except Exception as exc:
        logger.error("Failed to acknowledge case %s: %s", request.case_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Acknowledgement failed: {str(exc)}",
        )


# -----------------------------------------------------------------------------
# GET /api/v1/queue (Live Ordered Queue)
# -----------------------------------------------------------------------------
@router.get(
    "",
    response_model=QueueListResponse,
    status_code=status.HTTP_200_OK,
    summary="Get live priority-ordered queue",
    description="Returns live queue for a given partition, with computed positions and estimated waiting times.",
)
async def get_live_queue(
    partition: Optional[str] = Query(None, description="Queue partition (default: ed)"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(100, ge=1, le=500, description="Number of cases to return"),
    store: QueueStore = Depends(get_store),
    settings: Settings = Depends(get_settings),
):
    part = partition or settings.QUEUE_DEFAULT_PARTITION
    items = store.get_queue(partition=part, offset=offset, limit=limit)
    total = store.get_total_in_queue(partition=part)
    return QueueListResponse(
        partition=part,
        total=total,
        items=items,
    )


# -----------------------------------------------------------------------------
# GET /api/v1/queue/case/{case_id} (Single Case Details & Live Position)
# -----------------------------------------------------------------------------
@router.get(
    "/case/{case_id}",
    response_model=QueueItem,
    status_code=status.HTTP_200_OK,
    summary="Get case details and live position",
)
async def get_queue_case(
    case_id: str,
    store: QueueStore = Depends(get_store),
):
    item = store.get_case(case_id)
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Case '{case_id}' not found in active queues.",
        )
    return item


# -----------------------------------------------------------------------------
# GET /api/v1/queue/partitions (List Active Partitions)
# -----------------------------------------------------------------------------
@router.get(
    "/partitions",
    response_model=List[str],
    status_code=status.HTTP_200_OK,
    summary="List active queue partitions",
)
async def get_partitions(
    store: QueueStore = Depends(get_store),
):
    return store.get_active_partitions()


# -----------------------------------------------------------------------------
# GET /api/v1/queue/audit (Audit Log Query)
# -----------------------------------------------------------------------------
@router.get(
    "/audit",
    response_model=AuditLogListResponse,
    status_code=status.HTTP_200_OK,
    summary="Query audit log entries",
)
async def get_audit_trail(
    case_id: Optional[str] = Query(None, description="Filter by case ID"),
    limit: int = Query(50, ge=1, le=200, description="Max entries to return"),
    store: QueueStore = Depends(get_store),
):
    entries = store.get_audit_logs(case_id=case_id, limit=limit)
    return AuditLogListResponse(
        total=len(entries),
        case_id=case_id,
        entries=entries,
    )


# -----------------------------------------------------------------------------
# POST /api/v1/queue/events/verify-completed (Direct Ingest / Testing Endpoint)
# -----------------------------------------------------------------------------
@router.post(
    "/events/verify-completed",
    response_model=QueueItem,
    status_code=status.HTTP_201_CREATED,
    summary="Ingest trievo.verify.completed event directly via HTTP",
    description="Validates and enqueues a verified case, mirroring the Redis Pub/Sub subscriber path.",
)
async def ingest_verify_completed_event(
    event: VerifyCompletedEvent,
    store: QueueStore = Depends(get_store),
    publisher: EventPublisher = Depends(get_publisher),
):
    subscriber = EventSubscriber(queue_store=store, event_publisher=publisher)
    item = await subscriber.process_event(event.model_dump())
    if not item:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to ingest verified case payload.",
        )
    return item


# -----------------------------------------------------------------------------
# WebSocket: Live Dashboard Queue Streaming
# -----------------------------------------------------------------------------
@router.websocket("/ws")
@router.websocket("/ws/{partition}")
async def queue_websocket_stream(
    websocket: WebSocket,
    partition: Optional[str] = None,
    ws_manager: WebSocketManager = Depends(get_ws_manager),
    settings: Settings = Depends(get_settings),
):
    """
    WebSocket endpoint for real-time dashboard updates.
    On connection, sends initial queue state snapshot, then streams updates.
    """
    part = partition or settings.QUEUE_DEFAULT_PARTITION
    await ws_manager.connect(websocket, partition=part)
    try:
        store = QueueStore(settings=settings)
        initial_items = store.get_queue(part, offset=0, limit=100)
        total = store.get_total_in_queue(part)
        initial_payload = {
            "type": "INITIAL_STATE",
            "partition": part,
            "total": total,
            "cases": [item.model_dump() for item in initial_items],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await websocket.send_json(initial_payload)

        # Keep alive loop listening for client pings
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket, partition=part)
    except Exception as exc:
        logger.warning("WebSocket client connection closed: %s", exc)
        ws_manager.disconnect(websocket, partition=part)

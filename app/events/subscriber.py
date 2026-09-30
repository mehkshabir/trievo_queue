import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union
import redis
from pydantic import ValidationError

from app.core.config import Settings, get_settings
from app.events.publisher import EventPublisher
from app.models.events import QueueAlertEvent, QueueUpdatedEvent, VerifyCompletedEvent
from app.models.queue import PriorityTier, QueueItem
from app.queue.queue_store import QueueStore

logger = logging.getLogger("trievo.events.subscriber")


class EventSubscriber:
    """
    Subscribes ONLY to 'trievo.verify.completed'.
    Validates verified cases, computes queue scores, stores state in Redis,
    and publishes queue updates and critical alerts.
    """

    def __init__(
        self,
        queue_store: QueueStore,
        event_publisher: EventPublisher,
        settings: Optional[Settings] = None,
    ):
        self.store = queue_store
        self.publisher = event_publisher
        self.settings = settings or get_settings()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def process_event(self, raw_payload: Union[str, bytes, Dict[str, Any]]) -> Optional[QueueItem]:
        """
        Validate and ingest a verified case payload.
        Handles invalid payloads safely without raising unhandled exceptions.
        """
        # Parse into dict if needed
        data: Dict[str, Any]
        if isinstance(raw_payload, (str, bytes)):
            try:
                data = json.loads(raw_payload)
            except Exception as err:
                logger.error("Invalid JSON received for trievo.verify.completed: %s. Payload: %r", err, raw_payload)
                return None
        elif isinstance(raw_payload, dict):
            data = raw_payload
        else:
            logger.error("Unsupported payload type: %s", type(raw_payload))
            return None

        # Validate with Pydantic
        try:
            event = VerifyCompletedEvent.model_validate(data)
        except ValidationError as val_err:
            logger.warning(
                "Rejected invalid 'trievo.verify.completed' event schema: %s. Raw data: %s",
                val_err.errors(),
                data,
            )
            return None

        # Processing verified case
        partition = event.resolved_partition
        arrival_ts = event.parsed_arrival_timestamp

        logger.info(
            "Ingesting verified case: case_id=%s, priority=%s, partition=%s, arrival=%.0f",
            event.case_id,
            event.priority,
            partition,
            arrival_ts,
        )

        try:
            # Add or update in Redis Sorted Set
            item, is_new = self.store.add_or_update_case(
                partition=partition,
                case_id=event.case_id,
                priority=event.priority,
                arrival_timestamp=arrival_ts,
                patient_id=event.resolved_patient_id,
                priority_weight=event.priority_weight,
                metadata=event.metadata,
                status="WAITING",
            )

            total_in_queue = self.store.get_total_in_queue(partition)

            # Publish routine update event
            update_event = QueueUpdatedEvent(
                partition=partition,
                case_id=item.case_id,
                patient_id=item.patient_id,
                action="INSERT" if is_new else "UPDATE",
                queue_position=item.queue_position,
                estimated_wait_time_minutes=item.estimated_wait_time_minutes,
                score=item.score,
                priority=item.priority,
                effective_priority=item.effective_priority,
                is_stale=item.is_stale,
                escalation_level=item.escalation_level,
                total_in_queue=total_in_queue,
                summary=f"Case {item.case_id} ({item.priority}) added at position #{item.queue_position}",
            )
            await self.publisher.publish_queue_updated(update_event)

            # Check for CRITICAL case alert
            norm_priority = PriorityTier.from_str(item.priority)
            if norm_priority == PriorityTier.CRITICAL and self.settings.CRITICAL_ALERT_ENABLED:
                alert = QueueAlertEvent(
                    alert_id=str(uuid.uuid4()),
                    partition=partition,
                    case_id=item.case_id,
                    patient_id=item.patient_id,
                    severity="CRITICAL",
                    reason=f"CRITICAL case arrived in partition '{partition}'. Requires immediate attention.",
                    queue_position=item.queue_position,
                    wait_time_minutes=item.wait_time_minutes,
                    escalation_level=item.escalation_level,
                    metadata={"source": "trievo.verify.completed", **event.metadata},
                )
                await self.publisher.publish_queue_alert(alert)

            return item
        except Exception as exc:
            logger.error("Failed to process verified event for case %s: %s", event.case_id, exc)
            return None

    async def start_listening(self) -> None:
        """
        Background subscriber loop listening to Redis Pub/Sub topic 'trievo.verify.completed'.
        Includes auto-reconnect logic to prevent crashing on intermittent disconnects.
        """
        self._running = True
        channel_name = self.settings.CHANNEL_VERIFY_COMPLETED
        logger.info("Starting background subscriber for channel: %s", channel_name)

        while self._running:
            pubsub = None
            try:
                pubsub = self.store.client.pubsub()
                pubsub.subscribe(channel_name)
                logger.info("Subscribed to Redis channel '%s'", channel_name)

                while self._running:
                    # Non-blocking get_message with timeout in threadpool
                    message = await asyncio.to_thread(pubsub.get_message, ignore_subscribe_messages=True, timeout=1.0)
                    if message and message.get("type") == "message":
                        raw_data = message.get("data")
                        if raw_data:
                            await self.process_event(raw_data)
                    await asyncio.sleep(0.05)

            except Exception as exc:
                if not self._running:
                    break
                logger.warning(
                    "Redis subscriber connection lost (%s). Reconnecting in 3 seconds...",
                    exc,
                )
                await asyncio.sleep(3.0)
            finally:
                if pubsub:
                    try:
                        pubsub.close()
                    except Exception:
                        pass

    def stop(self) -> None:
        """Signal background subscriber to stop."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()

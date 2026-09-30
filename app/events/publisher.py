import asyncio
import logging
from typing import Callable, Dict, List, Optional
import redis

from app.alerts.alert_dispatcher import AlertDispatcher, get_alert_dispatcher
from app.core.config import Settings, get_settings
from app.models.events import QueueAlertEvent, QueueUpdatedEvent

logger = logging.getLogger("trievo.events.publisher")


class EventPublisher:
    """
    Publishes domain events (trievo.queue.updated and trievo.queue.alert)
    to Redis Pub/Sub and dispatches alerts through the decoupled AlertDispatcher.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        settings: Optional[Settings] = None,
        alert_dispatcher: Optional[AlertDispatcher] = None,
    ):
        self._redis = redis_client
        self.settings = settings or get_settings()
        self.alert_dispatcher = alert_dispatcher or get_alert_dispatcher()
        self._local_listeners: List[Callable[[QueueUpdatedEvent], None]] = []

    def register_local_listener(self, listener: Callable[[QueueUpdatedEvent], None]) -> None:
        """Register local listener for in-process WebSocket broadcasting."""
        self._local_listeners.append(listener)

    async def publish_queue_updated(self, event: QueueUpdatedEvent) -> bool:
        """
        Publish 'trievo.queue.updated' event to Redis and broadcast to local listeners.
        """
        try:
            payload = event.model_dump_json()
            channel = self.settings.CHANNEL_QUEUE_UPDATED

            # Publish to Redis Pub/Sub in threadpool to prevent blocking async event loop
            await asyncio.to_thread(self._redis.publish, channel, payload)
            logger.info(
                "Published queue.updated: partition=%s, case=%s, action=%s, pos=%d",
                event.partition,
                event.case_id,
                event.action,
                event.queue_position,
            )

            # Broadcast to in-process listeners (e.g. WebSockets)
            for listener in self._local_listeners:
                try:
                    listener(event)
                except Exception as exc:
                    logger.error("Error notifying local listener of queue update: %s", exc)

            return True
        except Exception as exc:
            logger.error("Failed to publish trievo.queue.updated: %s", exc)
            return False

    async def publish_queue_alert(self, alert: QueueAlertEvent) -> Dict[str, bool]:
        """
        Publish 'trievo.queue.alert' via decoupled AlertDispatcher.
        """
        logger.warning(
            "Triggering alert dispatch: id=%s, case=%s, severity=%s, reason=%s",
            alert.alert_id,
            alert.case_id,
            alert.severity,
            alert.reason,
        )
        return await self.alert_dispatcher.dispatch(alert)


_global_publisher: Optional[EventPublisher] = None


def get_event_publisher(redis_client: Optional[redis.Redis] = None) -> EventPublisher:
    global _global_publisher
    if _global_publisher is None:
        from app.queue.queue_store import QueueStore
        store = QueueStore()
        _global_publisher = EventPublisher(redis_client=store.client)
    return _global_publisher

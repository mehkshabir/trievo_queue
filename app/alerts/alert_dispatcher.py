"""
Decoupled Alert Dispatcher.
Dispatches critical and escalation alerts across multiple channels without coupling
queue storage to notification providers.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import redis

from app.core.config import Settings, get_settings
from app.models.events import QueueAlertEvent

logger = logging.getLogger("trievo.queue.alerts")


class AlertChannel(ABC):
    """
    Abstract interface for alert delivery channels.
    Allows new channels (SMS, Pager, Ward Display) to be added without touching queue logic.
    """

    @property
    @abstractmethod
    def channel_name(self) -> str:
        """Unique identifier for this channel."""
        pass

    @abstractmethod
    async def send_alert(self, alert: QueueAlertEvent) -> bool:
        """
        Send alert to destination.
        Returns True if delivered successfully.
        """
        pass


class LoggingAlertChannel(AlertChannel):
    """Logs alerts to standard application logging stream."""

    @property
    def channel_name(self) -> str:
        return "logging"

    async def send_alert(self, alert: QueueAlertEvent) -> bool:
        logger.warning(
            "ALERT [%s] [%s] Case=%s (Position #%d, Wait=%.1fm, Escalation=%d): %s",
            alert.severity,
            alert.partition,
            alert.case_id,
            alert.queue_position,
            alert.wait_time_minutes,
            alert.escalation_level,
            alert.reason,
        )
        return True


class RedisPubSubAlertChannel(AlertChannel):
    """Publishes alert events to Redis Pub/Sub channel 'trievo.queue.alert'."""

    def __init__(self, redis_client: redis.Redis, channel_name_topic: str = "trievo.queue.alert"):
        self._redis = redis_client
        self._topic = channel_name_topic

    @property
    def channel_name(self) -> str:
        return "redis_pubsub"

    async def send_alert(self, alert: QueueAlertEvent) -> bool:
        try:
            payload = alert.model_dump_json()
            # Run blocking redis publish in threadpool to keep async loop unblocked
            await asyncio.to_thread(self._redis.publish, self._topic, payload)
            logger.debug("Published alert %s to Redis channel %s", alert.alert_id, self._topic)
            return True
        except Exception as exc:
            logger.error("Failed to publish alert to Redis channel %s: %s", self._topic, exc)
            return False


class InAppDashboardAlertChannel(AlertChannel):
    """
    In-app alert channel that buffers alerts for dashboard WebSocket consumers.
    """

    def __init__(self, max_buffer_size: int = 100):
        self._alerts_buffer: List[QueueAlertEvent] = []
        self._max_size = max_buffer_size
        self._subscribers: List[asyncio.Queue] = []

    @property
    def channel_name(self) -> str:
        return "dashboard_in_app"

    def subscribe(self) -> asyncio.Queue:
        """Subscribe to live in-app alerts queue."""
        q = asyncio.Queue()
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        """Unsubscribe queue."""
        if q in self._subscribers:
            self._subscribers.remove(q)

    def get_recent_alerts(self, limit: int = 20) -> List[QueueAlertEvent]:
        return list(reversed(self._alerts_buffer[-limit:]))

    async def send_alert(self, alert: QueueAlertEvent) -> bool:
        self._alerts_buffer.append(alert)
        if len(self._alerts_buffer) > self._max_size:
            self._alerts_buffer.pop(0)

        # Broadcast to all connected subscriber queues
        dead_queues = []
        for q in self._subscribers:
            try:
                q.put_nowait(alert)
            except asyncio.QueueFull:
                dead_queues.append(q)

        for dead in dead_queues:
            if dead in self._subscribers:
                self._subscribers.remove(dead)

        return True


class AlertDispatcher:
    """
    Coordinates alert delivery across all registered channels.
    """

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self._channels: Dict[str, AlertChannel] = {}

    def register_channel(self, channel: AlertChannel) -> None:
        """Register a notification delivery channel."""
        self._channels[channel.channel_name] = channel
        logger.info("Registered alert channel: %s", channel.channel_name)

    def get_channel(self, name: str) -> Optional[AlertChannel]:
        return self._channels.get(name)

    async def dispatch(self, alert: QueueAlertEvent) -> Dict[str, bool]:
        """
        Dispatch alert to all registered channels concurrently and safely.
        """
        if not self.settings.CRITICAL_ALERT_ENABLED:
            logger.info("Critical alerts disabled by configuration. Skipping dispatch of %s", alert.alert_id)
            return {}

        results: Dict[str, bool] = {}
        for name, channel in self._channels.items():
            try:
                success = await channel.send_alert(alert)
                results[name] = bool(success)
            except Exception as exc:
                logger.error("Error dispatching alert to channel '%s': %s", name, exc)
                results[name] = False

        return results


# Global singleton instance holder
_global_dispatcher: Optional[AlertDispatcher] = None


def get_alert_dispatcher() -> AlertDispatcher:
    global _global_dispatcher
    if _global_dispatcher is None:
        _global_dispatcher = AlertDispatcher()
        _global_dispatcher.register_channel(LoggingAlertChannel())
    return _global_dispatcher

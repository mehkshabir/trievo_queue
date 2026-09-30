import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional
from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.alerts.alert_dispatcher import (
    InAppDashboardAlertChannel,
    LoggingAlertChannel,
    RedisPubSubAlertChannel,
    get_alert_dispatcher,
)
from app.api.v1.queue_routes import router as queue_router
from app.core.config import Settings, get_settings
from app.events.publisher import EventPublisher
from app.events.subscriber import EventSubscriber
from app.models.events import QueueUpdatedEvent
from app.queue.escalation_monitor import EscalationMonitor
from app.queue.queue_store import QueueStore
from app.websocket.connection_manager import get_ws_manager

# Configure logging
settings = get_settings()
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("trievo.queue.main")

# Background task handles
_subscriber_task: Optional[asyncio.Task] = None
_subscriber: Optional[EventSubscriber] = None
_escalation_monitor: Optional[EscalationMonitor] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.
    Initializes Redis, Alert Channels, Event Subscriber, and APScheduler Escalation Monitor.
    """
    global _subscriber_task, _subscriber, _escalation_monitor

    logger.info("Initializing trievo-queue microservice (env=%s)...", settings.APP_ENV)

    # 1. Initialize Queue Store
    store = QueueStore(settings=settings)

    # 2. Configure Alert Channels
    dispatcher = get_alert_dispatcher()
    dispatcher.register_channel(LoggingAlertChannel())
    dispatcher.register_channel(
        RedisPubSubAlertChannel(redis_client=store.client, channel_name_topic=settings.CHANNEL_QUEUE_ALERT)
    )
    in_app_channel = InAppDashboardAlertChannel()
    dispatcher.register_channel(in_app_channel)

    # 3. Configure Event Publisher
    publisher = EventPublisher(
        redis_client=store.client,
        settings=settings,
        alert_dispatcher=dispatcher,
    )

    # Attach to app.state for request reuse
    app.state.store = store
    app.state.publisher = publisher

    # Wire publisher updates directly to WebSocket Manager for live dashboard streaming
    ws_manager = get_ws_manager()

    def on_queue_updated(evt: QueueUpdatedEvent):
        asyncio.create_task(
            ws_manager.broadcast(
                {
                    "type": "QUEUE_UPDATED",
                    "event": evt.model_dump(),
                },
                partition=evt.partition,
            )
        )

    publisher.register_local_listener(on_queue_updated)

    # 4. Start Background Event Subscriber for 'trievo.verify.completed'
    if settings.ENABLE_BACKGROUND_SUBSCRIBER:
        _subscriber = EventSubscriber(queue_store=store, event_publisher=publisher, settings=settings)
        _subscriber_task = asyncio.create_task(_subscriber.start_listening())
        logger.info("Started background event subscriber for 'trievo.verify.completed'")

    # 5. Start APScheduler Escalation Monitor
    if settings.ENABLE_ESCALATION_MONITOR:
        _escalation_monitor = EscalationMonitor(queue_store=store, event_publisher=publisher, settings=settings)
        _escalation_monitor.start()
        logger.info("Started APScheduler escalation monitor")

    logger.info("trievo-queue microservice startup complete.")

    yield

    # Shutdown sequence
    logger.info("Shutting down trievo-queue microservice...")
    if _subscriber:
        _subscriber.stop()
    if _subscriber_task and not _subscriber_task.done():
        _subscriber_task.cancel()
        try:
            await _subscriber_task
        except asyncio.CancelledError:
            pass

    if _escalation_monitor:
        _escalation_monitor.stop()

    logger.info("trievo-queue microservice shutdown complete.")


# Initialize FastAPI App
app = FastAPI(
    title="trievo-queue Microservice",
    version="1.0.0",
    description=(
        "Real-time, priority-ordered patient queue microservice with arrival-time fairness, "
        "APScheduler stale-case escalation, Redis Sorted Sets, decoupled alert dispatching, "
        "and manual staff intervention audit logging."
    ),
    lifespan=lifespan,
)

# Enable CORS for Trievo frontend dashboard with compliant credentials flag
cors_origins = settings.CORS_ORIGINS
allow_creds = cors_origins != ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=allow_creds,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount API routes
app.include_router(queue_router)


# -----------------------------------------------------------------------------
# Health Check Endpoint
# -----------------------------------------------------------------------------
@app.get(
    "/health",
    tags=["System"],
    summary="Health check endpoint",
    description="Returns the operational status of the service, Redis connectivity, and active queue counts.",
)
async def health_check():
    store = QueueStore(settings=settings)
    redis_ok = store.ping()
    active_partitions = store.get_active_partitions() if redis_ok else []

    status_code = status.HTTP_200_OK if redis_ok else status.HTTP_503_SERVICE_UNAVAILABLE
    content = {
        "status": "HEALTHY" if redis_ok else "DEGRADED",
        "redis_connected": redis_ok,
        "app_env": settings.APP_ENV,
        "active_partitions": active_partitions,
        "escalation_monitor_running": _escalation_monitor is not None and _escalation_monitor._is_running,
        "subscriber_running": _subscriber_task is not None and not _subscriber_task.done(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return JSONResponse(status_code=status_code, content=content)

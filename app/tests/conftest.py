import asyncio
import pytest
import fakeredis
from fastapi.testclient import TestClient

from app.alerts.alert_dispatcher import AlertDispatcher, LoggingAlertChannel
from app.api.v1.queue_routes import get_publisher, get_store
from app.core.config import Settings, get_settings
from app.events.publisher import EventPublisher
from app.events.subscriber import EventSubscriber
from app.main import app
from app.queue.priority_scorer import PriorityScorer
from app.queue.queue_store import QueueStore


@pytest.fixture
def test_settings() -> Settings:
    """Fixture providing isolated settings for tests."""
    return Settings(
        APP_ENV="test",
        LOG_LEVEL="DEBUG",
        REDIS_URL="redis://localhost:6379/15",
        USE_FAKE_REDIS_FALLBACK=True,
        QUEUE_DEFAULT_PARTITION="ed",
        TIER_CRITICAL_WEIGHT=1_000_000.0,
        TIER_URGENT_WEIGHT=500_000.0,
        TIER_SEMI_URGENT_WEIGHT=200_000.0,
        TIER_NON_URGENT_WEIGHT=50_000.0,
        FAIRNESS_WEIGHT_PER_MINUTE=10.0,
        MAX_FAIRNESS_BONUS=40_000.0,
        DEFAULT_WAIT_TIME_MINUTES=15.0,
        QUEUE_STALE_THRESHOLD_MINUTES=45.0,
        STALE_THRESHOLD_CRITICAL_MINUTES=10.0,
        STALE_THRESHOLD_URGENT_MINUTES=30.0,
        STALE_THRESHOLD_SEMI_URGENT_MINUTES=60.0,
        STALE_THRESHOLD_NON_URGENT_MINUTES=120.0,
        MAX_ESCALATION_LEVEL=3,
        ESCALATION_BOOST_SCORE=25_000.0,
        ALERT_TRIGGER_ESCALATION_LEVEL=1,
        CRITICAL_ALERT_ENABLED=True,
        ENABLE_BACKGROUND_SUBSCRIBER=False,
        ENABLE_ESCALATION_MONITOR=False,
    )


@pytest.fixture
def fake_redis():
    """In-memory FakeRedis instance for deterministic unit/integration testing."""
    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server, decode_responses=True)
    yield client
    client.flushall()


@pytest.fixture
def test_store(test_settings, fake_redis) -> QueueStore:
    """QueueStore backed by FakeRedis."""
    return QueueStore(settings=test_settings, redis_client=fake_redis)


@pytest.fixture
def test_scorer(test_settings) -> PriorityScorer:
    """PriorityScorer with test configuration."""
    return PriorityScorer(settings=test_settings)


@pytest.fixture
def test_dispatcher(test_settings) -> AlertDispatcher:
    """AlertDispatcher configured for tests."""
    dispatcher = AlertDispatcher(settings=test_settings)
    dispatcher.register_channel(LoggingAlertChannel())
    return dispatcher


@pytest.fixture
def test_publisher(fake_redis, test_settings, test_dispatcher) -> EventPublisher:
    """EventPublisher configured with FakeRedis and test dispatcher."""
    return EventPublisher(
        redis_client=fake_redis,
        settings=test_settings,
        alert_dispatcher=test_dispatcher,
    )


@pytest.fixture
def test_subscriber(test_store, test_publisher, test_settings) -> EventSubscriber:
    """EventSubscriber configured with test store and publisher."""
    return EventSubscriber(
        queue_store=test_store,
        event_publisher=test_publisher,
        settings=test_settings,
    )


@pytest.fixture
def client(test_store, test_publisher, test_settings) -> TestClient:
    """FastAPI TestClient with overridden dependencies."""
    app.dependency_overrides[get_settings] = lambda: test_settings
    app.dependency_overrides[get_store] = lambda: test_store
    app.dependency_overrides[get_publisher] = lambda: test_publisher

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()

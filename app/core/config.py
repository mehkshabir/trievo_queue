from functools import lru_cache
from typing import Dict, List
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables and .env file.
    All operational thresholds are configurable without code modifications.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Application Info
    APP_NAME: str = "trievo-queue"
    APP_ENV: str = "development"
    LOG_LEVEL: str = "INFO"
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    CORS_ORIGINS: List[str] = ["*"]

    # Redis Connection Settings
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_CONNECT_TIMEOUT: float = 3.0
    REDIS_SOCKET_TIMEOUT: float = 3.0
    USE_FAKE_REDIS_FALLBACK: bool = True

    # Queue Key Management & Partitioning
    QUEUE_KEY_PREFIX: str = "queue:"
    QUEUE_DEFAULT_PARTITION: str = "ed"

    # Priority Tier Base Weights (Deterministic Scoring)
    # Gaps between tiers (300k, 300k, 150k) are strictly larger than MAX_FAIRNESS_BONUS (40k)
    TIER_CRITICAL_WEIGHT: float = 1_000_000.0
    TIER_URGENT_WEIGHT: float = 500_000.0
    TIER_SEMI_URGENT_WEIGHT: float = 200_000.0
    TIER_NON_URGENT_WEIGHT: float = 50_000.0

    # Arrival-Time Fairness Settings
    FAIRNESS_WEIGHT_PER_MINUTE: float = 10.0
    MAX_FAIRNESS_BONUS: float = 40_000.0

    # Wait Time Estimation
    DEFAULT_WAIT_TIME_MINUTES: float = 15.0

    # Stale Case Escalation Monitor
    ENABLE_ESCALATION_MONITOR: bool = True
    QUEUE_ESCALATION_INTERVAL_SECONDS: int = 30
    QUEUE_STALE_THRESHOLD_MINUTES: float = 45.0

    # Tier-specific Stale Thresholds (in minutes)
    STALE_THRESHOLD_CRITICAL_MINUTES: float = 10.0
    STALE_THRESHOLD_URGENT_MINUTES: float = 30.0
    STALE_THRESHOLD_SEMI_URGENT_MINUTES: float = 60.0
    STALE_THRESHOLD_NON_URGENT_MINUTES: float = 120.0

    # Escalation Rules
    MAX_ESCALATION_LEVEL: int = 3
    ESCALATION_BOOST_SCORE: float = 25_000.0
    ALERT_TRIGGER_ESCALATION_LEVEL: int = 1

    # Alert Dispatcher & Events
    CRITICAL_ALERT_ENABLED: bool = True
    ENABLE_BACKGROUND_SUBSCRIBER: bool = True
    CHANNEL_VERIFY_COMPLETED: str = "trievo.verify.completed"
    CHANNEL_QUEUE_UPDATED: str = "trievo.queue.updated"
    CHANNEL_QUEUE_ALERT: str = "trievo.queue.alert"

    def get_tier_weights(self) -> Dict[str, float]:
        """Returns map of normalized priority tier string to base score."""
        return {
            "CRITICAL": self.TIER_CRITICAL_WEIGHT,
            "URGENT": self.TIER_URGENT_WEIGHT,
            "SEMI_URGENT": self.TIER_SEMI_URGENT_WEIGHT,
            "NON_URGENT": self.TIER_NON_URGENT_WEIGHT,
        }

    def get_stale_threshold(self, priority: str) -> float:
        """Returns stale threshold in minutes for a given priority tier."""
        p = priority.upper().replace("-", "_").strip()
        if p == "CRITICAL":
            return self.STALE_THRESHOLD_CRITICAL_MINUTES
        elif p == "URGENT":
            return self.STALE_THRESHOLD_URGENT_MINUTES
        elif p in ("SEMI_URGENT", "SEMIURGENT"):
            return self.STALE_THRESHOLD_SEMI_URGENT_MINUTES
        elif p in ("NON_URGENT", "NONURGENT"):
            return self.STALE_THRESHOLD_NON_URGENT_MINUTES
        return self.QUEUE_STALE_THRESHOLD_MINUTES


@lru_cache()
def get_settings() -> Settings:
    """Cached factory function for settings instance."""
    return Settings()

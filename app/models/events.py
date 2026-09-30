import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union
from pydantic import BaseModel, Field, field_validator, model_validator


def _parse_timestamp(val: Any) -> float:
    """Parse various timestamp representations into a unix timestamp in seconds."""
    if val is None:
        return time.time()
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        val_str = val.strip()
        # Try numeric string
        try:
            return float(val_str)
        except ValueError:
            pass
        # Try ISO format
        try:
            # Handle trailing Z
            if val_str.endswith("Z"):
                val_str = val_str[:-1] + "+00:00"
            dt = datetime.fromisoformat(val_str)
            return dt.timestamp()
        except ValueError as err:
            raise ValueError(f"Invalid timestamp format: '{val}'") from err
    if isinstance(val, datetime):
        return val.timestamp()
    raise ValueError(f"Unsupported timestamp type: {type(val)}")


class VerifyCompletedEvent(BaseModel):
    """
    Event model for 'trievo.verify.completed'.
    Strictly validates input verified priority payload.
    """
    case_id: str = Field(..., min_length=1, description="Unique case identifier")
    priority: str = Field(..., min_length=1, description="Verified triage priority")
    arrival_timestamp: Union[float, int, str, datetime] = Field(
        ...,
        description="Arrival timestamp (unix float or ISO 8601 string)",
    )
    department: Optional[str] = Field(default=None, description="Department partition (e.g. ed, radiology)")
    niche: Optional[str] = Field(default=None, description="Niche queue (e.g. tb:genexpert, dengue:monitoring)")
    patient_id: Optional[str] = Field(default=None, description="Patient identifier")
    reference_id: Optional[str] = Field(default=None, description="Alternative reference identifier")
    priority_weight: Optional[float] = Field(default=None, description="Explicit priority weight override")
    source_event_timestamp: Optional[str] = Field(default=None, description="Timestamp from upstream event")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional clinical or workflow context")

    @field_validator("case_id")
    @classmethod
    def validate_case_id(cls, v: str) -> str:
        s = v.strip()
        if not s:
            raise ValueError("case_id cannot be empty or whitespace")
        return s

    @field_validator("priority")
    @classmethod
    def validate_priority(cls, v: str) -> str:
        s = v.strip().upper()
        if not s:
            raise ValueError("priority cannot be empty")
        from app.models.queue import PriorityTier
        tier = PriorityTier.from_str(s, allow_fallback=False)
        return tier.value

    @field_validator("arrival_timestamp")
    @classmethod
    def validate_arrival_timestamp(cls, v: Any) -> float:
        """Validate and convert arrival timestamp during model construction."""
        return _parse_timestamp(v)

    @property
    def parsed_arrival_timestamp(self) -> float:
        """Returns arrival timestamp in unix seconds."""
        return _parse_timestamp(self.arrival_timestamp)

    @property
    def resolved_partition(self) -> str:
        """
        Determines queue partition key (niche takes precedence if specified,
        otherwise department, defaulting to 'ed').
        """
        part = self.niche or self.department or "ed"
        # Sanitize partition name (lowercase, replace spaces with underscores)
        return part.strip().lower().replace(" ", "_")

    @property
    def resolved_patient_id(self) -> Optional[str]:
        return self.patient_id or self.reference_id


class QueueUpdatedEvent(BaseModel):
    """
    Event model for 'trievo.queue.updated'.
    Published whenever a queue is modified (new case, reorder, escalation, ack).
    """
    event_type: str = "trievo.queue.updated"
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="Event publication timestamp (ISO 8601)",
    )
    partition: str = Field(..., description="Queue partition/namespace")
    case_id: str = Field(..., description="Identifier of the modified or affected case")
    patient_id: Optional[str] = None
    action: str = Field(
        ...,
        description="Action triggering update: INSERT, REORDER, ESCALATION, REMOVE, ACK",
    )
    queue_position: int = Field(..., ge=1, description="1-based live queue position")
    estimated_wait_time_minutes: float = Field(..., ge=0.0, description="Estimated wait time in minutes")
    score: float = Field(..., description="Calculated queue score")
    priority: str = Field(..., description="Original triage priority tier")
    effective_priority: str = Field(..., description="Effective priority tier after escalation or reorder")
    is_stale: bool = Field(default=False, description="Whether case is marked as stale")
    escalation_level: int = Field(default=0, ge=0, description="Escalation count/level")
    total_in_queue: int = Field(..., ge=0, description="Total active cases in this partition")
    summary: Optional[str] = None


class QueueAlertEvent(BaseModel):
    """
    Event model for 'trievo.queue.alert'.
    Published for Critical cases and escalation threshold alerts.
    """
    event_type: str = "trievo.queue.alert"
    alert_id: str = Field(..., description="Unique alert identifier (UUID)")
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="Alert publication timestamp (ISO 8601)",
    )
    partition: str = Field(..., description="Queue partition")
    case_id: str = Field(..., description="Case identifier")
    patient_id: Optional[str] = None
    severity: str = Field(
        ...,
        description="Alert severity: CRITICAL, ESCALATION, HIGH",
    )
    reason: str = Field(..., description="Clinical or operational reason for alert")
    queue_position: int = Field(..., ge=1, description="Live queue position at time of alert")
    wait_time_minutes: float = Field(..., ge=0.0, description="Total waiting time in minutes")
    escalation_level: int = Field(default=0, description="Current escalation level")
    status: str = Field(default="ACTIVE", description="Alert status: ACTIVE or ACKNOWLEDGED")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Contextual clinical metadata")

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class PriorityTier(str, Enum):
    CRITICAL = "CRITICAL"
    URGENT = "URGENT"
    SEMI_URGENT = "SEMI_URGENT"
    NON_URGENT = "NON_URGENT"

    @classmethod
    def from_str(cls, val: str, allow_fallback: bool = False) -> "PriorityTier":
        """Normalize varied priority labels into standard tier."""
        clean = val.strip().upper().replace("-", "_").replace(" ", "_")
        mapping = {
            "CRITICAL": cls.CRITICAL,
            "EMERGENCY": cls.CRITICAL,
            "RESUSCITATION": cls.CRITICAL,
            "P1": cls.CRITICAL,
            "1": cls.CRITICAL,
            "URGENT": cls.URGENT,
            "VERY_URGENT": cls.URGENT,
            "P2": cls.URGENT,
            "2": cls.URGENT,
            "SEMI_URGENT": cls.SEMI_URGENT,
            "SEMIURGENT": cls.SEMI_URGENT,
            "URGENT_LOW": cls.SEMI_URGENT,
            "P3": cls.SEMI_URGENT,
            "3": cls.SEMI_URGENT,
            "NON_URGENT": cls.NON_URGENT,
            "NONURGENT": cls.NON_URGENT,
            "ROUTINE": cls.NON_URGENT,
            "STANDARD": cls.NON_URGENT,
            "P4": cls.NON_URGENT,
            "4": cls.NON_URGENT,
            "5": cls.NON_URGENT,
        }
        if clean in mapping:
            return mapping[clean]
        if allow_fallback:
            return cls.NON_URGENT
        valid_options = ", ".join(["CRITICAL (P1)", "URGENT (P2)", "SEMI_URGENT (P3)", "NON_URGENT (P4)"])
        raise ValueError(f"Unrecognized priority tier '{val}'. Must be one of: {valid_options}")


class QueueItem(BaseModel):
    """
    Detailed representation of a case in a live queue.
    """
    case_id: str
    patient_id: Optional[str] = None
    partition: str
    priority: str
    effective_priority: str
    score: float
    queue_position: int = Field(..., ge=1, description="1-based position in queue")
    estimated_wait_time_minutes: float = Field(..., ge=0.0)
    arrival_timestamp: float
    arrival_time_iso: str
    wait_time_minutes: float = Field(..., ge=0.0)
    is_stale: bool = False
    escalation_level: int = 0
    status: str = "WAITING"
    acknowledged_at: Optional[str] = None
    acknowledged_by: Optional[str] = None
    last_escalated_at: Optional[str] = None
    manual_override: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ReorderRequest(BaseModel):
    """
    Request model for manual queue reordering / priority override.
    """
    case_id: str = Field(..., min_length=1, description="Case to reorder")
    staff_id: str = Field(..., min_length=1, description="Staff user initiating override")
    reason: str = Field(..., min_length=3, description="Clinical or administrative justification")
    partition: Optional[str] = Field(default=None, description="Partition (optional if case exists)")
    new_priority: Optional[str] = Field(default=None, description="New priority override tier")
    target_position: Optional[int] = Field(default=None, ge=1, description="Requested 1-based position")
    score_offset: Optional[float] = Field(default=None, description="Direct score adjustment value")

    from pydantic import model_validator

    @model_validator(mode="after")
    def validate_actionable_override(self) -> "ReorderRequest":
        if self.new_priority is None and self.target_position is None and self.score_offset is None:
            raise ValueError(
                "Reorder request must specify at least one action: 'new_priority', 'target_position', or 'score_offset'."
            )
        if self.new_priority is not None:
            PriorityTier.from_str(self.new_priority)
        return self


class ReorderResponse(BaseModel):
    """
    Response returned after manual queue reorder.
    """
    status: str = "SUCCESS"
    case_id: str
    partition: str
    old_position: int
    new_position: int
    old_priority: str
    new_priority: str
    old_score: float
    new_score: float
    staff_id: str
    reason: str
    audit_id: str
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class AckRequest(BaseModel):
    """
    Request model for acknowledging an alert or queue intervention.
    """
    case_id: str = Field(..., min_length=1, description="Target case identifier")
    staff_id: str = Field(..., min_length=1, description="Staff member acknowledging")
    alert_id: Optional[str] = Field(default=None, description="Optional alert ID being acknowledged")
    partition: Optional[str] = Field(default=None, description="Queue partition")
    note: Optional[str] = Field(default=None, description="Optional clinical/nursing note")
    action_taken: Optional[str] = Field(default="ACKNOWLEDGED", description="Action performed")


class AckResponse(BaseModel):
    """
    Response model for alert/case acknowledgement.
    """
    status: str = "SUCCESS"
    case_id: str
    alert_id: Optional[str] = None
    staff_id: str
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    audit_id: str
    message: str = "Acknowledgement recorded successfully"


class QueueListResponse(BaseModel):
    """
    Response model for querying live queue.
    """
    partition: str
    total: int
    items: List[QueueItem]
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

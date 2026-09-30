import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class AuditLogEntry(BaseModel):
    """
    Audit log record capturing every manual intervention and automated escalation.
    """
    audit_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = Field(..., description="Action type: REORDER, ACK, ESCALATION, INSERT, REMOVE")
    case_id: str = Field(..., description="Target case ID")
    partition: str = Field(..., description="Queue partition key")
    staff_id: str = Field(..., description="Staff ID or 'SYSTEM' for automated jobs")
    old_state: Dict[str, Any] = Field(default_factory=dict, description="State before intervention")
    new_state: Dict[str, Any] = Field(default_factory=dict, description="State after intervention")
    reason: Optional[str] = Field(default=None, description="Reason, note or escalation trigger")
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class AuditLogListResponse(BaseModel):
    """
    Response model for audit log queries.
    """
    total: int
    case_id: Optional[str] = None
    entries: List[AuditLogEntry]

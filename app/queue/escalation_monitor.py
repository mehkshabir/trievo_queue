import asyncio
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import List, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.core.config import Settings, get_settings
from app.events.publisher import EventPublisher
from app.models.audit import AuditLogEntry
from app.models.events import QueueAlertEvent, QueueUpdatedEvent
from app.models.queue import PriorityTier, QueueItem
from app.queue.priority_scorer import PriorityScorer
from app.queue.queue_store import QueueStore

logger = logging.getLogger("trievo.queue.escalation")


class EscalationMonitor:
    """
    Background job managed via APScheduler.
    Periodically scans active queue partitions, detects stale cases exceeding wait thresholds,
    escalates their priority/score, marks them stale, records audit entries, and dispatches alerts.
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
        self.scorer = PriorityScorer(self.settings)
        self.scheduler = AsyncIOScheduler()
        self._is_running = False

    def start(self) -> None:
        """Start the APScheduler periodic background job."""
        if not self.settings.ENABLE_ESCALATION_MONITOR:
            logger.info("Escalation monitor is disabled by configuration.")
            return

        interval = self.settings.QUEUE_ESCALATION_INTERVAL_SECONDS
        self.scheduler.add_job(
            self.check_and_escalate_all,
            "interval",
            seconds=interval,
            id="stale_case_escalation_job",
            replace_existing=True,
        )
        self.scheduler.start()
        self._is_running = True
        logger.info("APScheduler escalation monitor started (interval: %ds).", interval)

    def stop(self) -> None:
        """Stop the scheduler."""
        if self._is_running and self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            self._is_running = False
            logger.info("APScheduler escalation monitor stopped.")

    async def check_and_escalate_all(self) -> List[QueueItem]:
        """
        Scan all active partitions for stale cases and apply escalations.
        Returns list of escalated QueueItems.
        """
        escalated_cases: List[QueueItem] = []
        partitions = self.store.get_active_partitions()

        for partition in partitions:
            try:
                items = await self.check_partition(partition)
                escalated_cases.extend(items)
            except Exception as exc:
                logger.error("Error running escalation check on partition '%s': %s", partition, exc)

        return escalated_cases

    async def check_partition(self, partition: str) -> List[QueueItem]:
        """
        Scan a specific partition for stale cases.
        """
        now = time.time()
        cases = self.store.get_queue(partition, offset=0, limit=500)
        escalated_items: List[QueueItem] = []

        for item in cases:
            # Skip acknowledged or completed cases if any
            if item.status in ("IN_TREATMENT", "DISCHARGED", "COMPLETED"):
                continue

            wait_minutes = self.scorer.calculate_wait_minutes(item.arrival_timestamp, now)
            threshold = self.settings.get_stale_threshold(item.priority)

            # Check if case exceeds threshold
            if wait_minutes >= threshold:
                # Rule: avoid repeatedly escalating the same case indefinitely
                if item.escalation_level >= self.settings.MAX_ESCALATION_LEVEL:
                    # Case already reached max allowed escalation level
                    continue

                # Rule: Cooldown check to avoid multi-escalation within the same check cycle
                # If escalated within the last (threshold / 2) or interval seconds, wait
                if item.last_escalated_at:
                    try:
                        last_esc_dt = datetime.fromisoformat(item.last_escalated_at)
                        elapsed_since_esc = (datetime.now(timezone.utc) - last_esc_dt).total_seconds()
                        if elapsed_since_esc < self.settings.QUEUE_ESCALATION_INTERVAL_SECONDS:
                            continue
                    except Exception:
                        pass

                # Apply escalation
                new_level = item.escalation_level + 1
                new_effective_priority = self._promote_effective_tier(item.effective_priority, new_level)

                # Record old state for audit
                old_state = {
                    "score": item.score,
                    "position": item.queue_position,
                    "escalation_level": item.escalation_level,
                    "effective_priority": item.effective_priority,
                    "is_stale": item.is_stale,
                }

                # Update case in store
                updated_item, _ = self.store.add_or_update_case(
                    partition=partition,
                    case_id=item.case_id,
                    priority=item.priority,
                    arrival_timestamp=item.arrival_timestamp,
                    patient_id=item.patient_id,
                    effective_priority=new_effective_priority,
                    escalation_level=new_level,
                    is_stale=True,
                    manual_override=item.manual_override,
                    metadata=item.metadata,
                    status=item.status,
                )

                # Update timestamp of last escalation in raw hash
                c_key = self.store._case_key(item.case_id)
                raw = self.store.client.get(c_key)
                if raw:
                    import json
                    d = json.loads(raw)
                    d["last_escalated_at"] = datetime.now(timezone.utc).isoformat()
                    self.store.client.set(c_key, json.dumps(d))

                new_state = {
                    "score": updated_item.score,
                    "position": updated_item.queue_position,
                    "escalation_level": updated_item.escalation_level,
                    "effective_priority": updated_item.effective_priority,
                    "is_stale": True,
                }

                reason = (
                    f"Patient waited {wait_minutes:.1f}m exceeding {item.priority} threshold "
                    f"({threshold:.0f}m). Escalation level increased to {new_level}."
                )

                # Record in Audit Log
                audit_entry = AuditLogEntry(
                    event_type="ESCALATION",
                    case_id=item.case_id,
                    partition=partition,
                    staff_id="SYSTEM",
                    old_state=old_state,
                    new_state=new_state,
                    reason=reason,
                )
                self.store.append_audit_log(audit_entry)

                # Publish routine queue updated event
                total = self.store.get_total_in_queue(partition)
                update_event = QueueUpdatedEvent(
                    partition=partition,
                    case_id=item.case_id,
                    patient_id=item.patient_id,
                    action="ESCALATION",
                    queue_position=updated_item.queue_position,
                    estimated_wait_time_minutes=updated_item.estimated_wait_time_minutes,
                    score=updated_item.score,
                    priority=item.priority,
                    effective_priority=updated_item.effective_priority,
                    is_stale=True,
                    escalation_level=new_level,
                    total_in_queue=total,
                    summary=reason,
                )
                await self.publisher.publish_queue_updated(update_event)

                # Publish alert if escalation level reaches alert threshold
                if new_level >= self.settings.ALERT_TRIGGER_ESCALATION_LEVEL:
                    alert = QueueAlertEvent(
                        alert_id=str(uuid.uuid4()),
                        partition=partition,
                        case_id=item.case_id,
                        patient_id=item.patient_id,
                        severity="ESCALATION",
                        reason=f"Stale patient alert: {reason}",
                        queue_position=updated_item.queue_position,
                        wait_time_minutes=wait_minutes,
                        escalation_level=new_level,
                        metadata={
                            "original_priority": item.priority,
                            "effective_priority": updated_item.effective_priority,
                            "threshold_minutes": threshold,
                        },
                    )
                    await self.publisher.publish_queue_alert(alert)

                escalated_items.append(updated_item)
                logger.info("Escalated case %s in %s: %s", item.case_id, partition, reason)

        return escalated_items

    def _promote_effective_tier(self, current_tier_str: str, escalation_level: int) -> str:
        """
        Dynamically promote effective tier for severe delays while respecting clinical constraints.
        NON_URGENT -> SEMI_URGENT -> URGENT.
        Does not automatically make a non-urgent case CRITICAL without clinician order.
        """
        tier = PriorityTier.from_str(current_tier_str)
        if tier == PriorityTier.NON_URGENT and escalation_level >= 1:
            return PriorityTier.SEMI_URGENT.value
        elif tier == PriorityTier.SEMI_URGENT and escalation_level >= 2:
            return PriorityTier.URGENT.value
        return current_tier_str

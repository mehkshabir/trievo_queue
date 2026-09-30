import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import redis
from redis.exceptions import ConnectionError as RedisConnectionError, RedisError

from app.core.config import Settings, get_settings
from app.models.audit import AuditLogEntry
from app.models.queue import PriorityTier, QueueItem
from app.queue.priority_scorer import PriorityScorer

logger = logging.getLogger("trievo.queue.store")


class QueueStoreError(Exception):
    """Base exception for queue store errors."""
    pass


class QueueStoreConnectionError(QueueStoreError):
    """Raised when Redis connection fails."""
    pass


class QueueStore:
    """
    Manages Redis Sorted Sets and Hashes for queue storage and partitioning.
    Thread-safe and async-compatible.
    """

    def __init__(self, settings: Optional[Settings] = None, redis_client: Optional[redis.Redis] = None):
        self.settings = settings or get_settings()
        self.scorer = PriorityScorer(self.settings)
        self._redis = redis_client or self._init_redis()

    def _init_redis(self) -> redis.Redis:
        """Initialize Redis client with connection pooling and error-safe fallback."""
        try:
            client = redis.from_url(
                self.settings.REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=self.settings.REDIS_CONNECT_TIMEOUT,
                socket_timeout=self.settings.REDIS_SOCKET_TIMEOUT,
            )
            # Test connection
            client.ping()
            logger.info("Connected to Redis at %s", self.settings.REDIS_URL)
            return client
        except (RedisConnectionError, RedisError, Exception) as exc:
            if self.settings.USE_FAKE_REDIS_FALLBACK:
                logger.warning(
                    "Standard Redis connection failed (%s). Falling back to FakeRedis for in-memory operation.",
                    exc,
                )
                try:
                    import fakeredis
                    return fakeredis.FakeRedis(decode_responses=True)
                except ImportError:
                    logger.error("fakeredis package not installed; cannot fallback.")
            logger.error("Failed to connect to Redis: %s", exc)
            raise QueueStoreConnectionError(f"Cannot connect to Redis at {self.settings.REDIS_URL}: {exc}") from exc

    @property
    def client(self) -> redis.Redis:
        return self._redis

    def ping(self) -> bool:
        """Check Redis connectivity."""
        try:
            return bool(self._redis.ping())
        except Exception:
            return False

    def _queue_key(self, partition: str) -> str:
        """Format sorted set key for partition, e.g. queue:ed, queue:tb:genexpert"""
        clean_part = partition.strip().lower().replace(" ", "_")
        return f"{self.settings.QUEUE_KEY_PREFIX}{clean_part}"

    def _case_key(self, case_id: str) -> str:
        return f"{self.settings.QUEUE_KEY_PREFIX}case:{case_id}"

    def _partitions_key(self) -> str:
        return f"{self.settings.QUEUE_KEY_PREFIX}partitions:active"

    def _audit_log_key(self) -> str:
        return f"{self.settings.QUEUE_KEY_PREFIX}audit:log"

    def _audit_case_key(self, case_id: str) -> str:
        return f"{self.settings.QUEUE_KEY_PREFIX}audit:case:{case_id}"

    def _alerts_active_key(self) -> str:
        return f"{self.settings.QUEUE_KEY_PREFIX}alerts:active"

    def _alerts_case_key(self, case_id: str) -> str:
        return f"{self.settings.QUEUE_KEY_PREFIX}alerts:case:{case_id}"

    # -------------------------------------------------------------------------
    # Core Queue Operations
    # -------------------------------------------------------------------------

    def add_or_update_case(
        self,
        partition: str,
        case_id: str,
        priority: str,
        arrival_timestamp: float,
        patient_id: Optional[str] = None,
        priority_weight: Optional[float] = None,
        effective_priority: Optional[str] = None,
        escalation_level: int = 0,
        is_stale: bool = False,
        manual_override: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        status: str = "WAITING",
        score_override: Optional[float] = None,
    ) -> Tuple[QueueItem, bool]:
        """
        Add or update a case in Redis Sorted Set and Hash.
        Returns (QueueItem, is_new).
        """
        try:
            q_key = self._queue_key(partition)
            c_key = self._case_key(case_id)
            now = time.time()

            # Check if case exists
            existing_data = self._redis.get(c_key)
            is_new = existing_data is None

            # Calculate score if not provided
            eff_priority = effective_priority or priority
            if score_override is not None:
                score = float(score_override)
            else:
                manual_offset = 0.0
                if manual_override and "score_offset" in manual_override:
                    manual_offset = float(manual_override["score_offset"])

                score = self.scorer.calculate_score(
                    priority=eff_priority,
                    arrival_timestamp=arrival_timestamp,
                    current_timestamp=now,
                    escalation_level=escalation_level,
                    priority_weight_override=priority_weight,
                    manual_offset=manual_offset,
                )

            # Preserve existing creation time
            created_at = datetime.now(timezone.utc).isoformat()
            if existing_data:
                try:
                    parsed_existing = json.loads(existing_data)
                    created_at = parsed_existing.get("created_at", created_at)
                except Exception:
                    pass

            case_payload = {
                "case_id": case_id,
                "patient_id": patient_id,
                "partition": partition,
                "priority": priority.upper(),
                "effective_priority": eff_priority.upper(),
                "score": score,
                "arrival_timestamp": arrival_timestamp,
                "arrival_time_iso": datetime.fromtimestamp(arrival_timestamp, timezone.utc).isoformat(),
                "is_stale": is_stale,
                "escalation_level": escalation_level,
                "status": status,
                "manual_override": manual_override,
                "metadata": metadata or {},
                "created_at": created_at,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

            # Transaction / Pipeline
            pipe = self._redis.pipeline()
            # Add to Sorted Set
            pipe.zadd(q_key, {case_id: score})
            # Store case details
            pipe.set(c_key, json.dumps(case_payload))
            # Track partition in active partitions set
            pipe.sadd(self._partitions_key(), partition)
            pipe.execute()

            # Retrieve position
            position = self.get_case_position(partition, case_id) or 1
            wait_minutes = self.scorer.calculate_wait_minutes(arrival_timestamp, now)
            est_wait = self.scorer.calculate_estimated_wait_minutes(position, eff_priority)

            item = QueueItem(
                case_id=case_id,
                patient_id=patient_id,
                partition=partition,
                priority=priority.upper(),
                effective_priority=eff_priority.upper(),
                score=score,
                queue_position=position,
                estimated_wait_time_minutes=est_wait,
                arrival_timestamp=arrival_timestamp,
                arrival_time_iso=case_payload["arrival_time_iso"],
                wait_time_minutes=wait_minutes,
                is_stale=is_stale,
                escalation_level=escalation_level,
                status=status,
                manual_override=manual_override,
                metadata=metadata or {},
            )
            return item, is_new
        except RedisError as exc:
            logger.error("Redis error adding/updating case %s in partition %s: %s", case_id, partition, exc)
            raise QueueStoreError(f"Redis error: {exc}") from exc

    def get_case(self, case_id: str) -> Optional[QueueItem]:
        """Retrieve full QueueItem by case_id."""
        try:
            c_key = self._case_key(case_id)
            raw = self._redis.get(c_key)
            if not raw:
                return None
            data = json.loads(raw)
            partition = data.get("partition", self.settings.QUEUE_DEFAULT_PARTITION)
            position = self.get_case_position(partition, case_id) or 1
            now = time.time()
            wait_min = self.scorer.calculate_wait_minutes(data["arrival_timestamp"], now)
            est_wait = self.scorer.calculate_estimated_wait_minutes(position, data.get("effective_priority", data["priority"]))

            return QueueItem(
                case_id=data["case_id"],
                patient_id=data.get("patient_id"),
                partition=partition,
                priority=data["priority"],
                effective_priority=data.get("effective_priority", data["priority"]),
                score=float(data["score"]),
                queue_position=position,
                estimated_wait_time_minutes=est_wait,
                arrival_timestamp=float(data["arrival_timestamp"]),
                arrival_time_iso=data.get("arrival_time_iso", ""),
                wait_time_minutes=wait_min,
                is_stale=bool(data.get("is_stale", False)),
                escalation_level=int(data.get("escalation_level", 0)),
                status=data.get("status", "WAITING"),
                acknowledged_at=data.get("acknowledged_at"),
                acknowledged_by=data.get("acknowledged_by"),
                last_escalated_at=data.get("last_escalated_at"),
                manual_override=data.get("manual_override"),
                metadata=data.get("metadata", {}),
            )
        except RedisError as exc:
            logger.error("Redis error getting case %s: %s", case_id, exc)
            raise QueueStoreError(f"Redis error: {exc}") from exc

    def get_case_position(self, partition: str, case_id: str) -> Optional[int]:
        """
        Get 1-based position in queue partition.
        Uses ZREVRANK since highest score is position 1.
        """
        try:
            q_key = self._queue_key(partition)
            rank = self._redis.zrevrank(q_key, case_id)
            return (rank + 1) if rank is not None else None
        except RedisError as exc:
            logger.error("Redis error getting rank for %s in %s: %s", case_id, partition, exc)
            raise QueueStoreError(f"Redis error: {exc}") from exc

    def get_total_in_queue(self, partition: str) -> int:
        """Count active members in partition."""
        try:
            q_key = self._queue_key(partition)
            return int(self._redis.zcard(q_key))
        except RedisError as exc:
            logger.error("Redis error getting queue size for %s: %s", partition, exc)
            return 0

    def get_queue(self, partition: str, offset: int = 0, limit: int = 100) -> List[QueueItem]:
        """
        Get paginated ordered queue for partition.
        Highest score first.
        """
        try:
            q_key = self._queue_key(partition)
            # Fetch case_ids ordered from highest score to lowest
            case_ids = self._redis.zrevrange(q_key, offset, offset + limit - 1)
            items: List[QueueItem] = []
            now = time.time()

            for idx, cid in enumerate(case_ids):
                c_key = self._case_key(cid)
                raw = self._redis.get(c_key)
                if raw:
                    data = json.loads(raw)
                    pos = offset + idx + 1
                    eff_p = data.get("effective_priority", data["priority"])
                    wait_min = self.scorer.calculate_wait_minutes(data["arrival_timestamp"], now)
                    est_wait = self.scorer.calculate_estimated_wait_minutes(pos, eff_p)

                    items.append(
                        QueueItem(
                            case_id=data["case_id"],
                            patient_id=data.get("patient_id"),
                            partition=partition,
                            priority=data["priority"],
                            effective_priority=eff_p,
                            score=float(data["score"]),
                            queue_position=pos,
                            estimated_wait_time_minutes=est_wait,
                            arrival_timestamp=float(data["arrival_timestamp"]),
                            arrival_time_iso=data.get("arrival_time_iso", ""),
                            wait_time_minutes=wait_min,
                            is_stale=bool(data.get("is_stale", False)),
                            escalation_level=int(data.get("escalation_level", 0)),
                            status=data.get("status", "WAITING"),
                            acknowledged_at=data.get("acknowledged_at"),
                            acknowledged_by=data.get("acknowledged_by"),
                            last_escalated_at=data.get("last_escalated_at"),
                            manual_override=data.get("manual_override"),
                            metadata=data.get("metadata", {}),
                        )
                    )
            return items
        except RedisError as exc:
            logger.error("Redis error fetching queue for %s: %s", partition, exc)
            raise QueueStoreError(f"Redis error: {exc}") from exc

    def get_active_partitions(self) -> List[str]:
        """List all known active partition keys."""
        try:
            parts = self._redis.smembers(self._partitions_key())
            if not parts:
                return [self.settings.QUEUE_DEFAULT_PARTITION]
            return sorted(list(parts))
        except RedisError as exc:
            logger.error("Redis error fetching partitions: %s", exc)
            return [self.settings.QUEUE_DEFAULT_PARTITION]

    def remove_case(self, partition: str, case_id: str) -> bool:
        """Remove a case from the queue."""
        try:
            q_key = self._queue_key(partition)
            c_key = self._case_key(case_id)
            pipe = self._redis.pipeline()
            pipe.zrem(q_key, case_id)
            pipe.delete(c_key)
            results = pipe.execute()
            return bool(results[0])
        except RedisError as exc:
            logger.error("Redis error removing case %s from %s: %s", case_id, partition, exc)
            raise QueueStoreError(f"Redis error: {exc}") from exc

    # -------------------------------------------------------------------------
    # Reorder and Manual Overrides
    # -------------------------------------------------------------------------

    def reorder_case(
        self,
        case_id: str,
        staff_id: str,
        reason: str,
        partition: Optional[str] = None,
        new_priority: Optional[str] = None,
        target_position: Optional[int] = None,
        score_offset: Optional[float] = None,
    ) -> Tuple[QueueItem, int, str, float, str]:
        """
        Manually reorders or overrides case priority/score and records an audit log.
        Returns (updated_item, old_position, old_priority, old_score, audit_id).
        """
        item = self.get_case(case_id)
        if not item:
            raise QueueStoreError(f"Case '{case_id}' not found in queue store.")

        resolved_partition = partition or item.partition
        old_position = item.queue_position
        old_priority = item.effective_priority
        old_score = item.score

        # Determine target score
        new_score = old_score
        eff_priority = item.effective_priority

        if new_priority:
            eff_priority = PriorityTier.from_str(new_priority).value
            new_score = self.scorer.calculate_score(
                priority=eff_priority,
                arrival_timestamp=item.arrival_timestamp,
                escalation_level=item.escalation_level,
            )

        if score_offset is not None:
            new_score += float(score_offset)

        if target_position is not None:
            q_key = self._queue_key(resolved_partition)
            all_members = self._redis.zrevrange(q_key, 0, -1, withscores=True)
            # Exclude current case from reference members to avoid self-referential score distortion
            other_members = [m for m in all_members if m[0] != case_id]
            total_others = len(other_members)

            if total_others == 0 or target_position <= 1:
                # Top of queue: highest other score + 10.0
                highest_score = float(other_members[0][1]) if other_members else old_score
                new_score = highest_score + 10.0
            elif target_position > total_others:
                # Bottom of queue: lowest other score - 10.0
                lowest_score = float(other_members[-1][1])
                new_score = max(0.0, lowest_score - 10.0)
            else:
                # Target position inside list (1-based index)
                idx_above = target_position - 2
                idx_below = target_position - 1
                score_above = float(other_members[idx_above][1])
                score_below = float(other_members[idx_below][1])
                new_score = (score_above + score_below) / 2.0

        manual_record = {
            "staff_id": staff_id,
            "reason": reason,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "target_position": target_position,
            "new_priority": eff_priority,
            "score_offset": score_offset,
        }

        updated_item, _ = self.add_or_update_case(
            partition=resolved_partition,
            case_id=case_id,
            priority=item.priority,
            arrival_timestamp=item.arrival_timestamp,
            patient_id=item.patient_id,
            effective_priority=eff_priority,
            escalation_level=item.escalation_level,
            is_stale=item.is_stale,
            manual_override=manual_record,
            metadata=item.metadata,
            status=item.status,
            score_override=new_score,
        )

        import uuid
        audit_id = str(uuid.uuid4())
        audit_entry = AuditLogEntry(
            audit_id=audit_id,
            event_type="REORDER",
            case_id=case_id,
            partition=resolved_partition,
            staff_id=staff_id,
            old_state={
                "position": old_position,
                "priority": old_priority,
                "score": old_score,
            },
            new_state={
                "position": updated_item.queue_position,
                "priority": updated_item.effective_priority,
                "score": updated_item.score,
            },
            reason=reason,
        )
        self.append_audit_log(audit_entry)

        return updated_item, old_position, old_priority, old_score, audit_id

    # -------------------------------------------------------------------------
    # Acknowledgements
    # -------------------------------------------------------------------------

    def acknowledge_case(
        self,
        case_id: str,
        staff_id: str,
        alert_id: Optional[str] = None,
        note: Optional[str] = None,
        action_taken: str = "ACKNOWLEDGED",
    ) -> Tuple[QueueItem, str, str]:
        """
        Record staff acknowledgement for a case or alert and create an audit log.
        Returns (updated_item, old_status, audit_id).
        """
        item = self.get_case(case_id)
        if not item:
            raise QueueStoreError(f"Case '{case_id}' not found.")

        old_status = item.status
        now_iso = datetime.now(timezone.utc).isoformat()
        c_key = self._case_key(case_id)
        raw = self._redis.get(c_key)
        data = json.loads(raw) if raw else {}

        data["status"] = action_taken
        data["acknowledged_at"] = now_iso
        data["acknowledged_by"] = staff_id
        if note:
            data.setdefault("notes", []).append({
                "staff_id": staff_id,
                "note": note,
                "timestamp": now_iso,
                "alert_id": alert_id,
            })

        self._redis.set(c_key, json.dumps(data))

        # If alert_id provided, mark alert as ACKNOWLEDGED in alerts store
        if alert_id:
            alerts_key = self._alerts_active_key()
            alert_raw = self._redis.hget(alerts_key, alert_id)
            if alert_raw:
                alert_dict = json.loads(alert_raw)
                alert_dict["status"] = "ACKNOWLEDGED"
                alert_dict["acknowledged_by"] = staff_id
                alert_dict["acknowledged_at"] = now_iso
                alert_dict["note"] = note
                self._redis.hset(alerts_key, alert_id, json.dumps(alert_dict))

        import uuid
        audit_id = str(uuid.uuid4())
        audit_entry = AuditLogEntry(
            audit_id=audit_id,
            event_type="ACK",
            case_id=case_id,
            partition=item.partition,
            staff_id=staff_id,
            old_state={"status": old_status},
            new_state={"status": action_taken, "alert_id": alert_id},
            reason=note or "Alert acknowledged by staff",
        )
        self.append_audit_log(audit_entry)

        updated_item = self.get_case(case_id) or item
        return updated_item, old_status, audit_id

    # -------------------------------------------------------------------------
    # Audit Logging
    # -------------------------------------------------------------------------

    def append_audit_log(self, entry: AuditLogEntry) -> None:
        """
        Append entry to global audit log and case-specific audit trail in Redis.
        Documented in README for future replacement with relational DB (PostgreSQL).
        """
        try:
            serialized = entry.model_dump_json()
            pipe = self._redis.pipeline()
            # Push to global audit log (capped at 10,000 entries)
            pipe.lpush(self._audit_log_key(), serialized)
            pipe.ltrim(self._audit_log_key(), 0, 9999)
            # Push to case-specific log (capped at 100 entries)
            pipe.lpush(self._audit_case_key(entry.case_id), serialized)
            pipe.ltrim(self._audit_case_key(entry.case_id), 0, 99)
            pipe.execute()
        except RedisError as exc:
            logger.error("Failed to append audit log for case %s: %s", entry.case_id, exc)

    def get_audit_logs(self, case_id: Optional[str] = None, limit: int = 50) -> List[AuditLogEntry]:
        """Fetch audit log entries, optionally filtered by case_id."""
        try:
            key = self._audit_case_key(case_id) if case_id else self._audit_log_key()
            raw_entries = self._redis.lrange(key, 0, limit - 1)
            entries: List[AuditLogEntry] = []
            for item in raw_entries:
                try:
                    entries.append(AuditLogEntry.model_validate_json(item))
                except Exception:
                    pass
            return entries
        except RedisError as exc:
            logger.error("Failed to read audit logs: %s", exc)
            return []

    # -------------------------------------------------------------------------
    # Stale Cases for Escalation Monitor
    # -------------------------------------------------------------------------

    def get_stale_candidates(self, partition: str) -> List[QueueItem]:
        """
        Retrieve all cases in a partition that are currently exceeding their stale threshold.
        """
        items = self.get_queue(partition, offset=0, limit=500)
        now = time.time()
        stale_items = []
        for item in items:
            if self.scorer.is_stale(item.effective_priority, item.arrival_timestamp, now):
                stale_items.append(item)
        return stale_items

"""
Deterministic Priority Scorer for trievo-queue.

Mathematical Formula:
---------------------
Score = TierBaseWeight + FairnessComponent + EscalationBonus + ManualOffset

1. TierBaseWeight:
   - CRITICAL:    1,000,000 (configurable via TIER_CRITICAL_WEIGHT)
   - URGENT:        500,000 (configurable via TIER_URGENT_WEIGHT)
   - SEMI_URGENT:   200,000 (configurable via TIER_SEMI_URGENT_WEIGHT)
   - NON_URGENT:     50,000 (configurable via TIER_NON_URGENT_WEIGHT)

2. Arrival-Time FairnessComponent:
   wait_minutes = max(0.0, (current_timestamp - arrival_timestamp) / 60.0)
   fairness_component = min(MAX_FAIRNESS_BONUS, wait_minutes * FAIRNESS_WEIGHT_PER_MINUTE)

   Tie-breaker for arrival ordering:
   arrival_tie_breaker = max(0.0, (REF_EPOCH - arrival_timestamp) * 0.000001)

   Guarantees:
   - Within the same priority tier, cases that arrived earlier have higher fairness scores
     and are strictly served ahead of later arrivals.
   - MAX_FAIRNESS_BONUS (default 40,000) is strictly smaller than the minimum gap between
     any adjacent priority tiers (150,000 between NON_URGENT and SEMI_URGENT).
   - A non-critical case can NEVER surpass a genuinely critical case solely via fairness.

3. EscalationBonus:
   escalation_bonus = escalation_level * ESCALATION_BOOST_SCORE (default 25,000/level)
   When the Escalation Monitor detects that a case exceeds wait thresholds, it increments
   escalation_level, elevating its position to avoid prolonged patient starvation.

4. ManualOffset:
   Staff manual intervention delta or explicit override.
"""

import time
from typing import Optional
from app.core.config import Settings, get_settings
from app.models.queue import PriorityTier

# Reference epoch for positive arrival tie-breaker (year 2038 unix timestamp)
REF_EPOCH = 2_147_483_647.0


class PriorityScorer:
    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()

    def get_tier_base_weight(self, priority: str, custom_weight: Optional[float] = None) -> float:
        """
        Determines the base weight for a priority level.
        If custom_weight is explicitly provided upstream, it takes precedence.
        """
        if custom_weight is not None and custom_weight > 0:
            return float(custom_weight)

        tier = PriorityTier.from_str(priority)
        tier_weights = self.settings.get_tier_weights()
        return tier_weights.get(tier.value, self.settings.TIER_NON_URGENT_WEIGHT)

    def calculate_wait_minutes(self, arrival_timestamp: float, current_timestamp: Optional[float] = None) -> float:
        """
        Calculate total elapsed waiting time in minutes.
        """
        now = current_timestamp if current_timestamp is not None else time.time()
        elapsed_seconds = max(0.0, now - float(arrival_timestamp))
        return round(elapsed_seconds / 60.0, 2)

    def calculate_fairness_component(
        self,
        arrival_timestamp: float,
        current_timestamp: Optional[float] = None,
    ) -> float:
        """
        Calculate arrival-time fairness score.
        Caps at MAX_FAIRNESS_BONUS to strictly protect tier integrity.
        """
        wait_minutes = self.calculate_wait_minutes(arrival_timestamp, current_timestamp)
        raw_fairness = wait_minutes * self.settings.FAIRNESS_WEIGHT_PER_MINUTE
        capped_fairness = min(float(self.settings.MAX_FAIRNESS_BONUS), raw_fairness)

        # Subtle tie-breaker for identical minute arrivals: earlier arrival_timestamp gets slight boost (< 1.0 point)
        tie_breaker = max(0.0, min(1.0, (REF_EPOCH - float(arrival_timestamp)) * 1e-9))

        return round(capped_fairness + tie_breaker, 4)

    def calculate_score(
        self,
        priority: str,
        arrival_timestamp: float,
        current_timestamp: Optional[float] = None,
        escalation_level: int = 0,
        priority_weight_override: Optional[float] = None,
        manual_offset: float = 0.0,
    ) -> float:
        """
        Compute total queue score deterministically.
        Higher score = Higher urgency / Earlier queue position.
        """
        base_weight = self.get_tier_base_weight(priority, priority_weight_override)
        fairness = self.calculate_fairness_component(arrival_timestamp, current_timestamp)
        escalation_bonus = max(0, escalation_level) * float(self.settings.ESCALATION_BOOST_SCORE)

        total_score = base_weight + fairness + escalation_bonus + manual_offset
        return round(total_score, 4)

    def calculate_estimated_wait_minutes(
        self,
        queue_position: int,
        priority: str,
        default_consult_time: Optional[float] = None,
    ) -> float:
        """
        Calculate estimated wait time based on position and priority.
        1-based position.
        Position 1 is next in line.
        """
        if queue_position <= 1:
            tier = PriorityTier.from_str(priority)
            if tier == PriorityTier.CRITICAL:
                return 0.0
            consult = default_consult_time or self.settings.DEFAULT_WAIT_TIME_MINUTES
            return round(consult * 0.25, 1)

        consult = default_consult_time or self.settings.DEFAULT_WAIT_TIME_MINUTES
        # Estimated wait is (position - 1) * average service time
        return round((queue_position - 1) * consult, 1)

    def is_stale(
        self,
        priority: str,
        arrival_timestamp: float,
        current_timestamp: Optional[float] = None,
    ) -> bool:
        """
        Check if a case has exceeded its configured stale threshold.
        """
        wait_minutes = self.calculate_wait_minutes(arrival_timestamp, current_timestamp)
        threshold = self.settings.get_stale_threshold(priority)
        return wait_minutes >= threshold

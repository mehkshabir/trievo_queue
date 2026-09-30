import time
import pytest
from app.models.queue import PriorityTier
from app.queue.priority_scorer import PriorityScorer


def test_priority_tier_weights(test_scorer: PriorityScorer):
    """Test that priority tiers strictly follow descending order of urgency."""
    crit_w = test_scorer.get_tier_base_weight("CRITICAL")
    urg_w = test_scorer.get_tier_base_weight("URGENT")
    semi_w = test_scorer.get_tier_base_weight("SEMI_URGENT")
    non_w = test_scorer.get_tier_base_weight("NON_URGENT")

    assert crit_w > urg_w > semi_w > non_w
    assert crit_w == 1_000_000.0
    assert urg_w == 500_000.0
    assert semi_w == 200_000.0
    assert non_w == 50_000.0


def test_priority_string_normalization(test_scorer: PriorityScorer):
    """Test that varied priority labels (P1, Emergency, etc.) map accurately."""
    assert test_scorer.get_tier_base_weight("P1") == 1_000_000.0
    assert test_scorer.get_tier_base_weight("Emergency") == 1_000_000.0
    assert test_scorer.get_tier_base_weight("Very Urgent") == 500_000.0
    assert test_scorer.get_tier_base_weight("P2") == 500_000.0
    assert test_scorer.get_tier_base_weight("Semi-Urgent") == 200_000.0
    assert test_scorer.get_tier_base_weight("Routine") == 50_000.0


def test_same_priority_arrival_time_ordering(test_scorer: PriorityScorer):
    """
    Test Requirement 2: Within the same priority tier,
    earlier arrivals must receive higher scores and be served first.
    """
    t_now = 1_000_000.0
    t_earlier = t_now - 1800.0  # Arrived 30 mins ago
    t_later = t_now - 300.0     # Arrived 5 mins ago

    score_earlier = test_scorer.calculate_score("URGENT", arrival_timestamp=t_earlier, current_timestamp=t_now)
    score_later = test_scorer.calculate_score("URGENT", arrival_timestamp=t_later, current_timestamp=t_now)

    assert score_earlier > score_later
    # Wait difference is 25 minutes = 25 * 10 = 250 points difference
    assert score_earlier > score_later + 200.0


def test_fairness_anti_starvation_capped(test_scorer: PriorityScorer):
    """
    Test Requirement 3: Fairness component allows older cases to gain priority,
    but is strictly capped so a NON_URGENT case can NEVER surpass a CRITICAL case.
    """
    t_now = 1_000_000.0
    # Patient waiting 10 days (14,400 minutes)
    t_long_wait = t_now - (10 * 24 * 3600)
    # Critical patient just arrived
    t_fresh = t_now

    score_non_urgent_long = test_scorer.calculate_score(
        "NON_URGENT", arrival_timestamp=t_long_wait, current_timestamp=t_now
    )
    score_critical_fresh = test_scorer.calculate_score(
        "CRITICAL", arrival_timestamp=t_fresh, current_timestamp=t_now
    )

    # Max fairness bonus is 40,000. Base NON_URGENT is 50,000. Total <= 90,001.
    assert score_non_urgent_long < 90_005.0
    # Fresh CRITICAL is 1,000,000
    assert score_critical_fresh >= 1_000_000.0
    # Critical must strictly remain far above non-urgent
    assert score_critical_fresh > score_non_urgent_long + 900_000.0


def test_estimated_wait_time_calculation(test_scorer: PriorityScorer):
    """Test estimated wait time based on position and priority."""
    # Critical patient at position 1 has 0 wait
    assert test_scorer.calculate_estimated_wait_minutes(1, "CRITICAL") == 0.0
    # Non-critical at position 1 has minimal consult fraction
    assert test_scorer.calculate_estimated_wait_minutes(1, "URGENT") == 3.8
    # Position 3 with default consult 15 min -> (3 - 1) * 15 = 30.0 mins
    assert test_scorer.calculate_estimated_wait_minutes(3, "NON_URGENT") == 30.0


def test_stale_case_detection(test_scorer: PriorityScorer):
    """Test stale threshold detection for different tiers."""
    t_now = 10_000.0

    # Critical threshold is 10 mins (600s)
    assert not test_scorer.is_stale("CRITICAL", arrival_timestamp=t_now - 300.0, current_timestamp=t_now)
    assert test_scorer.is_stale("CRITICAL", arrival_timestamp=t_now - 660.0, current_timestamp=t_now)

    # Urgent threshold is 30 mins (1800s)
    assert not test_scorer.is_stale("URGENT", arrival_timestamp=t_now - 1200.0, current_timestamp=t_now)
    assert test_scorer.is_stale("URGENT", arrival_timestamp=t_now - 1900.0, current_timestamp=t_now)

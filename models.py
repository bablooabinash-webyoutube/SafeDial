"""
CELL 2: models.py — Explicit State Machines for Agents and Calls

WHY THIS EXISTS:
Telecom webhooks arrive twice, arrive late, or arrive backwards (e.g., COMPLETED
arrives before RINGING). Ranking call states strictly forward (1 -> 6) lets us:
1. Fill in skipped intermediate steps automatically.
2. Ignore older/duplicate events safely—even if the provider sent no event_id.
"""

from dataclasses import dataclass
from typing import Dict, List, Set

CALL_RANK: Dict[str, int] = {
    "RESERVED": 1,
    "INITIATED": 2,
    "RINGING": 3,
    "ANSWERED": 4,
    "CONNECTED": 5,
    "COMPLETED": 6,
}

TERMINAL_CALL_STATES: Set[str] = {"COMPLETED", "FAILED", "CANCELLED"}

AGENT_TRANSITIONS: Dict[str, Set[str]] = {
    "OFFLINE":   {"AVAILABLE"},
    "AVAILABLE": {"PAUSED", "RESERVED", "OFFLINE"},
    "PAUSED":    {"AVAILABLE", "OFFLINE"},
    "RESERVED":  {"DIALING", "AVAILABLE", "PAUSED", "OFFLINE"},
    "DIALING":   {"CONNECTED", "AVAILABLE", "PAUSED", "OFFLINE"},
    "CONNECTED": {"WRAP_UP", "OFFLINE"},
    "WRAP_UP":   {"AVAILABLE", "PAUSED", "OFFLINE"},
}


def can_agent_transition(current: str, target: str) -> bool:
    return target in AGENT_TRANSITIONS.get(current, set())


def plan_call_transition(current_state: str, target_state: str) -> List[str]:
    """Returns ordered forward steps to apply, or [] if the event is stale/ignored."""
    if current_state in TERMINAL_CALL_STATES:
        return []
    if target_state in ("FAILED", "CANCELLED"):
        return [target_state]
    if current_state not in CALL_RANK or target_state not in CALL_RANK:
        return []

    curr_rank = CALL_RANK[current_state]
    targ_rank = CALL_RANK[target_state]
    if targ_rank <= curr_rank:
        return []

    ordered_states = sorted(CALL_RANK.keys(), key=lambda s: CALL_RANK[s])
    return [s for s in ordered_states if curr_rank < CALL_RANK[s] <= targ_rank]


@dataclass(frozen=True)
class CampaignSnapshot:
    timestamp: float
    campaign_paused: bool
    kill_switch: bool
    current_hour: int
    available_agents: int
    pending_borrowers: int
    healthy_provider_capacity: int
    all_providers_down: bool
    any_provider_degraded: bool


@dataclass(frozen=True)
class PacingRequest:
    requested_calls: int
    strategy: str

"""
CELL 4: safety.py — Non-Bypassable Safety Controller & Unforgeable DialPermit

WHY THIS EXISTS:
Enforces regulatory calling hours (08:00-19:00), kill switch, snapshot freshness,
and provider capacity. Mints a single-use `DialPermit` using a module-private
`_PERMIT_SECRET` so no other file can forge permission to dial.
"""

import sqlite3
import uuid
from dataclasses import dataclass
from typing import Optional, Tuple

from models import CampaignSnapshot, PacingRequest

_PERMIT_SECRET = object()


@dataclass(frozen=True)
class DialPermit:
    permit_id: str
    approved_calls: int
    expires_at: float
    reason_code: str
    _secret: object

    def __post_init__(self):
        if self._secret is not _PERMIT_SECRET:
            raise TypeError("DialPermit can ONLY be created by SafetyController!")


class SafetyController:
    CALLING_START_HOUR = 8
    CALLING_END_HOUR = 19
    MAX_SNAPSHOT_AGE_SEC = 1.0
    PERMIT_TTL_SEC = 1.0

    def evaluate(
        self,
        conn: sqlite3.Connection,
        snapshot: CampaignSnapshot,
        request: PacingRequest,
        now: float,
    ) -> Tuple[str, Optional[DialPermit]]:
        requested = request.requested_calls
        action = "APPROVE"
        reason = "WITHIN_LIMITS"
        approved = requested

        if (now - snapshot.timestamp) > self.MAX_SNAPSHOT_AGE_SEC:
            action, reason, approved = "REJECT", "STALE_SNAPSHOT", 0
        elif snapshot.kill_switch:
            action, reason, approved = "REJECT", "KILL_SWITCH", 0
        elif snapshot.campaign_paused:
            action, reason, approved = "REJECT", "CAMPAIGN_PAUSED", 0
        elif not (self.CALLING_START_HOUR <= snapshot.current_hour < self.CALLING_END_HOUR):
            action, reason, approved = "REJECT", "OUTSIDE_CALLING_HOURS", 0
        elif snapshot.all_providers_down:
            action, reason, approved = "REJECT", "NO_HEALTHY_PROVIDER", 0
        else:
            hard_cap = min(snapshot.available_agents, snapshot.healthy_provider_capacity)
            if requested > hard_cap:
                approved = hard_cap
                action = "REDUCE" if approved > 0 else "REJECT"
                reason = "AGENT_LIMIT" if snapshot.available_agents < snapshot.healthy_provider_capacity else "PROVIDER_CAPACITY"
            elif snapshot.any_provider_degraded and approved > 0:
                action = "REDUCE"
                reason = "PROVIDER_DEGRADED"

        permit_id = str(uuid.uuid4()) if approved > 0 else None
        expires_at = now + self.PERMIT_TTL_SEC

        conn.execute(
            """
            INSERT INTO safety_decisions
                (permit_id, created_at, expires_at, requested, approved, action, reason_code, consumed)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (permit_id, now, expires_at, requested, approved, action, reason),
        )

        if approved <= 0:
            return action, None

        return action, DialPermit(
            permit_id=permit_id,
            approved_calls=approved,
            expires_at=expires_at,
            reason_code=reason,
            _secret=_PERMIT_SECRET,
        )

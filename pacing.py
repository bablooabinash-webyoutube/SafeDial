"""
CELL 3: pacing.py — Structurally Isolated Progressive Pacing Engine

WHY THIS EXISTS:
Look at the imports: ONLY `models`. It does NOT import `db`, `providers`, or `engine`.
Pacing has zero code path to dial a phone or bypass the Safety Controller.
"""

from models import CampaignSnapshot, PacingRequest


class ProgressivePacingEngine:
    """Progressive Mode: 1 free agent -> request at most 1 outbound call."""

    def compute_request(self, snapshot: CampaignSnapshot) -> PacingRequest:
        desired = min(snapshot.available_agents, snapshot.pending_borrowers)
        return PacingRequest(requested_calls=max(0, desired), strategy="PROGRESSIVE")

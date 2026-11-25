"""
CELL 6: engine.py — Call Allocator, Event Handler, Recovery Sweeper & Dispatcher

WHY THIS EXISTS:
1. CallAllocator: Reserves agent + borrower in SQLite BEFORE calling any provider.
2. EventHandler: Applies duplicate/out-of-order webhooks atomically in 1 transaction.
3. RecoverySweeper: Leaderless cleanup for expired leases, dead heartbeats, and wrap-ups.
"""

import sqlite3
import uuid
from typing import List, Optional, Tuple

from models import CampaignSnapshot, plan_call_transition
from pacing import ProgressivePacingEngine
from providers import ProviderRouter
from safety import DialPermit, SafetyController

LEASE_SECONDS = 10.0
HEARTBEAT_TTL_SECONDS = 5.0
WRAP_UP_SECONDS = 30.0
RING_TIMEOUT_SECONDS = 45.0


class CallAllocator:
    def reserve_batch(self, conn: sqlite3.Connection, permit: DialPermit, now: float) -> List[dict]:
        if now > permit.expires_at:
            raise ValueError("Permit has expired!")

        reservations = []
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                "UPDATE safety_decisions SET consumed = 1 WHERE permit_id = ? AND consumed = 0 AND expires_at >= ?",
                (permit.permit_id, now),
            )
            if cur.rowcount != 1:
                conn.execute("ROLLBACK")
                raise ValueError("Permit already used or expired!")

            agents = conn.execute(
                "SELECT id FROM agents WHERE state = 'AVAILABLE' AND current_call_id IS NULL AND last_heartbeat >= ? LIMIT ?",
                (now - HEARTBEAT_TTL_SECONDS, permit.approved_calls),
            ).fetchall()

            borrowers = conn.execute(
                "SELECT id, phone FROM borrowers WHERE status = 'PENDING' AND active_call_id IS NULL AND attempts < max_attempts AND next_attempt_at <= ? ORDER BY priority ASC, next_attempt_at ASC LIMIT ?",
                (now, permit.approved_calls),
            ).fetchall()

            for agent_row, borrower_row in zip(agents, borrowers):
                call_id = f"call-{uuid.uuid4().hex[:8]}"
                idem_key = f"idem-{uuid.uuid4().hex[:10]}"
                agent_id, borrower_id = agent_row["id"], borrower_row["id"]

                conn.execute("SAVEPOINT pair_claim")
                a_cur = conn.execute(
                    "UPDATE agents SET state = 'RESERVED', current_call_id = ?, version = version + 1 WHERE id = ? AND state = 'AVAILABLE' AND current_call_id IS NULL",
                    (call_id, agent_id),
                )
                b_cur = conn.execute(
                    "UPDATE borrowers SET active_call_id = ? WHERE id = ? AND status = 'PENDING' AND active_call_id IS NULL",
                    (call_id, borrower_id),
                )

                if a_cur.rowcount == 1 and b_cur.rowcount == 1:
                    conn.execute(
                        "INSERT INTO calls (id, idempotency_key, agent_id, borrower_id, state, lease_expires_at, updated_at) VALUES (?, ?, ?, ?, 'RESERVED', ?, ?)",
                        (call_id, idem_key, agent_id, borrower_id, now + LEASE_SECONDS, now),
                    )
                    conn.execute("RELEASE SAVEPOINT pair_claim")
                    reservations.append({
                        "call_id": call_id,
                        "idempotency_key": idem_key,
                        "agent_id": agent_id,
                        "borrower_id": borrower_id,
                        "phone": borrower_row["phone"],
                    })
                else:
                    conn.execute("ROLLBACK TO SAVEPOINT pair_claim")
                    conn.execute("RELEASE SAVEPOINT pair_claim")

            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        return reservations

    def place_reserved_call(
        self, conn: sqlite3.Connection, router: ProviderRouter, res: dict, now: float,
        simulate_crash_before_db_update: bool = False
    ) -> str:
        provider_name, provider_call_id, status = router.place_call_safely(
            res["call_id"], res["phone"], res["idempotency_key"], now
        )
        if simulate_crash_before_db_update:
            return "CRASHED_BEFORE_RECORDING"

        if status == "PLACED":
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE calls SET state = 'INITIATED', provider_name = ?, provider_call_id = ?, updated_at = ?, version = version + 1 WHERE id = ? AND state = 'RESERVED'",
                (provider_name, provider_call_id, now, res["call_id"]),
            )
            conn.execute(
                "UPDATE agents SET state = 'DIALING', version = version + 1 WHERE id = ? AND current_call_id = ? AND state = 'RESERVED'",
                (res["agent_id"], res["call_id"]),
            )
            conn.execute("COMMIT")
            return "INITIATED"
        elif status == "AMBIGUOUS":
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE calls SET provider_name = ?, updated_at = ? WHERE id = ?", (provider_name, now, res["call_id"]))
            conn.execute("COMMIT")
            return "AMBIGUOUS"
        else:
            EventHandler().apply_event(conn, "ROUTER", f"rej-{res['call_id']}", res["call_id"], "FAILED", now, "PROVIDER_REJECTED")
            return "REJECTED"


class EventHandler:
    def apply_event(
        self, conn: sqlite3.Connection, provider_name: str, event_id: Optional[str],
        call_id: str, event_type: str, now: float, failure_reason: Optional[str] = None
    ) -> str:
        conn.execute("BEGIN IMMEDIATE")
        try:
            if event_id is not None:
                if conn.execute("SELECT 1 FROM provider_events WHERE provider_name = ? AND event_id = ?", (provider_name, event_id)).fetchone():
                    conn.execute("COMMIT")
                    return "DUPLICATE"

            call = conn.execute("SELECT * FROM calls WHERE id = ?", (call_id,)).fetchone()
            if not call:
                conn.execute("INSERT INTO provider_events (provider_name, event_id, call_id, event_type, outcome, received_at) VALUES (?, ?, ?, ?, 'UNKNOWN_CALL', ?)", (provider_name, event_id, call_id, event_type, now))
                conn.execute("COMMIT")
                return "UNKNOWN_CALL"

            steps = plan_call_transition(call["state"], event_type)
            if not steps:
                conn.execute("INSERT INTO provider_events (provider_name, event_id, call_id, event_type, outcome, received_at) VALUES (?, ?, ?, ?, 'STALE', ?)", (provider_name, event_id, call_id, event_type, now))
                conn.execute("COMMIT")
                return "STALE"

            for step in steps:
                conn.execute(
                    "UPDATE calls SET state = ?, updated_at = ?, version = version + 1, failure_reason = COALESCE(?, failure_reason) WHERE id = ?",
                    (step, now, failure_reason if step in ("FAILED", "CANCELLED") else None, call_id),
                )
                self._sync_agent_and_borrower(conn, call, step, failure_reason, now)

            conn.execute("INSERT INTO provider_events (provider_name, event_id, call_id, event_type, outcome, received_at) VALUES (?, ?, ?, ?, 'APPLIED', ?)", (provider_name, event_id, call_id, event_type, now))
            conn.execute("COMMIT")
            return "APPLIED"
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise

    def _sync_agent_and_borrower(self, conn: sqlite3.Connection, call: sqlite3.Row, step: str, failure_reason: Optional[str], now: float):
        agent_id, borrower_id, call_id = call["agent_id"], call["borrower_id"], call["id"]

        if step in ("INITIATED", "RINGING"):
            conn.execute("UPDATE agents SET state = 'DIALING', version = version + 1 WHERE id = ? AND current_call_id = ? AND state = 'RESERVED'", (agent_id, call_id))
        elif step in ("ANSWERED", "CONNECTED"):
            conn.execute("UPDATE agents SET state = 'CONNECTED', version = version + 1 WHERE id = ? AND current_call_id = ? AND state IN ('RESERVED', 'DIALING')", (agent_id, call_id))
            conn.execute("UPDATE borrowers SET status = 'IN_CALL' WHERE id = ? AND active_call_id = ?", (borrower_id, call_id))
        elif step == "COMPLETED":
            conn.execute("UPDATE borrowers SET status = 'DONE', active_call_id = NULL WHERE id = ? AND active_call_id = ?", (borrower_id, call_id))
            conn.execute("UPDATE agents SET state = 'WRAP_UP', current_call_id = NULL, wrap_up_until = ?, version = version + 1 WHERE id = ? AND current_call_id = ?", (now + WRAP_UP_SECONDS, agent_id, call_id))
        elif step in ("FAILED", "CANCELLED"):
            agent = conn.execute("SELECT pending_state FROM agents WHERE id = ?", (agent_id,)).fetchone()
            next_agent_state = agent["pending_state"] if (agent and agent["pending_state"]) else "AVAILABLE"
            conn.execute("UPDATE agents SET state = ?, current_call_id = NULL, pending_state = NULL, version = version + 1 WHERE id = ? AND current_call_id = ?", (next_agent_state, agent_id, call_id))

            if failure_reason in ("NO_ANSWER", "BUSY"):
                b = conn.execute("SELECT attempts, max_attempts FROM borrowers WHERE id = ?", (borrower_id,)).fetchone()
                new_attempts = b["attempts"] + 1
                new_status = "EXHAUSTED" if new_attempts >= b["max_attempts"] else "PENDING"
                conn.execute("UPDATE borrowers SET status = ?, attempts = ?, next_attempt_at = ?, active_call_id = NULL WHERE id = ? AND active_call_id = ?", (new_status, new_attempts, now + 7200.0, borrower_id, call_id))
            else:
                conn.execute("UPDATE borrowers SET status = 'PENDING', next_attempt_at = ?, active_call_id = NULL WHERE id = ? AND active_call_id = ?", (now + 3.0, borrower_id, call_id))


class RecoverySweeper:
    def sweep(self, conn: sqlite3.Connection, router: ProviderRouter, now: float) -> dict:
        stats = {"leases_recovered": 0, "stale_agents_offlined": 0, "wrap_ups_finished": 0, "ring_timeouts": 0}
        handler = EventHandler()

        for call in conn.execute("SELECT * FROM calls WHERE state = 'RESERVED' AND lease_expires_at <= ?", (now,)).fetchall():
            found_provider, found_record = None, None
            for p_name, provider in router.providers.items():
                rec = provider.get_status(call["idempotency_key"])
                if rec:
                    found_provider, found_record = p_name, rec
                    break
            if found_record:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("UPDATE calls SET provider_name = ?, provider_call_id = ? WHERE id = ?", (found_provider, found_record.provider_call_id, call["id"]))
                conn.execute("COMMIT")
                handler.apply_event(conn, found_provider, f"sweep-{call['id']}-{found_record.status}", call["id"], found_record.status, now)
            else:
                handler.apply_event(conn, "SWEEPER", f"lease-exp-{call['id']}", call["id"], "CANCELLED", now, "LEASE_EXPIRED")
            stats["leases_recovered"] += 1

        for agent in conn.execute("SELECT * FROM agents WHERE state != 'OFFLINE' AND last_heartbeat < ?", (now - HEARTBEAT_TTL_SECONDS,)).fetchall():
            call_id = agent["current_call_id"]
            if call_id:
                call = conn.execute("SELECT * FROM calls WHERE id = ?", (call_id,)).fetchone()
                if call and call["state"] in ("ANSWERED", "CONNECTED"):
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute("INSERT INTO incidents (incident_type, call_id, agent_id, created_at) VALUES (?, ?, ?, ?)", ("ABANDONED_AGENT_LOST", call_id, agent["id"], now))
                    conn.execute("COMMIT")
                if call and call["provider_name"] in router.providers:
                    router.providers[call["provider_name"]].hangup(call["idempotency_key"])
                handler.apply_event(conn, "SWEEPER", f"agent-lost-{call_id}", call_id, "FAILED", now, "AGENT_LOST")
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE agents SET state = 'OFFLINE', current_call_id = NULL, version = version + 1 WHERE id = ?", (agent["id"],))
            conn.execute("COMMIT")
            stats["stale_agents_offlined"] += 1

        for agent in conn.execute("SELECT id, pending_state FROM agents WHERE state = 'WRAP_UP' AND wrap_up_until <= ?", (now,)).fetchall():
            next_state = agent["pending_state"] or "AVAILABLE"
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE agents SET state = ?, pending_state = NULL, wrap_up_until = NULL, version = version + 1 WHERE id = ? AND state = 'WRAP_UP'", (next_state, agent["id"]))
            conn.execute("COMMIT")
            stats["wrap_ups_finished"] += 1

        for call in conn.execute("SELECT * FROM calls WHERE state IN ('INITIATED', 'RINGING') AND updated_at <= ?", (now - RING_TIMEOUT_SECONDS,)).fetchall():
            if call["provider_name"] in router.providers:
                router.providers[call["provider_name"]].hangup(call["idempotency_key"])
            handler.apply_event(conn, "SWEEPER", f"ring-timeout-{call['id']}", call["id"], "FAILED", now, "NO_ANSWER")
            stats["ring_timeouts"] += 1

        return stats


class Dispatcher:
    def __init__(self, router: ProviderRouter):
        self.pacing = ProgressivePacingEngine()
        self.safety = SafetyController()
        self.allocator = CallAllocator()
        self.router = router

    def take_snapshot(self, conn: sqlite3.Connection, now: float, current_hour: int = 10, campaign_paused: bool = False, kill_switch: bool = False) -> CampaignSnapshot:
        avail_agents = conn.execute("SELECT COUNT(*) FROM agents WHERE state = 'AVAILABLE' AND current_call_id IS NULL AND last_heartbeat >= ?", (now - HEARTBEAT_TTL_SECONDS,)).fetchone()[0]
        pend_borrowers = conn.execute("SELECT COUNT(*) FROM borrowers WHERE status = 'PENDING' AND active_call_id IS NULL AND attempts < max_attempts AND next_attempt_at <= ?", (now,)).fetchone()[0]
        cap, all_down, any_degraded = self.router.health_summary(now)
        return CampaignSnapshot(now, campaign_paused, kill_switch, current_hour, avail_agents, pend_borrowers, cap, all_down, any_degraded)

    def tick(self, conn: sqlite3.Connection, now: float, current_hour: int = 10) -> Tuple[str, List[dict]]:
        snap = self.take_snapshot(conn, now, current_hour=current_hour)
        req = self.pacing.compute_request(snap)
        action, permit = self.safety.evaluate(conn, snap, req, now)
        if not permit:
            return action, []
        reservations = self.allocator.reserve_batch(conn, permit, now)
        for res in reservations:
            self.allocator.place_reserved_call(conn, self.router, res, now)
        return action, reservations

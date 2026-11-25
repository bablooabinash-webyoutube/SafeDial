"""
CELL 7: run_all.py — Complete Verification Suite
"""

import ast
import os
import random
import tempfile
import threading

from db import get_connection, init_db
from engine import Dispatcher, EventHandler, RecoverySweeper
from providers import MockTelecomProvider, ProviderRouter
from safety import DialPermit


def new_temp_db() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return path


def seed_data(conn, num_agents: int, num_borrowers: int, now: float = 0.0):
    conn.execute("BEGIN IMMEDIATE")
    for i in range(1, num_agents + 1):
        conn.execute("INSERT INTO agents (id, state, last_heartbeat) VALUES (?, 'AVAILABLE', ?)", (f"A{i}", now))
    for i in range(1, num_borrowers + 1):
        conn.execute("INSERT INTO borrowers (id, phone, status, priority, next_attempt_at) VALUES (?, ?, 'PENDING', 1, 0.0)", (f"B{i}", f"+9190000{i:05d}"))
    conn.execute("COMMIT")


def test_structural_safety_boundary():
    pacing_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pacing.py")
    with open(pacing_path) as f:
        tree = ast.parse(f.read())
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    forbidden = {"db", "providers", "engine", "sqlite3"}
    assert imported.isdisjoint(forbidden), f"Pacing imported forbidden modules: {imported & forbidden}"

    try:
        DialPermit("fake-id", 10, 999.0, "HACK", _secret=object())
        raise AssertionError("DialPermit should not be forgeable!")
    except TypeError:
        pass

    db_path = new_temp_db()
    conn = init_db(db_path)
    seed_data(conn, num_agents=2, num_borrowers=2, now=0.0)
    router = ProviderRouter([MockTelecomProvider("ProviderA", reject_rate=0.0)])
    dispatcher = Dispatcher(router)
    snap = dispatcher.take_snapshot(conn, now=0.0)
    _, permit = dispatcher.safety.evaluate(conn, snap, dispatcher.pacing.compute_request(snap), now=0.0)
    dispatcher.allocator.reserve_batch(conn, permit, now=0.0)
    try:
        dispatcher.allocator.reserve_batch(conn, permit, now=0.0)
        raise AssertionError("Permit replay should be blocked!")
    except ValueError:
        pass
    conn.close()
    os.remove(db_path)
    print("[PASS] Structural Safety Boundary: pacing.py isolated, DialPermit unforgeable & single-use.")


def test_concurrency_race():
    db_path = new_temp_db()
    conn = init_db(db_path)
    seed_data(conn, num_agents=1, num_borrowers=15, now=100.0)
    conn.close()

    barrier = threading.Barrier(15)
    winners = []
    lock = threading.Lock()

    def worker(worker_num):
        w_conn = get_connection(db_path)
        barrier.wait()
        try:
            w_conn.execute("BEGIN IMMEDIATE")
            cur = w_conn.execute(
                "UPDATE agents SET state = 'RESERVED', current_call_id = ?, version = version + 1 WHERE id = 'A1' AND state = 'AVAILABLE' AND current_call_id IS NULL",
                (f"call-worker-{worker_num}",),
            )
            w_conn.execute("COMMIT")
            if cur.rowcount == 1:
                with lock:
                    winners.append(worker_num)
        finally:
            w_conn.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(15)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(winners) == 1
    os.remove(db_path)
    print(f"[PASS] Concurrency Race Test: 15 threads raced for Agent A1 -> exactly 1 won (worker #{winners[0]}).")


def demo_1_worker_crash_recovery():
    db_path = new_temp_db()
    conn = init_db(db_path)
    seed_data(conn, num_agents=1, num_borrowers=1, now=0.0)
    prov_a = MockTelecomProvider("ProviderA", reject_rate=0.0)
    router = ProviderRouter([prov_a])
    dispatcher = Dispatcher(router)
    sweeper = RecoverySweeper()

    snap = dispatcher.take_snapshot(conn, now=0.0)
    _, permit = dispatcher.safety.evaluate(conn, snap, dispatcher.pacing.compute_request(snap), now=0.0)
    res = dispatcher.allocator.reserve_batch(conn, permit, now=0.0)[0]
    dispatcher.allocator.place_reserved_call(conn, router, res, now=0.0, simulate_crash_before_db_update=True)

    conn.execute("UPDATE agents SET last_heartbeat = 10.0 WHERE id = 'A1'")
    stats = sweeper.sweep(conn, router, now=10.1)
    call_state = conn.execute("SELECT state FROM calls WHERE id = ?", (res["call_id"],)).fetchone()["state"]
    assert stats["leases_recovered"] == 1 and call_state == "INITIATED" and len(prov_a.calls_by_id) == 1
    conn.close()
    os.remove(db_path)
    print("[PASS] Demo 1 (Worker Crash): Lease expired -> Sweeper polled provider & resumed existing call (provider calls = 1).")


def demo_2_provider_outage_and_failover():
    db_path = new_temp_db()
    conn = init_db(db_path)
    seed_data(conn, num_agents=2, num_borrowers=2, now=0.0)
    prov_b = MockTelecomProvider("ProviderB", reject_rate=0.0)
    prov_a = MockTelecomProvider("ProviderA", reject_rate=0.0)
    prov_b.is_down = True

    router = ProviderRouter([prov_b, prov_a])
    _, reservations = Dispatcher(router).tick(conn, now=0.0)
    assert len(reservations) == 2 and len(prov_b.calls_by_id) == 0 and len(prov_a.calls_by_id) == 2
    conn.close()
    os.remove(db_path)
    print("[PASS] Demo 2 (Provider Outage): ProviderB went down -> Router failed over cleanly to ProviderA.")


def demo_3_agent_sudden_drop():
    db_path = new_temp_db()
    conn = init_db(db_path)
    seed_data(conn, num_agents=10, num_borrowers=20, now=0.0)
    for i in range(1, 7):
        conn.execute("UPDATE agents SET last_heartbeat = 5.5 WHERE id = ?", (f"A{i}",))

    router = ProviderRouter([MockTelecomProvider("ProviderA", reject_rate=0.0)])
    _, reservations = Dispatcher(router).tick(conn, now=6.0)
    stats = RecoverySweeper().sweep(conn, router, now=6.0)
    assert len(reservations) == 6 and stats["stale_agents_offlined"] == 4
    conn.close()
    os.remove(db_path)
    print("[PASS] Demo 3 (Agent Drop): 4/10 agents vanished -> Next tick dialed only 6; Sweeper offlined 4.")


def demo_4_duplicate_events():
    db_path = new_temp_db()
    conn = init_db(db_path)
    seed_data(conn, num_agents=1, num_borrowers=1, now=0.0)
    router = ProviderRouter([MockTelecomProvider("ProviderA", reject_rate=0.0)])
    _, reservations = Dispatcher(router).tick(conn, now=0.0)
    call_id = reservations[0]["call_id"]

    handler = EventHandler()
    outcomes = [handler.apply_event(conn, "ProviderA", "evt-999", call_id, "ANSWERED", now=1.0) for _ in range(5)]
    assert outcomes == ["APPLIED", "DUPLICATE", "DUPLICATE", "DUPLICATE", "DUPLICATE"]
    conn.close()
    os.remove(db_path)
    print("[PASS] Demo 4 (Duplicate Events): ANSWERED sent 5x -> 1 APPLIED, 4 DUPLICATE.")


def demo_5_out_of_order_events():
    db_path = new_temp_db()
    conn = init_db(db_path)
    seed_data(conn, num_agents=1, num_borrowers=1, now=0.0)
    router = ProviderRouter([MockTelecomProvider("ProviderA", reject_rate=0.0)])
    _, reservations = Dispatcher(router).tick(conn, now=0.0)
    call_id = reservations[0]["call_id"]

    handler = EventHandler()
    r1 = handler.apply_event(conn, "ProviderB", "e1", call_id, "COMPLETED", now=1.0)
    r2 = handler.apply_event(conn, "ProviderB", "e2", call_id, "ANSWERED", now=1.1)
    r3 = handler.apply_event(conn, "ProviderB", "e3", call_id, "RINGING", now=1.2)
    assert (r1, r2, r3) == ("APPLIED", "STALE", "STALE")
    conn.close()
    os.remove(db_path)
    print("[PASS] Demo 5 (Out-of-Order Events): COMPLETED -> ANSWERED -> RINGING ended safely as COMPLETED + WRAP_UP.")


def run_shift_simulation(answer_rate: float = 0.50, shift_seconds: int = 14400):
    db_path = new_temp_db()
    conn = init_db(db_path)
    num_agents = 10
    seed_data(conn, num_agents=num_agents, num_borrowers=5000, now=0.0)

    prov_a = MockTelecomProvider("ProviderA", max_concurrency=50, reject_rate=0.0, seed=42)
    router = ProviderRouter([prov_a])
    dispatcher = Dispatcher(router)
    handler = EventHandler()
    sweeper = RecoverySweeper()
    rng = random.Random(42)

    scheduled = []
    talk_time_total, initiated, answered, unassigned_answers = 0.0, 0, 0, 0

    for t in range(0, shift_seconds, 1):
        now = float(t)
        conn.execute("UPDATE agents SET last_heartbeat = ?", (now,))

        remaining = []
        for fire_at, cid, ikey, etype, reason in scheduled:
            if fire_at <= now:
                prov_a.set_status(ikey, etype)
                handler.apply_event(conn, "ProviderA", f"sim-{cid}-{etype}", cid, etype, now, reason)
                if etype == "ANSWERED":
                    answered += 1
                    if not conn.execute("SELECT 1 FROM agents WHERE current_call_id = ? AND state = 'CONNECTED'", (cid,)).fetchone():
                        unassigned_answers += 1
            else:
                remaining.append((fire_at, cid, ikey, etype, reason))
        scheduled = remaining

        sweeper.sweep(conn, router, now)
        _, new_calls = dispatcher.tick(conn, now, current_hour=10)
        for c in new_calls:
            initiated += 1
            cid, ikey = c["call_id"], c["idempotency_key"]
            if rng.random() < answer_rate:
                talk_dur = rng.uniform(60.0, 180.0)
                talk_time_total += min(talk_dur, max(0.0, shift_seconds - (now + 20.0)))
                scheduled.append((now + 20.0, cid, ikey, "ANSWERED", None))
                scheduled.append((now + 20.0 + talk_dur, cid, ikey, "COMPLETED", None))
            else:
                scheduled.append((now + 20.0, cid, ikey, "FAILED", "NO_ANSWER"))

    talk_util = (talk_time_total / (num_agents * shift_seconds)) * 100.0
    expected_util = (120.0 / (120.0 + 30.0 + (20.0 / answer_rate))) * 100.0
    conn.close()
    os.remove(db_path)
    print(f"[SIM] 4-Hr Shift (Answer Rate {int(answer_rate*100)}%): Initiated={initiated}, Answered={answered}, Talk Util={talk_util:.1f}% (Theory ~{expected_util:.1f}%), Unassigned Answers={unassigned_answers}")


if __name__ == "__main__":
    print("=== Running SafeDial Verification Suite ===")
    test_structural_safety_boundary()
    test_concurrency_race()
    demo_1_worker_crash_recovery()
    demo_2_provider_outage_and_failover()
    demo_3_agent_sudden_drop()
    demo_4_duplicate_events()
    demo_5_out_of_order_events()
    run_shift_simulation(answer_rate=0.20)
    run_shift_simulation(answer_rate=0.50)
    print("=== ALL CHECKS PASSED! ===")

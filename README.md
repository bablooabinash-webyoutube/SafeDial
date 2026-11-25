# SafeDial — Progressive Outbound Dialer with a Structural Safety Boundary

A zero-dependency Python prototype implementing progressive outbound dialing (`1 available agent -> 1 outbound call`) backed by SQLite (WAL mode) as the single source of truth.

## Architecture
`Campaign Snapshot -> Pacing Engine (isolated) -> Safety Controller (DialPermit) -> Call Allocator -> Provider Router + Circuit Breakers -> Idempotent Event Handler & Recovery Sweeper`

## Project Structure
- `db.py` — SQLite WAL schema, versioned rows, and partial unique indexes (`one_active_call_per_agent`, `one_active_call_per_borrower`)
- `models.py` — Explicit Agent & Call finite state machines with forward-only call state ranking
- `pacing.py` — Structurally isolated Progressive Pacing Engine (no DB or provider imports)
- `safety.py` — Non-bypassable Safety Controller issuing unforgeable, single-use `DialPermit` tokens
- `providers.py` — Mock Providers A & B, idempotent placement, Circuit Breaker, and safe timeout handling
- `engine.py` — Transactional `CallAllocator`, atomic `EventHandler`, leaderless `RecoverySweeper`, and `Dispatcher`
- `run_all.py` — Full verification suite (AST boundary check, 15-thread race test, 5 failure demos, 4-hour shift simulations)

## Quick Start (Python 3.10+, Standard Library Only)

```bash
python run_all.py
```

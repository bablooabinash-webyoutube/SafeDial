"""
CELL 1: db.py — Single Source of Truth (SQLite in WAL mode)

WHY THIS EXISTS:
If two workers keep separate in-memory copies of who is busy, they will double-book
agents. Here, 100% of state lives in SQLite.
- WAL Mode lets multiple workers read at the same time while one writes safely.
- Partial Unique Indexes physically block SQLite from ever allowing two active
  calls for the same agent or borrower, even if Python code had a bug.
"""

import sqlite3

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS agents (
    id TEXT PRIMARY KEY,
    state TEXT NOT NULL DEFAULT 'OFFLINE', -- OFFLINE, AVAILABLE, PAUSED, RESERVED, DIALING, CONNECTED, WRAP_UP
    current_call_id TEXT,
    pending_state TEXT,                    -- Stores 'PAUSED' or 'OFFLINE' if requested mid-call
    last_heartbeat REAL NOT NULL,          -- Detects disconnected/dead agents
    wrap_up_until REAL,                    -- When 30s post-call wrap-up finishes
    version INTEGER NOT NULL DEFAULT 0     -- Optimistic concurrency version counter
);

CREATE TABLE IF NOT EXISTS borrowers (
    id TEXT PRIMARY KEY,
    phone TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING, IN_CALL, DONE, EXHAUSTED
    priority INTEGER NOT NULL DEFAULT 1,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    active_call_id TEXT
);

CREATE TABLE IF NOT EXISTS calls (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,  -- Prevents double-dialing on retry
    agent_id TEXT NOT NULL,
    borrower_id TEXT NOT NULL,
    provider_name TEXT,
    provider_call_id TEXT,
    state TEXT NOT NULL,                   -- RESERVED, INITIATED, RINGING, ANSWERED, CONNECTED, COMPLETED, FAILED, CANCELLED
    lease_expires_at REAL NOT NULL,        -- Sweeper recovers if worker crashes before this timestamp
    updated_at REAL NOT NULL,
    version INTEGER NOT NULL DEFAULT 0,
    failure_reason TEXT
);

CREATE TABLE IF NOT EXISTS safety_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    permit_id TEXT UNIQUE,
    created_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    requested INTEGER NOT NULL,
    approved INTEGER NOT NULL,
    action TEXT NOT NULL,                  -- APPROVE, REDUCE, REJECT
    reason_code TEXT NOT NULL,
    consumed INTEGER NOT NULL DEFAULT 0    -- 0 = unused, 1 = already redeemed
);

CREATE TABLE IF NOT EXISTS provider_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_name TEXT NOT NULL,
    event_id TEXT,
    call_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    outcome TEXT NOT NULL,                 -- APPLIED, DUPLICATE, STALE, UNKNOWN_CALL
    received_at REAL NOT NULL,
    UNIQUE(provider_name, event_id)
);

CREATE TABLE IF NOT EXISTS incidents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_type TEXT NOT NULL,
    call_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    created_at REAL NOT NULL
);

-- Database-level safety net: impossible to have 2 active calls per agent or borrower
CREATE UNIQUE INDEX IF NOT EXISTS one_active_call_per_agent
    ON calls(agent_id)
    WHERE state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');

CREATE UNIQUE INDEX IF NOT EXISTS one_active_call_per_borrower
    ON calls(borrower_id)
    WHERE state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');
"""


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def init_db(db_path: str) -> sqlite3.Connection:
    conn = get_connection(db_path)
    conn.executescript(SCHEMA_SQL)
    return conn

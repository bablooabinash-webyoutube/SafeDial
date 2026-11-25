"""
CELL 5: providers.py — Mock Providers, Circuit Breaker, and Smart Router

WHY THIS EXISTS:
- Definite rejection (outage/503) -> safe to fail over to backup provider.
- Ambiguous timeout -> call might already be ringing! Retry SAME provider with
  SAME idempotency_key so we never dial the borrower twice.
"""

import random
from collections import deque
from dataclasses import dataclass
from typing import Dict, List, Optional


class ProviderRejectedError(Exception):
    """Definite failure: no call started. Safe to failover."""


class ProviderTimeoutError(Exception):
    """Ambiguous failure: call might have started. Retry same provider with same key."""


@dataclass
class ProviderCallRecord:
    provider_call_id: str
    call_id: str
    phone: str
    status: str


class MockTelecomProvider:
    def __init__(self, name: str, max_concurrency: int = 50, reject_rate: float = 0.01, timeout_rate: float = 0.0, seed: int = 42):
        self.name = name
        self.max_concurrency = max_concurrency
        self.reject_rate = reject_rate
        self.timeout_rate = timeout_rate
        self.is_down = False
        self.rng = random.Random(seed)
        self.calls_by_key: Dict[str, ProviderCallRecord] = {}
        self.calls_by_id: Dict[str, ProviderCallRecord] = {}

    def active_count(self) -> int:
        return sum(1 for c in self.calls_by_id.values() if c.status not in ("COMPLETED", "FAILED", "CANCELLED"))

    def remaining_capacity(self) -> int:
        return 0 if self.is_down else max(0, self.max_concurrency - self.active_count())

    def place_call(self, call_id: str, phone: str, idempotency_key: str) -> str:
        if self.is_down:
            raise ProviderRejectedError(f"{self.name} is down")
        if idempotency_key in self.calls_by_key:
            return self.calls_by_key[idempotency_key].provider_call_id
        if self.remaining_capacity() <= 0:
            raise ProviderRejectedError(f"{self.name} capacity full")

        roll = self.rng.random()
        if roll < self.reject_rate:
            raise ProviderRejectedError(f"{self.name} rejected call")
        if roll < self.reject_rate + self.timeout_rate:
            if self.rng.random() < 0.5:
                p_id = f"{self.name}-{len(self.calls_by_id) + 1}"
                rec = ProviderCallRecord(p_id, call_id, phone, "INITIATED")
                self.calls_by_key[idempotency_key] = rec
                self.calls_by_id[p_id] = rec
            raise ProviderTimeoutError(f"{self.name} timed out")

        p_id = f"{self.name}-{len(self.calls_by_id) + 1}"
        rec = ProviderCallRecord(p_id, call_id, phone, "INITIATED")
        self.calls_by_key[idempotency_key] = rec
        self.calls_by_id[p_id] = rec
        return p_id

    def get_status(self, idempotency_key: str) -> Optional[ProviderCallRecord]:
        return self.calls_by_key.get(idempotency_key)

    def hangup(self, idempotency_key: str) -> bool:
        rec = self.calls_by_key.get(idempotency_key)
        if not rec:
            return False
        if rec.status not in ("COMPLETED", "FAILED", "CANCELLED"):
            rec.status = "CANCELLED"
        return True

    def set_status(self, idempotency_key: str, new_status: str):
        if idempotency_key in self.calls_by_key:
            self.calls_by_key[idempotency_key].status = new_status


class CircuitBreaker:
    def __init__(self, window_size: int = 10, fail_threshold: float = 0.5, cooldown_sec: float = 10.0):
        self.window_size = window_size
        self.fail_threshold = fail_threshold
        self.cooldown_sec = cooldown_sec
        self.outcomes: deque = deque(maxlen=window_size)
        self.consecutive_failures = 0
        self.state = "CLOSED"
        self.opened_at: float = 0.0

    def allow_request(self, now: float) -> bool:
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN" and (now - self.opened_at) >= self.cooldown_sec:
            self.state = "HALF_OPEN"
            return True
        return False

    def record_success(self):
        self.outcomes.append(True)
        self.consecutive_failures = 0
        self.state = "CLOSED"

    def record_failure(self, now: float):
        self.outcomes.append(False)
        self.consecutive_failures += 1
        fail_rate = self.outcomes.count(False) / len(self.outcomes)
        if self.state == "HALF_OPEN" or self.consecutive_failures >= 3 or (len(self.outcomes) >= self.window_size and fail_rate > self.fail_threshold):
            self.state = "OPEN"
            self.opened_at = now


class ProviderRouter:
    def __init__(self, providers: List[MockTelecomProvider]):
        self.providers = {p.name: p for p in providers}
        self.order = [p.name for p in providers]
        self.breakers = {p.name: CircuitBreaker() for p in providers}

    def health_summary(self, now: float):
        total_cap = 0
        healthy_count = 0
        for name in self.order:
            breaker = self.breakers[name]
            if breaker.state == "CLOSED":
                total_cap += self.providers[name].remaining_capacity()
                healthy_count += 1
            elif breaker.state == "OPEN" and (now - breaker.opened_at) >= breaker.cooldown_sec:
                total_cap += 1
                healthy_count += 1
        return total_cap, (healthy_count == 0), (healthy_count < len(self.order))

    def place_call_safely(self, call_id: str, phone: str, idempotency_key: str, now: float):
        for name in self.order:
            breaker = self.breakers[name]
            provider = self.providers[name]
            if not breaker.allow_request(now):
                continue
            for attempt in range(2):
                try:
                    p_call_id = provider.place_call(call_id, phone, idempotency_key)
                    breaker.record_success()
                    return name, p_call_id, "PLACED"
                except ProviderTimeoutError:
                    breaker.record_failure(now)
                    if attempt == 0:
                        continue
                    return name, None, "AMBIGUOUS"
                except ProviderRejectedError:
                    breaker.record_failure(now)
                    break
        return None, None, "REJECTED"

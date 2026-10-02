from threading import Event
from time import monotonic
from uuid import uuid4

from market_execution_lab.observability import log_event


RENEW_LEASES = "for i,k in ipairs(KEYS) do if redis.call('GET', k) ~= ARGV[1] then return 0 end end; for i,k in ipairs(KEYS) do redis.call('EXPIRE', k, ARGV[2]) end; return 1"
RELEASE_LEASE = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end; return 0"


class LiveOwnership:
    def __init__(self, redis, keys, stop: Event, ttl_seconds=30):
        self.redis, self.keys, self.stop = redis, keys, stop
        self.owner = str(uuid4())
        self.ttl_seconds = ttl_seconds
        self.acquired = []
        self.renew_at = 0.0

    def acquire(self, timeout_seconds=0):
        deadline = monotonic() + timeout_seconds
        waiting = False
        while not self.stop.is_set():
            try:
                for key in self.keys:
                    if not self.redis.set(key, self.owner, nx=True, ex=self.ttl_seconds):
                        break
                    self.acquired.append(key)
                else:
                    self.check(force=True)
                    log_event("live_ownership_acquired", keys=len(self.keys), waited=waiting)
                    return True
            except Exception:
                self.release()
                raise
            self.release()
            remaining = deadline - monotonic()
            if remaining <= 0:
                log_event("live_ownership_timeout", keys=len(self.keys))
                raise RuntimeError("live partition already has an owner; startup wait timed out")
            if not waiting:
                log_event("live_ownership_waiting", keys=len(self.keys), timeout_seconds=timeout_seconds)
                waiting = True
            self.stop.wait(min(1, remaining))
        return False

    def check(self, force=False):
        if self.stop.is_set():
            raise InterruptedError("live worker stopping")
        if force or monotonic() >= self.renew_at:
            if not self.redis.eval(RENEW_LEASES, len(self.keys), *self.keys, self.owner, self.ttl_seconds):
                log_event("live_ownership_lost", keys=len(self.keys))
                raise RuntimeError("live partition ownership lost")
            self.renew_at = monotonic() + min(5, self.ttl_seconds / 3)

    def release(self):
        try:
            for key in self.acquired:
                self.redis.eval(RELEASE_LEASE, 1, key, self.owner)
        finally:
            self.acquired.clear()

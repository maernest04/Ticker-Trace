from threading import Event
from unittest.mock import Mock
from uuid import uuid4

import pytest

from market_execution_lab import live_ownership
from market_execution_lab.live_ownership import LiveOwnership, RELEASE_LEASE
from market_execution_lab.live_pipeline import LiveEngine


def test_partial_acquisition_releases_only_acquired_keys():
    redis = Mock()
    redis.set.side_effect = [True, False]
    ownership = LiveOwnership(redis, ["first", "occupied"], Event())
    with pytest.raises(RuntimeError, match="already has an owner"):
        ownership.acquire()
    redis.eval.assert_called_once_with(RELEASE_LEASE, 1, "first", ownership.owner)
    assert not ownership.acquired


def test_startup_retry_waits_for_expiry_without_deleting_competing_owner(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(live_ownership, "monotonic", lambda: clock[0])
    stop = Mock()
    stop.is_set.return_value = False
    stop.wait.side_effect = lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    redis = Mock()
    redis.set.side_effect = [False, False, True]
    redis.eval.return_value = 1
    ownership = LiveOwnership(redis, ["occupied"], stop)
    assert ownership.acquire(5)
    assert clock[0] == 2
    assert stop.wait.call_count == 2
    assert all(call.args[0] != RELEASE_LEASE for call in redis.eval.call_args_list)


def test_startup_timeout_is_bounded_and_cancellable(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(live_ownership, "monotonic", lambda: clock[0])
    stop = Mock()
    stop.is_set.return_value = False
    stop.wait.side_effect = lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    redis = Mock()
    redis.set.return_value = False
    ownership = LiveOwnership(redis, ["occupied"], stop)
    with pytest.raises(RuntimeError, match="timed out"):
        ownership.acquire(2.5)
    assert clock[0] == 2.5
    redis.eval.assert_not_called()
    stopped = Event()
    stopped.set()
    assert not LiveOwnership(redis, ["occupied"], stopped).acquire(45)


def test_renewal_fails_after_ownership_loss_and_never_reacquires():
    redis = Mock()
    redis.set.return_value = True
    redis.eval.side_effect = [1, 0, 0]
    ownership = LiveOwnership(redis, ["partition"], Event())
    ownership.acquire()
    with pytest.raises(RuntimeError, match="ownership lost"):
        ownership.check(force=True)
    ownership.release()
    assert redis.set.call_count == 1
    assert redis.eval.call_args.args == (RELEASE_LEASE, 1, "partition", ownership.owner)


def test_shutdown_interrupts_checks_before_processing():
    stop = Event()
    stop.set()
    redis = Mock()
    with pytest.raises(InterruptedError):
        LiveOwnership(redis, ["partition"], stop).check(force=True)
    redis.eval.assert_not_called()


def test_reconstruction_checks_ownership_between_bounded_batches():
    redis = Mock()
    redis.xinfo_groups.return_value = [{"name": "engine", "last-delivered-id": "2-0"}]
    redis.xrange.side_effect = [[("1-0", {"message_type": "replay.started.v1"})], [("2-0", {"message_type": "replay.started.v1"})], []]
    guard = Mock()
    LiveEngine(redis, uuid4(), 0, guard)
    assert redis.xrange.call_count == 3
    assert all(call.kwargs["count"] == 100 for call in redis.xrange.call_args_list)
    assert guard.call_count == 5
    assert redis.xrange.call_args_list[1].kwargs["min"] == "(1-0"


def test_reconstruction_stops_before_next_batch_when_ownership_is_lost():
    redis = Mock()
    redis.xinfo_groups.return_value = [{"name": "engine", "last-delivered-id": "2-0"}]
    redis.xrange.return_value = [("1-0", {"message_type": "replay.started.v1"})]
    guard = Mock(side_effect=[None, None, RuntimeError("ownership lost")])
    with pytest.raises(RuntimeError, match="ownership lost"):
        LiveEngine(redis, uuid4(), 0, guard)
    assert redis.xrange.call_count == 1

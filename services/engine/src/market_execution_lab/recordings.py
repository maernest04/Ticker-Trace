import fcntl
import json
import os
import re
from contextlib import contextmanager
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from threading import Event, Thread
from time import monotonic, time
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from market_execution_lab.live_pipeline import LIVE_SESSION_KEY, LIVE_STALE_SECONDS
from market_execution_lab.pipeline import partition_for_symbol
from market_execution_lab.streaming import event_from_json, partition_stream_name


MAX_EVENTS = 80_000
MAX_BYTES = 64 * 1024 * 1024
MAX_STORAGE = 256 * 1024 * 1024
MODEL_VERSION = "top-of-book-v1-entry-boundary"


class RecordingManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    recording_id: UUID
    schema_version: Literal["recording.v1"] = "recording.v1"
    model_version: Literal["top-of-book-v1-entry-boundary"] = MODEL_VERSION
    source_run_id: UUID
    feed: Literal["iex", "generated"]
    symbols: list[str] = Field(min_length=1, max_length=2)
    status: Literal["capturing", "finalizing", "ready", "incomplete", "failed"]
    started_at: datetime
    ended_at: datetime | None = None
    reason: str | None = None
    boundaries: dict[str, dict[str, str]]
    event_count: int = Field(ge=0, le=MAX_EVENTS)
    byte_count: int = Field(ge=0, le=MAX_BYTES)
    checksum: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    timestamp_precision: Literal["microseconds"] = "microseconds"
    units: Literal["USD prices; normalized share sizes"] = "USD prices; normalized share sizes"
    quality: list[str] = Field(default_factory=lambda: [
        "Observed normalized IEX top-of-book, not a consolidated feed or order book",
        "Original sub-microsecond precision is unavailable",
        "No guarantee of complete vendor delivery; rejected upstream messages may be absent",
        "Source-entry order is preserved per symbol; cross-partition order is not market chronology",
    ])

    @field_validator("started_at", "ended_at")
    @classmethod
    def timezone(cls, value):
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("capture timestamps require a timezone")
        return value

    @field_validator("symbols")
    @classmethod
    def valid_symbols(cls, value):
        if len(set(value)) != len(value) or any(not re.fullmatch(r"[A-Z0-9.-]{1,10}", symbol) for symbol in value):
            raise ValueError("recorded symbols must be normalized and distinct")
        return value

    @field_validator("boundaries")
    @classmethod
    def valid_boundaries(cls, value):
        if not value:
            raise ValueError("source boundaries are required")
        for partition, boundary in value.items():
            if not partition.isdigit() or not 0 <= int(partition) < 16 or set(boundary) != {"start", "end"} or any(not re.fullmatch(r"\d+-\d+", cursor) for cursor in boundary.values()):
                raise ValueError("invalid source boundary")
            if tuple(map(int, boundary["start"].split("-"))) > tuple(map(int, boundary["end"].split("-"))):
                raise ValueError("source boundary is reversed")
        return value

    @model_validator(mode="after")
    def complete_metadata(self):
        if self.ended_at is not None and self.ended_at < self.started_at:
            raise ValueError("capture interval is reversed")
        if self.status == "ready" and (self.ended_at is None or not self.reason or not self.checksum or not self.event_count or not self.byte_count):
            raise ValueError("ready recording requires complete provenance")
        return self


class RecordedEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")
    partition: int = Field(ge=0, lt=16)
    cursor: str = Field(pattern=r"^\d+-\d+$")
    event: dict


class RecordingStore:
    def __init__(self, directory: Path):
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)

    def path(self, identity: UUID, suffix: str) -> Path:
        return self.directory / f"{UUID(str(identity))}.{suffix}"

    @contextmanager
    def locked(self):
        with (self.directory / ".lock").open("a") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def used_bytes(self):
        return sum(path.stat().st_size for path in self.directory.iterdir() if path.is_file())

    def save_json(self, identity: UUID, suffix: str, value: dict):
        raw = json.dumps(value, separators=(",", ":")).encode()
        if len(raw) > MAX_BYTES or self.used_bytes() + len(raw) > MAX_STORAGE:
            raise ValueError("private recording storage limit reached")
        target = self.path(identity, suffix)
        temporary = self.path(identity, f"{suffix}.tmp")
        with temporary.open("wb") as handle:
            os.fchmod(handle.fileno(), 0o600)
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(target)

    def save_manifest(self, manifest: RecordingManifest):
        manifest = RecordingManifest.model_validate(manifest.model_dump())
        self.save_json(manifest.recording_id, "manifest.json", manifest.model_dump(mode="json"))

    def manifest(self, identity: UUID):
        return RecordingManifest.model_validate_json(self.path(identity, "manifest.json").read_bytes())

    def list(self):
        return sorted((RecordingManifest.model_validate_json(path.read_bytes()) for path in self.directory.glob("*.manifest.json")), key=lambda value: value.started_at, reverse=True)

    def load(self, identity: UUID, *, manifest=None) -> tuple[RecordingManifest, tuple[RecordedEntry, ...]]:
        manifest = manifest or self.manifest(identity)
        if manifest.status != "ready" or not manifest.checksum or not manifest.boundaries:
            raise ValueError("recording is not ready")
        path = self.path(identity, "events.jsonl")
        if path.stat().st_size > MAX_BYTES or path.stat().st_size != manifest.byte_count:
            raise ValueError("recording byte count mismatch")
        raw = path.read_bytes()
        if sha256(raw).hexdigest() != manifest.checksum:
            raise ValueError("recording checksum mismatch")
        entries = tuple(RecordedEntry.model_validate_json(line) for line in raw.splitlines())
        if len(entries) != manifest.event_count or not entries or len(entries) > MAX_EVENTS:
            raise ValueError("recording event count mismatch")
        last = {}
        for entry in entries:
            event = event_from_json(json.dumps(entry.event))
            boundary = manifest.boundaries.get(str(entry.partition))
            cursor = tuple(map(int, entry.cursor.split("-")))
            if not boundary or not tuple(map(int, boundary["start"].split("-"))) < cursor <= tuple(map(int, boundary["end"].split("-"))):
                raise ValueError("event outside source boundaries")
            if entry.partition in last and cursor <= last[entry.partition]:
                raise ValueError("ambiguous source-entry ordering")
            if event.run_id != manifest.source_run_id or event.symbol not in manifest.symbols or event.partition != entry.partition or partition_for_symbol(event.symbol) != entry.partition:
                raise ValueError("source identity or symbol mismatch")
            last[entry.partition] = cursor
        return manifest, entries


class CaptureService:
    def __init__(self, redis, files: RecordingStore):
        self.redis, self.files = redis, files
        self.stop = Event()
        self.thread = None
        self.active_id = None
        self.lock_handle = (files.directory / ".capture.lock").open("a")
        try:
            fcntl.flock(self.lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock_handle.close()
            raise RuntimeError("another private capture service owns this directory")
        for manifest in files.list():
            if manifest.status in {"capturing", "finalizing"}:
                files.save_manifest(manifest.model_copy(update={"status": "incomplete", "ended_at": datetime.now(UTC), "reason": "application restart interrupted capture"}))

    def start(self, symbols: list[str], seconds: int, permissions_confirmed: bool):
        if not permissions_confirmed:
            raise ValueError("confirm your account permits private recording/storage before capture")
        if not 1 <= seconds <= 600 or not 1 <= len(set(symbols)) <= 2 or len(set(symbols)) != len(symbols):
            raise ValueError("capture requires one or two distinct symbols and 1–600 seconds")
        with self.files.locked():
            if self.thread and self.thread.is_alive():
                raise ValueError("a recording is already capturing or finalizing")
            if any(json.loads(path.read_bytes())["status"] in {"queued", "running"} for path in self.files.directory.glob("*.experiment.json")):
                raise ValueError("finish the active experiment before capturing")
            if self.files.used_bytes() + MAX_BYTES + 1024 * 1024 > MAX_STORAGE:
                raise ValueError("insufficient reserved recording storage headroom")
            session = self.redis.hgetall(LIVE_SESSION_KEY)
            if not session or session.get("phase", "running") != "running" or session.get("status") != "connected" or time() - float(session.get("received_at", 0)) > LIVE_STALE_SECONDS:
                raise ValueError("actual live ingestion must be connected and fresh to capture")
            if not set(symbols) <= set(json.loads(session["symbols"])):
                raise ValueError("capture cannot change live subscriptions")
            boundaries = {}
            for partition in sorted({partition_for_symbol(symbol) for symbol in symbols}):
                stream = partition_stream_name(session["run_id"], partition)
                tail = self.redis.xrevrange(stream, count=1)
                if not tail:
                    raise ValueError("source history is unavailable")
                boundaries[str(partition)] = {"start": tail[0][0], "end": tail[0][0]}
            manifest = RecordingManifest(recording_id=uuid4(), source_run_id=UUID(session["run_id"]), feed="iex", symbols=symbols, status="capturing", started_at=datetime.now(UTC), boundaries=boundaries, event_count=0, byte_count=0)
            self.files.save_manifest(manifest)
            self.stop.clear()
            self.active_id = manifest.recording_id
            self.thread = Thread(target=self._capture, args=(manifest, seconds), daemon=True)
            self.thread.start()
            return manifest

    def request_stop(self, identity: UUID):
        if identity != self.active_id or not self.thread or not self.thread.is_alive():
            raise ValueError("recording is not active")
        self.stop.set()
        return self.files.manifest(identity)

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=15)
        if not self.thread or not self.thread.is_alive():
            self.lock_handle.close()

    def _capture(self, manifest: RecordingManifest, seconds: int):
        temporary = self.files.path(manifest.recording_id, "events.part")
        deadline = monotonic() + seconds
        digest = sha256()
        counts, size = 0, 0
        boundaries = {key: dict(value) for key, value in manifest.boundaries.items()}
        reason, status = "duration bound", "ready"
        final_tails = None
        try:
            with temporary.open("xb") as handle:
                os.fchmod(handle.fileno(), 0o600)
                while True:
                    session = self.redis.hgetall(LIVE_SESSION_KEY)
                    if session.get("run_id") != str(manifest.source_run_id) or session.get("phase", "running") != "running" or session.get("status") != "connected" or time() - float(session.get("received_at", 0)) > LIVE_STALE_SECONDS:
                        reason, status = "source rollover, interruption, or stale ingestion", "incomplete"
                        break
                    if final_tails is None and (self.stop.is_set() or monotonic() >= deadline):
                        reason = "user stop" if self.stop.is_set() else "duration bound"
                        final_tails = {}
                        for key in boundaries:
                            tail = self.redis.xrevrange(partition_stream_name(str(manifest.source_run_id), int(key)), count=1)
                            if not tail:
                                raise ValueError("source history lost")
                            final_tails[key] = tail[0][0]
                    bounded = False
                    for key, boundary in boundaries.items():
                        stream = partition_stream_name(str(manifest.source_run_id), int(key))
                        info = self.redis.xinfo_stream(stream)
                        if info["entries-added"] != info["length"]:
                            raise ValueError("source entries were trimmed or deleted")
                        if not self.redis.xrange(stream, min=boundary["start"], max=boundary["start"], count=1):
                            raise ValueError("capture boundary history lost")
                        entries = self.redis.xrange(stream, min=f"({boundary['end']}", max=final_tails[key] if final_tails else "+", count=256)
                        for cursor, message in entries:
                            if message.get("message_type") == "market.event.v1":
                                event = event_from_json(message["payload"])
                                if event.symbol in manifest.symbols:
                                    line = RecordedEntry(partition=int(key), cursor=cursor, event=event.model_dump(mode="json")).model_dump_json().encode() + b"\n"
                                    if size + len(line) > MAX_BYTES or counts >= MAX_EVENTS:
                                        bounded, reason = True, "event/byte bound"
                                        break
                                    handle.write(line)
                                    digest.update(line)
                                    counts += 1
                                    size += len(line)
                            boundary["end"] = cursor
                        if bounded:
                            break
                    if bounded or final_tails and all(boundaries[key]["end"] == end for key, end in final_tails.items()):
                        break
                    if final_tails:
                        continue
                    self.stop.wait(0.2)
                handle.flush()
                os.fsync(handle.fileno())
            manifest = manifest.model_copy(update={"status": "finalizing", "event_count": counts, "byte_count": size, "boundaries": boundaries, "ended_at": datetime.now(UTC), "reason": reason, "checksum": digest.hexdigest()})
            self.files.save_manifest(manifest)
            temporary.replace(self.files.path(manifest.recording_id, "events.jsonl"))
            if counts == 0 and status == "ready":
                status, reason = "incomplete", "no observed market events"
            manifest = manifest.model_copy(update={"status": status, "reason": reason})
            if status == "ready":
                self.files.load(manifest.recording_id, manifest=manifest)
            self.files.save_manifest(manifest)
        except Exception as error:
            self.files.save_manifest(manifest.model_copy(update={"status": "failed", "ended_at": datetime.now(UTC), "reason": f"capture failed: {type(error).__name__}"}))

import argparse
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from market_execution_lab.fixtures import replay_scenarios
from market_execution_lab.pipeline import partition_for_symbol
from market_execution_lab.recordings import RecordedEntry, RecordingManifest, RecordingStore


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    scenario = next(item for item in replay_scenarios() if item.name == "liquidity_replenishment_v1")
    store = RecordingStore(args.directory)
    identity = uuid4()
    partition = partition_for_symbol(scenario.order.symbol)
    entries = tuple(RecordedEntry(partition=partition, cursor=f"{index + 1}-0", event=event.model_copy(update={"partition": partition}).model_dump(mode="json")) for index, event in enumerate(scenario.events))
    raw = b"".join(entry.model_dump_json().encode() + b"\n" for entry in entries)
    path = store.path(identity, "events.jsonl")
    path.write_bytes(raw)
    path.chmod(0o600)
    now = datetime.now(UTC)
    store.save_manifest(RecordingManifest(recording_id=identity, source_run_id=scenario.order.run_id, feed="generated", symbols=[scenario.order.symbol], status="ready", started_at=now, ended_at=now, reason="generated presentation fixture", boundaries={str(partition): {"start": "0-0", "end": f"{len(entries)}-0"}}, event_count=len(entries), byte_count=len(raw), checksum=sha256(raw).hexdigest(), quality=["Generated NVDA fixture; no vendor data or brokerage execution", "Source-order top-of-book simulation; not exchange queue position"]))
    store.load(identity)
    print(f"Generated recording: {identity}; {len(entries)} events; SHA-256 {sha256(raw).hexdigest()}")


if __name__ == "__main__":
    main()

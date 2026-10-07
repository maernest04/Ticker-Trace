import subprocess
import sys
from pathlib import Path

from market_execution_lab.recordings import RecordingStore


def test_demo_recording_is_valid_generated_input(tmp_path):
    script = Path(__file__).resolve().parents[1] / "tools" / "create_demo_recording.py"
    result = subprocess.run([sys.executable, str(script), str(tmp_path)], check=True, capture_output=True, text=True)
    store = RecordingStore(tmp_path)
    manifests = store.list()
    assert len(manifests) == 1
    manifest, entries = store.load(manifests[0].recording_id)
    assert manifest.feed == "generated"
    assert manifest.symbols == ["NVDA"]
    assert manifest.event_count == len(entries) == 300
    assert manifest.checksum in result.stdout
    assert all(entry.event["event_id"].startswith("liquidity_replenishment_v1:") for entry in entries)
    assert store.path(manifest.recording_id, "events.jsonl").stat().st_mode & 0o777 == 0o600


def test_demo_recording_does_not_overwrite_existing_input(tmp_path):
    script = Path(__file__).resolve().parents[1] / "tools" / "create_demo_recording.py"
    subprocess.run([sys.executable, str(script), str(tmp_path)], check=True, capture_output=True)
    store = RecordingStore(tmp_path)
    first = store.list()[0]
    original = store.path(first.recording_id, "events.jsonl").read_bytes()
    subprocess.run([sys.executable, str(script), str(tmp_path)], check=True, capture_output=True)
    assert len(store.list()) == 2
    assert store.path(first.recording_id, "events.jsonl").read_bytes() == original
    store.load(first.recording_id)

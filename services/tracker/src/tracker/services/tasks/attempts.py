import json
from datetime import datetime
from typing import Any

from tracker.runtime.artifacts import ATTEMPT_RECORD, run_attempts_prefix, task_artifact_key
from tracker.runtime.storage import ObjectStore


async def keep_replaced_attempts(store: ObjectStore, benchmark_id: str, task_id: str, claimed_at: datetime) -> None:
    root = f"{run_attempts_prefix(benchmark_id)}{task_id}/"
    records: dict[str, dict[str, Any]] = {}
    kept: set[str] = set()
    async for stored in store.list_objects(root):
        attempt_id, _, name = stored.key.removeprefix(root).partition("/")
        if name != ATTEMPT_RECORD:
            kept.add(stored.key)
            continue
        records[attempt_id] = json.loads(await store.get_bytes(stored.key))
    if not records:
        return
    ordered = sorted(records.items(), key=lambda item: datetime.fromisoformat(item[1]["started_at"]))
    windows: dict[str, tuple[datetime, datetime]] = {}
    for index, (attempt_id, record) in enumerate(ordered):
        if "kept_until" not in record:
            later = ordered[index + 1][1]["started_at"] if index + 1 < len(ordered) else claimed_at.isoformat()
            record["kept_until"] = later
            await store.put_bytes(f"{root}{attempt_id}/{ATTEMPT_RECORD}", json.dumps(record, indent=2).encode())
        windows[attempt_id] = (
            datetime.fromisoformat(record["started_at"]),
            datetime.fromisoformat(record["kept_until"]),
        )
    source = task_artifact_key(benchmark_id, task_id, "")
    async for stored in store.list_objects(source):
        for attempt_id, (begins, ends) in windows.items():
            destination = f"{root}{attempt_id}/{stored.key.removeprefix(source)}"
            if stored.last_modified is not None and begins <= stored.last_modified < ends and destination not in kept:
                await store.copy(stored.key, destination)

from pathlib import Path

from tracker.services.datasets.ref import LocalDataset

LIVE_DATASET_NAME = "live-dataset"
LIVE_DATASET_TASK_IDS = [f"task-{index}" for index in range(5)]
_LIVE_TASK_TOML = """
[agent]
timeout_sec = 300

[verifier]
timeout_sec = 60

[environment]
docker_image = "python:3.11-slim"
cpus = 1
memory_mb = 2048
workdir = "/app"
"""


def write_live_dataset(root: Path) -> LocalDataset:
    for task_id in LIVE_DATASET_TASK_IDS:
        for relative, text in {
            "task.toml": _LIVE_TASK_TOML,
            "instruction.md": f"Instruction for {task_id}.",
            "tests/test.sh": "#!/bin/bash\necho 1 > /logs/verifier/reward.txt\n",
        }.items():
            path = root / task_id / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
    return LocalDataset(path=str(root))

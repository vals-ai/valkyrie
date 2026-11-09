# Valkyrie SDK

Async Python client for [Valkyrie](https://docs.valkyrie.vals.ai/introduction/what-is-valkyrie),
Vals AI's platform for running AI agents on benchmarks. Start runs, follow their progress,
read results, logs, and task details, and manage agents, projects, secrets, and benchmark
services from your own code.

**Documentation:** [docs.valkyrie.vals.ai](https://docs.valkyrie.vals.ai/sdk/quickstart) ·
**SDK reference:** [docs.valkyrie.vals.ai/reference/sdk](https://docs.valkyrie.vals.ai/reference/sdk)

## Install

The SDK needs Python 3.12 or later. With uv installed and a virtual environment activated:

```bash
uv pip install valkyrie-sdk
```

## Sign in

The SDK uses the same login and Tracker as the Valkyrie CLI. Sign in once with
[`valkyrie login`](https://docs.valkyrie.vals.ai/get-started/configuration#sign-in), or set
`VALKYRIE_API_KEY` to an
[access key](https://docs.valkyrie.vals.ai/get-started/configuration#create-an-access-key-for-ci).
The SDK doesn't install the CLI.

To choose the Tracker or access key in code, pass a `ValkyrieConfig`:

```python
from valkyrie.sdk import ValkyrieClient, ValkyrieConfig

config = ValkyrieConfig(url="https://tracker.example.com", api_key="ACCESS_KEY")
async with ValkyrieClient(config) as client:
    ...
```

## List your runs

```python
import asyncio

from valkyrie.sdk import ValkyrieClient


async def main() -> None:
    async with ValkyrieClient() as client:
        page = await client.runs.list()
        for run in page.benchmarks:
            print(run.id, run.name, run.status, run.final_score)


asyncio.run(main())
```

## Start a run and wait for its score

```python
import asyncio

from valkyrie.sdk import BenchmarkStatus, ValkyrieClient

FINAL_STATUSES = {BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED}


async def main() -> None:
    async with ValkyrieClient() as client:
        run = await client.runs.start(
            agent="my-agent",
            benchmark="swebench",
            model="provider/model-name",
            concurrency=10,
            label="nightly-swebench",
        )
        print("Started", run.benchmark_id)

        while (await client.runs.fetch(run.benchmark_id)).details.status not in FINAL_STATUSES:
            async for update in client.runs.stream(run.benchmark_id):
                details = update.details
                print(f"{details.status.value}: {details.finished_tasks}/{details.total_tasks} tasks")

        results = await client.runs.results(run.benchmark_id)
        if results.status == BenchmarkStatus.FINISHED and results.final_evaluation:
            print("Score:", results.final_evaluation.final_score)


asyncio.run(main())
```

`agent` is the name of an agent you
[uploaded](https://docs.valkyrie.vals.ai/agents/manage-agents) to your library. `stream`
yields the run's state each time it changes. It ends when the run finishes or when the
Tracker disconnects, so the loop reconnects until the run reaches a final status.

## Download outputs

Extract the files your agents wrote into a new directory:

```python
await client.runs.download_outputs(run_id, "./outputs", task_ids=["task-1"])
```

Leave out `task_ids` for the whole run.

## Look into failed tasks

```python
from valkyrie.sdk import FetchTasksRequest, TaskStatus

errors = FetchTasksRequest(status=[TaskStatus.ERROR])
async for task in client.benchmarks.iter_tasks(run_id, errors):
    print(task.task_id, task.error_message)

    for event in await client.logs.fetch_task(run_id, task.task_id, query="ERROR"):
        print(event.timestamp, event.message)
```

## Stop, resume, and retry runs

```python
await client.runs.stop(run_id)

await client.runs.resume(run_id, concurrency=20)

await client.runs.retry(run_id, task_ids=["task-1"])
```

See [Manage a run](https://docs.valkyrie.vals.ai/runs/manage) for what each action reruns.

## Handle errors

Every SDK exception inherits from `ValkyrieSDKError`. `ValkyrieAPIError` carries the HTTP
status and the Tracker's error detail:

```python
from valkyrie.sdk import ValkyrieAPIError, ValkyrieSDKError

try:
    run = await client.runs.fetch(run_id)
except ValkyrieAPIError as exc:
    print(exc.status_code, exc.detail)
except ValkyrieSDKError as exc:
    print(exc)
```

## Learn more

| Topic | Guide |
| --- | --- |
| Set up the SDK | [sdk/quickstart](https://docs.valkyrie.vals.ai/sdk/quickstart) |
| Start, follow, recover, and download runs | [sdk/runs](https://docs.valkyrie.vals.ai/sdk/runs) |
| Tasks, logs, artifacts, agents, secrets, and services | [sdk/resources](https://docs.valkyrie.vals.ai/sdk/resources) |
| Every client method, model, and field | [reference/sdk](https://docs.valkyrie.vals.ai/reference/sdk) |
| Every SDK exception | [reference/sdk/errors](https://docs.valkyrie.vals.ai/reference/sdk/errors) |
| Tracker, login, and environment variables | [get-started/configuration](https://docs.valkyrie.vals.ai/get-started/configuration) |

Complete example scripts are in
[docs/sdk/examples](https://github.com/vals-ai/Valkyrie/tree/prod/docs/sdk/examples).

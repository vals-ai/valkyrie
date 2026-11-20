# Let accepted requests finish before stopping a service

Draining means a service refuses new requests while the ones it already accepted finish. A deployment controller keeps the old container alive and sends new requests to a replacement, and `DrainState` is the local switch it flips.

## Turn on draining

Create one SQLite store before starting the service workers:

```python
from benchmark_service.draining import DrainState

state = DrainState.create("/tmp/benchmark-drain.sqlite")
```

- Set `BENCHMARK_SERVICE_DRAIN_STATE_PATH=/tmp/benchmark-drain.sqlite` before starting the service, with a fresh local path for each container lifetime.
- Don't put the store on a network filesystem, or replace it while workers can own requests.
- Workers start closed to new requests, and `/health` stays available.
- A missing or invalid store stops startup. Without the variable, nothing changes.

## Control new requests

- Call [`set_admission`](DRAINING_REFERENCE.md) from a process that can reach the local store, with a higher `revision` for each new command.
- Bind reports to the task, the `store_id`, and the requested revision.
- Publish the replacement's address before closing the old workers. Callers need a client with the [retry rules](DRAINING_REFERENCE.md#retry-rules) to move over.
- Before stopping a container, wait for accepted requests to finish. Account for detached work and later calls from the same run.

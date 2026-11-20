# Credited generation

Valkyrie gives each task one clock for the time an agent spends generating. For most benchmarks that's just `agent_timeout`, but a benchmark that runs its agents in rounds (with its own setup and grading in between) can report exactly which intervals count, and can run several agents in one episode.

## The generation clock

```python
from benchmark_service import CreditedGeneration

credited_generation = CreditedGeneration(allowance_seconds=5 * 60 * 60, stage_protocol="valkyrie-stage/1")
```

- `RetrieveTaskResponse.credited_generation` is `None` by default. The Tracker keeps one cumulative active-generation clock per task either way.
- The allowance is the explicit credited allowance when there is one, then `agent_timeout`, and otherwise there's no generation deadline.
- An explicit credited allowance must be positive and finite.
- `stage_protocol=None` charges until the agent command exits. `"valkyrie-stage/1"` charges only the intervals the benchmark reports, and needs that finite credited allowance.
- The Tracker charges a finite base `B`, plus capped optional accounting credit `C`, across intervals. Setup, evaluation, and the gaps between intervals don't use up the clock.

### Accounting credit

- A configured accounting proxy counts neutral Gateway overhead, which becomes the credit `C`. A task is eligible only with configured proxy access, a resolved native Gateway URL and key, and an attested model.
- Ineligible tasks use the base clock, and credit never creates a deadline on its own. A failed counter read on an eligible task doesn't silently fall back.

### Deadlines

- In a finite non-staged run, a command that exits strictly before the effective cutoff can still collect its result and output through that cutoff plus the 20-second grace. Otherwise normal deadline stop handling applies.
- A separate 166-hour wall applies to every task, including those with no generation deadline.
- Daytona requests a 167-hour sandbox TTL and local Docker a 167-hour keepalive, while Modal caps sandbox lifetime at 24 hours.

### What task results record

The result fields this clock produces, such as `base_generation_allowance_seconds` and `generation_elapsed_seconds`, are described in [Monitor runs](https://docs.valkyrie.vals.ai/runs/monitor).

## Report stages with `valkyrie-stage/1`

With `stage_protocol="valkyrie-stage/1"`, the benchmark's own supervisor tells the Tracker when model-capable work starts and stops. The Tracker charges only the time in between, and neither model output nor benchmark code can assert a generation credit.

- The Tracker gives the agent command (or episode orchestrator) `VALKYRIE_STAGE_DIR=/run/valkyrie-stage`. That sandbox-private directory holds a 64-hex-character `key` and an empty `ack/` directory.
- During `setup_task`, upload the bytes from `benchmark_service.valkyrie_stage_source()` as `valkyrie_stage.py`. Import `StageReporter` from that standalone file inside your supervisor.
- The supervisor owns stage reporting. BEGIN comes before anything model-capable runs, and END only after its absence is confirmed.
- Don't give nested Docker workloads the reporter key or the stage directory.
- The `Agents` helper copies the orchestrator environment, `VALKYRIE_STAGE_DIR` included, into each selected-agent child. It doesn't isolate those children from the stage files.

### Begin and end

- `StageReporter().begin(container)` writes a frame to stdout and blocks until the Tracker ACKs it. Call it before launching the model-capable container.
- `end()` reports that same container once it and its nested workload are confirmed gone, and blocks until the ACK. Call it before evaluation or the next round.
- `container` is the Docker container name, or `None` for a workload in the agent command's own process tree.

For a supervisor that runs several Docker-backed rounds within one agent command:

```python
import subprocess
from valkyrie_stage import StageReporter

reporter = StageReporter()
for round_number in range(1, 11):
    container = f"generation-{round_number}"
    reporter.begin(container)  # Tracker ACK arrives before Docker starts.
    subprocess.run(["docker", "run", "--name", container, "benchmark-image", "generate"], check=True)
    subprocess.run(["docker", "rm", "-f", container], check=True)
    assert subprocess.run(["docker", "inspect", container], capture_output=True).returncode != 0
    reporter.end()  # Only after confirmed absence; await ACK before evaluation or next round.
    evaluate_round(round_number)
```

- On failure, don't send END without proof the container is gone.
- The Tracker owns timeouts and terminal cleanup, including inspecting nested containers on abort.

### Frame format

The reporter writes frames for you, so this only matters when debugging a stuck BEGIN or END.

```text
VALKYRIE-STAGE/1 <payload> <mac>
```

- `payload` is unpadded base64url of `{"seq":<int>,"event":"begin" or "end","container":<str or null>}`. `mac` is hex HMAC-SHA256 of the payload keyed by `key`, which is why agent output can never pass as a stage event.
- `seq` starts at 1 and alternates odd `begin` and even `end`, and the Tracker answers each with `ack/<seq>`.
- The reporter resends the same frame every 3 seconds until the ACK arrives, with no timeout. So a BEGIN or END blocks for as long as the Tracker is unreachable.

## Run several agents in one episode

An episode lets a benchmark-owned orchestrator run selected-agent turns in named slots, with one shared workspace and one shared budget. It needs `credited_generation` with `stage_protocol="valkyrie-stage/1"`.

```python
RetrieveTaskResponse.episode = Episode(command="exec python /opt/bench/orchestrator.py", parallel_agents=5)
```

- Without an episode, the Tracker runs the agent command directly. With one, it runs your `episode.command` in its place.
- After setup, the Tracker writes the selected agent bundle to `$VALKYRIE_STAGE_DIR/agent.json`. Its required keys are `run_cmd`, `continue_cmd`, `command_bindings`, `interrupt_grace_seconds`, `container_name`, `final_output`, `parallel_agents`, and `slots_root`.
- The orchestrator must be able to write under `slots_root`. With `container_name` set, its environment also needs the `docker` CLI on `PATH` for named-container cleanup.
- Set `isolate_agent_uid` to true to run slot commands as UID/GID 65534. The orchestrator then runs as root, only `<slots_root>/<slot>/agent` is writable by that UID, and every input the commands read must be readable by it.

### How turn commands are built

- The Tracker owns the raw run and continue command templates, and the `command_bindings` map (validated contract kwargs plus the actual task ID).
- The stage library renders each turn. It merges the turn's `problem_statement_path` and `slot_dir` (and a configured `container_name`) over that map, then substitutes matching tokens in the original template once per turn.
- `{problem_statement_path}` is the turn's observation file, and `{slot_dir}` is `<slots_root>/<slot>/agent`.
- A configured `container_name` requires `parallel_agents=1`. `{container_name}` is bound to that template, with `{slot}` filled in first.
- Without a configured container, a contract kwarg can still bind `{container_name}`. Otherwise it stays literal, like any other unbound token.
- Replacements are inserted as-is (braces included), and aren't shell-escaped.

### Slots and turns

- The library owns a single `StageReporter`, and starts each turn in a new process group.
- The orchestrator names the slots. They share one workspace and budget, and each keeps separate runtime, session, and output state.
- More than `parallel_agents` live turns are refused, and a second turn on a slot needs `continue_cmd`.
- Children inherit the orchestrator's working directory, stdout, and stderr.
- Overlapping slots count as one union interval on the clock.

### Model routing

- When `VALKYRIE_GENERATION_MODEL_GATEWAY_URL` is set, `Agents` copies the environment for each child and sets only its `MODEL_GATEWAY_URL` to that generation URL, just before launch.
- The orchestrator and evaluation keep the native `MODEL_GATEWAY_URL`. Without the generation URL, children inherit the native route too.
- A custom staged orchestrator must route generation the same way, keeping the native route for evaluation even when generation and evaluation overlap.

### Turn output

- `final_output` may contain `{slot_dir}`, which is replaced for each configured path before launch.
- The Tracker requires that placeholder when `parallel_agents > 1` and `final_output` is set, so parallel slots don't snapshot the same path.
- If a turn's `final_output` exists, it's snapshotted after the turn's descendants (and any named container) are confirmed gone.
- When the last active turn finishes, END is sent and ACKed before its snapshot. Other turns can be snapshotted while generation is still active.
- The file or directory is copied to `<slots_root>/<slot>/turns/<n>/`, where `n` starts at 1 per slot and a file keeps its basename.
- Snapshots sit beside the agent workspace, not inside it. The Tracker archives `slots_root` as the episode output.
- Directory snapshots follow symlinks and copy their targets. The tree can't contain cyclic links or include its own snapshot destination.
- Copy errors propagate without a `SlotResult`, and may leave partial files.

### Drive agents from the orchestrator

Upload `valkyrie_stage_source()` as `valkyrie_stage.py` during setup, then import `Agents` from it (Python 3.8 or newer):

```python
from valkyrie_stage import Agents, Exhausted

agents = Agents()
agents.start("first", "/workspace/observations/first.txt")
result = agents.wait_any()  # SlotResult(slot, reason, exit_code)
agents.start("first", "/workspace/observations/next.txt")  # Requires continue_cmd.
result = agents.stop("first")  # SIGINT grace (or immediate SIGKILL for null), then confirmed absence.
```

- `start` reports BEGIN and waits for the ACK before spawning the first slot.
- Each turn has its own process owner. It stops and reaps the turn's descendants, even ones that leave its process group, before the turn completes or END is sent.
- The root command's exit status stays `SlotResult.exit_code`.
- During `stop`, the configured SIGINT grace still applies to the group even if the root exits mid-stop. A null grace means an immediate SIGKILL.
- `stop` returns `stopped` when it stopped an active slot, `exited` if the slot had already finished, or `exhausted` if the shared limit won. `wait_any` returns `exited` or `exhausted`.

### When the budget runs out

- When the shared budget expires, the Tracker writes `$VALKYRIE_STAGE_DIR/exhausted` before its ACK.
- The library then stops every active turn and finishes their cleanup and END. Any later `start` raises `Exhausted`.
- If the notice arrives with the BEGIN ACK, no child starts and no END is sent.

### Launch failures

- For a known no-process launch failure after an ordinary BEGIN, `Agents` closes that attempt's pipes, reports the matching END, and raises the original error.
- Time charged between that BEGIN and END isn't refunded, and trying again is an explicit later `start`.
- Unknown owner readiness isn't proof the child is gone, so it doesn't trigger END.
- If a named container's absence can't be confirmed after an owner exits, the slot keeps its initial completion reason and pending result. A later public call resumes Docker cleanup before END, the output snapshot, and the completed result.

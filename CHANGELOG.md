# Changelog

## v2026.11.27

### SDK 0.45.3

- Allow filelock 4.x.

## v2026.11.24.2

### CLI

- Stop `valkyrie dataset check` flagging empty verifier `mcp_servers` or `skills_dir` settings.
- Rename `run start --slice` to `--task-range`. `--slice` still works with a warning.

### Tracker

- Fix large regrade starts timing out with a 504 after the run was already created.
- Retry tasks automatically when the agent reports a hung command, up to three times.
- Fix Daytona v2 tasks failing or leaking sandboxes on rate limits or dropped connections.

## v2026.11.24

### Tracker

- Allow regrades after the source run's agent-only secrets are deleted.
- Report missing regrade grading secrets before creating a run.
- Reject regrades of tasks that grade in the agent sandbox before creating a run.
- Report a JSON secret missing a required key as a 400 with the command that fixes it.
- Save each dataset task's grader output as `verifier_output.tar.gz` after grading.
- Use your org's copy of a saved service secret before the shared one.
- Regrade older runs from their saved agent output when a declared artifact wasn't saved.

## v2026.11.23

### CLI

- **Breaking:** Keep the default Tracker on `login`. Run `valkyrie tracker use NAME` to switch.
- Start SDK-backed commands and their `--help` faster.
- Deprecate `run fetch` and `run status` for `run show`, and `--connect` for `--follow`.
- Deprecate `run output`, `outputs` and `artifacts` in favor of `run download` and `run files`.
- Deprecate `run retag` and `run shed` in favor of `run update --project` and `--shed`.
- Add `--json` and `--jsonl` to every data command and deprecate `--format`.
- Deprecate `--agent-name`, `--benchmark-name`, `--order-by` and `--task-ids-file`.
- Old command and flag names still work and print a warning naming the replacement.
- Add global `--version`, `--tracker`, `--verbose` and `--no-color` options.
- Add `completion bash|zsh|fish|nu` with descriptions for each command and flag.
- Retry `run start`, `retry` and `resume` once when the Tracker returns 503.
- Show errors as a headline, a detail line and a `try:` command to run next.
- Show lists as aligned tables that truncate long text on narrow terminals.
- Add `--page-size` and `--wide` to `run list`, whose pages now fill the terminal.
- Show `--follow` progress in a live display that leaves the terminal clean on Ctrl+C.
- Use one color, glyph and label for each run status across the CLI and TUI.
- Answer confirmation prompts with the arrow keys, Enter, `y` or `n`.
- Add `valkyrie tracker` to save Trackers by name, with `local` built in.
- Install on Python 3.13 and 3.14.
- Accept dataset runs in `valkyrie run regrade` with `--dataset BENCHMARK/DATASET`.
- Accept old `--format` values in any case, such as `--format JSON`.
- Start a local Tracker from a checkout with `valk tracker start`.
- Show one line per task error in `run errors`, with `-v` for full tracebacks.
- Show one upload count while `dataset install` and `dataset push` upload tasks.
- Say `(no sign-in needed)` in `valk whoami` on a local Tracker.
- Keep the saved Tracker name from `--tracker` in suggested next commands.
- Show each task's last log time in `valk run tasks` and allow `--sort last_log_at`.

### SDK 0.45.1

- **Breaking:** Save logins under `XDG_CONFIG_HOME` when it is set. Run `valkyrie login` again.
- Load models and the HTTP client on first use, so `import valkyrie.sdk` is faster.
- Add the `dataset` task failure category.
- Add `last_log_at` to run task models.

### Tracker

- Stop retrying sandbox requests that the provider always refuses.
- Fix Daytona v2 runs failing to start the separate verifier sandbox.
- Fix Modal runs failing at setup when a task allows a wildcard hostname.
- Run dataset tasks whose image sets a non-root user.
- Accept dataset tasks that declare more than 20 artifacts.
- Read a task's working directory from Dockerfile variables, heredocs and build stages.
- Run the verifier from the image's working directory and let any user write its logs.
- Let agents and oracle solutions on dataset tasks import files from their own folder.
- Let agents reach their model when a dataset task restricts network access.
- Let agents create output folders under `/logs` on images that run as a non-root user.
- Let agents write to log folders their install created on images with a non-root user.
- Regrade a dataset run's graded tasks from their saved artifacts on another dataset version.
- Start tasks whose benchmark service returns a sandbox checkpoint as the source.
- Report missing required agent outputs as agent failures.
- Fix tasks restored from Daytona v2 memory checkpoints being rejected for fresh env.
- Fix Daytona v2 files checkpoints failing when taken right after a sandbox starts.
- Read `.env` at the checkout root as the local secrets file by default.
- Mark tasks errored when a local Tracker stops mid-run so they can be retried.
- Reject folder datasets with an invalid `task.toml` before the run starts.
- Hide image pulls and access logs in `valk tracker start` output.
- Add `make local-stop` and `make local-reset` for a local Tracker.
- Name the missing key and how to recover when `.env` lacks an agent secret.
- Fix Daytona v2 checkpoints failing while a just-started sandbox finishes booting.
- Fix Daytona v2 memory restores failing while the saved network policy is still applying.
- Use the newest project's Gateway key when retrying or resuming a retagged run.
- Grade tasks whose verifier starts in a folder that the agent's artifacts replace.
- Record the time of each task's latest log line.
- Add `snapshot` and `snapshot_target` to dataset task environments to start from snapshots.

## v2026.11.20

### CLI

- Add `valkyrie dataset check` to list Harbor dataset problems before pushing.
- Report MCP servers and skills folders the Tracker would refuse in `valkyrie dataset check`.

### SDK 0.42.0

- Add `valkyrie.sdk.harbor_dataset` to write and check Harbor datasets.
- Add `client.projects.budget()` and `client.costs.run()`.
- Let `HarborTask` declare MCP servers and a skills folder for the agent.

### Tracker

- Report a missing secret as not set, with the command to set it.
- Show clearer wording and a fix in the wrong-region secret error.
- Fix runs whose model calls its provider directly failing on a Model Gateway token error.
- Reject a companion model the Model Gateway doesn't serve before the run starts.
- Fix limit-request access with organization IDs and current administrator roles.
- Create and manage sandboxes itself for benchmark services that opt in.
- Add a `resource` field to each row from `/costs/sandbox-rows`.
- Add dataset task search, file listing, and text previews for exact versions.
- Let any benchmark service report run costs into the run's cost ledger.
- Allow self-serve creation of up to 10 admin organizations per login.
- Add `[verifier].models` so dataset verifiers call Gateway models with a grading token.
- Add browser ZIP uploads with a task preview before publishing a dataset version.
- Refuse exchanged user-key sessions after the key owner leaves the organization.
- Add `GET /projects/{handle}/budget` to read a project's limit and all-time spend.
- Add `GET /benchmarks/{id}/costs` to read a run's costs by type and resource.

## v2026.11.17

### SDK 0.39.0

- Add `project` to `FetchBenchmarksRequest` and three fields to `BenchmarkTableRow`.
- Add `client.projects.get` and `client.costs.by_project`.
- Add `client.costs.project_model` with the `ProjectModelCosts` and `MonthModelCost` models.
- Add a `project` argument to `client.runs.filter_options`.

### Tracker

- Support dataset scoring scripts (`metric.py`) in every deployment.
- Pass the MCP servers and the skills folder that a benchmark service reports to the agent.
- Add the `{task_dir}` launch command value for the task folder that a benchmark service reports.
- Add a `project` filter to the run list and the run list filter options.
- Show each run's project, hosted dataset version and whether it ran a task subset.
- Add `GET /projects/{handle}` to read one project by its handle.
- Fix runs failing to read organization secrets referenced in a task's verifier env.
- Add `GET /costs/project-model` for a project's model spend per month.
- Org members can file a project's limit raises, and platform admins approve or deny them.
- Fix the run list's dataset filter and options for runs on hosted datasets.
- Org Valkyrie Admins and platform admins can set a project's spend limit.
- Show why a secret can't be read and how to fix it in run and task errors.
- Scale demand-scaled benchmark services in and out by setting their autoscaling floor.
- Lets platform admins list and decide every org's limit raises.

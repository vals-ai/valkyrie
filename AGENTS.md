# Agent rules

Python files carry no comments or docstrings (`scripts/check_no_comments.py` enforces this in `make lint`). Public SDK
docstrings under `packages/valkyrie-sdk/src` stay because they are the published API docs and feed the SDK reference.
Private names, nested functions, and `_`-prefixed modules there carry none, except the public models in `models/_generated`.
Documentation lives in framework metadata instead:

- FastAPI routes: `description=` on the route decorator.
- Click commands, groups, options, and arguments: `help=`.
- Tests: `@pytest.mark.description("...")`.

## Writing descriptions

`scripts/check_descriptions.py` runs in `make lint`. It fails on a description over its cap (120 characters for option and
argument help, 200 for any other paragraph, 120 for a `Test cases:` bullet), a semicolon, an em dash, or a test
without a marker.

Write one present-tense sentence: subject, action, the condition that matters, and the observable result.
Leave out implementation details unless a caller can see them.

- Routes: start with a verb (`Return`, `List`, `Start`, `Stop`) and name the resource and its key inputs.
  Add any state change and any non-obvious status code in a second sentence ("Returns 404 when ...").
- Commands: start with an imperative verb and name the object ("Stop a run by its ID."). Groups say what they
  manage. Put an example after a blank line as `Example:` followed by the command indented four spaces, only
  when it shows a non-obvious flag.
- Options and arguments: say what the value is or does, plus its default when that isn't obvious.
- Tests: state the behavior the test protects as a fact ("A failed delete is retried, and one that never
  succeeds goes to Sentry."). Never open with Test, Verify, Check, Validate, or Exercise, and don't repeat the
  function name. Add a `Test cases:` list (`- Capitalized, ending with a period.`) only when the test covers
  several separate outcomes, and drop bullets that repeat the headline.

Style for every description:

- End with a period. Put identifiers in backticks. Write "ID", not "id", and "run", not "benchmark", for a run.
- No semicolons, em dashes, or parentheticals. Use two short sentences instead.

## Repository

- Branch from `dev` and open PRs against it. `prod` deploys and only takes PRs whose head is `dev`.
- `.gitattributes` `export-ignore` rules decide what `public-sync.yaml` copies from `prod` to the public
  `vals-ai/Valkyrie` mirror. Vals AI-only code and notes go under `internal/`.
- Layout: `src/valkyrie` is the CLI and TUI, `packages/valkyrie-sdk` the SDK, `services/tracker` the Tracker,
  `services/run-monitor` and `services/egress_proxy` the side services, `infra` and `internal/operations-infra` the CDK
  stacks, and `docs` the public site. `DEVELOPMENT.md` has the client import boundaries.
- Any change under `packages/valkyrie-sdk` needs an SDK version bump and ships a PyPI release. Keep it out of unrelated
  edits.
- SDK wire models under `packages/valkyrie-sdk/src/valkyrie/sdk/models/_generated` are generated from
  `services/tracker/openapi.json`. Never edit them by hand. After a Tracker model change, run
  `uv run python generate_openapi.py` in `services/tracker`, then `make sdk-models`. Docstrings and patches to spec
  fields go in `scripts/sdk/model_overlay.py`. SDK-only fields and validators go on a base class in
  `models/_validation.py` that the overlay names in `base`.

## Skills

Read the matching skill before starting. `.claude/skills/` links to the same files.

- `.agents/writing-tests/SKILL.md`: adding or changing tests, and choosing which suites prove a change.
- `.agents/tracker-repositories/SKILL.md`: Tracker code that reads or writes the database.
- `.agents/updating-docs/SKILL.md`: public docs, generated CLI and SDK references, and code that makes docs stale.
  Pages under `docs/` also follow `docs/AGENTS.md`.

## Checks

Run `make style`, `make typecheck`, and `make test` from the root before pushing. Tracker tests run from
`services/tracker` with the explicit targets in its `README.md`. Broad discovery there collects live tests that need
credentials. `infra` and `internal/operations-infra` run their own `make lint typecheck test`.

### Adding CI and tests

- Put a new test in the job that already runs that area. Add a new job only when the test needs a runner, service or setup that no current job has.
- Before adding a workflow step, check whether an existing step already builds, installs or caches what it needs, and reuse it.
- Scope new jobs with path filters so they only run when the code they test changes.
- Cache dependencies and build outputs (uv, pnpm, Docker layers) rather than rebuilding them in each job.
- Gate slow or live tests the way `tracker-integration-tests.yaml` does: run them on release PRs or manual re-runs, not on every push.
- In the PR description, say which job the new tests run in and roughly how many CI minutes they add.

## Pull requests

- Titles follow `type(scope): summary`, for example `fix(tracker): ...` or `docs: ...`.
- Descriptions follow `.github/PULL_REQUEST_TEMPLATE.md`. In Claude Code, `.claude/hooks/pr-format-check.py` checks
  `gh` PR bodies against it. Other agents match the template by hand.

## Tracker services

`services/tracker/src/tracker/services/` holds the logic that routes, the executor, and the scheduler call. Name modules
there as follows.

- Name a service after the domain noun it owns, in the plural: `runs`, `tasks`, `sandboxes`, `results`.
- Put verbs on functions, not in module names. Write `runs/retry.py` with `prepare_retry_state`, not `run_retry.py`.
- When a noun grows past one module, make it a package with one module per use case: `services/runs/start.py`,
  `stop.py`, `retry.py`, `finalize.py`.
- Put clients that wrap an external system (benchmark service, S3, sandbox provider) in `tracker/clients/`, not in
  `services/`.
- Do not use `_control`, `_orchestration`, `_execution`, `_teardown`, `_utils`, `_helpers`, `_manager`, or `_service` in
  module names. Do not add `utils` or `helpers` modules.
- Database queries go in `tracker/repositories/`, per `.agents/tracker-repositories/SKILL.md`.
- `make lint` runs `scripts/check_tracker_no_utils.py`, which fails on any `utils/` folder under `services/tracker`.

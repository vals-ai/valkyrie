# Changelog

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

# Changelog

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

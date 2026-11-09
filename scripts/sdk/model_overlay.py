from typing import Any, NotRequired, TypedDict


class ModelOverlay(TypedDict):
    module: str
    doc: NotRequired[str]
    schema: NotRequired[str]
    aliases: NotRequired[tuple[str, ...]]
    referenced: NotRequired[bool]
    parent: NotRequired[str]
    base: NotRequired[str]
    config: NotRequired[dict[str, str]]
    drop: NotRequired[tuple[str, ...]]
    fields: NotRequired[dict[str, dict[str, Any]]]


REQUEST = "pydantic.BaseModel"
DATETIME: dict[str, Any] = {"format": "date-time", "x-python-type": "valkyrie.sdk.models._base.UTCDateTime"}
FINITE_NON_NEGATIVE: dict[str, Any] = {"minimum": 0, "x-python-type": "pydantic.FiniteFloat"}

MODELS: dict[str, ModelOverlay] = {
    "AccessKeyResponse": {
        "module": "account",
        "doc": "A new access key; Tracker returns `key` only when it creates the key.",
        "fields": {"key": {"x-repr": False}},
    },
    "IdentityResponse": {
        "module": "account",
        "doc": "The organization and user behind a request's credential.",
        "fields": {"credential": {"x-enum-field-as-literal": True}},
    },
    "InviteResponse": {"module": "account", "doc": "An invitation to the current organization."},
    "MemberEntry": {
        "module": "account",
        "doc": "A member of the current organization, or someone it invited.",
        "fields": {"status": {"x-enum-field-as-literal": True}},
    },
    "MembersResponse": {"module": "account", "doc": "The current organization's members and pending invitations."},
    "OrganizationEntry": {
        "module": "account",
        "doc": "One organization your login belongs to; `current` marks the one requests act for.",
    },
    "OrganizationResponse": {"module": "account", "doc": "A created organization."},
    "OrganizationsResponse": {
        "module": "account",
        "doc": "Your login's email, its organizations, and the organizations that invited it.",
    },
    "AgentContractRequest": {
        "module": "agents",
        "schema": "AgentContractRequest-Input",
        "aliases": ("AgentContractRequest-Output",),
        "doc": "Agent definition submitted when starting a run.",
        "base": "valkyrie.sdk.models._validation.AgentContractRequestBase",
    },
    "AgentDownloadURLResponse": {"module": "agents", "doc": "Temporary URL for downloading an uploaded agent archive."},
    "AgentEntry": {"module": "agents", "doc": "One uploaded agent visible to the configured tenant."},
    "AgentTurns": {"module": "agents", "base": REQUEST},
    "AgentsResponse": {"module": "agents", "doc": "Uploaded agents visible to the configured tenant."},
    "OutputArtifact": {
        "module": "agents",
        "schema": "OutputArtifact-Input",
        "aliases": ("OutputArtifact-Output",),
        "doc": "One artifact copied from a sandbox after an agent run.",
        "base": "valkyrie.sdk.models._validation.OutputArtifactBase",
    },
    "RunArtifactDownloadResponse": {"module": "artifacts", "doc": "A temporary URL for a single run artifact."},
    "RunArtifactEntry": {"module": "artifacts", "doc": "An object stored beneath one run's artifact directory."},
    "RunArtifactsResponse": {"module": "artifacts", "doc": "One page of run artifacts."},
    "BenchmarkStatusEntry": {
        "module": "benchmarks",
        "doc": "Lightweight status and task counts for one run.",
        "fields": {"task_state_counts": {"default": {}}, "finished_at": {**DATETIME}},
    },
    "BenchmarkStatusResponse": {"module": "benchmarks", "doc": "Status entries returned for a group of runs."},
    "SingleBenchmarkResponse": {
        "module": "benchmarks",
        "doc": "Hosted run detail used by benchmark inspection views.",
        "fields": {"task_state_counts": {"default": {}}, "started_at": {**DATETIME}, "finished_at": {**DATETIME}},
    },
    "SingleTaskResponse": {
        "module": "benchmarks",
        "doc": "Detailed state and evaluation output for one task.",
        "fields": {"started_at": {**DATETIME}, "finished_at": {**DATETIME}},
    },
    "TaskArtifactsResponse": {"module": "benchmarks", "doc": "Temporary artifact and log links for one task."},
    "TaskBreakdown": {"module": "benchmarks", "doc": "Per-task runtime and generation accounting metadata."},
    "TaskSummary": {
        "module": "benchmarks",
        "doc": "Summary information for one task in a paginated run.",
        "fields": {"started_at": {**DATETIME}, "finished_at": {**DATETIME}},
    },
    "TasksResponse": {"module": "benchmarks", "doc": "Paginated task summaries for one run."},
    "AWSResources": {
        "module": "config",
        "config": {"frozen": "True"},
        "base": REQUEST,
        "fields": {"log_group": {"minLength": 1}},
    },
    "LocalResources": {
        "module": "config",
        "config": {"frozen": "True"},
        "base": "valkyrie.sdk.models._validation.LocalResourcesBase",
    },
    "DatasetTaskResponse": {
        "module": "datasets",
        "doc": "One task archive stored for a dataset.",
        "fields": {"last_modified": {**DATETIME}},
    },
    "ResourceResponse": {
        "module": "datasets",
        "doc": "What a benchmark and a dataset have in common.",
        "schema": "BenchmarkDefinitionResponse",
        "referenced": False,
        "fields": {"created_at": {**DATETIME}, "updated_at": {**DATETIME}, "archived_at": {**DATETIME}},
    },
    "BenchmarkDefinitionResponse": {
        "module": "datasets",
        "doc": "One saved benchmark.",
        "parent": "ResourceResponse",
        "fields": {"created_at": {**DATETIME}, "updated_at": {**DATETIME}, "archived_at": {**DATETIME}},
    },
    "BenchmarkDefinitionsResponse": {"module": "datasets", "doc": "The organization's active benchmarks, by name."},
    "DatasetResponse": {
        "module": "datasets",
        "doc": "One saved dataset and the name of the benchmark it belongs to.",
        "parent": "ResourceResponse",
        "fields": {"created_at": {**DATETIME}, "updated_at": {**DATETIME}, "archived_at": {**DATETIME}},
    },
    "DatasetTaskDownloadResponse": {
        "module": "datasets",
        "doc": "A task and a temporary URL to download its archive.",
        "parent": "DatasetTaskResponse",
        "fields": {"last_modified": {**DATETIME}},
    },
    "DatasetTasksResponse": {
        "module": "datasets",
        "doc": "A page of a dataset's tasks, and where the next page starts.",
    },
    "DatasetsResponse": {"module": "datasets", "doc": "A benchmark's active datasets, by name."},
    "LogEvent": {"module": "logs", "schema": "LogEventResponse", "doc": "One log message returned by the tracker."},
    "LogPage": {
        "module": "logs",
        "schema": "LogPageResponse",
        "doc": "A bounded page of log events and its continuation cursor.",
        "drop": ("available",),
    },
    "ProjectCreateRequest": {
        "module": "projects",
        "doc": "A new project: its immutable handle, name, spend type and subcategory, and optional customer and extra tags.\n"
        "\n"
        "``handle`` is the string runs are started with (lowercase letters, digits and hyphens, e.g. ``my-project``) and\n"
        "``name`` is the label people read (``My Project``). The tracker accepts only the subcategories in\n"
        "``SPEND_SUBCATEGORIES`` for the spend type and requires ``customer`` when the spend type is ``customer``.",
        "config": {"extra": "'forbid'"},
        "base": REQUEST,
        "fields": {"extra_tags": {"default": {}}},
    },
    "NewProjectRequest": {
        "module": "projects",
        "doc": "A project to create with its main key's budget and reason.",
        "config": {"extra": "'forbid'"},
        "parent": "ProjectCreateRequest",
        "fields": {
            "budget": {
                "replace": {"type": ["number", "null"], "format": "decimal", "exclusiveMinimum": 0, "default": None}
            }
        },
    },
    "ProjectResponse": {
        "module": "projects",
        "doc": "One saved project.",
        "fields": {"extra_tags": {"default": {}}, "created_at": {**DATETIME}, "archived_at": {**DATETIME}},
    },
    "ProjectsResponse": {"module": "projects", "doc": "The organization's projects, by handle."},
    "SpendType": {"module": "projects", "doc": "Why a run spends money."},
    "AWSBenchmarkArguments": {"module": "runs", "doc": "Stored arguments for an AWS run."},
    "AnalyzeBenchmarkRequest": {
        "module": "runs",
        "doc": "Wire payload used to trigger Docent analysis for a run.",
        "base": REQUEST,
    },
    "AverageTaskBreakdown": {"module": "runs", "doc": "Average timing breakdown across completed tasks."},
    "BenchmarkStatus": {"module": "runs", "doc": "Lifecycle states for a benchmark run."},
    "BenchmarkTableRow": {
        "module": "runs",
        "doc": "Summary row returned by the run-list endpoint.",
        "fields": {"task_state_counts": {"default": {}}, "started_at": {**DATETIME}, "finished_at": {**DATETIME}},
    },
    "DatasetVersion": {"module": "runs", "doc": "Exact dataset release saved for a run."},
    "FailureCategory": {"module": "runs", "doc": "Coarse classification of a task or run failure."},
    "FetchBenchmarkMetadataResponse": {
        "module": "runs",
        "doc": "Stored launch metadata for one run.",
        "base": "valkyrie.sdk.models._validation.StoredArgumentsBase",
    },
    "FetchBenchmarksResponse": {"module": "runs", "doc": "Page of benchmark runs."},
    "FilterOptionsResponse": {
        "module": "runs",
        "doc": "Values available for run filters in the caller's organization.",
    },
    "FinalEvaluation": {
        "module": "runs",
        "doc": "Final aggregate evaluation stored for a run.",
        "base": "valkyrie.sdk.models._validation.FinalEvaluationBase",
        "drop": ("id",),
        "fields": {"properties": {"default": {}}, "org_id": {"replace": {"type": "string"}}},
    },
    "FinalViewResponse": {
        "module": "runs",
        "doc": "Inline final results for a benchmark run.",
        "base": "valkyrie.sdk.models._validation.StoredArgumentsBase",
    },
    "LocalBenchmarkArguments": {"module": "runs", "doc": "Stored arguments for a local run."},
    "Order": {"module": "runs", "doc": "Sort order for run listings."},
    "RetryMode": {"module": "runs", "doc": "Supported retry strategies."},
    "StatusResponse": {
        "module": "runs",
        "doc": "Generic status response.",
        "schema": "StopBenchmarkResponse",
        "referenced": False,
    },
    "RetryOrResumeBenchmarkResponse": {
        "module": "runs",
        "doc": "Response returned after retrying or resuming a run.",
        "parent": "StatusResponse",
    },
    "RunMonitoring": {
        "module": "runs",
        "doc": "Opt-in external monitoring under the monitor's built-in rules.",
        "config": {"extra": "'forbid'"},
        "base": REQUEST,
    },
    "RunMonitoringRules": {"module": "runs", "doc": "Monitor rules the benchmark declares for its runs."},
    "S3UploadResultsResponse": {"module": "runs", "doc": "URLs returned when final results are uploaded to S3."},
    "ShedBenchmarkRequest": {
        "module": "runs",
        "doc": "A concurrency limit below the run's current one; ``dry_run`` only previews the tasks to stop.",
        "fields": {"concurrency": {"x-strict": True}},
        "base": REQUEST,
    },
    "ShedBenchmarkResponse": {
        "module": "runs",
        "doc": "The new limit and the newest tasks stopped (or, on a dry run, selected) to get under it.",
    },
    "StartBenchmarkRequest": {
        "module": "runs",
        "doc": "Wire payload used to start a benchmark run.",
        "config": {"extra": "'forbid'"},
        "fields": {
            "priority": {"x-strict": True},
            "service_headers": {"x-repr": False, "default": {}},
            "environment": {"x-enum-field-as-literal": True},
            "sandbox_provider": {"replace": {"anyOf": [{"type": "string"}, {"type": "null"}], "default": None}},
        },
        "base": "valkyrie.sdk.models._validation.StartBenchmarkRequestBase",
        "drop": (
            "properties",
            "custom_benchmark_service",
            "sandbox_provider_secret_name",
            "service_auth_header_name",
            "service_auth_secret_name",
        ),
    },
    "StartBenchmarkResponse": {"module": "runs", "doc": "Response returned after starting a run."},
    "StopBenchmarkResponse": {
        "module": "runs",
        "doc": "Response returned after stopping a run.",
        "parent": "StatusResponse",
    },
    "TaskStatus": {"module": "runs", "doc": "Lifecycle states for one benchmark task."},
    "UpdateBenchmarkConcurrencyRequest": {
        "module": "runs",
        "doc": "A positive concurrency limit for an active run.",
        "fields": {"concurrency": {"x-strict": True}},
        "base": REQUEST,
    },
    "UpdateBenchmarkConcurrencyResponse": {
        "module": "runs",
        "doc": "The run state after changing its concurrency limit.",
    },
    "SchedulerActiveEntryResponse": {
        "module": "scheduler",
        "doc": "One task currently building, running, or evaluating.",
        "fields": {"started_at": {**DATETIME}},
    },
    "SchedulerActiveStatus": {"module": "scheduler", "doc": "Task states included in the scheduler's active entries."},
    "SchedulerCapacityDomainResponse": {
        "module": "scheduler",
        "doc": "Capacity scoped to one provider target and sandbox class.",
    },
    "SchedulerCapacityResponse": {
        "module": "scheduler",
        "doc": "CPU, memory, disk, and optional GPU capacity for one target and sandbox class.",
    },
    "SchedulerOverviewResponse": {
        "module": "scheduler",
        "doc": "Scheduler totals and bounded task entries for the authenticated organization.",
        "fields": {
            "waiting_next_offset": {"description": "Next waiting offset, or null when this list is exhausted."},
            "active_next_offset": {"description": "Next active offset, or null when this list is exhausted."},
            "observed_at": {**DATETIME},
        },
    },
    "SchedulerPoolResponse": {
        "module": "scheduler",
        "doc": "Waiting task count and optional capacity for one sandbox admission pool.",
    },
    "SchedulerResourceCapacityResponse": {
        "module": "scheduler",
        "doc": "Available and total capacity for one resource.",
        "fields": {"available": FINITE_NON_NEGATIVE, "total": FINITE_NON_NEGATIVE},
    },
    "SchedulerSummaryResponse": {
        "module": "scheduler",
        "doc": "Total waiting and active task counts, independent of entry limits.",
    },
    "SchedulerWaitingEntryResponse": {
        "module": "scheduler",
        "doc": "One waiting task with its priority and position within its pool.",
        "fields": {"enqueued_at": {**DATETIME}},
    },
    "SecretEntry": {
        "module": "secrets",
        "doc": "One stored secret: its short name, and the reference runs use to read it.",
    },
    "SecretsResponse": {"module": "secrets", "doc": "Secrets stored for the configured tenant, without their values."},
    "BenchmarkServiceCatalogResponse": {
        "module": "services",
        "doc": "Benchmark services visible to the configured tenant.",
    },
    "BenchmarkServiceEntry": {
        "module": "services",
        "doc": "One benchmark service available through the hosted catalog.",
        "base": "valkyrie.sdk.models._validation.BenchmarkServiceEntryBase",
    },
    "BenchmarkServiceHealth": {"module": "services", "doc": "Health-check result for one benchmark service."},
    "BenchmarkServicesRequest": {
        "module": "services",
        "doc": "Benchmark services to health-check through Tracker.",
        "base": REQUEST,
        "fields": {"services": {"default": []}},
    },
    "BenchmarkServicesResponse": {"module": "services", "doc": "Health-check results for benchmark services."},
    "FetchBenchmarkTasksRequest": {
        "module": "services",
        "doc": "Wire payload for discovering task IDs from a benchmark service.",
        "fields": {"service_headers": {"x-repr": False, "default": {}}},
        "base": REQUEST,
        "drop": ("custom_benchmark_service",),
    },
    "TaskIDsResponse": {
        "module": "services",
        "schema": "VerifyTaskIdsResponse",
        "doc": "Task IDs returned by a benchmark service through Tracker.",
    },
    "ProvidersResponse": {
        "module": "settings",
        "doc": "The organization's secret name for each sandbox provider; values are never returned.",
    },
    "RunSettingsResponse": {
        "module": "settings",
        "doc": "Your default sandbox provider and saved benchmark services; empty for access keys without a user.",
    },
    "SavedBenchmarkService": {
        "module": "settings",
        "doc": "A benchmark service URL saved for your runs, and the secret Tracker sends to it as a header.",
    },
}

from enum import StrEnum


class LogEvent(StrEnum):
    TASK_ATTEMPT_STARTED = "task.attempt.started"
    TASK_RETRY = "task.retry"
    TASK_FAILED = "task.failed"
    TASK_CANCELLED = "task.cancelled"
    TASK_UNHANDLED_ERROR = "task.unhandled_error"
    AGENT_RUN_COMPLETE = "agent.run.complete"
    TASK_EVALUATION_START = "task.evaluation.start"
    SANDBOX_CREATE = "sandbox.create"
    SANDBOX_CREATE_FAILED = "sandbox.create.failed"
    SANDBOX_DELETE = "sandbox.delete"
    RUN_FAILED = "run.failed"

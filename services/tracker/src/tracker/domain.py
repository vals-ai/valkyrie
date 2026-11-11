from enum import Enum


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    BUILDING = "BUILDING"
    IN_PROGRESS = "IN_PROGRESS"
    EVALUATING = "EVALUATING"
    STOPPED = "STOPPED"
    FINISHED = "FINISHED"
    ERROR = "ERROR"


class BenchmarkStatus(str, Enum):
    IN_PROGRESS = "IN_PROGRESS"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FINISHED = "FINISHED"
    ERROR = "ERROR"


class SpendType(str, Enum):
    DEVELOPMENT = "development"
    INTERNAL = "internal"
    CUSTOMER = "customer"


class ExecutorReleaseStatus(str, Enum):
    CANDIDATE = "CANDIDATE"
    ACTIVE = "ACTIVE"
    DRAINING = "DRAINING"
    RETIRED = "RETIRED"


class ExecutorDispatchKind(str, Enum):
    START = "START"
    RETRY = "RETRY"
    RESUME = "RESUME"


class DocentReadingStatus(str, Enum):
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    ERROR = "ERROR"
    DONE = "DONE"


class AgentCausedExitReason(str, Enum):
    TIMEOUT = "TIMEOUT"
    OS_KILLED = "OS_KILLED"


class FailureCategory(str, Enum):
    INFRASTRUCTURE = "infrastructure"
    BENCHMARK_SERVICE = "benchmark_service"
    AGENT = "agent"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


FailureCategory.__doc__ = "Coarse classification of a task or run failure, decided where the failure is caught."


class FailurePhase(str, Enum):
    ADMISSION = "admission"
    SANDBOX = "sandbox"
    SETUP = "setup"
    AGENT = "agent"
    EVALUATION = "evaluation"


class RetryMode(str, Enum):
    AUTO = "auto"
    FROM_SCRATCH = "from_scratch"

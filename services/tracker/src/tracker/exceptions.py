from typing import TYPE_CHECKING

from pydantic import ValidationError

if TYPE_CHECKING:
    from tracker.domain import FailurePhase


class TrackerServiceError(Exception):
    include_cause_in_task_error = True
    transient: bool | None = None

    def __init__(self, *args: object, transient: bool | None = None) -> None:
        super().__init__(*args)
        if transient is not None:
            self.transient = transient


class CreditedTaskWallTimeExceeded(TrackerServiceError):
    pass


class ExecutionAuthorityRevoked(TrackerServiceError):
    pass


class BundlerError(Exception):
    pass


class ContractValidationError(ValueError):
    FORMATTERS: dict[str, str] = {
        "missing": "'{field}' is required but was not provided",
        "literal_error": "'{field}' has an invalid value. {msg}",
    }

    def __init__(self, validation_error: ValidationError, context: str = "") -> None:
        messages: list[str] = []
        for err in validation_error.errors():
            field = ".".join(str(loc) for loc in err["loc"])
            template = self.FORMATTERS.get(err["type"])
            if template:
                messages.append(f"  - {template.format(field=field, msg=err['msg'])}")
            else:
                messages.append(f"  - '{field}': {err['msg']}")

        detail = "\n".join(messages)
        super().__init__(f"{context}\n{detail}" if context else detail)


class SandboxError(TrackerServiceError):
    def __str__(self) -> str:
        return "Sandbox error: " + super().__str__()


class SandboxImageBuildError(SandboxError):
    transient = False


class InvalidSandboxConfigurationError(SandboxError):
    transient = False


class OutputArtifactError(TrackerServiceError):
    transient = False

    def __init__(self, *args: object, missing: bool = False, transient: bool | None = None) -> None:
        super().__init__(*args, transient=transient)
        self.missing = missing

    def __str__(self) -> str:
        return "Output artifact error: " + super().__str__()


class AgentRunFailedError(SandboxError):
    transient = False


class EpisodeOrchestratorFailedError(AgentRunFailedError):
    pass


class ControlledGenerationError(SandboxError):
    pass


class ControlledGenerationTerminationUnconfirmedError(ControlledGenerationError):
    pass


class GenerationTerminationUnconfirmedError(ControlledGenerationTerminationUnconfirmedError):
    pass


class SandboxDNSResolutionError(SandboxError):
    transient = True


class SandboxSetupError(SandboxError):
    transient = True
    phase: "FailurePhase | None" = None
    cause_code: str | None = None


class DependencySetupExhaustedError(SandboxSetupError):
    pass


class SSLConnectionError(SandboxSetupError):
    pass


class S3Error(TrackerServiceError):
    def __str__(self) -> str:
        return "S3 error: " + super().__str__()


class CloudWatchError(TrackerServiceError):
    def __str__(self) -> str:
        return "CloudWatch error: " + super().__str__()


class LambdaError(TrackerServiceError):
    def __str__(self) -> str:
        return "Lambda error: " + super().__str__()


class SecretsError(TrackerServiceError):
    include_cause_in_task_error = False

    def __str__(self) -> str:
        return "Secret error: " + super().__str__()


class SecretNotFoundError(SecretsError):
    pass


class ProjectHandleTakenError(TrackerServiceError):
    def __init__(self, handle: str) -> None:
        super().__init__(f"Project '{handle}' already exists")


class ProjectNameTakenError(TrackerServiceError):
    def __init__(self, holder_name: str, holder_handle: str) -> None:
        super().__init__(
            f"A project named '{holder_name}' already exists (handle {holder_handle}). Choose another name."
        )


class BenchmarkNameTakenError(TrackerServiceError):
    def __init__(self, name: str) -> None:
        super().__init__(f"Benchmark '{name}' already exists")


class BenchmarkHasDatasetsError(TrackerServiceError):
    def __init__(self, name: str) -> None:
        super().__init__(f"Benchmark '{name}' still has active datasets")


class ResourceArchivedError(TrackerServiceError):
    def __init__(self, kind: str, name: str) -> None:
        super().__init__(f"{kind} '{name}' is archived; restore it first")


class ManifestMismatchError(TrackerServiceError): ...


class TagExistsError(TrackerServiceError):
    def __init__(self, tag: str) -> None:
        super().__init__(
            f"Tag '{tag}' already points at another version. Move it with `valkyrie dataset tag --force`, or publish under a new tag"
        )


class AmbiguousDigestError(TrackerServiceError):
    def __init__(self, prefix: str) -> None:
        super().__init__(f"The digest prefix '{prefix}' matches more than one version")


class DatasetNameTakenError(TrackerServiceError):
    def __init__(self, name: str) -> None:
        super().__init__(f"Dataset '{name}' already exists")

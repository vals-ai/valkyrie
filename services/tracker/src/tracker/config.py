import math
from enum import Enum
import os
from urllib.parse import urlsplit

from dotenv import load_dotenv

from tracker.logging import configure_logging
from tracker.outbound_security import validate_benchmark_name

load_dotenv()
configure_logging()


_BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE: str = os.environ.get("BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE", "local")
_CLOUDMAP_PORT = 8001
_BENCHMARK_SERVICE_BASE_URL: str | None = os.environ.get("BENCHMARK_SERVICE_BASE_URL")
BENCHMARK_CATALOG_URL = os.environ.get("BENCHMARK_CATALOG_URL", "").rstrip("/")
PASSKEY_ORGS = frozenset(name.strip() for name in os.environ.get("TRACKER_PASSKEY_ORGS", "").split(",") if name.strip())
GATEWAY_KEY_ORGS = frozenset({"vals.ai", *PASSKEY_ORGS})
ORG_RUN_LIMITS_PARAMETER = os.environ.get("ORG_RUN_LIMITS_PARAMETER") or None


class ProjectGatewayKeysMode(str, Enum):
    MANUAL = "manual"
    PROVISION = "provision"
    AUTOMATIC = "automatic"


def project_gateway_keys_mode() -> ProjectGatewayKeysMode:
    return ProjectGatewayKeysMode(os.environ.get("MODEL_GATEWAY_PROJECT_KEYS_MODE", "manual"))


class BenchmarkServiceDestination(Enum):
    HOSTED = "hosted"
    CUSTOM = "custom"


def create_benchmark_service_url(benchmark_name: str) -> str:
    benchmark_name = validate_benchmark_name(benchmark_name)

    if _BENCHMARK_SERVICE_BASE_URL:
        return f"https://{benchmark_name}.{_BENCHMARK_SERVICE_BASE_URL}"

    return f"http://{benchmark_name}.{_BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE}:{_CLOUDMAP_PORT}"


def _normalized_origin(url: str) -> tuple[str, str, int]:
    parsed = urlsplit(url)
    assert parsed.hostname is not None
    default_port = 443 if parsed.scheme == "https" else 80
    port = parsed.port if parsed.port is not None else default_port
    return parsed.scheme, parsed.hostname.lower(), port


def classify_benchmark_service_destination(
    benchmark_name: str,
    service_url: str | None,
) -> BenchmarkServiceDestination:
    try:
        hosted_url = create_benchmark_service_url(benchmark_name)
    except ValueError:
        if service_url is not None:
            return BenchmarkServiceDestination.CUSTOM
        raise
    effective_url = service_url or hosted_url
    if _normalized_origin(effective_url) == _normalized_origin(hosted_url):
        return BenchmarkServiceDestination.HOSTED
    return BenchmarkServiceDestination.CUSTOM


def _positive_int_setting(name: str, default: int) -> int:
    value = int(os.environ.get(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")

    return value


def _positive_float_setting(name: str) -> float:
    value = float(os.environ[name])
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number")

    return value


EXTERNAL_SERVICE_GATEWAY_URL = os.environ.get("EXTERNAL_SERVICE_GATEWAY_URL", "").rstrip("/") or None
EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN = os.environ.get("EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN")
EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS = (
    _positive_float_setting("EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS")
    if EXTERNAL_SERVICE_GATEWAY_URL is not None
    else None
)


def project_gateway_configuration() -> tuple[str, str]:
    url = os.environ["MODEL_GATEWAY_URL"]
    machine_api_key = os.environ["MODEL_GATEWAY_MACHINE_API_KEY"]
    if not url or not machine_api_key:
        raise ValueError("MODEL_GATEWAY_URL and MODEL_GATEWAY_MACHINE_API_KEY must be nonempty")
    return url, machine_api_key


def project_service_configuration() -> tuple[str, str]:
    url = os.environ["PROJECT_SERVICE_URL"]
    token = os.environ["PROJECT_SERVICE_TOKEN"]
    if not url or not token:
        raise ValueError("PROJECT_SERVICE_URL and PROJECT_SERVICE_TOKEN must be nonempty")
    return url, token


RUN_ACCESS_KEY_TTL_SECONDS = _positive_int_setting("RUN_ACCESS_KEY_TTL_SECONDS", 14 * 24 * 3600)
AGENT_UPLOAD_MAX_BYTES = _positive_int_setting("AGENT_UPLOAD_MAX_BYTES", 1073741824)
DATASET_TASK_UPLOAD_MAX_BYTES = _positive_int_setting("DATASET_TASK_UPLOAD_MAX_BYTES", 4294967296)
AGENT_ARCHIVE_MAX_EXPANDED_BYTES = _positive_int_setting("AGENT_ARCHIVE_MAX_EXPANDED_BYTES", 5368709120)
AGENT_ARCHIVE_MAX_ENTRIES = _positive_int_setting("AGENT_ARCHIVE_MAX_ENTRIES", 100000)

AWS_S3_BUCKET = os.environ.get("AWS_S3_BUCKET", "agentic-harness")
AWS_SHARED_SECRET_TENANTS = os.environ.get("AWS_SHARED_SECRET_TENANTS", "")
METRIC_RUNNER_FUNCTION = os.environ.get("METRIC_RUNNER_FUNCTION", "")
AWS_TENANT_ACCESS_ROLE_ARN = os.environ.get("AWS_TENANT_ACCESS_ROLE_ARN")
AWS_DEPLOYMENT_ACCOUNT_ID = os.environ.get("AWS_DEPLOYMENT_ACCOUNT_ID")
AWS_DEPLOYMENT_REGION = os.environ.get("AWS_DEPLOYMENT_REGION") or os.environ.get("AWS_REGION")
AWS_DEPLOYMENT_S3_BUCKET = os.environ.get("AWS_DEPLOYMENT_S3_BUCKET")
AWS_TENANT_BUCKET_PREFIX = os.environ.get("AWS_TENANT_BUCKET_PREFIX")
AWS_DEPLOYMENT_LOG_GROUP = os.environ.get("AWS_DEPLOYMENT_LOG_GROUP")
AWS_DEPLOYMENT_LOG_RETENTION_DAYS = os.environ.get("AWS_DEPLOYMENT_LOG_RETENTION_DAYS")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")


def build_database_url() -> str:
    if url := os.environ.get("DATABASE_URL"):
        return url
    db_user = os.environ.get("DB_USERNAME", "tracker")
    db_pass = os.environ.get("DB_PASSWORD", "tracker")
    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = os.environ.get("DB_PORT", "5432")
    db_name = os.environ.get("DB_NAME", "tracker")
    return f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"


AUTH_REQUIRED = os.environ.get("AUTH_REQUIRED", "false").lower() == "true"
SELF_SERVE_ORGS_ENABLED = os.environ.get("SELF_SERVE_ORGS_ENABLED", "false").lower() == "true"
VALKYRIE_STAGE = os.environ.get("VALKYRIE_STAGE", "")
SANDBOX_QUEUE_ENABLED = os.environ.get("SANDBOX_QUEUE_ENABLED", "false").lower() == "true"
SANDBOX_BUILD_RATE_PER_MINUTE = int(os.environ.get("SANDBOX_BUILD_RATE_PER_MINUTE", "0"))
DESCOPE_PROJECT_ID = os.environ.get("DESCOPE_PROJECT_ID", "")
DESCOPE_MANAGEMENT_KEY = os.environ.get("DESCOPE_MANAGEMENT_KEY", "")
DESCOPE_BASE_URL = os.environ.get("DESCOPE_BASE_URL", "https://api.descope.com").rstrip("/")
DESCOPE_CLI_CLIENT_ID = os.environ.get("DESCOPE_CLI_CLIENT_ID", "")
DESCOPE_ORG_ADMIN_ROLE = os.environ.get("DESCOPE_ORG_ADMIN_ROLE", "Valkyrie Admin")

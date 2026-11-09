import re
from dataclasses import replace

from fastapi import HTTPException

from tracker import config
from tracker.aws.clients import AWSClientProvider, TenantRoleAWSClientProvider
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.database.models import Org
from tracker.secret_access import may_use_shared_secrets


class ManagedAWSError(ValueError):
    pass


class ManagedAWSConfigurationError(ManagedAWSError):
    pass


class ManagedAWSStorageUnavailableError(ManagedAWSError):
    pass


def _managed_resources(s3_bucket: str) -> AWSResources:
    missing = [
        name
        for name, value in (
            ("AWS_DEPLOYMENT_REGION", config.AWS_DEPLOYMENT_REGION),
            ("AWS_DEPLOYMENT_LOG_GROUP", config.AWS_DEPLOYMENT_LOG_GROUP),
            ("AWS_DEPLOYMENT_LOG_RETENTION_DAYS", config.AWS_DEPLOYMENT_LOG_RETENTION_DAYS),
        )
        if not value
    ]
    if missing:
        raise ManagedAWSConfigurationError(f"Managed AWS configuration is missing {', '.join(missing)}")

    try:
        retention_days = int(config.AWS_DEPLOYMENT_LOG_RETENTION_DAYS or "")
    except ValueError as exc:
        raise ManagedAWSConfigurationError("AWS_DEPLOYMENT_LOG_RETENTION_DAYS must be an integer") from exc
    if retention_days <= 0:
        raise ManagedAWSConfigurationError("AWS_DEPLOYMENT_LOG_RETENTION_DAYS must be positive")

    assert config.AWS_DEPLOYMENT_REGION is not None
    assert config.AWS_DEPLOYMENT_LOG_GROUP is not None
    return AWSResources(
        region=config.AWS_DEPLOYMENT_REGION,
        s3_bucket=s3_bucket,
        log_group=config.AWS_DEPLOYMENT_LOG_GROUP,
        log_retention_days=retention_days,
    )


def _deployment_account_id() -> str:
    account_id = config.AWS_DEPLOYMENT_ACCOUNT_ID or ""
    if not re.fullmatch(r"[0-9]{12}", account_id):
        raise ManagedAWSConfigurationError("AWS_DEPLOYMENT_ACCOUNT_ID must be a 12-digit AWS account ID")

    return account_id


def _org_bucket(org: Org) -> str | None:
    if org.storage_key:
        return org.storage_key
    if not config.AUTH_REQUIRED:
        return config.AWS_DEPLOYMENT_S3_BUCKET
    return None


def deployment_aws_runtime(org: Org, properties: AWSResources | None = None) -> AWSRuntime:
    bucket = _org_bucket(org)
    if not bucket:
        raise ManagedAWSConfigurationError("This organization has no storage bucket")

    if properties is None:
        resources = _managed_resources(bucket)
    elif properties.s3_bucket != bucket:
        raise ManagedAWSStorageUnavailableError("This run's storage is no longer available.")
    else:
        resources = properties
    return AWSRuntime(
        resources=resources,
        clients=_client_provider(org, resources.region),
        expected_bucket_owner=_deployment_account_id(),
    )


def _client_provider(org: Org, region: str) -> AWSClientProvider:
    if not config.AWS_TENANT_ACCESS_ROLE_ARN:
        return AWSClientProvider(region)
    return TenantRoleAWSClientProvider(
        region=region,
        role_arn=config.AWS_TENANT_ACCESS_ROLE_ARN,
        org_id=str(org.id),
        shared_secrets=may_use_shared_secrets(org.name),
    )


def _http_deployment_runtime(org: Org, properties: AWSResources | None = None) -> AWSRuntime:
    try:
        return deployment_aws_runtime(org, properties)
    except ManagedAWSStorageUnavailableError as exc:
        raise HTTPException(status_code=410, detail=str(exc)) from exc
    except ManagedAWSConfigurationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def resolve_start_aws_runtime(
    org: Org,
    properties: AWSResources | None = None,
) -> AWSRuntime:
    runtime = _http_deployment_runtime(org)
    runtime = replace(
        runtime,
        resources=replace(
            runtime.resources,
            log_group=f"{runtime.resources.log_group.rstrip('/')}/tenants/{org.id}",
        ),
    )
    if properties is not None and properties != runtime.resources:
        raise HTTPException(
            status_code=400, detail="Managed run properties must match the organization's AWS resources"
        )
    return runtime


def resolve_run_aws_runtime(
    *,
    org: Org,
    properties: AWSResources | None = None,
) -> AWSRuntime:
    return _http_deployment_runtime(org, properties)


def resolve_run_metadata_aws_runtime(
    *,
    org: Org,
    properties: AWSResources | None = None,
) -> AWSRuntime | None:
    try:
        return deployment_aws_runtime(org, properties)
    except ManagedAWSStorageUnavailableError:
        return None
    except ManagedAWSConfigurationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def resolve_org_aws_runtime(org: Org) -> AWSRuntime:
    return _http_deployment_runtime(org)

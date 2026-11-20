from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypedDict

from botocore.exceptions import ClientError

from tracker.aws.tags import ORG_ID_TAG

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.literals import TransitionDefaultMinimumObjectSizeType
    from mypy_boto3_s3.type_defs import (
        CORSRuleTypeDef,
        CORSRuleUnionTypeDef,
        LifecycleRuleTypeDef,
        LifecycleRuleUnionTypeDef,
    )


def upload_lifecycle_rules() -> list["LifecycleRuleTypeDef"]:
    return [
        {
            "ID": "expire-dataset-upload-archives",
            "Status": "Enabled",
            "Filter": {"Prefix": "dataset-uploads/"},
            "Expiration": {"Days": 2},
            "NoncurrentVersionExpiration": {"NoncurrentDays": 1},
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
        },
        {
            "ID": "remove-dataset-upload-delete-markers",
            "Status": "Enabled",
            "Filter": {"Prefix": "dataset-uploads/"},
            "Expiration": {"ExpiredObjectDeleteMarker": True},
        },
    ]


def upload_cors_rule(origins: list[str]) -> "CORSRuleTypeDef":
    return {
        "ID": "dataset-uploads",
        "AllowedMethods": ["POST"],
        "AllowedOrigins": origins,
        "AllowedHeaders": ["content-type"],
        "MaxAgeSeconds": 300,
    }


@dataclass(frozen=True)
class BucketConfiguration:
    lifecycle: Sequence["LifecycleRuleUnionTypeDef"]
    cors: Sequence["CORSRuleUnionTypeDef"]
    transition_minimum: "TransitionDefaultMinimumObjectSizeType | None" = None


@dataclass(frozen=True)
class UploadStoragePlan:
    bucket: str
    account_id: str
    org_id: str
    before: BucketConfiguration
    after: BucketConfiguration


class BucketTarget(TypedDict):
    Bucket: str
    ExpectedBucketOwner: str


def _read(s3: "S3Client", bucket: str, account_id: str, org_id: str) -> BucketConfiguration:
    target: BucketTarget = {"Bucket": bucket, "ExpectedBucketOwner": account_id}
    tags = s3.get_bucket_tagging(**target)["TagSet"]
    if not any(tag["Key"] == ORG_ID_TAG and tag["Value"] == org_id for tag in tags):
        raise ValueError("The bucket does not have the expected organization tag")
    minimum: TransitionDefaultMinimumObjectSizeType | None = None
    try:
        lifecycle = s3.get_bucket_lifecycle_configuration(**target)
        rules = lifecycle["Rules"]
        minimum = lifecycle.get("TransitionDefaultMinimumObjectSize")
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "NoSuchLifecycleConfiguration":
            raise
        rules = []
    try:
        cors = s3.get_bucket_cors(**target)["CORSRules"]
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "NoSuchCORSConfiguration":
            raise
        cors = []
    return BucketConfiguration(rules, cors, minimum)


def plan_upload_storage(
    s3: "S3Client", bucket: str, account_id: str, org_id: str, origins: list[str]
) -> UploadStoragePlan:
    before = _read(s3, bucket, account_id, org_id)
    if before.lifecycle and before.transition_minimum is None:
        raise ValueError("S3 did not return the existing transition size policy. No replacement is safe to infer")
    managed = upload_lifecycle_rules()
    managed_ids = {rule.get("ID") for rule in managed}
    for rule in before.lifecycle:
        if rule.get("ID") in managed_ids and not (
            rule.get("Filter") == {"Prefix": "dataset-uploads/"}
            or ("Filter" not in rule and rule.get("Prefix") == "dataset-uploads/")
        ):
            raise ValueError("An upload lifecycle rule ID already controls another prefix")
    for rule in before.cors:
        if rule.get("ID") == "dataset-uploads" and rule["AllowedMethods"] != ["POST"]:
            raise ValueError("An upload CORS rule ID already controls another method")
    after = BucketConfiguration(
        [rule for rule in before.lifecycle if rule.get("ID") not in managed_ids] + managed,
        [rule for rule in before.cors if rule.get("ID") != "dataset-uploads"] + [upload_cors_rule(origins)],
        before.transition_minimum or "all_storage_classes_128K",
    )
    return UploadStoragePlan(bucket, account_id, org_id, before, after)


def apply_upload_storage(s3: "S3Client", plan: UploadStoragePlan) -> None:
    current = _read(s3, plan.bucket, plan.account_id, plan.org_id)
    if current != plan.before:
        raise ValueError("Bucket configuration changed after preview. Read a fresh preview before applying it")
    target: BucketTarget = {"Bucket": plan.bucket, "ExpectedBucketOwner": plan.account_id}
    if plan.before.lifecycle != plan.after.lifecycle:
        if plan.after.transition_minimum is None:
            s3.put_bucket_lifecycle_configuration(**target, LifecycleConfiguration={"Rules": plan.after.lifecycle})
        else:
            s3.put_bucket_lifecycle_configuration(
                **target,
                LifecycleConfiguration={"Rules": plan.after.lifecycle},
                TransitionDefaultMinimumObjectSize=plan.after.transition_minimum,
            )
    if plan.before.cors != plan.after.cors:
        s3.put_bucket_cors(**target, CORSConfiguration={"CORSRules": plan.after.cors})
    if _read(s3, plan.bucket, plan.account_id, plan.org_id) != plan.after:
        raise ValueError("Bucket readback differs from the requested configuration. Read a fresh preview")

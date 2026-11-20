import json
from collections.abc import Iterator
from contextlib import closing
from dataclasses import asdict
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from tracker.aws.dataset_upload_storage import (
    BucketConfiguration,
    apply_upload_storage,
    plan_upload_storage,
    upload_cors_rule,
    upload_lifecycle_rules,
)
from tracker.prepare_upload_bucket import main

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_ACCOUNT = "123456789012"
_ORG = "d251de62-40cb-4935-8fd0-a617918604ac"
_BUCKET = "tenant-existing"
_TARGET = {"Bucket": _BUCKET, "ExpectedBucketOwner": _ACCOUNT}
_ORIGINS = ["https://dashboards.vals.ai"]
_ARGUMENTS = [
    "--bucket",
    _BUCKET,
    "--account-id",
    _ACCOUNT,
    "--org-id",
    _ORG,
    "--profile",
    "bench",
    "--region",
    "us-east-1",
    "--origin",
    _ORIGINS[0],
]


@pytest.fixture
def storage_client() -> Iterator["S3Client"]:
    with closing(
        boto3.client(  # pyright: ignore[reportUnknownMemberType]
            "s3", region_name="us-east-1", aws_access_key_id="testing", aws_secret_access_key="testing"
        )
    ) as client:
        yield client


def _existing() -> BucketConfiguration:
    return BucketConfiguration(
        lifecycle=[
            {
                "ID": "retain-native-history",
                "Status": "Enabled",
                "Filter": {"Prefix": "datasets/"},
                "NoncurrentVersionExpiration": {"NoncurrentDays": 90},
            }
        ],
        cors=[{"ID": "reader", "AllowedMethods": ["GET"], "AllowedOrigins": ["https://reader.example"]}],
        transition_minimum="varies_by_storage_class",
    )


def _read(stubber: Stubber, configuration: BucketConfiguration, org_id: str = _ORG) -> None:
    stubber.add_response("get_bucket_tagging", {"TagSet": [{"Key": "valkyrie:org-id", "Value": org_id}]}, _TARGET)
    if configuration.lifecycle:
        lifecycle: dict[str, object] = {"Rules": configuration.lifecycle}
        if configuration.transition_minimum:
            lifecycle["TransitionDefaultMinimumObjectSize"] = configuration.transition_minimum
        stubber.add_response("get_bucket_lifecycle_configuration", lifecycle, _TARGET)
    else:
        stubber.add_client_error(
            "get_bucket_lifecycle_configuration", "NoSuchLifecycleConfiguration", expected_params=_TARGET
        )
    if configuration.cors:
        stubber.add_response("get_bucket_cors", {"CORSRules": configuration.cors}, _TARGET)
    else:
        stubber.add_client_error("get_bucket_cors", "NoSuchCORSConfiguration", expected_params=_TARGET)


class TestUploadStorage:
    @pytest.mark.description(
        "Preview leaves S3 unchanged and applying preserves unrelated rules and the transition size policy."
    )
    @pytest.mark.parametrize("empty", [False, True], ids=["existing-rules", "no-rules"])
    def test_preview_apply_and_repeat(self, storage_client: "S3Client", empty: bool) -> None:
        before = BucketConfiguration([], []) if empty else _existing()
        after = BucketConfiguration(
            [*before.lifecycle, *upload_lifecycle_rules()],
            [*before.cors, upload_cors_rule(_ORIGINS)],
            before.transition_minimum or "all_storage_classes_128K",
        )
        with Stubber(storage_client) as stubber:
            _read(stubber, before)

            plan = plan_upload_storage(storage_client, _BUCKET, _ACCOUNT, _ORG, _ORIGINS)

            assert plan.before == before
            assert plan.after == after
            stubber.assert_no_pending_responses()

            _read(stubber, before)
            lifecycle_request: dict[str, object] = {**_TARGET, "LifecycleConfiguration": {"Rules": after.lifecycle}}
            lifecycle_request["TransitionDefaultMinimumObjectSize"] = after.transition_minimum
            stubber.add_response("put_bucket_lifecycle_configuration", {}, lifecycle_request)
            stubber.add_response("put_bucket_cors", {}, {**_TARGET, "CORSConfiguration": {"CORSRules": after.cors}})
            _read(stubber, after)

            apply_upload_storage(storage_client, plan)

            _read(stubber, after)
            repeated = plan_upload_storage(storage_client, _BUCKET, _ACCOUNT, _ORG, _ORIGINS)
            _read(stubber, after)
            _read(stubber, after)

            apply_upload_storage(storage_client, repeated)

            assert repeated.before == repeated.after
            stubber.assert_no_pending_responses()

    @pytest.mark.description(
        "Applying refuses a configuration that changed after preview without overwriting the newer rules."
    )
    def test_changed_configuration(self, storage_client: "S3Client") -> None:
        before = _existing()
        with Stubber(storage_client) as stubber:
            _read(stubber, before)
            plan = plan_upload_storage(storage_client, _BUCKET, _ACCOUNT, _ORG, _ORIGINS)
            _read(stubber, BucketConfiguration([], before.cors))

            with pytest.raises(ValueError, match="changed after preview"):
                apply_upload_storage(storage_client, plan)

            stubber.assert_no_pending_responses()

    @pytest.mark.description("A wrong organization or denied access stops the preview before any configuration write.")
    @pytest.mark.parametrize("denied", [False, True], ids=["wrong-org", "access-denied"])
    def test_owner_and_permissions(self, storage_client: "S3Client", denied: bool) -> None:
        with Stubber(storage_client) as stubber:
            if denied:
                stubber.add_client_error("get_bucket_tagging", "AccessDenied", expected_params=_TARGET)
            else:
                stubber.add_response("get_bucket_tagging", {"TagSet": []}, _TARGET)

            with pytest.raises(ClientError if denied else ValueError):
                plan_upload_storage(storage_client, _BUCKET, _ACCOUNT, _ORG, _ORIGINS)

            stubber.assert_no_pending_responses()

    @pytest.mark.description("Matching legacy upload prefixes allow a new preview and retain the transition policy.")
    def test_legacy_upload_prefix(self, storage_client: "S3Client") -> None:
        rules = upload_lifecycle_rules()
        for rule in rules:
            rule.pop("Filter")
            rule["Prefix"] = "dataset-uploads/"
        before = BucketConfiguration(rules, [], "varies_by_storage_class")
        with Stubber(storage_client) as stubber:
            _read(stubber, before)

            plan = plan_upload_storage(storage_client, _BUCKET, _ACCOUNT, _ORG, _ORIGINS)

            assert plan.after.lifecycle == upload_lifecycle_rules()
            assert plan.after.cors == [upload_cors_rule(_ORIGINS)]
            assert plan.after.transition_minimum == before.transition_minimum
            stubber.assert_no_pending_responses()

    @pytest.mark.description("A managed rule ID cannot replace a rule for another prefix or HTTP method.")
    @pytest.mark.parametrize("collision", ["lifecycle", "legacy-lifecycle", "legacy-with-filter", "cors"])
    def test_rule_collision(self, storage_client: "S3Client", collision: str) -> None:
        before = _existing()
        if collision != "cors":
            before.lifecycle[0]["ID"] = "expire-dataset-upload-archives"
            if collision == "legacy-lifecycle":
                before.lifecycle[0].pop("Filter")
                before.lifecycle[0]["Prefix"] = "datasets/"
            elif collision == "legacy-with-filter":
                before.lifecycle[0]["Prefix"] = "dataset-uploads/"
        else:
            before.cors[0]["ID"] = "dataset-uploads"
        with Stubber(storage_client) as stubber:
            _read(stubber, before)

            with pytest.raises(ValueError, match="already controls another"):
                plan_upload_storage(storage_client, _BUCKET, _ACCOUNT, _ORG, _ORIGINS)

            stubber.assert_no_pending_responses()

    @pytest.mark.description(
        "The command prints the caller and exact proposed configuration without applying it by default."
    )
    def test_command_preview(
        self, storage_client: "S3Client", monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        session = MagicMock()
        sts = MagicMock()
        sts.get_caller_identity.return_value = {
            "Account": _ACCOUNT,
            "Arn": f"arn:aws:iam::{_ACCOUNT}:role/BenchEng-alpha",
        }
        clients = {"s3": storage_client, "sts": sts}

        def client(service: str, **_kwargs: object) -> object:
            return clients[service]

        session.client.side_effect = client
        monkeypatch.setattr(boto3, "Session", MagicMock(return_value=session))
        before = _existing()
        with Stubber(storage_client) as stubber:
            _read(stubber, before)

            main(_ARGUMENTS)

            output = json.loads(capsys.readouterr().out)
            assert output["before"] == asdict(before)
            assert output["after"]["cors"][-1]["AllowedOrigins"] == _ORIGINS
            assert output["caller"].endswith("role/BenchEng-alpha")
            stubber.assert_no_pending_responses()

    @pytest.mark.description(
        "The command rejects wildcard, insecure, and path-bearing origins before opening an AWS session."
    )
    @pytest.mark.parametrize("origin", ["http://console.example", "https://*.example", "https://console.example/path"])
    def test_invalid_origin(self, origin: str, monkeypatch: pytest.MonkeyPatch) -> None:
        session = MagicMock()
        monkeypatch.setattr(boto3, "Session", session)

        with pytest.raises(SystemExit) as result:
            main([*_ARGUMENTS[:-1], origin])

        assert result.value.code == 2
        session.assert_not_called()

    @pytest.mark.description(
        "After a CORS write fails, a fresh preview applies only the remaining change and preserves the lifecycle result."
    )
    def test_partial_apply_recovery(self, storage_client: "S3Client") -> None:
        before = _existing()
        with Stubber(storage_client) as stubber:
            _read(stubber, before)
            plan = plan_upload_storage(storage_client, _BUCKET, _ACCOUNT, _ORG, _ORIGINS)
            _read(stubber, before)
            stubber.add_response(
                "put_bucket_lifecycle_configuration",
                {},
                {
                    **_TARGET,
                    "LifecycleConfiguration": {"Rules": plan.after.lifecycle},
                    "TransitionDefaultMinimumObjectSize": before.transition_minimum,
                },
            )
            cors_request = {**_TARGET, "CORSConfiguration": {"CORSRules": plan.after.cors}}
            stubber.add_client_error("put_bucket_cors", "AccessDenied", expected_params=cors_request)

            with pytest.raises(ClientError, match="AccessDenied"):
                apply_upload_storage(storage_client, plan)

            partial = BucketConfiguration(plan.after.lifecycle, before.cors, before.transition_minimum)
            _read(stubber, partial)
            remaining = plan_upload_storage(storage_client, _BUCKET, _ACCOUNT, _ORG, _ORIGINS)
            _read(stubber, partial)
            stubber.add_response("put_bucket_cors", {}, cors_request)
            _read(stubber, plan.after)

            apply_upload_storage(storage_client, remaining)

            assert remaining.before.lifecycle == remaining.after.lifecycle
            stubber.assert_no_pending_responses()

    @pytest.mark.description("A profile for another account stops before any S3 read or write.")
    def test_wrong_account(self, monkeypatch: pytest.MonkeyPatch) -> None:
        session = MagicMock()
        session.client.return_value.get_caller_identity.return_value = {"Account": "999999999999"}
        monkeypatch.setattr(boto3, "Session", MagicMock(return_value=session))

        with pytest.raises(SystemExit) as result:
            main([*_ARGUMENTS, "--apply"])

        assert result.value.code == 1
        assert session.client.call_args.args == ("sts",)

    @pytest.mark.description("An unreadable transition policy prevents replacement of existing lifecycle rules.")
    def test_missing_transition_policy(self, storage_client: "S3Client") -> None:
        before = _existing()
        with Stubber(storage_client) as stubber:
            _read(stubber, BucketConfiguration(before.lifecycle, before.cors))

            with pytest.raises(ValueError, match="transition size policy"):
                plan_upload_storage(storage_client, _BUCKET, _ACCOUNT, _ORG, _ORIGINS)

            stubber.assert_no_pending_responses()

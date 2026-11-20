import argparse
import json
from collections.abc import Sequence
from contextlib import closing
from dataclasses import asdict
from urllib.parse import urlsplit
from uuid import UUID

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from tracker.aws.dataset_upload_storage import apply_upload_storage, plan_upload_storage


def _origin(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path
        or parsed.query
        or parsed.fragment
        or "*" in value
    ):
        raise argparse.ArgumentTypeError("Use an exact HTTPS origin without a path or wildcard")
    return value


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Preview or apply upload rules for one existing tenant bucket.")
    parser.add_argument("--bucket", required=True, help="Name of the existing tenant bucket.")
    parser.add_argument("--account-id", required=True, help="Expected AWS account ID for the caller and bucket.")
    parser.add_argument("--org-id", required=True, type=UUID, help="Expected organization ID from the bucket tag.")
    parser.add_argument("--profile", required=True, help="AWS profile authorized for this bucket.")
    parser.add_argument("--region", required=True, help="AWS region containing the bucket.")
    parser.add_argument(
        "--origin", required=True, action="append", type=_origin, help="Allowed HTTPS origin. Repeat for each origin."
    )
    parser.add_argument("--apply", action="store_true", help="Apply the previewed rules and read back the result.")
    args = parser.parse_args(argv)
    if len(args.account_id) != 12 or not args.account_id.isdecimal():
        parser.error("--account-id must contain 12 digits")
    client_config = Config(connect_timeout=5, read_timeout=30, retries={"total_max_attempts": 1})
    try:
        session = boto3.Session(profile_name=args.profile, region_name=args.region)
        with closing(session.client("sts", config=client_config)) as sts:  # pyright: ignore[reportUnknownMemberType]
            identity = sts.get_caller_identity()
        if identity["Account"] != args.account_id:
            raise ValueError("The AWS profile belongs to a different account")
        with closing(session.client("s3", config=client_config)) as s3:  # pyright: ignore[reportUnknownMemberType]
            plan = plan_upload_storage(s3, args.bucket, args.account_id, str(args.org_id), sorted(set(args.origin)))
            print(json.dumps({"caller": identity["Arn"], **asdict(plan)}, indent=2, default=str), flush=True)
            if args.apply:
                apply_upload_storage(s3, plan)
                print("Upload storage configuration applied and read back.")
    except (BotoCoreError, ClientError, ValueError) as error:
        parser.exit(1, f"{error}\nNo rollback was attempted. Read a fresh preview before retrying.\n")


if __name__ == "__main__":
    main()

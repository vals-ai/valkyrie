from typing import Any, cast

import boto3
from botocore.config import Config

from tracker import config

PARAMETER_CONFIG = Config(connect_timeout=2, read_timeout=2, retries={"max_attempts": 2, "mode": "standard"})


def read_parameter(name: str) -> str | None:
    client = cast(Any, boto3.client)("ssm", region_name=config.AWS_DEPLOYMENT_REGION, config=PARAMETER_CONFIG)
    try:
        return cast(str, client.get_parameter(Name=name)["Parameter"]["Value"])
    except client.exceptions.ParameterNotFound:
        return None

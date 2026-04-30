import json
import logging
import os
import time
import urllib.request
from dataclasses import dataclass
from random import randint

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, NoRegionError

logger = logging.getLogger(__name__)


def snake2kebab(x: str) -> str:
    return x.replace("_", "-")


def flatten(nested: list[list]) -> list:
    return [x for xs in nested for x in xs]


def current_time_millis():
    return round(time.time() * 1000)


def postwalk(obj, match, update):
    if isinstance(obj, dict):
        return {k: postwalk(v, match, update) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [postwalk(v, match, update) for v in obj]
    if isinstance(obj, set):
        return {postwalk(v, match, update) for v in obj}
    return update(obj) if match(obj) else obj


def replace(obj, **changes):
    def props(x):
        dunder = x.startswith("__")
        method = type(getattr(obj, x)).__name__ == "method"
        property = x in properties(obj)
        return not (dunder or method or property)

    result = type(obj)()
    [setattr(result, x, getattr(obj, x)) for x in filter(props, dir(obj))]
    for k, v in changes.items():
        setattr(result, k, v)
    return result


def properties(obj):
    result = []
    for name in dir(obj):
        attr = getattr(obj.__class__, name, None)
        if isinstance(attr, property):
            result.append(name)
    return result


def setter(obj, name):
    attr = getattr(obj.__class__, name, None)
    if attr:
        return getattr(attr, "setter", None)


@dataclass
class BackoffWithJitter:
    min: int = 10
    max: int = 10000
    k: int = 3
    state: int = 0

    def __call__(self):
        self.state = min(self.max, randint(self.min, max(self.min, self.state) * self.k))
        return self.state


def _get_region_from_metadata():
    """
    Attempts to retrieve the AWS region from ECS or EC2 metadata.

    Returns
    -------
    Optional[str]
        The AWS region string if found, otherwise None.
    """
    # ECS (used in AWS Batch)
    metadata_uri = os.environ.get("ECS_CONTAINER_METADATA_URI_V4") or os.environ.get("ECS_CONTAINER_METADATA_URI")
    if metadata_uri:
        try:
            with urllib.request.urlopen(metadata_uri, timeout=2) as response:
                metadata = json.load(response)
            cluster_label = metadata.get("Labels", {}).get("com.amazonaws.ecs.cluster", "")
            region = cluster_label.split(":")[0] if ":" in cluster_label else None
            if region:
                return region
        except Exception as e:
            logger.warning("Failed to get region from ECS metadata: %s", e)
    # EC2 fallback: use IMDSv2
    try:
        token_req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token",
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
        )
        with urllib.request.urlopen(token_req, timeout=2) as token_response:
            token = token_response.read().decode()
        region_req = urllib.request.Request(
            "http://169.254.169.254/latest/dynamic/instance-identity/document",
            headers={"X-aws-ec2-metadata-token": token},
        )
        with urllib.request.urlopen(region_req, timeout=2) as region_response:
            identity_doc = json.load(region_response)
        return identity_doc.get("region")
    except Exception as e:
        logger.warning("Failed to get region from EC2 metadata: %s", e)
    return


def get_client(name, region=None, default_region="us-east-1"):
    """
    Creates a robust boto3 client, determining the AWS region in the following order:
        1. Explicit argument
        2. AWS_REGION / AWS_DEFAULT_REGION environment variables
        3. boto3/botocore session
        4. ECS/EC2 metadata
        5. Fallback default region (us-east-1)

    Parameters
    ----------
    name : str
        The name of the AWS service client.
    region : Optional[str], default=None
        The AWS region to use.
    default_region : str, default="us-east-1"
        The fallback AWS region.

    Returns
    -------
    boto3.client
        A boto3 client for the specified service.
    """
    # Step 1–3: Try common boto3 config methods
    region = region or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not region:
        try:
            region = boto3.Session().region_name
        except (BotoCoreError, NoRegionError, ConnectionRefusedError):
            logger.debug("could not instantiate boto client...")
            pass
    # Step 4: Metadata if still no region
    if not region:
        logger.debug("inferring aws region from metadata")
        region = _get_region_from_metadata()
    # Step 5: Fallback default
    if not region:
        logger.warning(f"falling back to default region '{default_region}'")
        region = default_region
    config = Config(region_name=region, connect_timeout=5, retries={"max_attempts": 5, "mode": "adaptive"})
    return boto3.client(name, config=config)

"""Validate the deployed lab end to end. Skips itself when nothing is deployed."""

from __future__ import annotations

import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from aws_session import get_client  # noqa: E402

pytestmark = pytest.mark.cloud

INFRA = Path(__file__).resolve().parents[2] / "infra"


def wait_for(predicate, timeout: float, interval: float = 3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return None


@pytest.fixture(scope="module")
def outputs() -> dict:
    result = subprocess.run(
        ["terraform", f"-chdir={INFRA}", "output", "-json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or result.stdout.strip() in ("", "{}"):
        pytest.skip("terraform outputs unavailable: the lab is not deployed")
    return {name: item["value"] for name, item in json.loads(result.stdout).items()}


def aws_client(service: str):
    try:
        return get_client(service)
    except KeyError:
        pytest.skip("AWS credentials not configured (.env.credentials)")


@pytest.fixture(scope="module")
def kinesis():
    return aws_client("kinesis")


@pytest.fixture(scope="module")
def dynamodb():
    return aws_client("dynamodb")


def current_status(dynamodb, table: str, package_id: str) -> str | None:
    item = dynamodb.get_item(
        TableName=table, Key={"package_id": {"S": package_id}}
    ).get("Item")
    return item["status"]["S"] if item else None


def put_event(
    kinesis, stream: str, package_id: str, timestamp: str, status: str
) -> None:
    event = {
        "package_id": package_id,
        "timestamp": timestamp,
        "latitude": -12.0464,
        "longitude": -77.0428,
        "status": status,
    }
    kinesis.put_record(
        StreamName=stream,
        Data=(json.dumps(event) + "\n").encode(),
        PartitionKey=package_id,
    )


@pytest.fixture(scope="module")
def lifecycle(outputs, kinesis, dynamodb) -> str:
    """Publish a newer event, then an OLDER one for the same package."""
    package_id = f"TEST-{uuid.uuid4().hex[:8]}"
    stream = outputs["stream_name"]
    put_event(kinesis, stream, package_id, "2026-10-04T11:00:00Z", "OUT_FOR_DELIVERY")
    arrived = wait_for(
        lambda: current_status(dynamodb, outputs["table_name"], package_id), timeout=90
    )
    assert arrived == "OUT_FOR_DELIVERY", "first event never reached DynamoDB"
    put_event(kinesis, stream, package_id, "2026-10-04T10:00:00Z", "CREATED")
    time.sleep(20)  # longer than the batching window plus one invocation
    return package_id


def test_stream_is_active_with_two_shards(outputs, kinesis) -> None:
    summary = kinesis.describe_stream_summary(StreamName=outputs["stream_name"])
    description = summary["StreamDescriptionSummary"]
    assert description["StreamStatus"] == "ACTIVE"
    assert description["OpenShardCount"] == 2
    assert description["RetentionPeriodHours"] == 24


def test_state_table_is_keyed_by_package_id(outputs, dynamodb) -> None:
    table = dynamodb.describe_table(TableName=outputs["table_name"])["Table"]
    assert table["TableStatus"] == "ACTIVE"
    assert table["KeySchema"] == [{"AttributeName": "package_id", "KeyType": "HASH"}]


def test_event_source_mapping_is_enabled_with_partial_batch_responses(outputs) -> None:
    lambda_client = aws_client("lambda")
    mappings = lambda_client.list_event_source_mappings(
        FunctionName=outputs["lambda_function_name"]
    )["EventSourceMappings"]
    assert len(mappings) == 1
    assert mappings[0]["State"] == "Enabled"
    assert "ReportBatchItemFailures" in mappings[0]["FunctionResponseTypes"]


def test_firehose_delivery_stream_is_active(outputs) -> None:
    firehose = aws_client("firehose")
    description = firehose.describe_delivery_stream(
        DeliveryStreamName=outputs["firehose_delivery_stream_name"]
    )["DeliveryStreamDescription"]
    assert description["DeliveryStreamStatus"] == "ACTIVE"


def test_dynamodb_keeps_current_state_and_ignores_the_stale_event(
    outputs, dynamodb, lifecycle
) -> None:
    assert (
        current_status(dynamodb, outputs["table_name"], lifecycle) == "OUT_FOR_DELIVERY"
    )


@pytest.mark.slow
def test_s3_keeps_the_full_history_including_the_event_dynamodb_ignored(
    outputs, lifecycle
) -> None:
    s3 = aws_client("s3")
    bucket = outputs["datalake_bucket_name"]

    def statuses_for_package() -> set[str] | None:
        found: set[str] = set()
        pages = s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix="events/"
        )
        for page in pages:
            for entry in page.get("Contents", []):
                body = (
                    s3.get_object(Bucket=bucket, Key=entry["Key"])["Body"]
                    .read()
                    .decode()
                )
                for line in body.splitlines():
                    event = json.loads(line)
                    if event["package_id"] == lifecycle:
                        found.add(event["status"])
        return found if found >= {"OUT_FOR_DELIVERY", "CREATED"} else None

    # Firehose buffers (60 s by default) before it writes the object.
    assert wait_for(statuses_for_package, timeout=240, interval=10) is not None

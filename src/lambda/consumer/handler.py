"""Kinesis -> DynamoDB consumer: keeps the current state of each package."""

from __future__ import annotations

import base64
import json
import logging
import os
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

from contract import load_contract, validate_event

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Write only if the package is new or the incoming event is strictly newer.
# Timestamps are fixed-format UTC strings, so text comparison is chronological.
CONDITION = "attribute_not_exists(package_id) OR updated_at < :ts"
STALE_ERROR_CODE = "ConditionalCheckFailedException"

_table = None
_contract = None


def _get_table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])
    return _table


def _get_contract() -> dict:
    global _contract
    if _contract is None:
        _contract = load_contract()
    return _contract


def to_item(package_event: dict) -> dict:
    # boto3's DynamoDB resource rejects float; Decimal(str(x)) keeps the value exact.
    item = {
        "package_id": package_event["package_id"],
        "status": package_event["status"],
        "latitude": Decimal(str(package_event["latitude"])),
        "longitude": Decimal(str(package_event["longitude"])),
        "updated_at": package_event["timestamp"],
    }
    for optional in ("city", "driver_id"):
        if optional in package_event:
            item[optional] = package_event[optional]
    return item


def write_state(table, package_event: dict) -> bool:
    """Return True if the state was written, False if the event was stale."""
    item = to_item(package_event)
    try:
        table.put_item(
            Item=item,
            ConditionExpression=CONDITION,
            ExpressionAttributeValues={":ts": item["updated_at"]},
        )
    except ClientError as error:
        if error.response["Error"]["Code"] == STALE_ERROR_CODE:
            return False
        raise
    return True


def decode_record(record: dict) -> object:
    payload = base64.b64decode(record["kinesis"]["data"], validate=True)
    return json.loads(payload)


def process_batch(event: dict, table, contract: dict) -> dict:
    failures = []
    for record in event["Records"]:
        sequence_number = record["kinesis"]["sequenceNumber"]
        try:
            package_event = decode_record(record)
            validate_event(package_event, contract)
            if not write_state(table, package_event):
                logger.info(
                    "stale event ignored: package_id=%s timestamp=%s",
                    package_event["package_id"],
                    package_event["timestamp"],
                )
        except ValueError as error:
            # Bad base64, bad JSON or contract violation: retries will not fix it,
            # but reporting it sends it to the failure destination once retries end.
            logger.error("invalid record %s: %s", sequence_number, error)
            failures.append({"itemIdentifier": sequence_number})
        except Exception:
            logger.exception("unexpected error on record %s", sequence_number)
            failures.append({"itemIdentifier": sequence_number})
    return {"batchItemFailures": failures}


def lambda_handler(event: dict, context) -> dict:
    return process_batch(event, _get_table(), _get_contract())

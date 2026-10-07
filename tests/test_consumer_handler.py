from __future__ import annotations

import base64
import json
import sys
from decimal import Decimal
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src" / "lambda" / "consumer"))

import handler  # noqa: E402
from contract import load_contract  # noqa: E402

CONTRACT = load_contract(
    REPO_ROOT / "src" / "contracts" / "package_event_contract.json"
)


def client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "PutItem")


class FakeTable:
    """Mimics DynamoDB's conditional put on updated_at."""

    def __init__(self) -> None:
        self.items: dict[str, dict] = {}
        self.fail_for: dict[str, Exception] = {}

    def put_item(self, Item, ConditionExpression, ExpressionAttributeValues):  # noqa: N803
        assert ConditionExpression == handler.CONDITION
        package_id = Item["package_id"]
        if package_id in self.fail_for:
            raise self.fail_for[package_id]
        current = self.items.get(package_id)
        if (
            current is not None
            and not current["updated_at"] < ExpressionAttributeValues[":ts"]
        ):
            raise client_error("ConditionalCheckFailedException")
        self.items[package_id] = Item


def event_for(
    package_id="PKG-001", timestamp="2026-10-04T10:30:00Z", status="IN_TRANSIT", **extra
):
    return {
        "package_id": package_id,
        "timestamp": timestamp,
        "latitude": -12.0464,
        "longitude": -77.0428,
        "status": status,
        **extra,
    }


def record(payload, sequence_number: str) -> dict:
    if isinstance(payload, dict):
        payload = (json.dumps(payload) + "\n").encode()
    elif isinstance(payload, str):
        payload = payload.encode()
    return {
        "kinesis": {
            "sequenceNumber": sequence_number,
            "data": base64.b64encode(payload).decode(),
        }
    }


def run(records: list[dict], table: FakeTable) -> dict:
    return handler.process_batch({"Records": records}, table, CONTRACT)


def test_new_package_is_written_with_decimal_coordinates() -> None:
    table = FakeTable()
    result = run([record(event_for(), "1")], table)

    assert result == {"batchItemFailures": []}
    item = table.items["PKG-001"]
    assert item["status"] == "IN_TRANSIT"
    assert item["updated_at"] == "2026-10-04T10:30:00Z"
    assert item["latitude"] == Decimal("-12.0464")
    assert isinstance(item["longitude"], Decimal)
    assert "timestamp" not in item


def test_optional_fields_are_stored() -> None:
    table = FakeTable()
    run([record(event_for(city="Lima", driver_id="DRV-001"), "1")], table)
    assert table.items["PKG-001"]["city"] == "Lima"
    assert table.items["PKG-001"]["driver_id"] == "DRV-001"


def test_newer_event_replaces_current_state() -> None:
    table = FakeTable()
    run(
        [
            record(event_for(timestamp="2026-10-04T10:00:00Z", status="CREATED"), "1"),
            record(
                event_for(timestamp="2026-10-04T10:30:00Z", status="IN_TRANSIT"), "2"
            ),
        ],
        table,
    )
    assert table.items["PKG-001"]["status"] == "IN_TRANSIT"


def test_stale_event_does_not_overwrite_and_is_not_a_failure() -> None:
    table = FakeTable()
    result = run(
        [
            record(
                event_for(timestamp="2026-10-04T11:20:00Z", status="OUT_FOR_DELIVERY"),
                "1",
            ),
            record(
                event_for(timestamp="2026-10-04T10:30:00Z", status="IN_TRANSIT"), "2"
            ),
        ],
        table,
    )
    assert result == {"batchItemFailures": []}
    assert table.items["PKG-001"]["status"] == "OUT_FOR_DELIVERY"


def test_replayed_event_with_same_timestamp_is_a_harmless_noop() -> None:
    table = FakeTable()
    same = event_for(timestamp="2026-10-04T10:30:00Z")
    result = run([record(same, "1"), record(same, "2")], table)
    assert result == {"batchItemFailures": []}
    assert table.items["PKG-001"]["updated_at"] == "2026-10-04T10:30:00Z"


def test_poison_records_are_reported_and_valid_ones_still_written() -> None:
    table = FakeTable()
    broken_base64 = {"kinesis": {"sequenceNumber": "2", "data": "@@not-base64@@"}}
    result = run(
        [
            record(event_for("PKG-001"), "1"),
            broken_base64,
            record("{not json", "3"),
            record(event_for("PKG-002", status="LOST"), "4"),
            record(event_for("PKG-003"), "5"),
        ],
        table,
    )
    assert result == {
        "batchItemFailures": [
            {"itemIdentifier": "2"},
            {"itemIdentifier": "3"},
            {"itemIdentifier": "4"},
        ]
    }
    assert set(table.items) == {"PKG-001", "PKG-003"}


def test_nan_coordinates_are_reported_not_written() -> None:
    table = FakeTable()
    nan_payload = (
        '{"package_id":"PKG-001","timestamp":"2026-10-04T10:30:00Z",'
        '"latitude":NaN,"longitude":-77.0,"status":"CREATED"}'
    )
    result = run([record(nan_payload, "1")], table)
    assert result == {"batchItemFailures": [{"itemIdentifier": "1"}]}
    assert table.items == {}


def test_unexpected_dynamodb_error_is_reported_for_retry() -> None:
    table = FakeTable()
    table.fail_for["PKG-002"] = client_error("ProvisionedThroughputExceededException")
    result = run(
        [record(event_for("PKG-001"), "1"), record(event_for("PKG-002"), "2")],
        table,
    )
    assert result == {"batchItemFailures": [{"itemIdentifier": "2"}]}
    assert "PKG-001" in table.items


def test_empty_batch_returns_no_failures() -> None:
    assert run([], FakeTable()) == {"batchItemFailures": []}


def test_lambda_handler_wires_table_and_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = FakeTable()
    monkeypatch.setattr(handler, "_get_table", lambda: table)
    monkeypatch.setattr(handler, "_get_contract", lambda: CONTRACT)

    result = handler.lambda_handler({"Records": [record(event_for(), "1")]}, None)

    assert result == {"batchItemFailures": []}
    assert "PKG-001" in table.items

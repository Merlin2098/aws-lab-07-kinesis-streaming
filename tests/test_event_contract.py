from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src" / "lambda" / "consumer"))

from contract import ContractError, load_contract, validate_event  # noqa: E402

CONTRACT_PATH = REPO_ROOT / "src" / "contracts" / "package_event_contract.json"


@pytest.fixture(scope="module")
def contract() -> dict:
    return load_contract(CONTRACT_PATH)


def valid_event(**overrides) -> dict:
    event = {
        "package_id": "PKG-001",
        "timestamp": "2026-10-04T10:30:00Z",
        "latitude": -12.0464,
        "longitude": -77.0428,
        "status": "IN_TRANSIT",
    }
    event.update(overrides)
    return event


def test_contract_declares_the_six_statuses(contract) -> None:
    assert contract["properties"]["status"]["enum"] == [
        "CREATED",
        "PICKED_UP",
        "IN_TRANSIT",
        "OUT_FOR_DELIVERY",
        "DELIVERED",
        "CANCELLED",
    ]


def test_valid_event_passes(contract) -> None:
    validate_event(valid_event(), contract)


def test_valid_event_with_optional_fields_passes(contract) -> None:
    validate_event(valid_event(city="Lima", driver_id="DRV-001"), contract)


@pytest.mark.parametrize(
    "event",
    [
        "not an object",
        ["a", "list"],
        None,
    ],
)
def test_non_object_is_rejected(contract, event) -> None:
    with pytest.raises(ContractError, match="JSON object"):
        validate_event(event, contract)


@pytest.mark.parametrize(
    "field", ["package_id", "timestamp", "latitude", "longitude", "status"]
)
def test_missing_required_field_is_rejected(contract, field) -> None:
    event = valid_event()
    del event[field]
    with pytest.raises(ContractError, match=field):
        validate_event(event, contract)


def test_unknown_field_is_rejected(contract) -> None:
    with pytest.raises(ContractError, match="unknown fields: speed"):
        validate_event(valid_event(speed=10), contract)


def test_unknown_status_is_rejected(contract) -> None:
    with pytest.raises(ContractError, match="status"):
        validate_event(valid_event(status="LOST"), contract)


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-10-04T10:30:00+00:00",  # offset instead of Z
        "2026-10-04T10:30:00.123Z",  # fractional seconds
        "2026-10-04 10:30:00Z",  # no T
        "2026-10-04T10:30:00Z\n",  # trailing newline
        "2026-13-45T10:30:00Z",  # matches the pattern, impossible date
        "2026-10-04T25:00:00Z",  # impossible hour
    ],
)
def test_bad_timestamp_is_rejected(contract, timestamp) -> None:
    with pytest.raises(ContractError, match="timestamp"):
        validate_event(valid_event(timestamp=timestamp), contract)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("latitude", 91),
        ("latitude", -90.0001),
        ("longitude", 180.5),
        ("latitude", "12.0"),
        ("latitude", True),
        ("longitude", None),
        ("latitude", float("nan")),
        ("longitude", float("inf")),
    ],
)
def test_bad_coordinates_are_rejected(contract, field, value) -> None:
    with pytest.raises(ContractError, match=field):
        validate_event(valid_event(**{field: value}), contract)


@pytest.mark.parametrize("package_id", ["", "PKG 001", "PKG-001\n", "x" * 65])
def test_bad_package_id_is_rejected(contract, package_id) -> None:
    with pytest.raises(ContractError, match="package_id"):
        validate_event(valid_event(package_id=package_id), contract)


def test_contract_file_is_valid_json() -> None:
    json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))

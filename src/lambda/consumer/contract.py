"""Validate package events against package_event_contract.json (stdlib only)."""

from __future__ import annotations

import json
import math
import re
from datetime import datetime
from pathlib import Path


class ContractError(ValueError):
    """The event does not satisfy the contract."""


def load_contract(path: Path | None = None) -> dict:
    # In the Lambda zip the contract sits next to this file.
    contract_path = path or Path(__file__).with_name("package_event_contract.json")
    return json.loads(contract_path.read_text(encoding="utf-8"))


def validate_event(event: object, contract: dict) -> None:
    if not isinstance(event, dict):
        raise ContractError("event must be a JSON object")

    properties = contract["properties"]
    missing = [name for name in contract["required"] if name not in event]
    if missing:
        raise ContractError(f"missing required fields: {', '.join(sorted(missing))}")

    if contract.get("additionalProperties") is False:
        unknown = sorted(set(event) - set(properties))
        if unknown:
            raise ContractError(f"unknown fields: {', '.join(unknown)}")

    for name, value in event.items():
        _validate_field(name, value, properties[name])


def _validate_field(name: str, value: object, rule: dict) -> None:
    if rule["type"] == "string":
        _validate_string(name, value, rule)
    elif rule["type"] == "number":
        _validate_number(name, value, rule)
    else:  # pragma: no cover - contract only uses string and number
        raise ContractError(f"{name}: unsupported contract type {rule['type']}")


def _validate_string(name: str, value: object, rule: dict) -> None:
    if not isinstance(value, str):
        raise ContractError(f"{name} must be a string")
    if "pattern" in rule and not re.fullmatch(rule["pattern"], value):
        raise ContractError(f"{name} does not match {rule['pattern']}")
    if "enum" in rule and value not in rule["enum"]:
        raise ContractError(f"{name} must be one of {', '.join(rule['enum'])}")
    if "minLength" in rule and len(value) < rule["minLength"]:
        raise ContractError(f"{name} is shorter than {rule['minLength']}")
    if "maxLength" in rule and len(value) > rule["maxLength"]:
        raise ContractError(f"{name} is longer than {rule['maxLength']}")
    if name == "timestamp":
        try:
            datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError as error:
            raise ContractError(f"timestamp is not a real date: {value}") from error


def _validate_number(name: str, value: object, rule: dict) -> None:
    # bool is an int subclass; a JSON true must not pass as a number.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractError(f"{name} must be a number")
    if not math.isfinite(value):
        raise ContractError(f"{name} must be finite")
    if "minimum" in rule and value < rule["minimum"]:
        raise ContractError(f"{name} is below {rule['minimum']}")
    if "maximum" in rule and value > rule["maximum"]:
        raise ContractError(f"{name} is above {rule['maximum']}")

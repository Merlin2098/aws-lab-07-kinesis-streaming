from __future__ import annotations

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads(
    (REPO_ROOT / "src" / "contracts" / "package_event_contract.json").read_text(
        encoding="utf-8"
    )
)
GO_SOURCE = (REPO_ROOT / "producer" / "internal" / "event" / "event.go").read_text(
    encoding="utf-8"
)


def go_constant(name: str) -> str:
    match = re.search(rf"{name}\s*=\s*`([^`]+)`", GO_SOURCE)
    assert match, f"{name} not found in event.go"
    return match.group(1)


def test_go_statuses_match_the_contract_enum() -> None:
    block = re.search(r"var Statuses = \[\]string\{([^}]*)\}", GO_SOURCE)
    assert block, "Statuses not found in event.go"
    assert (
        re.findall(r'"([A-Z_]+)"', block.group(1))
        == CONTRACT["properties"]["status"]["enum"]
    )


def test_go_patterns_match_the_contract() -> None:
    properties = CONTRACT["properties"]
    assert go_constant("PackageIDPattern") == properties["package_id"]["pattern"]
    assert go_constant("TimestampPattern") == properties["timestamp"]["pattern"]

"""Static checks for the AGENTS.md Terraform guardrails (work without a provider)."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

INFRA = Path(__file__).resolve().parents[1] / "infra"
MODULES = ["kinesis", "dynamodb", "consumer", "datalake"]
TAGGABLE = [
    "aws_kinesis_stream",
    "aws_dynamodb_table",
    "aws_lambda_function",
    "aws_lambda_event_source_mapping",
    "aws_sqs_queue",
    "aws_iam_role",
    "aws_s3_bucket",
    "aws_cloudwatch_log_group",
    "aws_kinesis_firehose_delivery_stream",
]


def module_text(name: str) -> str:
    folder = INFRA / "modules" / name
    return "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(folder.glob("*.tf"))
    )


def all_text() -> str:
    return "\n".join(module_text(name) for name in MODULES)


def blocks(text: str, resource_type: str) -> list[str]:
    found = []
    for match in re.finditer(rf'resource "{resource_type}" "[^"]+" \{{', text):
        depth, index = 1, match.end()
        while depth:
            depth += {"{": 1, "}": -1}.get(text[index], 0)
            index += 1
        found.append(text[match.start() : index])
    return found


@pytest.mark.parametrize("module", MODULES)
def test_module_exposes_required_outputs(module: str) -> None:
    outputs = (INFRA / "modules" / module / "outputs.tf").read_text(encoding="utf-8")
    for name in ("resource_arn", "log_group_name", "log_group_arn"):
        assert f'output "{name}"' in outputs, f"{module} must expose {name}"


@pytest.mark.parametrize("module", MODULES)
def test_module_accepts_tags(module: str) -> None:
    variables = (INFRA / "modules" / module / "variables.tf").read_text(
        encoding="utf-8"
    )
    assert 'variable "tags"' in variables


def test_every_log_group_sets_retention() -> None:
    groups = blocks(all_text(), "aws_cloudwatch_log_group")
    assert groups, "expected at least one explicit log group"
    for block in groups:
        assert "retention_in_days" in block, block.splitlines()[0]


@pytest.mark.parametrize("resource_type", TAGGABLE)
def test_taggable_resources_are_tagged(resource_type: str) -> None:
    for block in blocks(all_text(), resource_type):
        assert "tags" in block, block.splitlines()[0]


def test_s3_versioning_is_never_enabled_by_default() -> None:
    assert "aws_s3_bucket_versioning" not in all_text()


def test_datalake_bucket_blocks_public_access() -> None:
    text = module_text("datalake")
    block = blocks(text, "aws_s3_bucket_public_access_block")[0]
    for flag in (
        "block_public_acls",
        "block_public_policy",
        "ignore_public_acls",
        "restrict_public_buckets",
    ):
        assert re.search(rf"{flag}\s*=\s*true", block), flag


def test_dlq_send_policy_lives_in_the_consumer_module() -> None:
    assert "sqs:SendMessage" in module_text("consumer")
    for other in ("kinesis", "dynamodb", "datalake"):
        assert "sqs:SendMessage" not in module_text(other)


@pytest.mark.parametrize("module", ["consumer", "datalake"])
def test_roles_explicitly_deny_unused_actions(module: str) -> None:
    assert re.search(r'effect\s*=\s*"Deny"', module_text(module))


@pytest.mark.parametrize("module", MODULES)
def test_root_wires_every_module_with_common_tags(module: str) -> None:
    main = (INFRA / "main.tf").read_text(encoding="utf-8")
    match = re.search(rf'module "{module}" \{{(.*?)\n\}}', main, re.S)
    assert match, f"module {module} is not wired in infra/main.tf"
    body = match.group(1)
    assert re.search(rf'source\s*=\s*"\./modules/{module}"', body)
    assert re.search(r"tags\s*=\s*local\.common_tags", body)


def test_root_budget_is_tagged() -> None:
    main = (INFRA / "main.tf").read_text(encoding="utf-8")
    budgets = blocks(main, "aws_budgets_budget")
    assert budgets
    for block in budgets:
        assert "tags" in block

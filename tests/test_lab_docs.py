"""Checks that the demo runbook and its config examples do not mislead the instructor."""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS = ["README.md", "docs/deploy-guide.md", "docs/producer-api.md"]


def read(relative: str) -> str:
    return (REPO_ROOT / relative).read_text(encoding="utf-8")


def demo_section() -> str:
    return read("README.md").split("## 3. Demo")[1].split("## Notas para la clase")[0]


def active_assignments(text: str) -> dict[str, str]:
    return {
        match.group(1): match.group(2)
        for match in re.finditer(r"^([A-Z_]+)=(.*)$", text, re.MULTILINE)
    }


def test_tfvars_example_sets_cost_center_through_its_own_variable() -> None:
    # infra/main.tf merges var.cost_center AFTER var.tags, so a CostCenter key
    # inside `tags` is silently overwritten.
    text = read("infra/terraform.tfvars.example")
    assert re.search(r'^cost_center\s*=\s*"data-platform"', text, re.MULTILINE)
    assert "CostCenter" not in text


def test_credentials_template_is_the_single_file_for_keys_and_producer_settings() -> (
    None
):
    values = active_assignments(read(".env.credentials.example"))
    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_DEFAULT_REGION",
        "STREAM_NAME",
        "PORT",
        "EMIT_PACKAGES",
    ):
        assert name in values, name
    assert values["EMIT_INTERVAL"] == "25s"
    assert re.fullmatch(r"\d+", values["EMIT_PACKAGES"])


def test_credentials_template_does_not_set_an_aws_profile() -> None:
    # A profile name that does not exist makes the AWS CLI and boto3 fail with
    # "The config profile (...) could not be found", even when static keys are set.
    assert "AWS_PROFILE" not in active_assignments(read(".env.credentials.example"))


def test_credentials_file_is_git_ignored_but_its_template_is_not() -> None:
    lines = read(".gitignore").splitlines()
    assert ".env.*" in lines
    assert "!.env.credentials.example" in lines


def test_docs_use_the_single_credentials_file_and_no_aws_profiles() -> None:
    for path in DOCS:
        text = read(path)
        assert ".env.example" not in text, path
        assert "--profile" not in text, path
        # every `source .env…` must load .env.credentials
        assert not re.search(r"source \.env(?!\.credentials)", text), path


def test_producer_guide_tells_not_to_set_an_aws_profile() -> None:
    assert "no definas `AWS_PROFILE`" in read("docs/producer-api.md")


def test_demo_terminal_loads_the_credentials_before_using_the_aws_cli() -> None:
    demo = demo_section()
    assert "source .env.credentials" in demo
    assert demo.index("source .env.credentials") < demo.index("aws kinesis")


def test_demo_warns_that_the_first_read_can_lag_a_few_seconds() -> None:
    assert re.search(r"[Ee]spera[^\n]*\d+-\d+\s*s", demo_section())


def test_producer_guide_documents_the_automatic_emitter() -> None:
    guide = read("docs/producer-api.md")
    for needle in ("EMIT_INTERVAL", "EMIT_PACKAGES", "cada 25 s"):
        assert needle in guide, needle
    # the real-time demo must start the emitter, not rely on manual sends
    assert "EMIT_INTERVAL=25s" in guide.split("## 4. Ver el pipeline en tiempo real")[1]


def test_readme_mentions_the_automatic_emitter() -> None:
    assert "EMIT_INTERVAL" in read("README.md")


def test_producer_guide_explains_firehose_writes_one_object_per_shard() -> None:
    # Verified live: with 2 shards Firehose wrote 2 objects ~5 s apart (one per shard buffer).
    guide = read("docs/producer-api.md")
    assert "un objeto por shard" in guide
    assert "aproximadamente un objeto nuevo por minuto" not in guide


def line_count(path: str) -> int:
    return len(read(path).splitlines())


def test_guides_stay_short_enough_to_follow() -> None:
    assert line_count("docs/deploy-guide.md") <= 110
    assert line_count("docs/producer-api.md") <= 170


def test_deploy_guide_only_deploys() -> None:
    # Running the API and watching events is the producer guide's single job.
    text = read("docs/deploy-guide.md")
    assert "producer.exe" not in text
    assert "producer-api.md" in text


def test_each_guide_has_one_consecutive_numbered_path() -> None:
    for path in ("docs/deploy-guide.md", "docs/producer-api.md"):
        numbers = re.findall(r"^## (\d+)\. ", read(path), re.MULTILINE)
        assert numbers == [str(i) for i in range(1, len(numbers) + 1)], (path, numbers)
        # no second numbering hidden inside a section ("Paso 1", "Paso 2"...)
        assert "### Paso" not in read(path), path


def test_readme_does_not_point_to_removed_guide_sections() -> None:
    assert "sección 5.1" not in read("README.md")


def test_local_links_in_the_docs_resolve() -> None:
    for path in DOCS:
        base = (REPO_ROOT / path).parent
        for target in re.findall(r"\]\(([^)#\s]+)(?:#[^)]*)?\)", read(path)):
            if target.startswith(("http://", "https://", "mailto:")):
                continue
            assert (base / target).exists(), f"{path} links to missing {target}"


def test_docs_do_not_ask_to_save_the_plan() -> None:
    # `apply` shows the plan and asks for confirmation; no plan file is needed.
    for path in DOCS:
        text = read(path)
        assert "tfplan" not in text, path
        assert "-out=" not in text, path


def test_tfvars_is_optional_not_a_step_of_the_main_path() -> None:
    # No terraform.tfvars exists and every variable has a default: `init` never reads it,
    # `plan`/`apply` only pick it up when the file is there.
    guide = read("docs/deploy-guide.md")
    plan_section = guide.split("## 3. Plan")[1].split("## 4.")[0]
    commands = "\n".join(re.findall(r"```bash\n(.*?)```", plan_section, re.DOTALL))
    assert "tfvars" not in commands
    assert "opcional" in plan_section.lower() and "terraform.tfvars" in plan_section
    readme_commands = "\n".join(
        re.findall(r"```bash\n(.*?)```", read("README.md"), re.DOTALL)
    )
    assert "cp terraform.tfvars.example" not in readme_commands

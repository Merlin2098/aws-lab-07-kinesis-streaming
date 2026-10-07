# Lab Streaming AWS — Plan de implementación

> **Para agentes:** SUB-SKILL REQUERIDO: usar superpowers:executing-plans (ejecución nativa, elegida por el instructor) para implementar este plan tarea por tarea. Los pasos usan casillas `- [ ]`.

**Goal:** Implementar el lab de la Sesión 7: Go producer → Kinesis (2 shards) → Lambda Python → DynamoDB (estado actual) y → Firehose → S3 (histórico), desplegable con Terraform por módulos.

**Architecture:** Un stream Kinesis con dos consumidores independientes. La Lambda valida contra un contrato JSON y escribe en DynamoDB con escritura condicional por `updated_at`; Firehose entrega JSON Lines a S3 con particionado temporal configurable. Terraform en módulos `kinesis`, `dynamodb`, `consumer`, `datalake` cableados desde `infra/main.tf`.

**Tech Stack:** Go 1.26 + AWS SDK for Go v2; Python 3.12 (runtime Lambda) / 3.14 (local) + boto3 + pytest; Terraform 1.14 + provider `aws ~> 5.0` + `archive ~> 2.4`; `uv` para Python.

**Spec:** [docs/specs/2026-10-06-streaming-lab-design.md](../specs/2026-10-06-streaming-lab-design.md) (diseño) y [spec funcional](../specs/spec_sesion_7_streaming_aws_laboratorio_refinado.md). ADR: [0001](../internal/adr/0001-streaming-architecture.md).

## Global Constraints

- Región por defecto `us-east-1`; Terraform `>= 1.6.0`; provider `hashicorp/aws ~> 5.0`.
- Stream: `logistics-events`, `PROVISIONED`, **2 shards**, retención **24 h**; partition key = `package_id`.
- Estados válidos exactos: `CREATED`, `PICKED_UP`, `IN_TRANSIT`, `OUT_FOR_DELIVERY`, `DELIVERED`, `CANCELLED`.
- Timestamp: UTC con formato exacto `YYYY-MM-DDTHH:MM:SSZ` (sin offsets ni fracciones), para que la comparación de texto sea válida.
- Contrato: `package_id`, `timestamp`, `latitude`, `longitude`, `status` obligatorios; `city` y `driver_id` opcionales; sin otros campos. (El ejemplo abreviado del spec §11 es ilustrativo; el esquema mínimo del §7 manda.)
- Cada registro publicado termina en `\n` (JSON Lines en S3).
- DynamoDB: solo estado actual, clave `package_id`, `PAY_PER_REQUEST`. Condición: `attribute_not_exists(package_id) OR updated_at < :ts`.
- Lambda: `ReportBatchItemFailures`, reintentos acotados, SQS como destino de fallos; solo stdlib + boto3 (sin `jsonschema` en el zip).
- Particionado S3: variable `hour` (defecto) o `minute`; buffer Firehose 60 s por defecto.
- AGENTS.md: tags comunes con `CostCenter` en todo recurso; `aws_cloudwatch_log_group` explícito con `retention_in_days`; cada módulo expone `log_group_name`, `log_group_arn`, `resource_arn`; todo permiso IAM declarado en Terraform (con `Deny` explícito de acciones no usadas); buckets privados con bloqueo de acceso público; sin versioning de S3; valores por ambiente en variables/`.env`, nunca en código; sin secretos en el repo.
- **Nunca** ejecutar `terraform apply` / `destroy` sin aprobación explícita del instructor. No tocar ni borrar `terraform.tfstate`.
- El directorio no es un repo git: los pasos "Checkpoint" reemplazan a los commits. Si el instructor ejecuta `git init`, cada checkpoint pasa a ser un commit.
- Comandos con Git Bash desde la raíz del repo. Python: `uv run python scripts/testing/run_pytest.py <args>` (el wrapper fija el path; `pytest` directo no encuentra `scripts`). Ruff: `uv run python scripts/testing/run_ruff_check.py` y `run_ruff_format.py`.
- Estado conocido del repo (anterior a este plan, no se arregla aquí): `tests/test_installer.py` y `tests/test_restore_project.py` no se recolectan y 4 pruebas de `test_install_entrypoints.py` fallan porque faltan los scripts de instalación. Ejecutar siempre con `--ignore=tests/test_installer.py --ignore=tests/test_restore_project.py --deselect tests/test_install_entrypoints.py` para ver solo las pruebas de este plan.

## Review Focus

Entradas/condiciones que el spec implica y que más probablemente rompan el lab (cada una tiene su prueba en la tarea indicada):

1. **Evento viejo llega después de uno nuevo** (reintento, orden entre lotes): no debe pisar el estado actual ni contar como fallo → Task 2.
2. **Registro envenenado dentro de un lote con registros válidos** (base64 roto, JSON roto, contrato violado): solo el roto se reporta; los demás se escriben → Task 2.
3. **Timestamp con offset (`+00:00`), con fracción, o imposible (`2026-13-45T…`)** y **`NaN`/`Infinity`** en latitud/longitud: se rechazan igual en Python y Go → Tasks 1 y 3.
4. **`PutRecords` con éxito parcial** (HTTP 200 con `FailedRecordCount > 0`): el producer devuelve 207 con el error por registro → Task 3.
5. **Lat/long como `float`**: boto3 no acepta `float` en DynamoDB; se convierten a `Decimal` → Task 2. Y **lotes vacíos o de más de 500 eventos** y **cuerpos enormes** al producer → Task 3.

---

## Task 0: Desbloquear `terraform validate` (diagnóstico acotado)

**Contexto:** el plugin `terraform-provider-aws_v5.100.0` arranca, hace el handshake y sale con `exit status 1` durante `GetProviderSchema` (sin panic visible). Sin esto no se puede validar Terraform localmente. Límite de tiempo: 20 minutos; si no se resuelve, se documenta como brecha y se valida en otra máquina/CI.

**Files:** ninguno (diagnóstico). Si se cambia la versión del provider: `infra/providers.tf`.

- [ ] **Step 1: Medir memoria disponible**

Run: `powershell -Command "Get-CimInstance Win32_OperatingSystem | Select FreePhysicalMemory,TotalVirtualMemorySize,FreeVirtualMemory"`
Expected: el schema del provider AWS necesita ~1.5 GB libres. Si `FreePhysicalMemory` < 2 GB (en KB), cerrar aplicaciones pesadas y reintentar.

- [ ] **Step 2: Reintentar con menos presión de memoria**

Run (en `infra/`): `GOMEMLIMIT=1500MiB TF_LOG_PROVIDER=TRACE terraform validate -no-color 2>&1 | tail -20`
Expected: `Success! The configuration is valid.` Si falla igual, seguir al paso 3.

- [ ] **Step 3: Probar con el binario del provider aislado del antivirus**

Run: `powershell -Command "Get-MpComputerStatus | Select RealTimeProtectionEnabled; Get-MpThreatDetection | Select -First 3 Resources"`
Si Defender u otro antivirus detectó/bloqueó `terraform-provider-aws*.exe`, pedir al instructor que añada `infra/.terraform/` a las exclusiones (es una decisión suya, no la tomo yo).

- [ ] **Step 4: Decidir**

Si algún paso funciona, anotar la causa en `docs/known-issues.md` (archivo que `tests/aws/aws_session.py` ya referencia) y continuar. Si ninguno funciona, anotar "terraform validate bloqueado localmente" en ese mismo archivo y continuar: en las tareas Terraform se usa `terraform fmt -check -recursive` como verificación local y la validación completa queda para el despliegue aprobado o CI.

- [ ] **Checkpoint:** Terraform validable (sí/no) y decisión registrada.

---

## Task 1: Contrato del evento y validador (Python)

**Files:**
- Create: `src/contracts/package_event_contract.json`
- Create: `src/lambda/consumer/contract.py`
- Test: `tests/test_event_contract.py`
- Delete: `src/contracts/.gitkeep`

**Interfaces:**
- Produces: `contract.ContractError(ValueError)`; `contract.load_contract(path: Path | None = None) -> dict`; `contract.validate_event(event: object, contract: dict) -> None` (lanza `ContractError`).

- [ ] **Step 1: Crear el contrato**

`src/contracts/package_event_contract.json`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "PackageEvent",
  "type": "object",
  "additionalProperties": false,
  "required": ["package_id", "timestamp", "latitude", "longitude", "status"],
  "properties": {
    "package_id": {
      "type": "string",
      "pattern": "^[A-Za-z0-9-]{1,64}$"
    },
    "timestamp": {
      "type": "string",
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
    },
    "latitude": { "type": "number", "minimum": -90, "maximum": 90 },
    "longitude": { "type": "number", "minimum": -180, "maximum": 180 },
    "status": {
      "type": "string",
      "enum": [
        "CREATED",
        "PICKED_UP",
        "IN_TRANSIT",
        "OUT_FOR_DELIVERY",
        "DELIVERED",
        "CANCELLED"
      ]
    },
    "city": { "type": "string", "minLength": 1, "maxLength": 100 },
    "driver_id": { "type": "string", "minLength": 1, "maxLength": 64 }
  }
}
```

- [ ] **Step 2: Escribir las pruebas que fallan**

`tests/test_event_contract.py`:

```python
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
```

- [ ] **Step 3: Verificar que fallan**

Run: `uv run python scripts/testing/run_pytest.py tests/test_event_contract.py -q`
Expected: error de recolección `ModuleNotFoundError: No module named 'contract'`.

- [ ] **Step 4: Implementar el validador**

`src/lambda/consumer/contract.py`:

```python
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
```

- [ ] **Step 5: Verificar que pasan**

Run: `uv run python scripts/testing/run_pytest.py tests/test_event_contract.py -q`
Expected: todas pasan (`passed`, 0 failed).

- [ ] **Step 6: Lint y formato**

Run: `uv run python scripts/testing/run_ruff_check.py src/lambda tests/test_event_contract.py && uv run python scripts/testing/run_ruff_format.py src/lambda tests/test_event_contract.py`
Expected: sin errores (el formato puede reescribir archivos; aceptar). Luego `rm src/contracts/.gitkeep`.

- [ ] **Checkpoint:** `tests/test_event_contract.py` en verde y ruff limpio.

---

## Task 2: Lambda consumidora (handler Python)

**Files:**
- Create: `src/lambda/consumer/handler.py`
- Test: `tests/test_consumer_handler.py`
- Delete: `src/lambda/consumer/.gitkeep`

**Interfaces:**
- Consumes: `contract.ContractError`, `contract.load_contract`, `contract.validate_event` (Task 1).
- Produces: `handler.process_batch(event: dict, table, contract: dict) -> dict` devolviendo `{"batchItemFailures": [{"itemIdentifier": "<sequenceNumber>"}]}`; `handler.lambda_handler(event, context) -> dict` (usa `TABLE_NAME` del entorno); `handler.write_state(table, package_event: dict) -> bool` (`False` = evento viejo ignorado); `handler.to_item(package_event: dict) -> dict`.

- [ ] **Step 1: Escribir las pruebas que fallan**

`tests/test_consumer_handler.py`:

```python
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

CONTRACT = load_contract(REPO_ROOT / "src" / "contracts" / "package_event_contract.json")


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
        if current is not None and not current["updated_at"] < ExpressionAttributeValues[":ts"]:
            raise client_error("ConditionalCheckFailedException")
        self.items[package_id] = Item


def event_for(package_id="PKG-001", timestamp="2026-10-04T10:30:00Z", status="IN_TRANSIT", **extra):
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
            record(event_for(timestamp="2026-10-04T10:30:00Z", status="IN_TRANSIT"), "2"),
        ],
        table,
    )
    assert table.items["PKG-001"]["status"] == "IN_TRANSIT"


def test_stale_event_does_not_overwrite_and_is_not_a_failure() -> None:
    table = FakeTable()
    result = run(
        [
            record(event_for(timestamp="2026-10-04T11:20:00Z", status="OUT_FOR_DELIVERY"), "1"),
            record(event_for(timestamp="2026-10-04T10:30:00Z", status="IN_TRANSIT"), "2"),
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


def test_lambda_handler_wires_table_and_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    table = FakeTable()
    monkeypatch.setattr(handler, "_get_table", lambda: table)
    monkeypatch.setattr(handler, "_get_contract", lambda: CONTRACT)

    result = handler.lambda_handler({"Records": [record(event_for(), "1")]}, None)

    assert result == {"batchItemFailures": []}
    assert "PKG-001" in table.items
```

- [ ] **Step 2: Verificar que fallan**

Run: `uv run python scripts/testing/run_pytest.py tests/test_consumer_handler.py -q`
Expected: `ModuleNotFoundError: No module named 'handler'`.

- [ ] **Step 3: Implementar el handler**

`src/lambda/consumer/handler.py`:

```python
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
```

- [ ] **Step 4: Verificar que pasan**

Run: `uv run python scripts/testing/run_pytest.py tests/test_consumer_handler.py tests/test_event_contract.py -q`
Expected: todas pasan.

- [ ] **Step 5: Lint y formato**

Run: `uv run python scripts/testing/run_ruff_check.py src/lambda tests/test_consumer_handler.py && uv run python scripts/testing/run_ruff_format.py src/lambda tests/test_consumer_handler.py`; después `rm src/lambda/consumer/.gitkeep`.
Expected: sin errores.

- [ ] **Checkpoint:** Tasks 1–2 en verde.

---

## Task 3: Producer Go (API HTTP → Kinesis)

**Files:**
- Create: `producer/go.mod`, `producer/go.sum`
- Create: `producer/internal/event/event.go`, `producer/internal/event/event_test.go`
- Create: `producer/internal/publisher/publisher.go`, `producer/internal/publisher/publisher_test.go`
- Create: `producer/internal/server/server.go`, `producer/internal/server/server_test.go`
- Create: `producer/cmd/producer/main.go`
- Create: `tests/test_contract_parity.py`
- Delete: `producer/cmd/producer/.gitkeep`, `producer/internal/.gitkeep`

**Interfaces (producidas, usadas entre sub-pasos):**
- `event.Event{PackageID, Timestamp string; Latitude, Longitude *float64; Status, City, DriverID string}`
- `event.Parse(io.Reader) (Event, error)`, `event.ParseBatch(io.Reader) ([]Event, error)`, `(Event).Validate() error`, `(Event).Encode() ([]byte, error)` (JSON + `\n`)
- `event.PackageIDPattern`, `event.TimestampPattern` (strings), `event.Statuses` (`[]string`)
- `publisher.KinesisAPI` (interfaz con `PutRecord` y `PutRecords`), `publisher.Publisher{API KinesisAPI; StreamName string}`, `(*Publisher).Publish(ctx, event.Event) (Result, error)`, `(*Publisher).PublishBatch(ctx, []event.Event) ([]Result, error)`, `publisher.Result{PackageID, ShardID, SequenceNumber, Error string}`, `publisher.MaxBatchSize = 500`, `publisher.ErrBatchSize`
- `server.Publisher` (interfaz con `Publish`/`PublishBatch`), `server.New(Publisher) http.Handler`

- [ ] **Step 1: Inicializar el módulo y dependencias**

Run (en `producer/`): 
```bash
go mod init producer
go get github.com/aws/aws-sdk-go-v2/aws github.com/aws/aws-sdk-go-v2/config github.com/aws/aws-sdk-go-v2/service/kinesis
rm cmd/producer/.gitkeep internal/.gitkeep
```
Expected: `go.mod` con `module producer` y las tres dependencias (versiones fijadas por `go.sum`; no se editan a mano).

- [ ] **Step 2: Pruebas del paquete `event` (fallan)**

`producer/internal/event/event_test.go`:

```go
package event

import (
	"encoding/json"
	"strings"
	"testing"
)

func body(t *testing.T, mutate func(m map[string]any)) string {
	t.Helper()
	m := map[string]any{
		"package_id": "PKG-001",
		"timestamp":  "2026-10-04T10:30:00Z",
		"latitude":   -12.0464,
		"longitude":  -77.0428,
		"status":     "IN_TRANSIT",
	}
	if mutate != nil {
		mutate(m)
	}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestParseValid(t *testing.T) {
	e, err := Parse(strings.NewReader(body(t, func(m map[string]any) {
		m["city"] = "Lima"
		m["driver_id"] = "DRV-001"
	})))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.PackageID != "PKG-001" || e.Status != "IN_TRANSIT" || e.City != "Lima" {
		t.Fatalf("unexpected event: %+v", e)
	}
	if *e.Latitude != -12.0464 {
		t.Fatalf("latitude = %v", *e.Latitude)
	}
}

func TestParseRejects(t *testing.T) {
	cases := []struct {
		name   string
		body   string
		wanted string
	}{
		{"missing latitude", body(t, func(m map[string]any) { delete(m, "latitude") }), "latitude is required"},
		{"null latitude", body(t, func(m map[string]any) { m["latitude"] = nil }), "latitude is required"},
		{"offset timestamp", body(t, func(m map[string]any) { m["timestamp"] = "2026-10-04T10:30:00+00:00" }), "timestamp must be UTC"},
		{"fractional timestamp", body(t, func(m map[string]any) { m["timestamp"] = "2026-10-04T10:30:00.123Z" }), "timestamp must be UTC"},
		{"impossible date", body(t, func(m map[string]any) { m["timestamp"] = "2026-13-45T10:30:00Z" }), "not a real date"},
		{"unknown status", body(t, func(m map[string]any) { m["status"] = "LOST" }), "status must be one of"},
		{"latitude out of range", body(t, func(m map[string]any) { m["latitude"] = 91 }), "latitude must be between"},
		{"longitude out of range", body(t, func(m map[string]any) { m["longitude"] = -180.5 }), "longitude must be between"},
		{"empty package id", body(t, func(m map[string]any) { m["package_id"] = "" }), "package_id must match"},
		{"unknown field", body(t, func(m map[string]any) { m["speed"] = 10 }), "invalid JSON"},
		{"not json", "nope", "invalid JSON"},
		{"trailing data", body(t, nil) + " {}", "unexpected data"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(strings.NewReader(tc.body))
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wanted) {
				t.Fatalf("error %q does not contain %q", err, tc.wanted)
			}
		})
	}
}

func TestEncodeEndsWithNewlineAndRoundTrips(t *testing.T) {
	e, err := Parse(strings.NewReader(body(t, nil)))
	if err != nil {
		t.Fatal(err)
	}
	data, err := e.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(data), "\n") || strings.Count(string(data), "\n") != 1 {
		t.Fatalf("expected exactly one trailing newline, got %q", data)
	}
	again, err := Parse(strings.NewReader(string(data)))
	if err != nil {
		t.Fatalf("encoded event does not parse: %v", err)
	}
	if again.PackageID != e.PackageID || *again.Latitude != *e.Latitude {
		t.Fatalf("round trip changed the event: %+v vs %+v", again, e)
	}
}

func TestParseBatch(t *testing.T) {
	good := body(t, nil)
	events, err := ParseBatch(strings.NewReader("[" + good + "," + good + "]"))
	if err != nil || len(events) != 2 {
		t.Fatalf("got %d events, err=%v", len(events), err)
	}

	bad := body(t, func(m map[string]any) { m["status"] = "LOST" })
	_, err = ParseBatch(strings.NewReader("[" + good + "," + bad + "]"))
	if err == nil || !strings.Contains(err.Error(), "event 1") {
		t.Fatalf("expected an error naming event 1, got %v", err)
	}

	if _, err = ParseBatch(strings.NewReader(good)); err == nil {
		t.Fatal("a single object is not a batch")
	}
}
```

- [ ] **Step 3: Verificar que fallan**

Run (en `producer/`): `go test ./internal/event/`
Expected: FAIL (`undefined: Parse`, etc.).

- [ ] **Step 4: Implementar `event`**

`producer/internal/event/event.go`:

```go
// Package event defines the package-tracking event and its validation rules.
// The rules mirror src/contracts/package_event_contract.json;
// tests/test_contract_parity.py keeps both in sync.
package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"
)

const (
	PackageIDPattern = `^[A-Za-z0-9-]{1,64}$`
	TimestampPattern = `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$`
)

var Statuses = []string{"CREATED", "PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "CANCELLED"}

var (
	packageIDRe = regexp.MustCompile(PackageIDPattern)
	timestampRe = regexp.MustCompile(TimestampPattern)
)

// Event is one tracking update. Latitude and Longitude are pointers so a
// missing or null value can be told apart from a legitimate 0.
type Event struct {
	PackageID string   `json:"package_id"`
	Timestamp string   `json:"timestamp"`
	Latitude  *float64 `json:"latitude"`
	Longitude *float64 `json:"longitude"`
	Status    string   `json:"status"`
	City      string   `json:"city,omitempty"`
	DriverID  string   `json:"driver_id,omitempty"`
}

func (e Event) Validate() error {
	if !packageIDRe.MatchString(e.PackageID) {
		return fmt.Errorf("package_id must match %s", PackageIDPattern)
	}
	if !timestampRe.MatchString(e.Timestamp) {
		return errors.New("timestamp must be UTC formatted as YYYY-MM-DDTHH:MM:SSZ")
	}
	if _, err := time.Parse(time.RFC3339, e.Timestamp); err != nil {
		return fmt.Errorf("timestamp is not a real date: %s", e.Timestamp)
	}
	if err := checkRange("latitude", e.Latitude, -90, 90); err != nil {
		return err
	}
	if err := checkRange("longitude", e.Longitude, -180, 180); err != nil {
		return err
	}
	if !slices.Contains(Statuses, e.Status) {
		return fmt.Errorf("status must be one of %s", strings.Join(Statuses, ", "))
	}
	if len(e.City) > 100 {
		return errors.New("city is longer than 100 characters")
	}
	if len(e.DriverID) > 64 {
		return errors.New("driver_id is longer than 64 characters")
	}
	return nil
}

func checkRange(name string, value *float64, lo, hi float64) error {
	if value == nil {
		return fmt.Errorf("%s is required", name)
	}
	if math.IsNaN(*value) || math.IsInf(*value, 0) || *value < lo || *value > hi {
		return fmt.Errorf("%s must be between %g and %g", name, lo, hi)
	}
	return nil
}

// Encode returns the JSON record with a trailing newline. Firehose concatenates
// records without a separator, so the newline keeps the S3 objects JSON Lines.
func (e Event) Encode() ([]byte, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func Parse(r io.Reader) (Event, error) {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	var e Event
	if err := decoder.Decode(&e); err != nil {
		return Event{}, fmt.Errorf("invalid JSON: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return Event{}, errors.New("unexpected data after the JSON body")
	}
	if err := e.Validate(); err != nil {
		return Event{}, err
	}
	return e, nil
}

func ParseBatch(r io.Reader) ([]Event, error) {
	decoder := json.NewDecoder(r)
	var raw []json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("body must be a JSON array of events: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("unexpected data after the JSON body")
	}
	events := make([]Event, 0, len(raw))
	for i, item := range raw {
		e, err := Parse(bytes.NewReader(item))
		if err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}
		events = append(events, e)
	}
	return events, nil
}
```

- [ ] **Step 5: Verificar `event`**

Run (en `producer/`): `go test ./internal/event/ -v`
Expected: PASS.

- [ ] **Step 6: Pruebas del paquete `publisher` (fallan)**

`producer/internal/publisher/publisher_test.go`:

```go
package publisher

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"producer/internal/event"
)

type fakeAPI struct {
	putRecordIn   *kinesis.PutRecordInput
	putRecordsIn  *kinesis.PutRecordsInput
	putRecordOut  *kinesis.PutRecordOutput
	putRecordsOut *kinesis.PutRecordsOutput
	err           error
}

func (f *fakeAPI) PutRecord(_ context.Context, in *kinesis.PutRecordInput, _ ...func(*kinesis.Options)) (*kinesis.PutRecordOutput, error) {
	f.putRecordIn = in
	return f.putRecordOut, f.err
}

func (f *fakeAPI) PutRecords(_ context.Context, in *kinesis.PutRecordsInput, _ ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
	f.putRecordsIn = in
	return f.putRecordsOut, f.err
}

func sample(id string) event.Event {
	lat, lon := -12.0464, -77.0428
	return event.Event{PackageID: id, Timestamp: "2026-10-04T10:30:00Z", Latitude: &lat, Longitude: &lon, Status: "IN_TRANSIT"}
}

func TestPublishUsesPackageIDAsPartitionKey(t *testing.T) {
	api := &fakeAPI{putRecordOut: &kinesis.PutRecordOutput{ShardId: aws.String("shardId-000000000001"), SequenceNumber: aws.String("4959")}}
	p := &Publisher{API: api, StreamName: "logistics-events"}

	res, err := p.Publish(context.Background(), sample("PKG-001"))
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(api.putRecordIn.StreamName) != "logistics-events" {
		t.Fatalf("stream = %q", aws.ToString(api.putRecordIn.StreamName))
	}
	if aws.ToString(api.putRecordIn.PartitionKey) != "PKG-001" {
		t.Fatalf("partition key = %q", aws.ToString(api.putRecordIn.PartitionKey))
	}
	if !strings.HasSuffix(string(api.putRecordIn.Data), "\n") {
		t.Fatalf("record must end with a newline: %q", api.putRecordIn.Data)
	}
	if res.ShardID != "shardId-000000000001" || res.SequenceNumber != "4959" || res.PackageID != "PKG-001" {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestPublishWrapsAPIError(t *testing.T) {
	boom := errors.New("boom")
	p := &Publisher{API: &fakeAPI{err: boom}, StreamName: "s"}
	if _, err := p.Publish(context.Background(), sample("PKG-001")); !errors.Is(err, boom) {
		t.Fatalf("expected wrapped boom, got %v", err)
	}
}

func TestPublishBatchReportsPartialFailure(t *testing.T) {
	api := &fakeAPI{putRecordsOut: &kinesis.PutRecordsOutput{
		FailedRecordCount: aws.Int32(1),
		Records: []types.PutRecordsResultEntry{
			{ShardId: aws.String("shardId-000000000000"), SequenceNumber: aws.String("1")},
			{ErrorCode: aws.String("ProvisionedThroughputExceededException"), ErrorMessage: aws.String("slow down")},
		},
	}}
	p := &Publisher{API: api, StreamName: "s"}

	results, err := p.PublishBatch(context.Background(), []event.Event{sample("PKG-001"), sample("PKG-002")})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Error != "" || results[0].ShardID != "shardId-000000000000" {
		t.Fatalf("first result: %+v", results[0])
	}
	if !strings.Contains(results[1].Error, "ProvisionedThroughputExceededException") || results[1].PackageID != "PKG-002" {
		t.Fatalf("second result: %+v", results[1])
	}
	if aws.ToString(api.putRecordsIn.Records[1].PartitionKey) != "PKG-002" {
		t.Fatal("each record must use its own package_id as partition key")
	}
}

func TestPublishBatchRejectsBadSizesWithoutCallingKinesis(t *testing.T) {
	api := &fakeAPI{}
	p := &Publisher{API: api, StreamName: "s"}

	if _, err := p.PublishBatch(context.Background(), nil); !errors.Is(err, ErrBatchSize) {
		t.Fatalf("empty batch: %v", err)
	}
	tooMany := make([]event.Event, MaxBatchSize+1)
	for i := range tooMany {
		tooMany[i] = sample("PKG-001")
	}
	if _, err := p.PublishBatch(context.Background(), tooMany); !errors.Is(err, ErrBatchSize) {
		t.Fatalf("oversized batch: %v", err)
	}
	if api.putRecordsIn != nil {
		t.Fatal("Kinesis must not be called for an invalid batch size")
	}
}

func TestPublishBatchDetectsResultCountMismatch(t *testing.T) {
	api := &fakeAPI{putRecordsOut: &kinesis.PutRecordsOutput{Records: []types.PutRecordsResultEntry{}}}
	p := &Publisher{API: api, StreamName: "s"}
	if _, err := p.PublishBatch(context.Background(), []event.Event{sample("PKG-001")}); err == nil {
		t.Fatal("expected an error when Kinesis returns fewer results than events")
	}
}
```

- [ ] **Step 7: Verificar que fallan**

Run: `go test ./internal/publisher/`
Expected: FAIL (`undefined: Publisher`).

- [ ] **Step 8: Implementar `publisher`**

`producer/internal/publisher/publisher.go`:

```go
// Package publisher sends events to Kinesis Data Streams.
package publisher

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"producer/internal/event"
)

// MaxBatchSize is the PutRecords limit of records per request.
const MaxBatchSize = 500

var ErrBatchSize = fmt.Errorf("batch must contain between 1 and %d events", MaxBatchSize)

// KinesisAPI is the subset of *kinesis.Client the producer uses.
type KinesisAPI interface {
	PutRecord(ctx context.Context, in *kinesis.PutRecordInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordOutput, error)
	PutRecords(ctx context.Context, in *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error)
}

type Result struct {
	PackageID      string `json:"package_id"`
	ShardID        string `json:"shard_id,omitempty"`
	SequenceNumber string `json:"sequence_number,omitempty"`
	Error          string `json:"error,omitempty"`
}

type Publisher struct {
	API        KinesisAPI
	StreamName string
}

// Publish sends one event. The partition key is package_id, which decides the
// shard: all events of one package land on the same shard, in order.
func (p *Publisher) Publish(ctx context.Context, e event.Event) (Result, error) {
	data, err := e.Encode()
	if err != nil {
		return Result{}, err
	}
	out, err := p.API.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   aws.String(p.StreamName),
		PartitionKey: aws.String(e.PackageID),
		Data:         data,
	})
	if err != nil {
		return Result{}, fmt.Errorf("put record: %w", err)
	}
	return Result{
		PackageID:      e.PackageID,
		ShardID:        aws.ToString(out.ShardId),
		SequenceNumber: aws.ToString(out.SequenceNumber),
	}, nil
}

// PublishBatch sends many events in one PutRecords call. The call can succeed
// (HTTP 200) while some records fail, so every record gets its own Result.
func (p *Publisher) PublishBatch(ctx context.Context, events []event.Event) ([]Result, error) {
	if len(events) == 0 || len(events) > MaxBatchSize {
		return nil, ErrBatchSize
	}
	entries := make([]types.PutRecordsRequestEntry, len(events))
	for i, e := range events {
		data, err := e.Encode()
		if err != nil {
			return nil, err
		}
		entries[i] = types.PutRecordsRequestEntry{Data: data, PartitionKey: aws.String(e.PackageID)}
	}
	out, err := p.API.PutRecords(ctx, &kinesis.PutRecordsInput{
		StreamName: aws.String(p.StreamName),
		Records:    entries,
	})
	if err != nil {
		return nil, fmt.Errorf("put records: %w", err)
	}
	if len(out.Records) != len(events) {
		return nil, fmt.Errorf("kinesis returned %d results for %d events", len(out.Records), len(events))
	}
	results := make([]Result, len(events))
	for i, entry := range out.Records {
		result := Result{PackageID: events[i].PackageID}
		if entry.ErrorCode != nil {
			result.Error = aws.ToString(entry.ErrorCode) + ": " + aws.ToString(entry.ErrorMessage)
		} else {
			result.ShardID = aws.ToString(entry.ShardId)
			result.SequenceNumber = aws.ToString(entry.SequenceNumber)
		}
		results[i] = result
	}
	return results, nil
}
```

- [ ] **Step 9: Verificar `publisher`**

Run: `go test ./internal/publisher/ -v`
Expected: PASS.

- [ ] **Step 10: Pruebas del paquete `server` (fallan)**

`producer/internal/server/server_test.go`:

```go
package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"producer/internal/event"
	"producer/internal/publisher"
)

type fakePublisher struct {
	publishCalls int
	batchCalls   int
	result       publisher.Result
	batchResults []publisher.Result
	err          error
}

func (f *fakePublisher) Publish(_ context.Context, _ event.Event) (publisher.Result, error) {
	f.publishCalls++
	return f.result, f.err
}

func (f *fakePublisher) PublishBatch(_ context.Context, _ []event.Event) ([]publisher.Result, error) {
	f.batchCalls++
	return f.batchResults, f.err
}

const goodEvent = `{"package_id":"PKG-001","timestamp":"2026-10-04T10:30:00Z","latitude":-12.0464,"longitude":-77.0428,"status":"CREATED"}`

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealth(t *testing.T) {
	rec := do(t, New(&fakePublisher{}), http.MethodGet, "/health", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"ok"`) {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
}

func TestPostEventReturnsShardAndSequenceNumber(t *testing.T) {
	pub := &fakePublisher{result: publisher.Result{PackageID: "PKG-001", ShardID: "shardId-000000000001", SequenceNumber: "4959"}}
	rec := do(t, New(pub), http.MethodPost, "/events", goodEvent)

	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var got publisher.Result
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || got.ShardID != "shardId-000000000001" || got.SequenceNumber != "4959" {
		t.Fatalf("unexpected body %s (err=%v)", rec.Body, err)
	}
}

func TestPostEventRejectsInvalidWithoutPublishing(t *testing.T) {
	pub := &fakePublisher{}
	h := New(pub)
	for _, body := range []string{
		`{"package_id":"PKG-001"}`,
		strings.Replace(goodEvent, "CREATED", "LOST", 1),
		strings.Replace(goodEvent, "10:30:00Z", "10:30:00+00:00", 1),
		`{"package_id":"PKG-001","speed":1}`,
		`not json`,
	} {
		if rec := do(t, h, http.MethodPost, "/events", body); rec.Code != http.StatusBadRequest {
			t.Fatalf("body %q: code=%d", body, rec.Code)
		}
	}
	if pub.publishCalls != 0 {
		t.Fatal("invalid events must never reach Kinesis")
	}
}

func TestPostEventPublisherFailureIs502(t *testing.T) {
	rec := do(t, New(&fakePublisher{err: errors.New("kinesis down")}), http.MethodPost, "/events", goodEvent)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestPostEventOversizedBodyIs413(t *testing.T) {
	huge := `{"package_id":"` + strings.Repeat("a", 2<<20) + `"}`
	rec := do(t, New(&fakePublisher{}), http.MethodPost, "/events", huge)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestBatchAllSucceed(t *testing.T) {
	pub := &fakePublisher{batchResults: []publisher.Result{{PackageID: "PKG-001", ShardID: "s0", SequenceNumber: "1"}, {PackageID: "PKG-001", ShardID: "s0", SequenceNumber: "2"}}}
	rec := do(t, New(pub), http.MethodPost, "/events/batch", "["+goodEvent+","+goodEvent+"]")

	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), `"failed":0`) || !strings.Contains(rec.Body.String(), `"count":2`) {
		t.Fatalf("body=%s", rec.Body)
	}
}

func TestBatchPartialFailureIs207(t *testing.T) {
	pub := &fakePublisher{batchResults: []publisher.Result{
		{PackageID: "PKG-001", ShardID: "s0", SequenceNumber: "1"},
		{PackageID: "PKG-001", Error: "ProvisionedThroughputExceededException: slow down"},
	}}
	rec := do(t, New(pub), http.MethodPost, "/events/batch", "["+goodEvent+","+goodEvent+"]")

	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), `"failed":1`) {
		t.Fatalf("body=%s", rec.Body)
	}
}

func TestBatchRejectsBadInputWithoutPublishing(t *testing.T) {
	pub := &fakePublisher{}
	h := New(pub)

	tooMany := "[" + strings.TrimSuffix(strings.Repeat(goodEvent+",", publisher.MaxBatchSize+1), ",") + "]"
	cases := map[string]string{
		"empty array":      "[]",
		"not an array":     goodEvent,
		"one invalid":      "[" + goodEvent + "," + strings.Replace(goodEvent, "CREATED", "LOST", 1) + "]",
		"more than 500":    tooMany,
		"trailing garbage": "[" + goodEvent + "] x",
	}
	for name, body := range cases {
		if rec := do(t, h, http.MethodPost, "/events/batch", body); rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: code=%d body=%s", name, rec.Code, rec.Body)
		}
	}
	if pub.batchCalls != 0 {
		t.Fatal("invalid batches must never reach Kinesis")
	}
}
```

Nota: el cuerpo de `tooMany` (501 × ~130 B ≈ 65 KB) está por debajo del límite de 1 MiB, así que la prueba ejerce el límite de 500 eventos y no el de tamaño.

- [ ] **Step 11: Verificar que fallan**

Run: `go test ./internal/server/`
Expected: FAIL (`undefined: New`).

- [ ] **Step 12: Implementar `server`**

`producer/internal/server/server.go`:

```go
// Package server exposes the producer HTTP API.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"producer/internal/event"
	"producer/internal/publisher"
)

const maxBodyBytes = 1 << 20 // 1 MiB

// Publisher is what the handlers need from internal/publisher.
type Publisher interface {
	Publish(ctx context.Context, e event.Event) (publisher.Result, error)
	PublishBatch(ctx context.Context, events []event.Event) ([]publisher.Result, error)
}

type Server struct {
	pub Publisher
}

func New(pub Publisher) http.Handler {
	s := &Server{pub: pub}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("POST /events", s.postEvent)
	mux.HandleFunc("POST /events/batch", s.postBatch)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func badRequest(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, errors.New("request body too large"))
		return
	}
	writeError(w, http.StatusBadRequest, err)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) postEvent(w http.ResponseWriter, r *http.Request) {
	e, err := event.Parse(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		badRequest(w, err)
		return
	}
	result, err := s.pub.Publish(r.Context(), e)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) postBatch(w http.ResponseWriter, r *http.Request) {
	events, err := event.ParseBatch(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		badRequest(w, err)
		return
	}
	if len(events) == 0 || len(events) > publisher.MaxBatchSize {
		writeError(w, http.StatusBadRequest, publisher.ErrBatchSize)
		return
	}
	results, err := s.pub.PublishBatch(r.Context(), events)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	failed := 0
	for _, result := range results {
		if result.Error != "" {
			failed++
		}
	}
	status := http.StatusOK
	if failed > 0 {
		status = http.StatusMultiStatus
	}
	writeJSON(w, status, map[string]any{"count": len(results), "failed": failed, "results": results})
}
```

- [ ] **Step 13: Verificar `server`**

Run: `go test ./internal/server/ -v`
Expected: PASS.

- [ ] **Step 14: Punto de entrada**

`producer/cmd/producer/main.go`:

```go
// Command producer runs the local HTTP API that publishes tracking events to Kinesis.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	"producer/internal/publisher"
	"producer/internal/server"
)

func main() {
	streamName := os.Getenv("STREAM_NAME")
	if streamName == "" {
		log.Fatal("STREAM_NAME is required (see .env.example)")
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Credentials come from the SDK default chain (AWS_PROFILE, SSO, env vars).
	// For production use a dedicated role assumed via AssumeRole instead.
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("load AWS config: %v", err)
	}
	if cfg.Region == "" {
		log.Fatal("AWS_REGION (or AWS_DEFAULT_REGION) is required")
	}

	pub := &publisher.Publisher{API: kinesis.NewFromConfig(cfg), StreamName: streamName}

	// The API has no authentication, so it only listens on the loopback interface.
	srv := &http.Server{
		Addr:              "127.0.0.1:" + port,
		Handler:           server.New(pub),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("producer listening on http://%s, stream=%s region=%s", srv.Addr, streamName, cfg.Region)
	log.Fatal(srv.ListenAndServe())
}
```

- [ ] **Step 15: Compilar, analizar y formatear todo Go**

Run (en `producer/`): `go mod tidy && go vet ./... && test -z "$(gofmt -l .)" && go test ./... && go build -o bin/producer.exe ./cmd/producer`
Expected: todo OK; `gofmt -l` sin salida. Si `gofmt -l` lista archivos, ejecutar `gofmt -w .` y repetir.

- [ ] **Step 16: Prueba de paridad Python ↔ Go (falla si divergen)**

`tests/test_contract_parity.py`:

```python
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
    assert re.findall(r'"([A-Z_]+)"', block.group(1)) == CONTRACT["properties"]["status"]["enum"]


def test_go_patterns_match_the_contract() -> None:
    properties = CONTRACT["properties"]
    assert go_constant("PackageIDPattern") == properties["package_id"]["pattern"]
    assert go_constant("TimestampPattern") == properties["timestamp"]["pattern"]
```

- [ ] **Step 17: Verificar la paridad y el lint de Python**

Run: `uv run python scripts/testing/run_pytest.py tests/test_contract_parity.py -q && uv run python scripts/testing/run_ruff_check.py tests/test_contract_parity.py && uv run python scripts/testing/run_ruff_format.py tests/test_contract_parity.py`
Expected: pasan y ruff sin errores.

- [ ] **Checkpoint:** `go test ./...` y `tests/test_contract_parity.py` en verde; `producer/bin/producer.exe` compila.

---

## Task 4: Módulos `kinesis` y `dynamodb` + prueba de guardarraíles Terraform

**Contexto:** `terraform validate` puede no estar disponible (Task 0). Para no depender de él, una prueba estática en Python verifica las reglas obligatorias de AGENTS.md sobre los `.tf` de los cuatro módulos. Las pruebas de `consumer` y `datalake` fallarán hasta las Tasks 5 y 6; es esperado.

**Files:**
- Create: `tests/test_terraform_guardrails.py`
- Modify (reemplazar el contenido del scaffold): `infra/modules/kinesis/{main,variables,outputs}.tf`, `infra/modules/dynamodb/{main,variables,outputs}.tf`

**Interfaces:**
- Produces (kinesis): `output "stream_name"`, `output "resource_arn"` (ARN del stream), `output "log_group_name"` y `output "log_group_arn"` (vacíos: Kinesis no emite logs).
- Produces (dynamodb): `output "table_name"`, `output "resource_arn"` (ARN de la tabla), `log_group_name`/`log_group_arn` vacíos.
- Consumes: variable `tags` (mapa) de cada módulo.

- [ ] **Step 1: Escribir la prueba de guardarraíles (falla)**

`tests/test_terraform_guardrails.py`:

```python
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
    "aws_sqs_queue",
    "aws_iam_role",
    "aws_s3_bucket",
    "aws_cloudwatch_log_group",
    "aws_kinesis_firehose_delivery_stream",
]


def module_text(name: str) -> str:
    folder = INFRA / "modules" / name
    return "\n".join(path.read_text(encoding="utf-8") for path in sorted(folder.glob("*.tf")))


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
    variables = (INFRA / "modules" / module / "variables.tf").read_text(encoding="utf-8")
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
```

- [ ] **Step 2: Verificar que falla**

Run: `uv run python scripts/testing/run_pytest.py tests/test_terraform_guardrails.py -q -k "kinesis or dynamodb"`
Expected: FAIL (los outputs/variables del scaffold son solo comentarios).

- [ ] **Step 3: Módulo `kinesis`**

`infra/modules/kinesis/variables.tf`:

```hcl
variable "stream_name" {
  description = "Kinesis data stream name."
  type        = string
}

variable "shard_count" {
  description = "Number of shards (PROVISIONED mode). Two shards make the partition key visible in the demo."
  type        = number

  validation {
    condition     = var.shard_count >= 1
    error_message = "shard_count must be at least 1."
  }
}

variable "retention_hours" {
  description = "Hours Kinesis retains records (24 to 8760)."
  type        = number

  validation {
    condition     = var.retention_hours >= 24 && var.retention_hours <= 8760
    error_message = "retention_hours must be between 24 and 8760."
  }
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
```

`infra/modules/kinesis/main.tf`:

```hcl
resource "aws_kinesis_stream" "this" {
  name             = var.stream_name
  shard_count      = var.shard_count
  retention_period = var.retention_hours

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  # debt: no server-side encryption, to keep the demo free of KMS key policies.
  # Upgrade trigger: before handling real data, set encryption_type = "KMS" and
  # grant kms:Decrypt to the consumer and Firehose roles.

  tags = var.tags
}
```

`infra/modules/kinesis/outputs.tf`:

```hcl
output "stream_name" {
  description = "Name of the Kinesis data stream."
  value       = aws_kinesis_stream.this.name
}

output "resource_arn" {
  description = "ARN of the Kinesis data stream."
  value       = aws_kinesis_stream.this.arn
}

output "log_group_name" {
  description = "Empty by design: Kinesis Data Streams does not emit logs."
  value       = ""
}

output "log_group_arn" {
  description = "Empty by design: Kinesis Data Streams does not emit logs."
  value       = ""
}
```

- [ ] **Step 4: Módulo `dynamodb`**

`infra/modules/dynamodb/variables.tf`:

```hcl
variable "table_name" {
  description = "DynamoDB table holding the current state of each package."
  type        = string
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
```

`infra/modules/dynamodb/main.tf`:

```hcl
resource "aws_dynamodb_table" "current_state" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "package_id"

  # Lab environment: the table must be removable with terraform destroy.
  deletion_protection_enabled = false

  attribute {
    name = "package_id"
    type = "S"
  }

  tags = var.tags
}
```

`infra/modules/dynamodb/outputs.tf`:

```hcl
output "table_name" {
  description = "Name of the current-state table."
  value       = aws_dynamodb_table.current_state.name
}

output "resource_arn" {
  description = "ARN of the current-state table."
  value       = aws_dynamodb_table.current_state.arn
}

output "log_group_name" {
  description = "Empty by design: DynamoDB does not emit logs."
  value       = ""
}

output "log_group_arn" {
  description = "Empty by design: DynamoDB does not emit logs."
  value       = ""
}
```

- [ ] **Step 5: Verificar**

Run: `uv run python scripts/testing/run_pytest.py tests/test_terraform_guardrails.py -q -k "kinesis or dynamodb"` y `terraform -chdir=infra fmt -check -recursive`
Expected: pasan los casos de `kinesis`/`dynamodb`; `fmt -check` sin salida.

- [ ] **Checkpoint:** módulos `kinesis` y `dynamodb` listos.

---

## Task 5: Módulo `consumer` (Lambda + event source mapping + SQS)

**Nota IAM (AGENTS.md: preguntar antes de cambios de IAM):** este plan declara aquí los permisos; la revisión y aprobación del plan por el instructor cubre esa pregunta.

**Files:**
- Modify (reemplazar scaffold): `infra/modules/consumer/{main,variables,outputs}.tf`
- Test: `tests/test_terraform_guardrails.py` (casos `consumer`)

**Interfaces:**
- Consumes (variables): `function_name`, `source_files` (mapa `nombre dentro del zip → ruta local`), `table_name`, `table_arn`, `stream_arn`, `log_retention_days`, `tags`; con defaults `runtime = "python3.12"`, `batch_size = 100`, `max_batching_window_seconds = 5`, `max_retry_attempts = 3`, `max_record_age_seconds = 3600`.
- Produces (outputs): `function_name`, `resource_arn` (ARN de la Lambda), `log_group_name`, `log_group_arn`, `failure_queue_url`, `failure_queue_arn`.

- [ ] **Step 1: Verificar que las pruebas de `consumer` fallan**

Run: `uv run python scripts/testing/run_pytest.py tests/test_terraform_guardrails.py -q -k consumer`
Expected: FAIL.

- [ ] **Step 2: Variables**

`infra/modules/consumer/variables.tf`:

```hcl
variable "function_name" {
  description = "Lambda function name; also used to name its role, log group and failure queue."
  type        = string
}

variable "runtime" {
  description = "Lambda runtime."
  type        = string
  default     = "python3.12"
}

variable "source_files" {
  description = "Files packaged into the Lambda zip: map of file name inside the zip to local source path."
  type        = map(string)
}

variable "table_name" {
  description = "Current-state DynamoDB table name, passed to the function as TABLE_NAME."
  type        = string
}

variable "table_arn" {
  description = "ARN of the current-state table (scopes dynamodb:PutItem)."
  type        = string
}

variable "stream_arn" {
  description = "ARN of the Kinesis stream the function consumes."
  type        = string
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days."
  type        = number
}

variable "batch_size" {
  description = "Maximum records per Lambda invocation."
  type        = number
  default     = 100
}

variable "max_batching_window_seconds" {
  description = "Seconds Lambda waits to fill a batch."
  type        = number
  default     = 5
}

variable "max_retry_attempts" {
  description = "Retries of a failing batch before it goes to the failure queue."
  type        = number
  default     = 3
}

variable "max_record_age_seconds" {
  description = "Records older than this are skipped and sent to the failure queue."
  type        = number
  default     = 3600
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
```

- [ ] **Step 3: Recursos**

`infra/modules/consumer/main.tf`:

```hcl
data "archive_file" "package" {
  type        = "zip"
  output_path = "${path.root}/.build/${var.function_name}.zip"

  dynamic "source" {
    for_each = var.source_files
    content {
      filename = source.key
      content  = file(source.value)
    }
  }
}

resource "aws_cloudwatch_log_group" "this" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# Kinesis failure destinations carry only metadata (shard and sequence-number
# range), not the record bodies; the events stay readable in the stream until
# its retention expires.
resource "aws_sqs_queue" "failures" {
  name                      = "${var.function_name}-failures"
  sqs_managed_sse_enabled   = true
  message_retention_seconds = 1209600
  tags                      = var.tags
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "this" {
  name               = "${var.function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "permissions" {
  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.this.arn}:*"]
  }

  statement {
    sid = "ReadStream"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
    ]
    resources = [var.stream_arn]
  }

  statement {
    sid       = "WriteCurrentState"
    actions   = ["dynamodb:PutItem"]
    resources = [var.table_arn]
  }

  # The failure queue lives in this module, so its permission is declared here
  # (AGENTS.md: IAM cross-module placement rule).
  statement {
    sid       = "SendToFailureQueue"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.failures.arn]
  }

  statement {
    sid    = "DenyUnusedActions"
    effect = "Deny"
    actions = [
      "dynamodb:DeleteItem",
      "dynamodb:DeleteTable",
      "dynamodb:UpdateTable",
      "iam:*",
      "s3:*",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "this" {
  name   = "${var.function_name}-permissions"
  role   = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.permissions.json
}

resource "aws_lambda_function" "this" {
  function_name    = var.function_name
  role             = aws_iam_role.this.arn
  runtime          = var.runtime
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.package.output_path
  source_code_hash = data.archive_file.package.output_base64sha256
  timeout          = 30
  memory_size      = 128

  environment {
    variables = {
      TABLE_NAME = var.table_name
    }
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.this.name
  }

  depends_on = [aws_cloudwatch_log_group.this, aws_iam_role_policy.this]
  tags       = var.tags
}

resource "aws_lambda_event_source_mapping" "stream" {
  event_source_arn                   = var.stream_arn
  function_name                      = aws_lambda_function.this.arn
  starting_position                  = "LATEST"
  batch_size                         = var.batch_size
  maximum_batching_window_in_seconds = var.max_batching_window_seconds
  maximum_retry_attempts             = var.max_retry_attempts
  maximum_record_age_in_seconds      = var.max_record_age_seconds
  bisect_batch_on_function_error     = true
  function_response_types            = ["ReportBatchItemFailures"]

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.failures.arn
    }
  }

  depends_on = [aws_iam_role_policy.this]
}
```

- [ ] **Step 4: Outputs**

`infra/modules/consumer/outputs.tf`:

```hcl
output "function_name" {
  description = "Name of the consumer Lambda function."
  value       = aws_lambda_function.this.function_name
}

output "resource_arn" {
  description = "ARN of the consumer Lambda function."
  value       = aws_lambda_function.this.arn
}

output "log_group_name" {
  description = "CloudWatch log group of the consumer."
  value       = aws_cloudwatch_log_group.this.name
}

output "log_group_arn" {
  description = "ARN of the consumer log group."
  value       = aws_cloudwatch_log_group.this.arn
}

output "failure_queue_url" {
  description = "URL of the SQS queue that receives failed batches."
  value       = aws_sqs_queue.failures.url
}

output "failure_queue_arn" {
  description = "ARN of the SQS queue that receives failed batches."
  value       = aws_sqs_queue.failures.arn
}
```

- [ ] **Step 5: Verificar**

Run: `uv run python scripts/testing/run_pytest.py tests/test_terraform_guardrails.py -q -k "consumer or kinesis or dynamodb"` y `terraform -chdir=infra fmt -check -recursive`
Expected: PASS y `fmt` sin salida.

- [ ] **Checkpoint:** módulo `consumer` listo.

---

## Task 6: Módulo `datalake` (S3 + Firehose)

**Files:**
- Modify (reemplazar scaffold): `infra/modules/datalake/{main,variables,outputs}.tf`
- Test: `tests/test_terraform_guardrails.py` (casos `datalake`)

**Interfaces:**
- Consumes (variables): `name_prefix`, `stream_arn`, `partition_granularity` (`"hour"` | `"minute"`), `buffer_interval_seconds` (0–900), `force_destroy` (bool), `log_retention_days`, `tags`.
- Produces (outputs): `bucket_name`, `bucket_arn`, `delivery_stream_name`, `resource_arn` (ARN del delivery stream), `log_group_name`, `log_group_arn`.

- [ ] **Step 1: Verificar que las pruebas de `datalake` fallan**

Run: `uv run python scripts/testing/run_pytest.py tests/test_terraform_guardrails.py -q -k datalake`
Expected: FAIL.

- [ ] **Step 2: Variables**

`infra/modules/datalake/variables.tf`:

```hcl
variable "name_prefix" {
  description = "Prefix for resource names, e.g. logistics-streaming-dev."
  type        = string
}

variable "stream_arn" {
  description = "ARN of the Kinesis stream Firehose reads from."
  type        = string
}

variable "partition_granularity" {
  description = "Time partitioning of S3 keys: hour (realistic) or minute (live demo only; produces many small objects)."
  type        = string

  validation {
    condition     = contains(["hour", "minute"], var.partition_granularity)
    error_message = "partition_granularity must be \"hour\" or \"minute\"."
  }
}

variable "buffer_interval_seconds" {
  description = "Seconds Firehose buffers before writing an object to S3 (0 to 900)."
  type        = number

  validation {
    condition     = var.buffer_interval_seconds >= 0 && var.buffer_interval_seconds <= 900
    error_message = "buffer_interval_seconds must be between 0 and 900."
  }
}

variable "force_destroy" {
  description = "Whether terraform destroy may delete the bucket while it still holds objects."
  type        = bool
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days."
  type        = number
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
```

- [ ] **Step 3: Recursos**

`infra/modules/datalake/main.tf`:

```hcl
data "aws_caller_identity" "current" {}

locals {
  bucket_name   = "${var.name_prefix}-${data.aws_caller_identity.current.account_id}-datalake"
  firehose_name = "${var.name_prefix}-events-to-s3"

  # The !{timestamp:...} namespace uses the time the record ARRIVES at Firehose,
  # not the event's own timestamp.
  date_prefix   = "year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
  minute_prefix = var.partition_granularity == "minute" ? "minute=!{timestamp:mm}/" : ""
  events_prefix = "events/${local.date_prefix}${local.minute_prefix}"
  errors_prefix = "errors/!{firehose:error-output-type}/${local.date_prefix}"
}

resource "aws_s3_bucket" "datalake" {
  bucket        = local.bucket_name
  force_destroy = var.force_destroy
  tags          = var.tags
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${local.firehose_name}"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_cloudwatch_log_stream" "delivery" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${local.firehose_name}-role"
  assume_role_policy = data.aws_iam_policy_document.assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "permissions" {
  statement {
    sid = "WriteToBucket"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
    ]
    resources = [
      aws_s3_bucket.datalake.arn,
      "${aws_s3_bucket.datalake.arn}/*",
    ]
  }

  statement {
    sid = "ReadSourceStream"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
    ]
    resources = [var.stream_arn]
  }

  statement {
    sid       = "WriteDeliveryLogs"
    actions   = ["logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.firehose.arn}:*"]
  }

  statement {
    sid       = "DenyUnusedActions"
    effect    = "Deny"
    actions   = ["dynamodb:*", "lambda:*", "iam:*", "sqs:*"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "${local.firehose_name}-permissions"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.permissions.json
}

resource "aws_kinesis_firehose_delivery_stream" "events" {
  name        = local.firehose_name
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = var.stream_arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = aws_s3_bucket.datalake.arn
    prefix              = local.events_prefix
    error_output_prefix = local.errors_prefix
    buffering_size      = 1 # MB, the minimum; the interval is what the demo waits on
    buffering_interval  = var.buffer_interval_seconds
    compression_format  = "UNCOMPRESSED"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.delivery.name
    }
  }

  depends_on = [aws_iam_role_policy.firehose]
  tags       = var.tags
}
```

- [ ] **Step 4: Outputs**

`infra/modules/datalake/outputs.tf`:

```hcl
output "bucket_name" {
  description = "Name of the S3 data lake bucket."
  value       = aws_s3_bucket.datalake.bucket
}

output "bucket_arn" {
  description = "ARN of the S3 data lake bucket."
  value       = aws_s3_bucket.datalake.arn
}

output "delivery_stream_name" {
  description = "Name of the Firehose delivery stream."
  value       = aws_kinesis_firehose_delivery_stream.events.name
}

output "resource_arn" {
  description = "ARN of the Firehose delivery stream."
  value       = aws_kinesis_firehose_delivery_stream.events.arn
}

output "log_group_name" {
  description = "CloudWatch log group of the Firehose delivery stream."
  value       = aws_cloudwatch_log_group.firehose.name
}

output "log_group_arn" {
  description = "ARN of the Firehose log group."
  value       = aws_cloudwatch_log_group.firehose.arn
}
```

- [ ] **Step 5: Verificar toda la suite de guardarraíles**

Run: `uv run python scripts/testing/run_pytest.py tests/test_terraform_guardrails.py -q` y `terraform -chdir=infra fmt -check -recursive`
Expected: todas pasan; `fmt` sin salida. Format: si `fmt -check` lista archivos, `terraform -chdir=infra fmt -recursive` y repetir.

- [ ] **Checkpoint:** los cuatro módulos pasan la prueba de guardarraíles.

---

## Task 7: Cableado del módulo raíz (`infra/`)

**Files:**
- Modify: `infra/main.tf` (reemplazar el comentario de módulos), `infra/variables.tf` (añadir variables), `infra/outputs.tf` (añadir outputs), `infra/providers.tf` (añadir `archive`), `infra/terraform.tfvars.example`
- Test: `tests/test_terraform_guardrails.py` (añadir caso de cableado)

**Interfaces:**
- Consumes: outputs de las Tasks 4–6 (`module.kinesis.stream_name`/`resource_arn`, `module.dynamodb.table_name`/`resource_arn`, `module.consumer.*`, `module.datalake.*`).
- Produces (outputs raíz, los lee `tests/aws/` y el README): `stream_name`, `table_name`, `lambda_function_name`, `failure_queue_url`, `datalake_bucket_name`, `firehose_delivery_stream_name`, `consumer_log_group_name`, `firehose_log_group_name`.

- [ ] **Step 1: Añadir la prueba de cableado (falla)**

Añadir al final de `tests/test_terraform_guardrails.py`:

```python
@pytest.mark.parametrize("module", MODULES)
def test_root_wires_every_module_with_common_tags(module: str) -> None:
    main = (INFRA / "main.tf").read_text(encoding="utf-8")
    match = re.search(rf'module "{module}" \{{(.*?)\n\}}', main, re.S)
    assert match, f"module {module} is not wired in infra/main.tf"
    body = match.group(1)
    assert re.search(rf'source\s*=\s*"\./modules/{module}"', body)
    assert re.search(r"tags\s*=\s*local\.common_tags", body)
```

Run: `uv run python scripts/testing/run_pytest.py tests/test_terraform_guardrails.py -q -k root_wires`
Expected: FAIL (4 casos).

- [ ] **Step 2: Añadir el provider `archive`**

En `infra/providers.tf`, dentro de `required_providers`, tras el bloque `aws`:

```hcl
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
```

- [ ] **Step 3: Añadir variables raíz**

Añadir al final de `infra/variables.tf`:

```hcl
variable "stream_name" {
  description = "Kinesis data stream name."
  type        = string
  default     = "logistics-events"
}

variable "shard_count" {
  description = "Kinesis shards (PROVISIONED). Two make the partition key visible in the demo."
  type        = number
  default     = 2
}

variable "stream_retention_hours" {
  description = "Hours Kinesis retains records."
  type        = number
  default     = 24
}

variable "datalake_partition_granularity" {
  description = "S3 time partitioning: \"hour\" (realistic) or \"minute\" (live demo only)."
  type        = string
  default     = "hour"
}

variable "firehose_buffer_interval_seconds" {
  description = "Seconds Firehose buffers before writing to S3. 60 keeps the demo short."
  type        = number
  default     = 60
}

variable "datalake_force_destroy" {
  description = "Whether terraform destroy may delete the data lake bucket while it holds objects. Keep true for the lab."
  type        = bool
  default     = true
}
```

- [ ] **Step 4: Cablear los módulos**

En `infra/main.tf`, reemplazar el comentario "Streaming lab modules …" (tres líneas) por:

```hcl
module "kinesis" {
  source = "./modules/kinesis"

  stream_name     = var.stream_name
  shard_count     = var.shard_count
  retention_hours = var.stream_retention_hours
  tags            = local.common_tags
}

module "dynamodb" {
  source = "./modules/dynamodb"

  table_name = "${local.name_prefix}-package-state"
  tags       = local.common_tags
}

module "consumer" {
  source = "./modules/consumer"

  function_name = "${local.name_prefix}-consumer"
  source_files = {
    "handler.py"                  = "${path.module}/../src/lambda/consumer/handler.py"
    "contract.py"                 = "${path.module}/../src/lambda/consumer/contract.py"
    "package_event_contract.json" = "${path.module}/../src/contracts/package_event_contract.json"
  }
  table_name         = module.dynamodb.table_name
  table_arn          = module.dynamodb.resource_arn
  stream_arn         = module.kinesis.resource_arn
  log_retention_days = var.log_retention_days
  tags               = local.common_tags
}

module "datalake" {
  source = "./modules/datalake"

  name_prefix             = local.name_prefix
  stream_arn              = module.kinesis.resource_arn
  partition_granularity   = var.datalake_partition_granularity
  buffer_interval_seconds = var.firehose_buffer_interval_seconds
  force_destroy           = var.datalake_force_destroy
  log_retention_days      = var.log_retention_days
  tags                    = local.common_tags
}
```

- [ ] **Step 5: Añadir outputs raíz**

Añadir al final de `infra/outputs.tf`:

```hcl
output "stream_name" {
  description = "Kinesis data stream name (STREAM_NAME for the producer)."
  value       = module.kinesis.stream_name
}

output "table_name" {
  description = "DynamoDB current-state table name."
  value       = module.dynamodb.table_name
}

output "lambda_function_name" {
  description = "Consumer Lambda function name."
  value       = module.consumer.function_name
}

output "failure_queue_url" {
  description = "SQS queue receiving failed batches (metadata only)."
  value       = module.consumer.failure_queue_url
}

output "datalake_bucket_name" {
  description = "S3 data lake bucket name."
  value       = module.datalake.bucket_name
}

output "firehose_delivery_stream_name" {
  description = "Firehose delivery stream name."
  value       = module.datalake.delivery_stream_name
}

output "consumer_log_group_name" {
  description = "CloudWatch log group of the consumer Lambda."
  value       = module.consumer.log_group_name
}

output "firehose_log_group_name" {
  description = "CloudWatch log group of the Firehose delivery stream."
  value       = module.datalake.log_group_name
}
```

- [ ] **Step 6: `terraform.tfvars.example`**

Añadir al final (ejemplo de la demo en vivo, comentado):

```hcl
# Live demo: make the flow visible minute by minute (not a production pattern).
# datalake_partition_granularity = "minute"
```

- [ ] **Step 7: Verificar**

Run: `uv run python scripts/testing/run_pytest.py tests/test_terraform_guardrails.py -q` y `terraform -chdir=infra fmt -check -recursive`
Expected: todas pasan; `fmt` sin salida (si lista archivos: `terraform -chdir=infra fmt -recursive` y repetir).

Run (solo si Task 0 lo desbloqueó): `terraform -chdir=infra init -backend=false -input=false && terraform -chdir=infra validate`
Expected: `Success! The configuration is valid.` Si sigue bloqueado, anotar "validate no ejecutado" en el checkpoint.

- [ ] **Checkpoint:** infraestructura cableada; prueba de guardarraíles en verde; `validate` en verde o declarado bloqueado.

---

## Task 8: Pruebas de validación en AWS (`tests/aws/`)

**Contexto:** AGENTS.md exige pruebas boto3 al desplegar infraestructura. Usan el helper existente `tests/aws/aws_session.py` (`get_client(service)`, claves de `.env.credentials`) y se saltan solas si no hay despliegue o credenciales. Se ejecutan con `uv run python scripts/testing/run_cloud_tests.py`.

**Files:**
- Create: `tests/aws/test_streaming_pipeline.py`
- Modify: `pyproject.toml` (marcador `slow`)

**Interfaces:**
- Consumes: outputs raíz de la Task 7 (vía `terraform output -json`); `aws_session.get_client`.

- [ ] **Step 1: Registrar el marcador `slow`**

En `pyproject.toml`, `[tool.pytest.ini_options]`, ampliar `markers`:

```toml
markers = [
    "cloud: tests that require live AWS credentials (skipped without them)",
    "slow: tests that wait for Firehose buffering (minutes)",
]
```

- [ ] **Step 2: Escribir las pruebas**

`tests/aws/test_streaming_pipeline.py`:

```python
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
    item = dynamodb.get_item(TableName=table, Key={"package_id": {"S": package_id}}).get("Item")
    return item["status"]["S"] if item else None


def put_event(kinesis, stream: str, package_id: str, timestamp: str, status: str) -> None:
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
    assert current_status(dynamodb, outputs["table_name"], lifecycle) == "OUT_FOR_DELIVERY"


@pytest.mark.slow
def test_s3_keeps_the_full_history_including_the_event_dynamodb_ignored(
    outputs, lifecycle
) -> None:
    s3 = aws_client("s3")
    bucket = outputs["datalake_bucket_name"]

    def statuses_for_package() -> set[str] | None:
        found: set[str] = set()
        pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="events/")
        for page in pages:
            for entry in page.get("Contents", []):
                body = s3.get_object(Bucket=bucket, Key=entry["Key"])["Body"].read().decode()
                for line in body.splitlines():
                    event = json.loads(line)
                    if event["package_id"] == lifecycle:
                        found.add(event["status"])
        return found if found >= {"OUT_FOR_DELIVERY", "CREATED"} else None

    # Firehose buffers (60 s by default) before it writes the object.
    assert wait_for(statuses_for_package, timeout=240, interval=10) is not None
```

- [ ] **Step 3: Verificar que se recolectan y se saltan sin despliegue**

Run: `uv run python scripts/testing/run_cloud_tests.py`
Expected: las pruebas se recolectan y quedan `skipped` ("lab is not deployed" o "AWS credentials not configured"); código de salida 0.

- [ ] **Step 4: Lint y formato**

Run: `uv run python scripts/testing/run_ruff_check.py tests/aws/test_streaming_pipeline.py && uv run python scripts/testing/run_ruff_format.py tests/aws/test_streaming_pipeline.py`
Expected: sin errores.

- [ ] **Checkpoint:** pruebas cloud recolectadas y saltadas correctamente.

---

## Task 9: Configuración y documentación de la demo (README, ejemplos, `.env.example`)

**Files:**
- Create: `README.md` (no existe un README equivalente)
- Create: `producer/examples/01_created.json`, `02_in_transit.json`, `03_out_for_delivery.json`, `04_delivered.json`, `batch.json`
- Modify: `.env.example`, `.gitignore`
- Delete: `producer/examples/.gitkeep`

- [ ] **Step 1: Eventos de ejemplo**

`producer/examples/01_created.json`:
```json
{"package_id": "PKG-001", "timestamp": "2026-10-04T10:00:00Z", "latitude": -12.0464, "longitude": -77.0428, "status": "CREATED"}
```
`02_in_transit.json`:
```json
{"package_id": "PKG-001", "timestamp": "2026-10-04T10:30:00Z", "latitude": -12.0731, "longitude": -77.0817, "status": "IN_TRANSIT"}
```
`03_out_for_delivery.json`:
```json
{"package_id": "PKG-001", "timestamp": "2026-10-04T11:20:00Z", "latitude": -12.0899, "longitude": -77.0505, "status": "OUT_FOR_DELIVERY"}
```
`04_delivered.json`:
```json
{"package_id": "PKG-001", "timestamp": "2026-10-04T12:05:00Z", "latitude": -12.0953, "longitude": -77.0361, "status": "DELIVERED"}
```
`batch.json`:
```json
[
  {"package_id": "PKG-002", "timestamp": "2026-10-04T10:00:00Z", "latitude": -12.0464, "longitude": -77.0428, "status": "CREATED"},
  {"package_id": "PKG-003", "timestamp": "2026-10-04T10:05:00Z", "latitude": -12.0464, "longitude": -77.0428, "status": "CREATED"},
  {"package_id": "PKG-002", "timestamp": "2026-10-04T10:40:00Z", "latitude": -12.0560, "longitude": -77.0610, "status": "PICKED_UP"},
  {"package_id": "PKG-003", "timestamp": "2026-10-04T10:50:00Z", "latitude": -12.0710, "longitude": -77.0700, "status": "IN_TRANSIT"},
  {"package_id": "PKG-004", "timestamp": "2026-10-04T10:55:00Z", "latitude": -12.0464, "longitude": -77.0428, "status": "CREATED"},
  {"package_id": "PKG-002", "timestamp": "2026-10-04T11:30:00Z", "latitude": -12.0850, "longitude": -77.0520, "status": "IN_TRANSIT"}
]
```
Luego `rm producer/examples/.gitkeep`.

- [ ] **Step 2: `.env.example`**

Reemplazar el contenido por:

```bash
# Copy to .env (producer) and/or .env.credentials (AWS validation tests).
# Both are git-ignored. Do not commit real credentials.

# --- Producer (Go), read from the shell: set -a; source .env; set +a ---
# Credentials come from the AWS SDK default chain. Preferred: a named profile or SSO.
# For production use a dedicated role assumed via AssumeRole instead.
AWS_PROFILE=your-profile
AWS_REGION=us-east-1
# Output of `terraform output -raw stream_name`.
STREAM_NAME=logistics-events
PORT=8080

# --- tests/aws (loaded from .env.credentials by tests/aws/aws_session.py) ---
AWS_ACCESS_KEY_ID=your-access-key-id
AWS_SECRET_ACCESS_KEY=your-secret-access-key
# Optional, only needed for temporary credentials.
AWS_SESSION_TOKEN=
# Keep this aligned with infra/terraform.tfvars unless you intentionally override it.
AWS_DEFAULT_REGION=us-east-1
```

- [ ] **Step 3: `.gitignore`**

Añadir al final:

```gitignore
# Lab build outputs
infra/.build/
producer/bin/
```

- [ ] **Step 4: `README.md` (runbook de la demo)**

````markdown
# Lab Sesión 7 — Data Streaming en AWS

Empresa de paquetería: un mismo flujo de eventos tiene **dos destinos**.

```text
Go API → Kinesis (2 shards) ─┬→ Lambda (Python) → DynamoDB   = ¿cuál es el estado ACTUAL?
                             └→ Firehose → S3 (Data Lake)    = ¿qué EVENTOS ocurrieron?
```

Diseño: [docs/specs/2026-10-06-streaming-lab-design.md](docs/specs/2026-10-06-streaming-lab-design.md) ·
Spec funcional: [docs/specs/spec_sesion_7_streaming_aws_laboratorio_refinado.md](docs/specs/spec_sesion_7_streaming_aws_laboratorio_refinado.md) ·
ADR: [docs/internal/adr/0001-streaming-architecture.md](docs/internal/adr/0001-streaming-architecture.md)

## Requisitos

AWS CLI con un perfil válido, Terraform ≥ 1.6, Go ≥ 1.23, `uv`, Git Bash. Copia `.env.example` a `.env` y ajusta `AWS_PROFILE`.

## 1. Desplegar (instructor)

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply          # crea recursos de pago: ~USD 0.03/h por los 2 shards
```

Para ver el flujo minuto a minuto en clase, pon `datalake_partition_granularity = "minute"` en `terraform.tfvars` (solo demo; en producción usa `hour`).

## 2. Levantar el producer

```bash
cd producer && go build -o bin/producer.exe ./cmd/producer && cd ..
set -a; source .env; set +a
STREAM_NAME=$(terraform -chdir=infra output -raw stream_name) ./producer/bin/producer.exe
```

## 3. Demo (pasos del spec)

En otra terminal. Variables útiles:

```bash
TABLE=$(terraform -chdir=infra output -raw table_name)
BUCKET=$(terraform -chdir=infra output -raw datalake_bucket_name)
```

1. **Problema:** necesitamos el estado actual y, a la vez, el historial completo.
2. **Producer vivo:** `curl -s localhost:8080/health`
3. **Primer evento:** `curl -s -X POST localhost:8080/events -d @producer/examples/01_created.json`
   La respuesta trae `shard_id` y `sequence_number`: producer, record, partition key (`package_id`), shard y sequence number.
4. **Kinesis:** `aws kinesis list-shards --stream-name logistics-events` (2 shards).
5. **Lambda → DynamoDB:** `aws dynamodb get-item --table-name $TABLE --key '{"package_id":{"S":"PKG-001"}}'` → `CREATED`.
   Logs: `aws logs tail $(terraform -chdir=infra output -raw consumer_log_group_name) --since 5m`
6. **Segundo evento:** `curl -s -X POST localhost:8080/events -d @producer/examples/02_in_transit.json` y repite el `get-item` → `IN_TRANSIT`. El valor anterior se **actualiza**.
7. **Firehose:** el mismo evento sigue hacia S3 (buffer de 60 s).
8. **Histórico:** `aws s3 ls s3://$BUCKET/events/ --recursive` y
   `aws s3 cp s3://$BUCKET/<key> -` → aparecen `CREATED` **e** `IN_TRANSIT`.
   DynamoDB = estado actual · S3 = historial.
9. **Varios paquetes:** `curl -s -X POST localhost:8080/events/batch -d @producer/examples/batch.json`
   (usa `PutRecords`; la respuesta trae el resultado por registro, `207` si alguno falla).
10. **Data Lake:** observa las particiones `year=/month=/day=/hour=[/minute=]` en S3. Glue/Athena se explica conceptualmente.

Para repetir la demo con el mismo paquete usa otro `package_id` (o `aws dynamodb delete-item`): un evento con `timestamp` igual o menor al guardado se ignora a propósito.

## Notas para la clase

- **Partition key:** `package_id` es una decisión didáctica; en producción puede crear shards calientes.
- **Orden y estado:** DynamoDB solo escribe si el evento es más nuevo (`updated_at`); un evento viejo se ignora sin error.
- **Fallos:** la Lambda reporta solo los registros malos (`ReportBatchItemFailures`); tras los reintentos van a una cola SQS. Esa cola solo guarda metadatos (shard y sequence numbers): el evento se recupera del stream mientras dure la retención (24 h).
- **Particiones S3:** usan la hora de llegada a Firehose, no la del evento. `minute` es solo para la demo.
- **Seguridad:** el producer escucha solo en `127.0.0.1` (sin autenticación). Usa las credenciales por defecto del SDK; **en producción se recomienda un rol dedicado con `AssumeRole`**.
- **Kafka/MSK:** mismo patrón (Producer → Topic → Partition → Consumer) frente a (Producer → Stream → Shard → Consumer); solo conceptual.

## 4. Pruebas

```bash
uv run python scripts/testing/run_pytest.py          # unitarias (sin AWS)
(cd producer && go test ./...)
uv run python scripts/testing/run_cloud_tests.py     # contra el lab desplegado (usa .env.credentials)
```

## 5. Limpieza (obligatoria al terminar)

```bash
terraform -chdir=infra destroy    # requiere aprobación del instructor
```
````

- [ ] **Step 5: Verificar**

Run: `for f in producer/examples/*.json; do python -c "import json,sys; json.load(open(sys.argv[1]))" "$f" || echo "BAD $f"; done`
Expected: sin `BAD`.

- [ ] **Checkpoint:** README, ejemplos y `.env.example` listos.

---

## Task 10: Verificación final y despliegue con aprobación

- [ ] **Step 1: Suite local completa**

Run:
```bash
uv run python scripts/testing/run_pytest.py --ignore=tests/test_installer.py --ignore=tests/test_restore_project.py --deselect tests/test_install_entrypoints.py -q
(cd producer && go vet ./... && test -z "$(gofmt -l .)" && go test ./...)
uv run python scripts/testing/run_ruff_check.py && uv run python scripts/testing/run_ruff_format.py --check
terraform -chdir=infra fmt -check -recursive
```
Expected: todo en verde (los fallos preexistentes de instaladores quedan fuera por las exclusiones). Si ruff sobre todo el repo reporta problemas ajenos a este plan, limitar a los archivos creados aquí y anotarlo.

- [ ] **Step 2: Validación de Terraform**

Si Task 0 lo desbloqueó: `terraform -chdir=infra validate` → `Success!`. Si no, declararlo explícitamente como no ejecutado.

- [ ] **Step 3: Parar y pedir aprobación del instructor**

Mostrar el resultado de `terraform -chdir=infra plan` (si el provider funciona) y el costo estimado (2 shards ≈ USD 0.03/h + Firehose/S3 marginales). **No ejecutar `terraform apply` sin un "sí" explícito.**

- [ ] **Step 4: Tras la aprobación del apply**

Run: `terraform -chdir=infra apply`, luego levantar el producer (README §2), ejecutar la demo (README §3) y `uv run python scripts/testing/run_cloud_tests.py`.
Expected: pruebas cloud en verde (la prueba `slow` tarda hasta ~4 min).

- [ ] **Step 5: Limpieza**

Tras la demo y con aprobación explícita: `terraform -chdir=infra destroy`. Verificar en la consola que no quedan stream, Firehose ni bucket.

- [ ] **Step 6: Cierre documental**

Cambiar el estado del ADR 0001 de "Propuesto" a "Aceptado" cuando el instructor lo apruebe, y el estado del spec de diseño a "aprobado".

---

## Auto-revisión contra el spec de diseño

| Requisito del spec de diseño | Tarea |
|---|---|
| §3 estructura del repo y limpieza del template | ya aplicado (reestructuración) + Tasks 4–7, 9 |
| §4.1 producer (health, events, batch, `\n`, `PutRecords` parcial, env vars, loopback) | Task 3, 9 |
| §4.2 stream 2 shards / 24 h | Task 4, 7 |
| §4.3 Lambda (mapping, condicional, ReportBatchItemFailures, SQS, limitación de metadatos) | Tasks 2, 5, README |
| §4.4 DynamoDB estado actual, on-demand, sin protección | Task 4 |
| §4.5 Firehose/S3, prefijos hour/minute, buffer 60 s, privado, sin versioning | Task 6, 7 |
| §5 IAM por módulo, colocación de SQS, sin comodines de recurso (los únicos `*` son `Deny`) | Tasks 5, 6, guardarraíles |
| §6 tags, log groups con retención, outputs por módulo, budget opcional, config en variables | Tasks 4–7, guardarraíles |
| §7 pruebas pytest / Go / `tests/aws` | Tasks 1–3, 8 |
| §8 costo y limpieza | Tasks 9, 10 |
| §9 README, ADR, notas de clase | Task 9, 10 |

Sin `TBD`/`TODO`. Los nombres usados entre tareas son consistentes: `process_batch`, `write_state`, `to_item`, `load_contract`, `validate_event`, `ContractError`, `event.Parse/ParseBatch/Encode`, `publisher.Publisher/Result/MaxBatchSize/ErrBatchSize`, `server.New`, y los outputs raíz de la Task 7 que consume la Task 8.

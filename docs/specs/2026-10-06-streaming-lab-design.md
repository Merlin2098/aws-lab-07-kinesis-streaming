# Diseño de implementación — Lab Sesión 7: Data Streaming en AWS

**Fecha:** 2026-10-06
**Spec funcional de origen:** [spec_sesion_7_streaming_aws_laboratorio_refinado.md](spec_sesion_7_streaming_aws_laboratorio_refinado.md)
**ADR asociado:** [0001-streaming-architecture.md](../internal/adr/0001-streaming-architecture.md)
**Estado:** pendiente de revisión del instructor

Este documento fija *cómo* se implementa el lab. El *qué* y el guion de la demo
están en el spec funcional; aquí solo se registran las decisiones de diseño y
las precisiones que el spec funcional deja abiertas.

---

## 1. Intención y alcance

- **Formato:** demo del instructor que los alumnos replican siguiendo la clase o
  el video. El repo funciona como runbook: `terraform apply` + pocos comandos
  reproducen el resultado.
- **Mensaje central:** un mismo stream de eventos tiene dos destinos. DynamoDB
  guarda el estado actual; S3 guarda el histórico.
- **Principios de implementación:** nada que no se vea en la demo (YAGNI),
  costo predecible, `destroy` limpio al terminar la clase.
- **Fuera de alcance:** el del spec funcional (§21 "No incluye"). Glue/Athena
  solo se explican conceptualmente.

## 2. Decisiones tomadas

| Tema | Decisión |
|---|---|
| Lenguaje de la Lambda | Python (Go solo para el producer) |
| Capacidad de Kinesis | Provisioned, **2 shards**, retención 24 h |
| Credenciales del producer | Cadena por defecto del SDK (`AWS_PROFILE`/SSO). En producción se recomienda un rol dedicado con `AssumeRole`; se documenta en README y ADR |
| Robustez de la Lambda | Escritura condicional + `ReportBatchItemFailures` + reintentos acotados + SQS como destino de fallos |
| Terraform | **Módulos por componente** (`kinesis`, `dynamodb`, `consumer`, `datalake`) |
| Template existente | Se reemplaza lo específico de Glue/orders; se conserva la base (providers, backend, tags, presupuesto opcional, scripts) |
| Granularidad del Data Lake | Variable; por defecto `hour`, `minute` para la demo en vivo |

## 3. Estructura del repo

```text
producer/                      Go: go.mod, cmd/producer, internal/…
  examples/                    JSON de la secuencia de demo
src/lambda/consumer/handler.py Lambda Python
src/contracts/package_event_contract.json   contrato del evento (reemplaza orders)
infra/
  main.tf variables.tf outputs.tf providers.tf
  modules/
    kinesis/    stream logistics-events
    dynamodb/   tabla de estado actual
    consumer/   Lambda + event source mapping + SQS de fallos + log group
    datalake/   bucket S3 + Firehose + log group
tests/                         pytest del handler
tests/aws/                     validación boto3 del despliegue
docs/internal/adr/0001-…       ADR
```

**Se elimina del template:** rol `data_job_execution` de Glue, bucket de
artefactos con su zip, log group genérico `data-jobs`, `example_job.py`,
`job_config.yaml`, `orders_contract.json`, `orders_to_curated.sql` y sus pruebas.
La Lambda se empaqueta con `archive_file`. No se tocan `scripts/`, el instalador
ni las pruebas del template que no dependen de lo eliminado.

## 4. Componentes

### 4.1 Producer Go

- `GET /health`.
- `POST /events`: valida (campos obligatorios, `status` dentro del enum,
  `timestamp` UTC con formato `...Z`) y llama a `PutRecord` con
  `PartitionKey = package_id`. La respuesta incluye `shard_id` y
  `sequence_number`.
- `POST /events/batch`: recibe un arreglo y usa `PutRecords`. Revisa
  `FailedRecordCount` y devuelve el resultado por registro (el servicio puede
  fallar parcialmente con HTTP 200).
- Cada registro termina en `\n`: Firehose concatena registros sin separador y,
  sin él, los objetos de S3 no serían JSON Lines consultables.
- Configuración por variables de entorno (`STREAM_NAME`, `AWS_REGION`, puerto),
  documentadas en `.env.example`. Sin secretos en el repo.

### 4.2 Kinesis Data Streams

Stream `logistics-events`, 2 shards, retención 24 h. Dos shards hacen visible el
efecto de la partition key (PKG-001 y PKG-002 pueden caer en shards distintos,
cada uno con su sequence number).

### 4.3 Lambda consumidora (Python)

- Event source mapping: `starting_position = LATEST`, lote 100, ventana 1–5 s,
  reintentos acotados, `bisect_batch_on_function_error`,
  `function_response_types = ["ReportBatchItemFailures"]`, destino de fallos SQS.
- Flujo por registro: decodificar base64 → parsear JSON → validar contra el
  contrato (Python estándar, sin `jsonschema` en el zip) → transformar →
  `UpdateItem`/`PutItem` condicional.
- Condición: `attribute_not_exists(package_id) OR updated_at < :ts`. Los
  timestamps son UTC con formato fijo, por lo que la comparación de texto es
  válida.
- Evento más viejo que el guardado: la condición falla, se registra y se
  continúa; **no** es un fallo.
- Evento inválido o error inesperado: se reporta en `batchItemFailures`; tras
  agotar reintentos pasa al destino SQS.
- **Limitación a explicar en clase:** para Kinesis el destino de fallos recibe
  solo metadatos (shard y rango de sequence numbers), no el cuerpo. El evento se
  recupera del stream mientras dure la retención.

### 4.4 DynamoDB

Tabla de estado actual, clave de partición `package_id`, billing on-demand.
Atributos: `status`, `latitude`, `longitude`, `updated_at` y los opcionales que
defina el contrato. No guarda histórico. Sin protección contra borrado.

### 4.5 Firehose y S3

- Fuente: el stream (Kinesis Data Streams como source). Lambda y Firehose leen
  de forma independiente; a este volumen compartir throughput de lectura no es
  problema.
- Destino S3 con prefijo `events/year=…/month=…/day=…/hour=…/`. Con la
  variable en `minute` se añade `/minute=…`. Prefijo de errores obligatorio.
- Buffer de 60 s, configurable por variable.
- **Precisión a explicar:** el particionado usa la hora de llegada a Firehose,
  no la hora del evento.
- Bucket privado, bloqueo de acceso público, cifrado, `force_destroy` por
  variable, versioning desactivado.

## 5. IAM

- Un rol por módulo. El ARN del stream entra como variable a `consumer` y
  `datalake` (no hay dependencia circular).
- La política `sqs:SendMessage` sobre la cola de fallos se declara en el módulo
  `consumer`, donde vive la cola (regla de colocación entre módulos de AGENTS.md).
- Todos los permisos explícitos en Terraform, sin comodines de recurso.

## 6. Transversales (AGENTS.md)

- Tags comunes con `CostCenter` en todos los recursos.
- `aws_cloudwatch_log_group` explícito con `retention_in_days` (variable, 7 por
  defecto) para Lambda y Firehose.
- Cada módulo expone `log_group_name`, `log_group_arn` y `resource_arn`.
- Presupuesto bajo `enable_budget_guardrail` (default `false`), ya presente.
- Valores por ambiente en variables/`.env`, nunca en código.
- Nunca `terraform apply`/`destroy` sin aprobación del instructor.

## 7. Pruebas

- **pytest (unit):** handler con tabla simulada, sin dependencias nuevas.
  Cubre evento válido, evento viejo, evento inválido y fallo parcial del lote.
- **Go:** pruebas del producer con una interfaz falsa de Kinesis.
- **`tests/aws/` (boto3, marcadas `cloud`):** stream ACTIVE con 2 shards, tabla
  existente, mapping Enabled, Firehose ACTIVE, evento publicado visible en
  DynamoDB, y una prueba lenta que verifica el objeto en S3 tras el buffer.

## 8. Costo y limpieza

Costo dominante: 2 shards provisionados (aprox. USD 0.03/h en total) más
Firehose y almacenamiento marginales. Todo se destruye con `terraform destroy`
al terminar la clase; el README lo indica como paso final de la demo.

## 9. Entregables de documentación

- README como runbook de la demo (pasos 1–10 del spec funcional).
- ADR 0001 (antes de implementar, requerido por AGENTS.md).
- Notas de clase: partition key como decisión de diseño, sequence number,
  hora de llegada vs hora del evento, metadatos del destino de fallos,
  `AssumeRole` para producción, comparación conceptual con Kafka/MSK.

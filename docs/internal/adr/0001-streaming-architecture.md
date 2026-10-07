# ADR 0001 — Arquitectura de streaming del lab (Kinesis → Lambda/Firehose)

**Estado:** Propuesto (pendiente de aprobación del instructor)
**Fecha:** 2026-10-06

## Contexto

El template original solo contenía infraestructura orientada a Glue (rol,
bucket de artefactos, log group). El lab de la Sesión 7 necesita demostrar un
pipeline de streaming con dos finalidades: estado actual y histórico de
eventos. Se agregan servicios nuevos (Kinesis Data Streams, Lambda, DynamoDB,
Firehose, SQS) y se cambia el contrato de datos (de `orders` a evento de
paquete). Ver el diseño en
[2026-10-06-streaming-lab-design.md](../../specs/2026-10-06-streaming-lab-design.md).

## Decisión

- Un único stream Kinesis `logistics-events`, provisioned con 2 shards, con dos
  consumidores independientes: Lambda (Python) hacia DynamoDB y Firehose hacia
  S3.
- DynamoDB guarda solo el estado actual con escritura condicional por
  `updated_at`; S3 guarda el histórico con particionado temporal por Firehose.
- La Lambda usa `ReportBatchItemFailures`, reintentos acotados y SQS como
  destino de fallos.
- Producer en Go con la cadena de credenciales por defecto del SDK.
- Terraform en módulos por componente: `kinesis`, `dynamodb`, `consumer`,
  `datalake`.
- Se retiran del template los recursos específicos de Glue y el contrato
  `orders`.
- Kafka/MSK queda fuera de la implementación; se trata de forma conceptual.

## Consecuencias

- Costo continuo mientras el stream exista (shards provisionados); mitigado con
  `terraform destroy` al terminar la demo.
- Dos lectores del mismo stream comparten el throughput de lectura de cada
  shard; irrelevante al volumen del lab.
- El destino de fallos de Kinesis conserva solo metadatos; los eventos se
  recuperan del stream dentro de la retención.
- El particionado de S3 usa la hora de llegada a Firehose, no la del evento.
- Credenciales por defecto del SDK en el producer son aceptables para la demo;
  en producción se recomienda un rol dedicado con `AssumeRole`.
- Los módulos añaden indirección frente al template plano a cambio de seguir la
  convención de AGENTS.md (outputs y colocación de IAM por módulo).

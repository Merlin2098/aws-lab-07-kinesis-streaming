# SPEC — Sesión 7: Data Streaming en AWS
## Laboratorio refinado — Empresa de paquetería

**Curso:** AWS Data Engineer  
**Sesión:** 7 de 9  
**Tema:** Data Streaming en AWS  
**Laboratorio:** Real-Time Logistics Tracking  
**Servicios principales:** Amazon Kinesis Data Streams, Amazon Kinesis Data Firehose, AWS Lambda, Amazon DynamoDB, Amazon S3  
**Producer local:** Go + AWS SDK for Go  
**Kafka/MSK:** contenido conceptual, fuera del laboratorio principal

---

# 1. Objetivo del laboratorio

Construir una arquitectura de streaming para una empresa de paquetería que recibe continuamente eventos sobre la ubicación y estado de sus paquetes.

El laboratorio debe demostrar que un mismo flujo de eventos puede tener **dos finalidades diferentes**:

1. **Estado operacional actual** → DynamoDB.
2. **Histórico completo de eventos** → S3 Data Lake.

La arquitectura debe responder dos preguntas diferentes:

> **DynamoDB:** ¿Cuál es el estado actual de este paquete?

> **S3:** ¿Qué eventos ocurrieron y cómo evolucionó este paquete?

---

# 2. Caso de negocio

Una empresa de paquetería necesita recibir eventos generados por sus aplicaciones de tracking.

Cada evento representa un cambio o actualización en el estado de un paquete.

Ejemplo:

```json
{
  "package_id": "PKG-001",
  "timestamp": "2026-10-04T10:30:00Z",
  "latitude": -12.0464,
  "longitude": -77.0428,
  "status": "IN_TRANSIT"
}
```

Posibles estados:

```text
CREATED
PICKED_UP
IN_TRANSIT
OUT_FOR_DELIVERY
DELIVERED
CANCELLED
```

Un mismo paquete puede generar múltiples eventos durante su ciclo de vida.

---

# 3. Principio fundamental: evento vs estado

Este concepto debe explicarse antes de implementar DynamoDB.

## Un cambio de estado NO modifica el evento anterior

Si un paquete pasa de:

```text
CREATED
```

a:

```text
IN_TRANSIT
```

no se modifica el evento histórico `CREATED`.

Se genera un nuevo evento:

```text
10:00 → CREATED
10:30 → IN_TRANSIT
```

Por tanto:

```text
EVENT STREAM

PKG-001 → CREATED
PKG-001 → IN_TRANSIT
PKG-001 → OUT_FOR_DELIVERY
PKG-001 → DELIVERED
```

El stream representa una secuencia de eventos.

---

# 4. Arquitectura del laboratorio

```text
                         LOCAL MACHINE
                    ┌─────────────────────┐
                    │      Go API         │
                    │                     │
                    │ GET  /health        │
                    │ POST /events        │
                    │ POST /events/batch  │
                    └──────────┬──────────┘
                               │
                               │ AWS SDK for Go
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Kinesis Data Streams │
                    │                     │
                    │ logistics-events    │
                    └──────────┬──────────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
                    ▼                     ▼
             ┌────────────┐       ┌─────────────┐
             │   Lambda   │       │   Firehose  │
             │  Consumer  │       │             │
             └─────┬──────┘       └──────┬──────┘
                   │                     │
                   ▼                     ▼
             ┌────────────┐       ┌─────────────┐
             │ DynamoDB   │       │     S3      │
             │            │       │ Data Lake   │
             │ Current    │       │             │
             │ State      │       │ Event       │
             └────────────┘       │ History     │
                                  └──────┬──────┘
                                         │
                                         ▼
                                   Glue / Athena
                                   (integración
                                   conceptual)
```

---

# 5. Responsabilidad de cada componente

| Componente | Responsabilidad |
|---|---|
| Go API | Simular una aplicación productora de eventos |
| Kinesis Data Streams | Transportar y retener temporalmente los eventos |
| Lambda | Consumir eventos y actualizar el estado actual |
| DynamoDB | Mantener el estado actual de cada paquete |
| Kinesis Data Firehose | Entregar eventos hacia S3 |
| S3 | Conservar el histórico de eventos |
| Glue/Athena | Procesar/consultar posteriormente el histórico |

La separación de responsabilidades debe ser explícita.

---

# 6. Go como Producer

Go no es el objetivo principal del laboratorio.

Su función es simular una aplicación real que genera eventos.

## API mínima

```text
GET  /health
POST /events
POST /events/batch
```

### `GET /health`

Permite comprobar que el producer está funcionando.

### `POST /events`

Publica un único evento.

### `POST /events/batch`

Permite generar múltiples eventos y mostrar el concepto de publicación por lotes.

---

# 7. Evento de negocio

Esquema mínimo:

```json
{
  "package_id": "PKG-001",
  "timestamp": "2026-10-04T10:30:00Z",
  "latitude": -12.0464,
  "longitude": -77.0428,
  "status": "IN_TRANSIT"
}
```

Opcionalmente:

```json
{
  "package_id": "PKG-001",
  "timestamp": "2026-10-04T10:30:00Z",
  "latitude": -12.0464,
  "longitude": -77.0428,
  "status": "IN_TRANSIT",
  "city": "Lima",
  "driver_id": "DRV-001"
}
```

No agregar campos innecesarios si no aportan a la explicación del pipeline.

---

# 8. Partition Key

La propuesta inicial es utilizar:

```text
package_id
```

como Partition Key.

Ejemplo:

```text
PKG-001
PKG-002
PKG-003
```

Esto permite utilizar el mismo identificador de negocio para explicar cómo los eventos se distribuyen en Kinesis.

La explicación debe enfatizar que la elección de la partition key es una decisión de diseño y puede afectar la distribución de carga.

No asumir que `package_id` es universalmente la mejor clave para producción; se utiliza aquí porque es intuitiva para el caso didáctico.

---

# 9. Kinesis Data Streams

Crear un stream:

```text
logistics-events
```

El producer publica eventos utilizando AWS SDK for Go.

Operaciones principales:

```text
PutRecord
PutRecords
```

## Conceptos a observar

- stream;
- record;
- partition key;
- shard;
- sequence number;
- producer;
- consumer;
- retention.

El objetivo no es realizar tuning avanzado del stream, sino hacer visibles estos conceptos dentro de una arquitectura funcional.

---

# 10. Lambda como Consumer

Lambda representa el consumidor operacional.

Flujo:

```text
Kinesis
   │
   ▼
Lambda
   │
   ├── Validate event
   ├── Transform event
   └── Update current state
              │
              ▼
          DynamoDB
```

Lambda debe recibir el evento desde Kinesis y actualizar el estado del paquete.

---

# 11. Modelo de DynamoDB

DynamoDB no debe almacenar todo el histórico de eventos.

Su responsabilidad es representar el **estado actual**.

Ejemplo:

```text
PKG-001
----------------------------
status       = IN_TRANSIT
latitude     = -12.0464
longitude    = -77.0428
updated_at   = 2026-10-04T10:30:00Z
```

Si posteriormente llega:

```json
{
  "package_id": "PKG-001",
  "timestamp": "2026-10-04T11:20:00Z",
  "status": "OUT_FOR_DELIVERY"
}
```

Lambda actualiza:

```text
PKG-001
----------------------------
status       = OUT_FOR_DELIVERY
updated_at   = 2026-10-04T11:20:00Z
```

No se crea un nuevo registro histórico para cada estado en DynamoDB en este laboratorio.

La pregunta que DynamoDB responde es:

> **¿Cuál es el estado actual de PKG-001?**

---

# 12. S3 como histórico de eventos

Firehose entrega los eventos hacia un bucket S3.

Ejemplo conceptual:

```text
s3://logistics-datalake/events/
    year=2026/
        month=10/
            day=04/
                hour=10/
                    ...
```

Cada cambio de estado genera un nuevo evento que forma parte del histórico.

Por ejemplo:

```text
PKG-001 | 10:00 | CREATED
PKG-001 | 10:30 | IN_TRANSIT
PKG-001 | 11:20 | OUT_FOR_DELIVERY
PKG-001 | 12:05 | DELIVERED
```

La pregunta que S3 responde es:

> **¿Qué eventos ocurrieron y cómo evolucionó PKG-001?**

---

# 13. Particionamiento del Data Lake

## Recomendación para el laboratorio

Utilizar particionamiento temporal:

```text
year/month/day/hour
```

Ejemplo:

```text
events/
├── year=2026/
│   └── month=10/
│       └── day=04/
│           ├── hour=10/
│           ├── hour=11/
│           └── hour=12/
```

### ¿Por qué?

Permite:

- consultar por ventanas temporales;
- demostrar particionamiento;
- conectar el streaming con Athena/Glue;
- evitar una fragmentación excesiva por minuto.

---

# 14. Particionamiento por minuto: uso didáctico opcional

Para una demo en vivo puede utilizarse:

```text
year/month/day/hour/minute
```

Ejemplo:

```text
year=2026/
month=10/
day=04/
hour=19/
├── minute=40/
├── minute=41/
└── minute=42/
```

Esto hace muy visible el comportamiento de streaming durante una sesión corta.

Sin embargo, debe explicarse:

> **El particionamiento por minuto se utiliza aquí para hacer observable el flujo durante la demo; no debe asumirse como patrón universal de producción.**

Un exceso de granularidad puede generar demasiados objetos pequeños y perjudicar el rendimiento de consultas y procesamiento.

---

# 15. Un evento puede cambiar de estado

Ejemplo completo:

```text
                    PKG-001

10:00 ─────── CREATED
                 │
                 ▼
10:30 ─────── IN_TRANSIT
                 │
                 ▼
11:20 ─────── OUT_FOR_DELIVERY
                 │
                 ▼
12:05 ─────── DELIVERED
```

### DynamoDB

Solo mantiene:

```text
PKG-001 → DELIVERED
```

### S3

Mantiene:

```text
10:00 → CREATED
10:30 → IN_TRANSIT
11:20 → OUT_FOR_DELIVERY
12:05 → DELIVERED
```

Este ejemplo debe ser uno de los puntos centrales del laboratorio.

---

# 16. Current State vs Event History

Este es el principal concepto arquitectónico del laboratorio.

| Pregunta | Sistema |
|---|---|
| ¿Cuál es el estado actual del paquete? | DynamoDB |
| ¿Cuál fue el último estado conocido? | DynamoDB |
| ¿Qué eventos ocurrieron? | S3 |
| ¿Cómo evolucionó el paquete? | S3 |
| ¿Qué paquetes estaban IN_TRANSIT ayer? | S3 + Athena |
| ¿Cuál es el estado de PKG-001 ahora? | DynamoDB |

La arquitectura separa:

```text
OPERATIONAL VIEW
      │
      ▼
  DynamoDB
 Current State


ANALYTICAL / HISTORICAL VIEW
      │
      ▼
      S3
 Event History
```

---

# 17. Conexión con Glue y Athena

No es necesario implementar nuevamente Glue/Athena durante este laboratorio.

La conexión debe explicarse conceptualmente:

```text
Kinesis
   │
   ▼
Firehose
   │
   ▼
S3 Data Lake
   │
   ▼
Glue
   │
   ▼
Athena
```

Ejemplo de pregunta analítica:

> ¿Cuántos paquetes estuvieron en `IN_TRANSIT` durante una determinada ventana?

Otro ejemplo:

> ¿Cuál fue la secuencia de estados de `PKG-001`?

Esto conecta directamente Streaming con la arquitectura Data Lake construida en la Sesión 4.

---

# 18. Flujo de demostración recomendado

## Paso 1 — Presentar el problema

Mostrar:

> "Necesitamos conocer el estado actual de nuestros paquetes y, al mismo tiempo, conservar el historial completo de lo ocurrido."

---

## Paso 2 — Levantar Go

```text
GET /health
```

Mostrar que la aplicación está funcionando.

---

## Paso 3 — Crear un evento

```text
POST /events
```

Con:

```text
PKG-001 → CREATED
```

---

## Paso 4 — Observar Kinesis

Mostrar que el evento llega al stream.

Explicar:

- Producer;
- Record;
- Partition Key;
- Shard;
- Sequence Number.

---

## Paso 5 — Observar Lambda

Lambda recibe el evento y actualiza DynamoDB.

Mostrar:

```text
PKG-001 → CREATED
```

---

## Paso 6 — Enviar un segundo evento

```text
PKG-001 → IN_TRANSIT
```

Mostrar:

```text
DynamoDB
PKG-001 → IN_TRANSIT
```

El valor anterior se actualiza.

---

## Paso 7 — Mostrar Firehose

El evento continúa hacia Firehose.

Mostrar que termina en:

```text
S3
```

---

## Paso 8 — Mostrar el histórico

En S3 deben poder identificarse ambos eventos:

```text
CREATED
IN_TRANSIT
```

Esto demuestra visualmente la diferencia:

```text
DynamoDB → estado actual

S3 → histórico
```

---

## Paso 9 — Generar varios eventos

Utilizar:

```text
POST /events/batch
```

o un pequeño generador en Go.

Generar múltiples paquetes y cambios de estado.

---

## Paso 10 — Observar el Data Lake

Mostrar cómo los eventos llegan al particionamiento temporal.

Para una demo rápida:

```text
year/month/day/hour/minute
```

Para una estructura más realista:

```text
year/month/day/hour
```

---

# 19. Opcional: comparación con Kafka/MSK

No construir un segundo laboratorio.

Después de terminar la implementación, introducir:

```text
Kafka / MSK
       │
       │ mismo patrón conceptual
       ▼
Producer → Topic → Partition → Consumer
```

comparándolo con:

```text
Kinesis
       │
       ▼
Producer → Stream → Shard → Consumer
```

La finalidad es demostrar que el patrón arquitectónico es general y que AWS ofrece distintas plataformas para implementarlo.

---

# 20. Preguntas de certificación vinculadas al laboratorio

## Kinesis Data Streams

- ¿Qué determina la distribución de los registros?
- ¿Qué función cumple la Partition Key?
- ¿Qué representa un Sequence Number?
- ¿Qué es un Shard?
- ¿Cómo se relacionan productores y consumidores?

## Lambda

- ¿Cómo puede Lambda consumir eventos de Kinesis?
- ¿Qué ocurre cuando llegan múltiples registros?
- ¿Qué consideraciones existen respecto al procesamiento y reintentos?

## Firehose

- ¿Cuándo utilizar Firehose en lugar de Data Streams?
- ¿Qué destinos soporta?
- ¿Qué problema resuelve el buffering?

## DynamoDB vs S3

Pregunta conceptual:

> Una aplicación necesita obtener rápidamente el estado actual de un paquete, mientras otra aplicación necesita analizar el histórico completo de eventos. ¿Qué servicios son apropiados?

Respuesta esperada:

```text
Estado actual → DynamoDB
Histórico → S3
```

---

# 21. Alcance del laboratorio

## Incluye

- Go HTTP API;
- AWS SDK for Go;
- Kinesis Data Streams;
- Producer;
- Partition Key;
- Lambda Consumer;
- DynamoDB;
- Kinesis Data Firehose;
- S3 Data Lake;
- particionamiento temporal;
- eventos de cambio de estado;
- current state vs event history;
- conexión conceptual con Glue/Athena.

## No incluye

- Apache Kafka operativo;
- Amazon MSK desplegado por los alumnos;
- administración avanzada de Kafka;
- procesamiento con Flink;
- Kinesis Data Analytics;
- arquitecturas multi-región;
- tuning avanzado de Kinesis;
- implementación completa de un sistema productivo de tracking.

---

# 22. Resultado final esperado

Al finalizar el laboratorio, el estudiante debe haber construido:

```text
                    Go Producer
                         │
                         ▼
               Kinesis Data Streams
                         │
              ┌──────────┴──────────┐
              │                     │
              ▼                     ▼
           Lambda                Firehose
              │                     │
              ▼                     ▼
          DynamoDB                  S3
              │                     │
              │                     ▼
              │                Data Lake
              │                     │
              │                     ▼
              │                Glue/Athena
              │
              ▼
        Current State

                         +

                    Event History
```

Y debe poder explicar:

> **Cada cambio de estado genera un nuevo evento. Kinesis transporta esos eventos; Lambda actualiza el estado operacional actual en DynamoDB; Firehose entrega los eventos al Data Lake en S3, donde se conserva el histórico para análisis posterior.**

---

# 23. Decisión de diseño del laboratorio

La decisión final es **no incluir Kafka/MSK como implementación práctica**.

Kafka/MSK permanecerá en la parte conceptual de la sesión porque:

1. el tiempo disponible es limitado;
2. el objetivo principal es Data Streaming, no administración de Kafka;
3. Kinesis permite una integración más directa con los servicios AWS ya estudiados;
4. el laboratorio puede profundizar en Producer → Stream → Consumer;
5. DynamoDB y S3 permiten demostrar dos patrones de consumo diferentes;
6. el resultado mantiene continuidad con las sesiones 3, 4 y 6.

La práctica debe priorizar **profundidad en una arquitectura completa** sobre amplitud superficial de múltiples plataformas de streaming.

# API del producer (Go) y demo en tiempo real

El producer es una API local en Go que publica eventos de paquetes en Kinesis. Con el emisor automático activado genera un lote de eventos **cada 25 s** por sí solo, y puedes ver cómo se procesan en DynamoDB y en el Data Lake. Requiere la infraestructura desplegada ([deploy-guide.md](deploy-guide.md)). Código en [producer/](../producer/).

## 1. Por qué no hace falta un túnel

Un túnel (Cloudflare Tunnel, ngrok) sirve para exponer un servicio local a internet: tráfico de **entrada**. El producer hace tráfico de **salida**:

```text
tu máquina:  API Go (127.0.0.1:8080) ──HTTPS saliente (443)──► kinesis.us-east-1.amazonaws.com
```

- Quien llama a la API está en tu máquina; escucha solo en `127.0.0.1` (no tiene autenticación, nadie de fuera debe alcanzarla).
- Hacia AWS la conexión la inicia tu máquina, como cualquier página web. Kinesis nunca se conecta a ti.
- AWS sabe quién eres porque el SDK firma cada llamada con tus claves, y comprueba que tienen permiso `kinesis:PutRecord(s)`.

Resumen para la clase: *el producer es un cliente de Kinesis, no un servidor; los clientes salen a internet sin túnel.*

## 2. Levantar la API

Todo se configura en `.env.credentials` (claves, región y ajustes del producer). Los tres pasos se hacen en la misma terminal, desde la raíz del repo, en Git Bash:

```bash
# 1) Cargar credenciales y variables (una vez por terminal)
cp .env.credentials.example .env.credentials   # solo la primera vez; pon tus claves
set -a; source .env.credentials; set +a
aws sts get-caller-identity                    # debe mostrar tu cuenta

# 2) Compilar (la primera vez y cuando cambie producer/)
(cd producer && go build -o bin/producer.exe ./cmd/producer)

# 3) Arrancar (déjala abierta; se detiene con Ctrl+C)
./producer/bin/producer.exe
```

Debe imprimir:

```text
producer listening on http://127.0.0.1:8080, stream=logistics-events region=us-east-1
simulator enabled: 3 packages, one batch every 25s (Ctrl+C to stop)
```

Notas del paso 1: no definas `AWS_PROFILE` (con claves en el entorno no hace falta, y un perfil que no exista rompe el CLI y el producer). Si cambiaste `stream_name` en Terraform, ajusta `STREAM_NAME` en el archivo: `terraform -chdir=infra output -raw stream_name`. Si tus claves empiezan por `ASIA`, añade `AWS_SESSION_TOKEN`.

## 3. Emisión automática cada 25 s

Se controla con dos variables de `.env.credentials`:

| Variable | Defecto | Qué hace |
|---|---|---|
| `EMIT_INTERVAL` | `25s` en la plantilla | Un lote cada intervalo. Vacío o `0` lo desactiva; mínimo `1s` |
| `EMIT_PACKAGES` | `3` | Paquetes simulados a la vez (1 a 100) |

Cómo funciona:

- Al arrancar publica de inmediato y luego cada 25 s: un evento por paquete, en un solo `PutRecords`.
- Cada paquete avanza un estado por tick: `CREATED → PICKED_UP → IN_TRANSIT → OUT_FOR_DELIVERY → DELIVERED` (~2 min por paquete). Al llegar a `DELIVERED` se reemplaza por uno nuevo, así el flujo no se agota.
- Los ids son `SIM-<hhmmss>-<n>` y los timestamps son la hora UTC actual. Si un tick falla se registra y el siguiente lo reintenta: el producer no se cae.

Cada tick imprime una línea por evento con su shard (los paquetes se reparten entre los 2 shards):

```text
simulator: SIM-232842-1 CREATED -> shardId-000000000000
simulator: SIM-232842-2 CREATED -> shardId-000000000001
simulator: emitted 3 events (0 failed)
```

## 4. Ver el pipeline en tiempo real

Con la API corriendo (paso 3 de la sección 2; ya emite sola porque `.env.credentials` trae `EMIT_INTERVAL=25s`), abre **otras terminales** y en cada una carga el entorno:

```bash
cd "<ruta>/lab_streaming_aws"
set -a; source .env.credentials; set +a
export MSYS_NO_PATHCONV=1     # Git Bash: evita que /aws/lambda/... se convierta en ruta de Windows
TABLE=$(terraform -chdir=infra output -raw table_name)
BUCKET=$(terraform -chdir=infra output -raw datalake_bucket_name)
LOG=$(terraform -chdir=infra output -raw consumer_log_group_name)
```

**Terminal A — logs de la Lambda en vivo:**

```bash
aws logs tail "$LOG" --follow --format short
```

**Terminal B — estado actual en DynamoDB (se refresca cada 2 s):**

```bash
while true; do
  clear; date
  aws dynamodb scan --table-name "$TABLE" \
    --query "Items[?starts_with(package_id.S,'SIM')].[package_id.S,status.S,updated_at.S]" \
    --output table
  sleep 2
done
```

**Terminal C — histórico en S3 (ejecútalo cada minuto):**

```bash
aws s3 ls s3://$BUCKET/events/ --recursive | tr -d '\r'
KEY=$(aws s3 ls s3://$BUCKET/events/ --recursive | tr -d '\r' | tail -1 | awk '{print $4}')
aws s3 cp s3://$BUCKET/$KEY - | tr -d '\r' | grep SIM
```

Qué verás y cuándo:

| Dónde | Qué aparece | Cuándo |
|---|---|---|
| Terminal de la API | Una línea por evento y `emitted N events` | cada 25 s |
| Terminal A (Lambda) | `START` / `END` / `REPORT` por invocación | 5–10 s tras cada lote |
| Terminal B (DynamoDB) | Una fila por paquete cuyo `status` va cambiando hasta `DELIVERED` | 5–10 s tras cada lote |
| Terminal C (S3) | Objetos nuevos con **todos** los eventos (`CREATED`, `PICKED_UP`, …) | 1–2 min la primera vez |

**DynamoDB cambia; S3 acumula.** DynamoDB responde «¿cuál es el estado actual?» (una fila por paquete); S3 responde «¿qué eventos ocurrieron?» (todos, sin perder ninguno).

Detalles que conviene saber:

- Firehose guarda un buffer de 60 s **por shard** y escribe un objeto por buffer: con 2 shards verás hasta un objeto por shard cada minuto, casi a la vez, y cada objeto agrupa 2–3 ticks. El tick más reciente aún no está en S3: es el retraso normal, no una pérdida.
- Para ver una carpeta nueva por minuto, crea `infra/terraform.tfvars` (copia el `.example`) con `datalake_partition_granularity = "minute"` y vuelve a desplegar ([deploy-guide.md](deploy-guide.md); requiere aprobación del instructor).
- Las métricas de CloudWatch van 1–3 min atrasadas: en la demo confía en estos comandos y no en los gráficos.

## 5. Enviar un evento a mano (opcional)

```bash
curl -s http://127.0.0.1:8080/health                                             # {"status":"ok"}
curl -s -X POST http://127.0.0.1:8080/events -d @producer/examples/01_created.json
curl -s -X POST http://127.0.0.1:8080/events/batch -d @producer/examples/batch.json
```

`/events` responde con el `shard_id` y el `sequence_number` del registro; `/events/batch` acepta de 1 a 500 eventos y responde `207` si alguno falla. Los eventos deben cumplir el contrato de [package_event_contract.json](../src/contracts/package_event_contract.json): `package_id`, `timestamp` UTC con formato `YYYY-MM-DDTHH:MM:SSZ`, `latitude`, `longitude` y un `status` válido. Un evento inválido responde `400` con el motivo.

Un evento con `timestamp` igual o menor al guardado en DynamoDB se ignora a propósito (los ejemplos tienen fechas fijas: para repetirlos usa otro `package_id`).

## 6. Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| `failed to get shared config profile, your-profile` o `The config profile (…) could not be found` | Quedó un `AWS_PROFILE` en esa terminal. Bórralo de `.env.credentials`, ejecuta `unset AWS_PROFILE` y vuelve a hacer `source` |
| `STREAM_NAME is required` o `AWS_REGION … is required` | Falta cargar el entorno: `set -a; source .env.credentials; set +a` y vuelve a arrancar |
| `InvalidClientTokenId`, `502` al publicar | Claves inválidas o sin permiso `kinesis:PutRecord(s)`: `aws sts get-caller-identity` y corrige las claves |
| El producer no emite solo | `EMIT_INTERVAL` vacío o sin cargar; debe arrancar con la línea `simulator enabled: …` |
| `Solo se permite un uso de cada dirección…` | Puerto 8080 ocupado por otra instancia: ciérrala o usa `PORT=18080` |
| `aws logs tail` falla con `logGroupName … failed to satisfy constraint` | Falta `export MSYS_NO_PATHCONV=1` |
| DynamoDB vacío o sin cambios | Espera 5–10 s; revisa que el mapping de la Lambda esté `Enabled` ([deploy-guide.md](deploy-guide.md), paso 5) |
| Nada en S3 todavía | Firehose espera su buffer de 60 s: espera 1–2 minutos |

## Seguridad

- La API solo escucha en `127.0.0.1` y no tiene autenticación: no la expongas (túnel, `0.0.0.0`) sin añadirle autenticación.
- En producción se recomienda un rol de IAM dedicado con `AssumeRole` y mínimo privilegio, y que el producer corra dentro de AWS, sin claves de larga duración.
- Los datos de prueba (`SIM-*`, `TEST-*`) se quedan en DynamoDB y S3 hasta `terraform destroy`.

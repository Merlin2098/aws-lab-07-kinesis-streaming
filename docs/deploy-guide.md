# Guía de despliegue

Despliega el lab en AWS con Terraform (16 recursos) y lo destruye al terminar. Para levantar la API Go y ver los eventos fluir, sigue [producer-api.md](producer-api.md).

> `terraform apply` y `terraform destroy` crean y borran recursos de pago: solo con aprobación explícita del instructor.

**Qué se crea** (prefijo `logistics-streaming-dev`): stream Kinesis `logistics-events` (2 shards), Lambda consumidora con su cola SQS de fallos, tabla DynamoDB de estado actual, Firehose hacia un bucket S3 (el Data Lake), roles IAM y log groups de 7 días.

**Costo:** ~USD 0.03 por hora por los 2 shards, aunque no entren datos; el resto es marginal. Despliega poco antes de la clase y destruye al terminar.

## 1. Requisitos

AWS CLI v2, Terraform ≥ 1.6, Go ≥ 1.23, `uv`, Git Bash y una access key de AWS con permiso para crear los servicios de arriba y roles IAM (en una cuenta de laboratorio, `AdministratorAccess`).

## 2. Credenciales

```bash
cp .env.credentials.example .env.credentials   # solo la primera vez; pon tus claves
set -a; source .env.credentials; set +a        # en CADA terminal nueva
aws sts get-caller-identity                    # debe mostrar tu cuenta
```

Terraform y el AWS CLI no leen el archivo por sí solos, por eso el `source`. No definas `AWS_PROFILE`: con claves en el entorno no hace falta. Si tus claves empiezan por `ASIA` (temporales), añade también `AWS_SESSION_TOKEN`.

## 3. Plan

```bash
cd infra
terraform init
terraform plan
```

Debe terminar en **`Plan: 16 to add, 0 to change, 0 to destroy.`**

**Opcional: `terraform.tfvars`.** No lo necesitas: todas las variables tienen un valor por defecto. Solo si quieres cambiar algo, copia `terraform.tfvars.example` a `infra/terraform.tfvars` (`plan` y `apply` lo leen solos si existe; `init` nunca lo lee). Por ejemplo, `datalake_partition_granularity = "minute"` crea una carpeta nueva por minuto en S3 (solo para la demo; el defecto es `hour`). El ejemplo cambia además la etiqueta `CostCenter` de `engineering` a `data-platform`.

El estado de Terraform es local (`infra/terraform.tfstate`): no lo borres ni lo edites, sin él `destroy` no sabe qué eliminar.

## 4. Desplegar

Requiere aprobación del instructor.

```bash
terraform apply                   # muestra el plan otra vez y pide escribir "yes"; tarda 1–3 minutos
terraform output                  # nombres del stream, tabla, bucket, etc.
```

## 5. Verificar

Desde la raíz del repo, con las credenciales cargadas:

```bash
uv run python scripts/testing/run_cloud_tests.py tests/aws -m cloud -v
```

Esperado: **6 passed** (la última tarda 1–4 minutos: espera el buffer de Firehose). Confirma que el stream, la tabla, la Lambda y Firehose están activos y que un evento llega a DynamoDB y a S3.

Los consumidores leen desde que quedan activos, así que publica eventos **después** de este paso. Siguiente: [producer-api.md](producer-api.md).

## 6. Destruir

Requiere aprobación del instructor. Detén antes el producer (`Ctrl+C`).

```bash
terraform destroy                 # debe terminar en: Destroy complete! Resources: 16 destroyed.
terraform state list              # debe quedar vacío
```

El bucket se vacía solo porque `datalake_force_destroy = true`.

## Problemas frecuentes

| Síntoma | Causa | Qué hacer |
|---|---|---|
| `plan`: `No valid credential sources found` y luego `no EC2 IMDS role found` | No cargaste `.env.credentials` en esa terminal, o las claves son inválidas. El error de IMDS es solo el último intento y confunde | `source .env.credentials` y `aws sts get-caller-identity`. Si da `InvalidClientTokenId`, corrige las claves en IAM |
| `The config profile (…) could not be found` o `failed to get shared config profile` | Hay un `AWS_PROFILE` en el entorno que no existe (p. ej. `your-profile`) | Bórralo de `.env.credentials`, ejecuta `unset AWS_PROFILE` y vuelve a hacer `source` |
| `Failed to load plugin schemas` | Poca memoria libre para el provider de AWS (necesita ~1.5 GB) | Cierra aplicaciones y reintenta |
| `AccessDenied` al crear roles o recursos | Permisos insuficientes de la clave | Usa una clave con permisos IAM o pide acceso al administrador |
| `ResourceNotFoundException` con el CLI | Región o cuenta distintas a las del despliegue | `source .env.credentials` y revisa `AWS_DEFAULT_REGION` |
| `destroy` falla con `BucketNotEmpty` | `datalake_force_destroy` estaba en `false` | Ponlo en `true`, `terraform apply` y repite `destroy` |

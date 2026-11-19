# VidalStore Admin

Microservicio administrativo para gestionar la infraestructura de mensajeria y consultar la auditoria de VidalStore.

## Responsabilidades

- Gestionar colas RabbitMQ.
- Gestionar exchanges RabbitMQ.
- Consultar bindings.
- Publicar mensajes en exchanges.
- Consultar eventos de auditoria.
- Consultar mensajes muertos.
- Gestionar topics, particiones, configs y offsets de consumer groups en Kafka.
- Verificar la salud de RabbitMQ y PostgreSQL.

## Puerto

El servicio escucha en el puerto `3020` por defecto. Puede modificarse mediante `PORT`.

## Endpoints

| Metodo | Ruta | Descripcion |
| --- | --- | --- |
| GET | `/` | Estado de RabbitMQ y PostgreSQL |
| GET | `/salud` | Alias del endpoint de salud |
| GET | `/v1/admin/colas` | Lista las colas |
| POST | `/v1/admin/colas` | Crea una cola |
| DELETE | `/v1/admin/colas/:nombre` | Elimina una cola |
| GET | `/v1/admin/exchanges` | Lista los exchanges |
| POST | `/v1/admin/exchanges` | Crea un exchange |
| DELETE | `/v1/admin/exchanges/:nombre` | Elimina un exchange |
| GET | `/v1/admin/colas/:nombre/bindings` | Lista los bindings de una cola |
| POST | `/v1/admin/exchanges/:nombre/publicar` | Publica un mensaje |
| GET | `/v1/admin/auditoria` | Consulta los eventos auditados |
| GET | `/v1/admin/mensajes-muertos` | Consulta la DLQ persistida |
| GET | `/v1/admin/kafka/topics` | Lista topics con particiones, replicas y configs |
| POST | `/v1/admin/kafka/topics` | Crea un topic |
| PATCH | `/v1/admin/kafka/topics/:nombre/config` | Cambia configs de un topic |
| POST | `/v1/admin/kafka/topics/:nombre/particiones` | Aumenta las particiones de un topic |
| DELETE | `/v1/admin/kafka/topics/:nombre` | Borra un topic |
| GET | `/v1/admin/kafka/grupos` | Lista consumer groups con su lag |
| POST | `/v1/admin/kafka/grupos/:id/offsets` | Mueve los offsets de un grupo |

## Kafka (E3)

Las rutas de Kafka usan el `Admin` de `@platformatic/kafka`, el mismo cliente que usa
`vidalstore-eventos`. No pasan por el gateway: se llaman directo al admin (en local,
`http://localhost:3020`) con un access token de Cognito del grupo `administradores`.
El servicio escucha los brokers de `KAFKA_BROKERS` y cierra el cliente en el apagado.

### Variables de entorno

```env
KAFKA_BROKERS=localhost:9092,localhost:9094,localhost:9096
```

En el compose, `vidalstore-plataforma` reemplaza el valor por
`broker-1:19092,broker-2:19092,broker-3:19092`.

### Ejemplos con curl

```bash
export ADMIN=http://localhost:3020
export TOKEN=<access_token_de_cognito>

# 1. Listar topics
curl -s "$ADMIN/v1/admin/kafka/topics" -H "Authorization: Bearer $TOKEN"

# 2. Crear un topic (201)
curl -s -X POST "$ADMIN/v1/admin/kafka/topics" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"nombre":"juego.eventos","particiones":6,"replicas":3,"configs":{"retention.ms":"604800000","cleanup.policy":"delete"}}'

# 3. Cambiar configs (200)
curl -s -X PATCH "$ADMIN/v1/admin/kafka/topics/juego.eventos/config" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"retention.ms":"86400000"}'

# 4. Aumentar particiones (200; el total que debe quedar)
curl -s -X POST "$ADMIN/v1/admin/kafka/topics/juego.eventos/particiones" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"particiones":9}'

# 5. Borrar un topic (200). Los vidalstore.* exigen repetir el nombre en ?confirmar=
curl -s -X DELETE "$ADMIN/v1/admin/kafka/topics/vidalstore.pedidos?confirmar=vidalstore.pedidos" \
  -H "Authorization: Bearer $TOKEN"

# 6. Listar consumer groups y su lag
curl -s "$ADMIN/v1/admin/kafka/grupos" -H "Authorization: Bearer $TOKEN"

# 7. Mover offsets de un grupo (200). Tambien acepta {"hacia":"final"} o
#    {"hacia":"fecha","fecha":"2026-11-07T00:30:00Z","topic":"juego.eventos"}
curl -s -X POST "$ADMIN/v1/admin/kafka/grupos/vidalstore-panel/offsets" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"hacia":"principio"}'
```

### Codigos de respuesta

| Codigo | Cuando |
| --- | --- |
| `200` / `201` | Operacion aplicada (201 solo al crear un topic) |
| `400` | DTO invalido, nombre de topic/grupo invalido, bajar particiones, borrar un `vidalstore.*` sin `?confirmar=`, cambiar configs sin ninguna config o pedir `hacia=fecha` sin fecha |
| `401` | Token ausente, vencido, de otra app o con firma invalida |
| `403` | Token valido sin el grupo `administradores` |
| `404` | El topic o el consumer group no existe |
| `409` | El topic ya existe o el grupo tiene consumidores conectados (Kafka no mueve offsets con el grupo activo) |
| `503` | Kafka no responde |

## Variables de entorno

```env
PORT=3020
RABBITMQ_MGMT_URL=http://rabbitmq:15672/api
RABBITMQ_USER=vidalstore
RABBITMQ_PASS=vidalstore
DB_HOST=postgres
DB_PORT=5432
DB_USER=vidalstore
DB_PASSWORD=vidalstore
DB_NAME=vidalstore
KAFKA_BROKERS=localhost:9092,localhost:9094,localhost:9096
```

En Docker Compose, los hosts internos son `rabbitmq` y `postgres`.

## Desarrollo

```bash
npm install
npm run start:dev
```

## Verificacion

```bash
npm run build
npm run lint
npm test
npm run test:e2e
```

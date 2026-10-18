# VidalStore Admin

Microservicio administrativo para gestionar la infraestructura de mensajeria y consultar la auditoria de VidalStore.

## Responsabilidades

- Gestionar colas RabbitMQ.
- Gestionar exchanges RabbitMQ.
- Consultar bindings.
- Publicar mensajes en exchanges.
- Consultar eventos de auditoria.
- Consultar mensajes muertos.
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
```

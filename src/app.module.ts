import {
  Module,
  Controller,
  Get,
  Post,
  Delete,
  Param,
  Body,
  HttpException,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import pg from 'pg';

const { Pool } = pg;

const EXCHANGE_TYPES = ['direct', 'topic', 'fanout', 'headers'];
const NAME_RE = /^[a-zA-Z0-9._-]{1,255}$/;

function validarNombre(nombre: string, campo: string) {
  if (!nombre || typeof nombre !== 'string' || !NAME_RE.test(nombre)) {
    throw new HttpException(
      { error: `${campo} inválido: debe tener entre 1 y 255 caracteres (letras, números, punto, guión, guión bajo).` },
      HttpStatus.BAD_REQUEST,
    );
  }
}
function validarTipoExchange(tipo: string) {
  if (!EXCHANGE_TYPES.includes(tipo)) {
    throw new HttpException(
      { error: `tipo inválido: debe ser uno de ${EXCHANGE_TYPES.join(', ')}.` },
      HttpStatus.BAD_REQUEST,
    );
  }
}

async function rabbit(method: string, path: string, body?: object) {
  const base = process.env.RABBITMQ_MGMT_URL || 'http://vidalstore:vidalstore@rabbitmq:15672/api';
  const res = await fetch(`${base}${path}`, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  if (!res.ok) {
    throw new HttpException(
      { error: `RabbitMQ Management API: ${res.status} ${text}` },
      res.status >= 400 && res.status < 500 ? HttpStatus.BAD_REQUEST : HttpStatus.BAD_GATEWAY,
    );
  }
  return text ? JSON.parse(text) : {};
}

@Controller()
export class HealthController {
  @Get()
  check() {
    return { status: 'ok', service: 'vidalstore-admin', timestamp: new Date().toISOString() };
  }
}

@Controller('v1/admin')
export class AdminController {
  private readonly logger = new Logger(AdminController.name);
  private pool: pg.Pool;

  constructor() {
    this.pool = new Pool({
      host:     process.env.DB_HOST     || 'postgres',
      port:     Number(process.env.DB_PORT) || 5432,
      user:     process.env.DB_USER     || 'vidalstore',
      password: process.env.DB_PASSWORD || 'vidalstore',
      database: process.env.DB_NAME     || 'vidalstore',
    });
  }

  @Get('colas')
  listarColas() {
    return rabbit('GET', '/queues');
  }

  @Post('colas')
  crearCola(@Body() body: { nombre: string; durable?: boolean }) {
    if (!body || Object.keys(body).filter(k => !['nombre','durable'].includes(k)).length > 0) {
      throw new HttpException({ error: 'Campos no permitidos en la petición.' }, HttpStatus.BAD_REQUEST);
    }
    validarNombre(body.nombre, 'nombre');
    const durable = body.durable !== undefined ? Boolean(body.durable) : true;
    return rabbit('PUT', `/queues/%2F/${encodeURIComponent(body.nombre)}`, { durable });
  }

  @Delete('colas/:nombre')
  eliminarCola(@Param('nombre') nombre: string) {
    validarNombre(nombre, 'nombre');
    return rabbit('DELETE', `/queues/%2F/${encodeURIComponent(nombre)}`);
  }

  @Get('exchanges')
  listarExchanges() {
    return rabbit('GET', '/exchanges');
  }

  @Post('exchanges')
  crearExchange(@Body() body: { nombre: string; tipo: string; durable?: boolean }) {
    if (!body || Object.keys(body).filter(k => !['nombre','tipo','durable'].includes(k)).length > 0) {
      throw new HttpException({ error: 'Campos no permitidos en la petición.' }, HttpStatus.BAD_REQUEST);
    }
    validarNombre(body.nombre, 'nombre');
    validarTipoExchange(body.tipo);
    const durable = body.durable !== undefined ? Boolean(body.durable) : true;
    return rabbit('PUT', `/exchanges/%2F/${encodeURIComponent(body.nombre)}`, { type: body.tipo, durable });
  }

  @Delete('exchanges/:nombre')
  eliminarExchange(@Param('nombre') nombre: string) {
    validarNombre(nombre, 'nombre');
    return rabbit('DELETE', `/exchanges/%2F/${encodeURIComponent(nombre)}`);
  }

  @Get('colas/:nombre/bindings')
  listarBindings(@Param('nombre') nombre: string) {
    validarNombre(nombre, 'nombre');
    return rabbit('GET', `/queues/%2F/${encodeURIComponent(nombre)}/bindings`);
  }

  @Post('exchanges/:nombre/publicar')
  publicarMensaje(
    @Param('nombre') nombre: string,
    @Body() body: { routingKey: string; payload: string },
  ) {
    if (!body || Object.keys(body).filter(k => !['routingKey','payload'].includes(k)).length > 0) {
      throw new HttpException({ error: 'Campos no permitidos en la petición.' }, HttpStatus.BAD_REQUEST);
    }
    validarNombre(nombre, 'nombre');
    if (!body.routingKey || typeof body.routingKey !== 'string' || body.routingKey.length > 255) {
      throw new HttpException({ error: 'routingKey inválida.' }, HttpStatus.BAD_REQUEST);
    }
    if (body.payload === undefined || body.payload === null) {
      throw new HttpException({ error: 'payload es obligatorio.' }, HttpStatus.BAD_REQUEST);
    }
    return rabbit('POST', `/exchanges/%2F/${encodeURIComponent(nombre)}/publish`, {
      routing_key: body.routingKey,
      payload: body.payload,
      payload_encoding: 'string',
      properties: {},
    });
  }

  @Get('auditoria')
  async getAuditLogs() {
    const res = await this.pool.query(
      'SELECT * FROM eventos_auditoria ORDER BY recibido_en DESC LIMIT 50',
    );
    return { success: true, count: res.rows.length, data: res.rows };
  }

  @Get('mensajes-muertos')
  async getMensajesMuertos() {
    const res = await this.pool.query(
      'SELECT * FROM mensajes_muertos ORDER BY recibido_en DESC LIMIT 50',
    );
    return { success: true, count: res.rows.length, data: res.rows };
  }
}

@Module({
  controllers: [HealthController, AdminController],
})
export class AppModule {}

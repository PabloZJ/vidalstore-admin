import { randomUUID } from 'node:crypto';
import { Injectable, NotFoundException } from '@nestjs/common';
import { DatabaseService } from '../database/database.service.js';
import { RabbitAdminService } from './rabbit-admin.service.js';

/**
 * Responsabilidad: consultas a la base de datos del microservicio de administración.
 * La lógica AMQP (colas, exchanges, bindings) vive en RabbitAdminService.
 */
@Injectable()
export class AdminService {
  constructor(
    private readonly database: DatabaseService,
    private readonly rabbitAdmin: RabbitAdminService,
  ) {}

  async audit() {
    const result = await this.database.query(
      'SELECT * FROM eventos_auditoria ORDER BY recibido_en DESC LIMIT 50',
    );
    return { success: true, count: result.rows.length, data: result.rows };
  }

  async deadLetters() {
    const result = await this.database.query(
      'SELECT * FROM mensajes_muertos ORDER BY recibido_en DESC LIMIT 50',
    );
    return { success: true, count: result.rows.length, data: result.rows };
  }

  async reprocesarMensajeMuerto(id: string) {
    const result = await this.database.query<{
      id: string;
      cola_origen: string;
      routing_key: string;
      payload: string;
    }>(
      'SELECT id, cola_origen, routing_key, payload FROM mensajes_muertos WHERE id = $1',
      [id],
    );
    const mensaje = result.rows[0];
    if (!mensaje) throw new NotFoundException(`Mensaje muerto ${id} no encontrado`);

    const exchange = ['vidalstore.correos', 'vidalstore.correos.dlq'].includes(mensaje.cola_origen)
      ? 'vidalstore.comandos'
      : 'vidalstore.eventos';
    const eventoId = randomUUID();
    await this.rabbitAdmin.republicar(exchange, mensaje.routing_key, mensaje.payload, {
      'x-evento-id': eventoId,
      'x-emitido-en': new Date().toISOString(),
      'x-reprocesado-de': mensaje.id,
    });
    await this.database.query('DELETE FROM mensajes_muertos WHERE id = $1', [id]);
    return { success: true, id, eventoId, exchange, routingKey: mensaje.routing_key };
  }
}

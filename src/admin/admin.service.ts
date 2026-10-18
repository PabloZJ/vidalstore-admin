import { BadRequestException, Injectable } from '@nestjs/common';
import { DatabaseService } from '../database/database.service.js';
import { RabbitMqManagementService } from '../rabbitmq/rabbitmq-management.service.js';

const exchangeTypes = ['direct', 'topic', 'fanout', 'headers'];
const namePattern = /^[a-zA-Z0-9._-]{1,255}$/;

@Injectable()
export class AdminService {
  constructor(private readonly rabbitmq: RabbitMqManagementService, private readonly database: DatabaseService) {}
  private validateName(value: string, field: string) { if (!value || !namePattern.test(value)) throw new BadRequestException(`${field} inválido.`); }
  private validateExchangeType(value: string) { if (!exchangeTypes.includes(value)) throw new BadRequestException(`tipo inválido: ${exchangeTypes.join(', ')}.`); }
  listQueues() { return this.rabbitmq.request('GET', '/queues'); }
  createQueue(body: { nombre: string; durable?: boolean }) { this.validateName(body?.nombre, 'nombre'); return this.rabbitmq.request('PUT', `/queues/%2F/${encodeURIComponent(body.nombre)}`, { durable: body.durable ?? true }); }
  deleteQueue(name: string) { this.validateName(name, 'nombre'); return this.rabbitmq.request('DELETE', `/queues/%2F/${encodeURIComponent(name)}`); }
  listExchanges() { return this.rabbitmq.request('GET', '/exchanges'); }
  createExchange(body: { nombre: string; tipo: string; durable?: boolean }) { this.validateName(body?.nombre, 'nombre'); this.validateExchangeType(body?.tipo); return this.rabbitmq.request('PUT', `/exchanges/%2F/${encodeURIComponent(body.nombre)}`, { type: body.tipo, durable: body.durable ?? true }); }
  deleteExchange(name: string) { this.validateName(name, 'nombre'); return this.rabbitmq.request('DELETE', `/exchanges/%2F/${encodeURIComponent(name)}`); }
  listBindings(name: string) { this.validateName(name, 'nombre'); return this.rabbitmq.request('GET', `/queues/%2F/${encodeURIComponent(name)}/bindings`); }
  publish(name: string, body: { routingKey: string; payload: string }) { this.validateName(name, 'nombre'); if (!body?.routingKey || body.routingKey.length > 255 || body.payload == null) throw new BadRequestException('Mensaje inválido.'); return this.rabbitmq.request('POST', `/exchanges/%2F/${encodeURIComponent(name)}/publish`, { routing_key: body.routingKey, payload: body.payload, payload_encoding: 'string', properties: {} }); }
  async audit() { const result = await this.database.query('SELECT * FROM eventos_auditoria ORDER BY recibido_en DESC LIMIT 50'); return { success: true, count: result.rows.length, data: result.rows }; }
  async deadLetters() { const result = await this.database.query('SELECT * FROM mensajes_muertos ORDER BY recibido_en DESC LIMIT 50'); return { success: true, count: result.rows.length, data: result.rows }; }
}

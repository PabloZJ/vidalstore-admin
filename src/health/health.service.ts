import { Injectable } from '@nestjs/common';
import { DatabaseService } from '../database/database.service.js';
import { RabbitMqManagementService } from '../rabbitmq/rabbitmq-management.service.js';

@Injectable()
export class HealthService {
  constructor(private readonly database: DatabaseService, private readonly rabbitmq: RabbitMqManagementService) {}
  async check() {
    const db = await this.database.isHealthy(); let rabbit = false;
    try { await this.rabbitmq.checkConnection(); rabbit = true; } catch {}
    return { status: db && rabbit ? 'ok' : 'degradado', service: 'vidalstore-admin', db, rabbit, timestamp: new Date().toISOString() };
  }
}

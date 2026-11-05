import { Injectable } from '@nestjs/common';
import { DatabaseService } from '../database/database.service.js';

@Injectable()
export class HealthService {
  private readonly urlAdmin: string;
  private readonly autorizacion: string;

  constructor(private readonly database: DatabaseService) {
    const host   = process.env.RABBITMQ_HOST     ?? 'rabbitmq';
    const port   = process.env.RABBITMQ_MGMT_PORT ?? '15672';
    const url    = process.env.RABBITMQ_MGMT_URL  ?? `http://${host}:${port}`;
    const user   = process.env.RABBITMQ_USER       ?? 'vidalstore';
    const pass   = process.env.RABBITMQ_PASS       ?? 'vidalstore';
    this.urlAdmin      = url;
    this.autorizacion  = 'Basic ' + Buffer.from(`${user}:${pass}`).toString('base64');
  }

  async check() {
    const db = await this.database.isHealthy();
    let rabbit = false;
    try {
      const r = await fetch(`${this.urlAdmin}/api/health/checks/alarms`, {
        headers: { authorization: this.autorizacion },
        signal:  AbortSignal.timeout(3000),
      });
      rabbit = r.ok;
    } catch {}
    return {
      status: db && rabbit ? 'ok' : 'degradado',
      service: 'vidalstore-admin',
      db,
      rabbit,
      timestamp: new Date().toISOString(),
    };
  }
}

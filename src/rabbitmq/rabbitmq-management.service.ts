import { HttpException, HttpStatus, Injectable } from '@nestjs/common';

@Injectable()
export class RabbitMqManagementService {
  private readonly baseUrl = process.env.RABBITMQ_MGMT_URL || 'http://vidalstore:vidalstore@rabbitmq:15672/api';

  async request<T>(method: string, path: string, body?: object): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, { method, headers: { 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
    const text = await response.text();
    if (!response.ok) throw new HttpException({ error: `RabbitMQ Management API: ${response.status} ${text}` }, response.status < 500 ? HttpStatus.BAD_REQUEST : HttpStatus.BAD_GATEWAY);
    return (text ? JSON.parse(text) : {}) as T;
  }
  checkConnection() { return this.request('GET', '/overview'); }
}

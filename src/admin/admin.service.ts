import { Injectable } from '@nestjs/common';
import { DatabaseService } from '../database/database.service.js';

/**
 * Responsabilidad: consultas a la base de datos del microservicio de administración.
 * La lógica AMQP (colas, exchanges, bindings) vive en RabbitAdminService.
 */
@Injectable()
export class AdminService {
  constructor(private readonly database: DatabaseService) {}

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
}

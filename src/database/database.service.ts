import { Injectable, OnModuleDestroy } from '@nestjs/common';
import pg from 'pg';

const { Pool } = pg;

@Injectable()
export class DatabaseService implements OnModuleDestroy {
  private readonly pool = new Pool({
    host: process.env.DB_HOST || 'postgres', port: Number(process.env.DB_PORT) || 5432,
    user: process.env.DB_USER || 'vidalstore', password: process.env.DB_PASSWORD || 'vidalstore',
    database: process.env.DB_NAME || 'vidalstore',
  });

  query<T extends pg.QueryResultRow>(text: string, values?: unknown[]) { return this.pool.query<T>(text, values); }
  async isHealthy() { try { await this.pool.query('SELECT 1'); return true; } catch { return false; } }
  onModuleDestroy() { return this.pool.end(); }
}

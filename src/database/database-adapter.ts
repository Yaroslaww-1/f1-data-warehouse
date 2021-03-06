import { Pool } from 'pg';
import { DatabaseConfig } from './config';

const pool = new Pool({ connectionString: DatabaseConfig.DATABASE_CONNECTION_URL });

class DatabaseAdapter {
  constructor(private readonly _pool: Pool) {}

  async query<T>(query: string, values?: unknown[]): Promise<T[]> {
    const connection = await this._pool.connect();
    try {
      const result = await connection.query(query, values);
      connection.release();
      return result?.rows as T[];
    } catch (e) {
      console.error(e);
    }
  }
}

export const databaseAdapter = new DatabaseAdapter(pool);
import { Pool } from 'pg';
import { DatabaseConfig } from './config';

const pool = new Pool({ connectionString: DatabaseConfig.DATABASE_CONNECTION_URL });

class DatabaseAdapter {
  constructor(private readonly _pool: Pool) {}

  async query<T>(query: string, values?: unknown[]): Promise<T[]> {
    const connection = await this._pool.connect();
    const result = await connection.query(query, values);
    console.log(`Query result: ${result}`);
    connection.release();
    return result?.rows as T[];
  }
}

export const databaseAdapter = new DatabaseAdapter(pool);
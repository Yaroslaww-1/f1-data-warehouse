import * as fs from 'fs/promises';
import * as path from 'path';
import { databaseAdapter } from '../../../../../database/database-adapter';

export class RdStaging {
  static async create() {
    const query = await fs.readFile(path.join(__dirname, 'create-rd-staging-schema.sql'));
    await databaseAdapter.query<void>(query.toString());
  }

  static async drop() {
    const query = await fs.readFile(path.join(__dirname, 'drop-rd-staging-schema.sql'));
    await databaseAdapter.query<void>(query.toString());
  }

  static async insertInto(tableName: string, data: unknown[]) {
    const columnNames = Object.keys(data[0]).filter(name => name !== 'id');
    const values = data.map(value => {
      const valueProperties = columnNames.map(c => {
        if (value[c] === null) return 'NULL';
        if (typeof value[c] === 'string') return `'${value[c]}'`;
        else return value[c];
      });
      return `(${valueProperties.join(',')})`;
    });
    const query = `
      INSERT INTO ${tableName} (${columnNames.join(',')})
      VALUES ${values.join(',')}
      RETURNING *
    `;
    await databaseAdapter.query<void>(query);
  }
}
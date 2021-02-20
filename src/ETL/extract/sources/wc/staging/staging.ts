import * as fs from 'fs/promises';
import * as path from 'path';
import { databaseAdapter } from '../../../../../database/database-adapter';

export class WcStaging {
  static async create() {
    const query = await fs.readFile(path.join(__dirname, 'create-wc-staging-schema.sql'));
    await databaseAdapter.query<void>(query.toString());
  }
  static async insertInto(tableName: string, data: unknown[]) {
    const columnNames = Object.keys(data[0]);
    const values = data.map(value => {
      const valueProperties = columnNames.map(c => {
        if (typeof value[c] === 'string') return `'${value[c]}'`;
        else return value[c];
      }).join(',');
      return `(${valueProperties})`;
    }).join(',');
    const query = `
      INSERT INTO ${tableName} (${columnNames.join(',')})
      VALUES ${values}
      RETURNING *
    `;
    await databaseAdapter.query<void>(query);
  }
}
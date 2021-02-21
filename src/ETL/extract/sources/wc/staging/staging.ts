import * as fs from 'fs/promises';
import * as path from 'path';
import { databaseAdapter } from '../../../../../database/database-adapter';

export class WcStaging {
  static async create() {
    const query = await fs.readFile(path.join(__dirname, 'create-wc-staging-schema.sql'));
    await databaseAdapter.query<void>(query.toString());
  }

  static async drop() {
    const query = await fs.readFile(path.join(__dirname, 'drop-wc-staging-schema.sql'));
    await databaseAdapter.query<void>(query.toString());
  }

  static async insertInto(tableName: string, data: unknown[]) {
    const columnNames = Object.keys(data[0]).filter(name => name !== 'id');
    const values = data.map(value => {
      const valueProperties = columnNames.map(c => {
        // console.log(c, c === null);
        if (value[c] === null) return 'NULL';
        if (typeof value[c] === 'string') return `'${value[c]}'`;
        else return value[c];
      });
      // console.log(valueProperties);
      // if (valueProperties.some(v => v === 'NULL')) {
      //   console.log(valueProperties.join(','));
      // }
      return `(${valueProperties.join(',')})`;
    });
    // console.log(data[data.length - 1]);
    const query = `
      INSERT INTO ${tableName} (${columnNames.join(',')})
      VALUES ${values.join(',')}
      RETURNING *
    `;
    // console.log(query);
    await databaseAdapter.query<void>(query);
  }
}
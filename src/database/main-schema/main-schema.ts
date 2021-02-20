import * as fs from 'fs/promises';
import * as path from 'path';
import { databaseAdapter } from '../database-adapter';

export class MainSchema {
  static async create() {
    const query = await fs.readFile(path.join(__dirname, 'create-main-schema.sql'));
    await databaseAdapter.query<void>(query.toString());
  }
  static async drop() {
    const query = await fs.readFile(path.join(__dirname, 'drop-main-schema.sql'));
    await databaseAdapter.query<void>(query.toString());
  }
}

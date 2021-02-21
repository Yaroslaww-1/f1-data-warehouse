import * as fs from 'fs/promises';
import * as path from 'path';
import { databaseAdapter } from '@src/database/database-adapter';
import { LoadToRdStaging } from '../load-to-rc-staging';

export class RdStaging {
  static async create() {
    const query = await fs.readFile(path.join(__dirname, 'create-rd-staging-schema.sql'));
    await databaseAdapter.query<void>(query.toString());
  }

  static async drop() {
    const query = await fs.readFile(path.join(__dirname, 'drop-rd-staging-schema.sql'));
    await databaseAdapter.query<void>(query.toString());
  }

  static async load() {
    await LoadToRdStaging.loadCircuitDimension();
    await LoadToRdStaging.loadRaceDimension();
  }
}
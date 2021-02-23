import * as fs from 'fs/promises';
import * as path from 'path';
import { databaseAdapter } from '@src/database/database-adapter';
import { LoadFiaRdStaging } from '../load-to-fia-staging';

export class FiaStaging {
  static async create() {
    const query = await fs.readFile(path.join(__dirname, 'create-fia-staging-schema.sql'));
    await databaseAdapter.query<void>(query.toString());
  }

  static async drop() {
    const query = await fs.readFile(path.join(__dirname, 'drop-fia-staging-schema.sql'));
    await databaseAdapter.query<void>(query.toString());
  }

  static async load() {
    try {
      await this.drop();
    } catch (e) {
      console.error(e);
    }

    try {
      await this.create();
    } catch (e) {
      console.error(e);
    }

    await LoadFiaRdStaging.loadTeamDimension();
  }
}
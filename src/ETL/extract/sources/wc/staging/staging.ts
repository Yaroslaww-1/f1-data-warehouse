import * as fs from 'fs/promises';
import * as path from 'path';
import { databaseAdapter } from '@src/database/database-adapter';
import { LoadToWcStaging } from '../load-to-wc-staging';

export class WcStaging {
  static async create() {
    const query = await fs.readFile(path.join(__dirname, 'create-wc-staging-schema.sql'));
    await databaseAdapter.query<void>(query.toString());
  }

  static async drop() {
    const query = await fs.readFile(path.join(__dirname, 'drop-wc-staging-schema.sql'));
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

    await LoadToWcStaging.loadCircuitDimension();
    await LoadToWcStaging.loadDriverDimension();
    await LoadToWcStaging.loadRaceDimension();
    await LoadToWcStaging.loadTeamDimension();
    await LoadToWcStaging.loadStatusDimension();
    await LoadToWcStaging.loadLapsStatsDimension();
    await LoadToWcStaging.loadPitStopsStatsDimension();
    await LoadToWcStaging.loadQualifyingDimension();
    await LoadToWcStaging.loadDriverRaceResultFact();
  }
}
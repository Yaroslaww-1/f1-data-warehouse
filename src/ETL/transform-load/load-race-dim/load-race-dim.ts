import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { RcStagingTable } from '@src/ETL/extract/sources/rd/table-names.enum';
import { DimTable } from '../table-names.enum';
import { isIncrementalLoad } from '../utils/is-incremental-load';

const mapRaceToTable = race => ({
  name: race.name,
});

export class LoadRaceDim {
  static async load() {
    const isIncremental = await isIncrementalLoad(DimTable.RACE);
    const races = await this.getRaces();
    if (!isIncremental) {
      await this.insertNewRaces(races);
    }
  }

  private static async getRaces() {
    const races = await databaseAdapter.query(`SELECT * FROM ${RcStagingTable.RACE}`);
    return races;
  }

  private static async insertNewRaces(races) {
    console.log(races[0]);
    await insertIntoTable(DimTable.RACE, races.map(mapRaceToTable));
  }
}
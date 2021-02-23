import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { getWcSourceKey } from '@src/ETL/extract/sources/wc/load-to-wc-staging';
import { WcStagingTable } from '@src/ETL/extract/sources/wc/table-names.enum';
import { DimTable } from '../table-names.enum';
import { addMetaInformation } from '../utils/add-meta-information-to-table';
import { isIncrementalLoad } from '../utils/is-incremental-load';
import { updateDim } from '../utils/update-dim';

export const getDimLapStatsSourceKey = lapStats => getWcSourceKey(`${lapStats.race_id}-${lapStats.driver_id}`);

const mapLapStatsToTable = lapStats => ({
  laps_count: lapStats.laps_count,
  fastest_lap_time_in_milliseconds: lapStats.fastest_lap_time_in_milliseconds,
  ...addMetaInformation(lapStats),
});

export class LoadLapsStatsDim {
  static async load() {
    const isIncremental = await isIncrementalLoad(DimTable.LAP_STATS);
    const lapsStats = await this.getLapsStats();
    if (!isIncremental) {
      await this.insertNewLapsStats(lapsStats);
    } else {
      await this.updateLapsStats(lapsStats);
    }
  }

  static async getLapsStats() {
    const query = `
      SELECT
        race_id,
        driver_id,
        COUNT(id) AS "laps_count",
        MIN(time_in_milliseconds) AS "fastest_lap_time_in_milliseconds"
      FROM ${WcStagingTable.LAP_STATS} lsd
      GROUP BY lsd.driver_id, lsd.race_id;
    `;
    const lapsStats = await databaseAdapter.query<any>(query);
    return lapsStats.map(lapStats => ({
      ...lapStats,
      source_key: getDimLapStatsSourceKey(lapStats),
    }));
  }

  private static async insertNewLapsStats(lapsStats) {
    await insertIntoTable(DimTable.LAP_STATS, lapsStats.map(mapLapStatsToTable));
  }

  private static async updateLapsStats(lapsStats: any[]) {
    await updateDim({
      dataFromStaging: lapsStats.map(mapLapStatsToTable),
      dimTableName: DimTable.LAP_STATS,
    });
  }
}
import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { WcStagingTable } from '@src/ETL/extract/sources/wc/table-names.enum';
import { DimTable } from '../table-names.enum';
import { addMetaInformation } from '../utils/add-meta-information-to-table';
import { isIncrementalLoad } from '../utils/is-incremental-load';
import { updateDim } from '../utils/update-dim';

const mapPitStopsStatsToTable = (isIncrementalLoad: boolean) => pitStopsStats => ({
  pit_stops_count: pitStopsStats.pit_stops_count,
  summary_pit_stops_time_in_milliseconds: pitStopsStats.summary_pit_stops_time_in_milliseconds,
  ...addMetaInformation(pitStopsStats, isIncrementalLoad),
});

export class LoadPitStopsStatsDim {
  static async load() {
    const isIncremental = await isIncrementalLoad(DimTable.PIT_STOPS_STATS);
    const pitStopsStats = await this.getPitStopsStats();
    if (!isIncremental) {
      await this.insertNewPitStopsStats(pitStopsStats);
    } else {
      await this.updatePitStopsStats(pitStopsStats);
    }
  }

  private static async getPitStopsStats() {
    const query = `
      SELECT
        race_id,
        driver_id,
        source_key,
        COUNT(id) AS "pit_stops_count",
        MIN(duration_in_milliseconds) AS "summary_pit_stops_time_in_milliseconds"
      FROM ${WcStagingTable.PIT_STOPS_STATS} psd
      GROUP BY psd.driver_id, psd.race_id, psd.source_key;
    `;
    const pitStopsStats = await databaseAdapter.query<any>(query);
    return pitStopsStats;
  }

  private static async insertNewPitStopsStats(pitStopsStats) {
    await insertIntoTable(DimTable.PIT_STOPS_STATS, pitStopsStats.map(mapPitStopsStatsToTable(false)));
  }

  private static async updatePitStopsStats(pitStopsStats: any[]) {
    await updateDim({
      dataFromStaging: pitStopsStats,
      dimTableName: DimTable.PIT_STOPS_STATS,
      mapDataItemToTableItemIncremental: mapPitStopsStatsToTable(true),
    });
  }
}
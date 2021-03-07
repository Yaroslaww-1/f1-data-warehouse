import { databaseAdapter } from '@src/database/database-adapter';
import { WcStagingTable } from '@src/ETL/extract/sources/wc/table-names.enum';

const mapPitStopsStatsToTable = pitStopsStats => ({
  race_id: pitStopsStats.race_id,
  driver_id: pitStopsStats.driver_id,
  pit_stops_count: pitStopsStats.pit_stops_count,
  summary_pit_stops_time_in_milliseconds: pitStopsStats.summary_pit_stops_time_in_milliseconds,
});

export class LoadPitStopsStats {
  static async getPitStopsStats() {
    const query = `
      SELECT
        race_id,
        driver_id,
        COUNT(id) AS "pit_stops_count",
        MIN(duration_in_milliseconds) AS "summary_pit_stops_time_in_milliseconds"
      FROM ${WcStagingTable.PIT_STOPS_STATS} psd
      GROUP BY psd.race_id, psd.driver_id;
    `;
    const pitStopsStats = await databaseAdapter.query<any>(query);
    return pitStopsStats.map(mapPitStopsStatsToTable);
  }
}
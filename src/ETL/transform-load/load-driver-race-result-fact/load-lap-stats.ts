import { databaseAdapter } from '@src/database/database-adapter';
import { WcStagingTable } from '@src/ETL/extract/sources/wc/table-names.enum';

export class LoadLapsStats {
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
    return lapsStats;
  }
}
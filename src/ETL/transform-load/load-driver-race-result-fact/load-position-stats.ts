import { databaseAdapter } from '@src/database/database-adapter';
import { WcStagingTable } from '@src/ETL/extract/sources/wc/table-names.enum';

const mapPositionsStatsToTable = positionsStats => ({
  driver_id: positionsStats.driver_id,
  race_id: positionsStats.race_id,
  starting_position: positionsStats.starting_position,
  finishing_position: positionsStats.finishing_position,
});

export class LoadPositionStats {
  static async getPositionsStats() {
    const query = `
      SELECT
        race_id,
        driver_id,
        starting_position,
        finishing_position
      FROM ${WcStagingTable.DRIVER_RACE_RESULT}
    `;
    const positionsStats = await databaseAdapter.query<any>(query);
    return positionsStats.map(mapPositionsStatsToTable);
  }
}
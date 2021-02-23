import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { getWcSourceKey } from '@src/ETL/extract/sources/wc/load-to-wc-staging';
import { WcStagingTable } from '@src/ETL/extract/sources/wc/table-names.enum';
import { DimTable } from '../table-names.enum';
import { addMetaInformation } from '../utils/add-meta-information-to-table';
import { isIncrementalLoad } from '../utils/is-incremental-load';
import { updateDim } from '../utils/update-dim';

export const getDimPositionsStatsSourceKey = positionsStats => getWcSourceKey(`${positionsStats.race_id}-${positionsStats.driver_id}`);

const mapPositionsStatsToTable = positionsStats => ({
  starting_position: positionsStats.starting_position,
  finishing_position: positionsStats.finishing_position,
  ...addMetaInformation(positionsStats),
});

export class LoadPositionStatsDim {
  static async load() {
    const isIncremental = await isIncrementalLoad(DimTable.POSITIONS_STATS);
    const positionsStats = await this.getPositionsStats();
    if (!isIncremental) {
      await this.insertNewPositionsStats(positionsStats);
    } else {
      await this.updatePositionsStats(positionsStats);
    }
  }

  private static async getPositionsStats() {
    const query = `
      SELECT
        race_id,
        driver_id,
        starting_position,
        finishing_position
      FROM ${WcStagingTable.DRIVER_RACE_RESULT}
    `;
    const positionsStats = await databaseAdapter.query<any>(query);
    return positionsStats.map(stats => ({
      ...stats,
      source_key: getDimPositionsStatsSourceKey(stats),
    })).map(mapPositionsStatsToTable);
  }

  private static async insertNewPositionsStats(positionsStats) {
    await insertIntoTable(DimTable.POSITIONS_STATS, positionsStats);
  }

  private static async updatePositionsStats(positionsStats: any[]) {
    await updateDim({
      dataFromStaging: positionsStats,
      dimTableName: DimTable.POSITIONS_STATS,
    });
  }
}
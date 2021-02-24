import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { getWcSourceKey } from '@src/ETL/extract/sources/wc/load-to-wc-staging';
import { WcStagingTable } from '@src/ETL/extract/sources/wc/table-names.enum';
import { DimTable } from '../table-names.enum';
import { addMetaInformation } from '../utils/add-meta-information-to-table';
import { isIncrementalLoad } from '../utils/is-incremental-load';
import { updateDim } from '../utils/update-dim';

export const getDimQualifySourceKey = qualify => getWcSourceKey(`${qualify.race_id}-${qualify.driver_id}`);

const mapQualifyingToTable = qualify => ({
  position: qualify.position,
  ...addMetaInformation(qualify),
});

export class LoadQualifyingDim {
  static async load() {
    const isIncremental = await isIncrementalLoad(DimTable.QUALIFYING);
    const qualifying = await this.getQualifyingStats();
    if (!isIncremental) {
      await this.insertNewQualifyingStats(qualifying);
    } else {
      await this.updateQualifyingStats(qualifying);
    }
  }

  private static async getQualifyingStats() {
    const query = `
      SELECT
        race_id,
        driver_id,
        position
      FROM ${WcStagingTable.QUALIFYING}
    `;
    const qualifying = await databaseAdapter.query<any>(query);
    return qualifying.map(qualify => ({
      ...qualify,
      source_key: getDimQualifySourceKey(qualify),
    })).map(mapQualifyingToTable);
  }

  private static async insertNewQualifyingStats(qualifying) {
    await insertIntoTable(DimTable.QUALIFYING, qualifying);
  }

  private static async updateQualifyingStats(qualifying: any[]) {
    await updateDim({
      dataFromStaging: qualifying,
      dimTableName: DimTable.QUALIFYING,
    });
  }
}
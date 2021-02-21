import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { WcStagingTable } from '@src/ETL/extract/sources/wc/table-names.enum';
import { DimTable } from '../table-names.enum';
import { addMetaInformation } from '../utils/add-meta-information-to-table';
import { isIncrementalLoad } from '../utils/is-incremental-load';
import { updateDim } from '../utils/update-dim';

const mapStatusToTable = (isIncrementalLoad: boolean) => status => ({
  name: status.name,
  ...addMetaInformation(status, isIncrementalLoad),
});

export class LoadStatusDim {
  static async load() {
    const isIncremental = await isIncrementalLoad(DimTable.STATUS);
    const statuses = await this.getStatuses();
    if (!isIncremental) {
      await this.insertNewStatuses(statuses);
    } else {
      await this.updateStatuses(statuses);
    }
  }

  private static async getStatuses() {
    const statuses = await databaseAdapter.query(`SELECT * FROM ${WcStagingTable.STATUS}`);
    return statuses;
  }

  private static async insertNewStatuses(statuses) {
    await insertIntoTable(DimTable.STATUS, statuses.map(mapStatusToTable(false)));
  }

  private static async updateStatuses(statuses: any[]) {
    await updateDim({
      dataFromStaging: statuses,
      dimTableName: DimTable.STATUS,
      mapDataItemToTableItemIncremental: mapStatusToTable(true),
    });
  }
}
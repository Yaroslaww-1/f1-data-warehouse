import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { WcStagingTable } from '@src/ETL/extract/sources/wc/table-names.enum';
import { DimTable } from '../table-names.enum';
import { addMetaInformation } from '../utils/add-meta-information-to-table';
import { isIncrementalLoad } from '../utils/is-incremental-load';
import { updateDim } from '../utils/update-dim';

const mapDriverToTable = driver => ({
  ref: driver.ref,
  code: driver.code,
  ...addMetaInformation(driver),
});

export class LoadDriverDim {
  static async load() {
    const isIncremental = await isIncrementalLoad(DimTable.DRIVER);
    const drivers = await this.getDrivers();
    if (!isIncremental) {
      await this.insertNewDrivers(drivers);
    } else {
      await this.updateDrivers(drivers);
    }
  }

  private static async getDrivers() {
    const drivers = await databaseAdapter.query(`SELECT * FROM ${WcStagingTable.DRIVER}`);
    return drivers.map(mapDriverToTable);
  }

  private static async insertNewDrivers(drivers) {
    await insertIntoTable(DimTable.DRIVER, drivers);
  }

  private static async updateDrivers(drivers: any[]) {
    await updateDim({
      dataFromStaging: drivers,
      dimTableName: DimTable.DRIVER,
    });
  }
}
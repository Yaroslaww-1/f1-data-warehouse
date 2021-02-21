import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { RcStagingTable } from '@src/ETL/extract/sources/rd/table-names.enum';
import { DEFAULT_VALID_FROM, DEFAULT_VALID_TO } from '../constants';
import { DimTable } from '../table-names.enum';
import { isIncrementalLoad } from '../utils/is-incremental-load';
import { updateDim } from '../utils/update-dim';

const mapValidRage = item => ({
  valid_from: item.valid_from || DEFAULT_VALID_FROM,
  valid_to: item.valid_to || DEFAULT_VALID_TO,
});

const mapCircuitToTable = (isIncrementalLoad: boolean) => circuit => ({
  name: circuit.name,
  ref: circuit.ref,
  is_incremental_load: isIncrementalLoad,
  source_key: circuit.source_key,
  ...mapValidRage(circuit),
});

export class LoadCircuitDim {
  static async load() {
    const isIncremental = await isIncrementalLoad(DimTable.CIRCUIT);
    const circuits = await this.getCircuits();
    if (!isIncremental) {
      await this.insertNewCircuits(circuits);
    } else {
      await this.updateCircuits(circuits);
    }
  }

  private static async getCircuits() {
    const circuits = await databaseAdapter.query(`SELECT * FROM ${RcStagingTable.CIRCUIT}`);
    return circuits;
  }

  private static async insertNewCircuits(circuits) {
    await insertIntoTable(DimTable.CIRCUIT, circuits.map(mapCircuitToTable(false)));
  }

  private static async updateCircuits(circuits: any[]) {
    await updateDim({
      dataFromStaging: circuits,
      dimTableName: DimTable.CIRCUIT,
      mapDataItemToTableItemIncremental: mapCircuitToTable(true),
    });
  }
}
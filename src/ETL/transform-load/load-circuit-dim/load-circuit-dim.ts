import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { RdStagingTable } from '@src/ETL/extract/sources/rd/table-names.enum';
import { DEFAULT_VALID_FROM, DEFAULT_VALID_TO } from '../constants';
import { DimTable } from '../table-names.enum';
import { isIncrementalLoad } from '../utils/is-incremental-load';
import { updateDim } from '../utils/update-dim';

const mapValidRage = item => ({
  valid_from: item.valid_from || DEFAULT_VALID_FROM,
  valid_to: item.valid_to || DEFAULT_VALID_TO,
});

const mapCircuitToTable = circuit => ({
  name: circuit.name,
  ref: circuit.ref,
  country_id: circuit.country_id,
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
    const countries = await databaseAdapter.query<any>(`SELECT * FROM ${DimTable.COUNTRY}`);
    const circuits = await databaseAdapter.query<any>(`SELECT * FROM ${RdStagingTable.CIRCUIT}`);
    return circuits.map(circuit => ({
      ...circuit,
      country_id: countries.find(country => country.name === circuit.country)?.id,
    })).map(mapCircuitToTable);
  }

  private static async insertNewCircuits(circuits) {
    await insertIntoTable(DimTable.CIRCUIT, circuits);
  }

  private static async updateCircuits(circuits: any[]) {
    await updateDim({
      dataFromStaging: circuits.map(mapCircuitToTable),
      dimTableName: DimTable.CIRCUIT,
    });
  }
}
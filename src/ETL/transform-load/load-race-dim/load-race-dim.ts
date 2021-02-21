import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { getRdSourceKey } from '@src/ETL/extract/sources/rd/load-to-rd-staging';
import { RdStagingTable } from '@src/ETL/extract/sources/rd/table-names.enum';
import { DimTable } from '../table-names.enum';
import { addMetaInformation } from '../utils/add-meta-information-to-table';
import { getDateId } from '../utils/get-date-id';
import { isIncrementalLoad } from '../utils/is-incremental-load';
import { updateDim } from '../utils/update-dim';

const mapRaceToTable = (isIncrementalLoad: boolean) => race => ({
  name: race.name,
  date_id: race.date_id,
  circuit_id: race.circuit_id,
  ...addMetaInformation(race, isIncrementalLoad),
});

export class LoadRaceDim {
  static async load() {
    const isIncremental = await isIncrementalLoad(DimTable.RACE);
    const races = await this.getRaces();
    if (!isIncremental) {
      await this.insertNewRaces(races);
    } else {
      await this.updateRaces(races);
    }
  }

  private static async getRaces() {
    const races = await databaseAdapter.query(`SELECT * FROM ${RdStagingTable.RACE}`);
    const circuitsFromDW = await databaseAdapter.query<any>(`SELECT * FROM ${DimTable.CIRCUIT}`);
    const datesFromDW = await databaseAdapter.query<any>(`SELECT * FROM ${DimTable.DATE}`);

    const getCircuitIdFromDW = (circuitIdFromStaging: number) => {
      const circuit = circuitsFromDW.find(c => c.source_key === getRdSourceKey(circuitIdFromStaging));
      return circuit.id;
    }; 

    return races.map((race: any) => ({
      ...race,
      circuit_id: getCircuitIdFromDW(race.circuit_id),
      date_id: getDateId(datesFromDW, race.date), 
    }));
  }

  private static async insertNewRaces(races) {
    await insertIntoTable(DimTable.RACE, races.map(mapRaceToTable(false)));
  }

  private static async updateRaces(races: any[]) {
    await updateDim({
      dataFromStaging: races,
      dimTableName: DimTable.RACE,
      mapDataItemToTableItemIncremental: mapRaceToTable(true),
    });
  }
}
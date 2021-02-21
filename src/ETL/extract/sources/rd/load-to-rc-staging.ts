import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { extract } from './extract';
import { RcStagingTable } from './table-names.enum';

const valueOrDefault = (value, def = null) => {
  if (value === '\\N') {
    return def;
  } else {
    return value;
  }
};

const getSourceKey = itemId => `RD|${itemId}`;

const mapCircuitToTable = circuit => ({
  ref: valueOrDefault(circuit.circuitRef),
  name: circuit.name,
  source_key: getSourceKey(circuit.circuitId),
});

const mapRaceToTable = race => ({
  name: race.name,
  date: race.date,
  circuit_id: race.circuitId,
  source_key: getSourceKey(race.raceId),
});

export class LoadToRdStaging {
  static async loadCircuitDimension() {
    const circuits = await extract('circuits');
    await insertIntoTable(RcStagingTable.CIRCUIT, circuits.map(mapCircuitToTable));
  }

  static async loadRaceDimension() {
    const races = await extract('races');
    await insertIntoTable(RcStagingTable.RACE, races.map(mapRaceToTable));
  }
}
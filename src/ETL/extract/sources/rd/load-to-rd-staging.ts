import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { extract } from './extract';
import { RdStagingTable } from './table-names.enum';

const valueOrDefault = (value, def = null) => {
  if (value === '\\N') {
    return def;
  } else {
    return value;
  }
};

export const getRdSourceKey = itemId => `RD|${itemId}`;

const mapCircuitToTable = circuit => ({
  ref: valueOrDefault(circuit.circuitRef),
  name: circuit.name,
  country: circuit.country,
  source_key: getRdSourceKey(circuit.circuitId),
});

const mapRaceToTable = race => ({
  name: race.name,
  date: race.date,
  circuit_id: race.circuitId,
  source_key: getRdSourceKey(race.raceId),
});

export class LoadToRdStaging {
  static async loadCircuitDimension() {
    const circuits = await extract('circuits');
    await insertIntoTable(RdStagingTable.CIRCUIT, circuits.map(mapCircuitToTable));
  }

  static async loadRaceDimension() {
    const races = await extract('races');
    await insertIntoTable(RdStagingTable.RACE, races.map(mapRaceToTable));
  }
}
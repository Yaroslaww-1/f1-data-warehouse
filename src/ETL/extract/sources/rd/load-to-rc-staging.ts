import { extract } from './extract';
import { RdStaging } from './staging/staging';

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
  static async load() {
    await this.loadCircuitDimension();
    await this.loadRaceDimension();
  }

  private static async loadCircuitDimension() {
    const circuits = await extract('circuits');
    await RdStaging.insertInto('stg_rd.circuit_dim', circuits.map(mapCircuitToTable));
  }

  private static async loadRaceDimension() {
    const races = await extract('races');
    await RdStaging.insertInto('stg_rd.race_dim', races.map(mapRaceToTable));
  }
}
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { extract } from './extract';
import { WcStagingTable } from './table-names.enum';

const valueOrDefault = (value, def = null) => {
  if (value === '\\N') {
    return def;
  } else {
    return value;
  }
};

export const getWcSourceKey = itemId => `WC|${itemId}`;
export const getWcLapStatsSourceKey = lapStats => getWcSourceKey(`${lapStats.raceId}-${lapStats.driverId}-${lapStats.lap}`);

const mapTeamToTable = team => ({
  ref: valueOrDefault(team.constructorRef),
  name: team.name,
  source_key: getWcSourceKey(team.constructorId),
});

const mapStatusToTable = status => ({
  name: status.status,
  source_key: getWcSourceKey(status.statusId),
});

const mapCircuitToTable = circuit => ({
  ref: valueOrDefault(circuit.circuitRef),
  name: circuit.name,
  source_key: getWcSourceKey(circuit.circuitId),
});

const mapDriverToTable = driver => ({
  ref: valueOrDefault(driver.driverRef),
  code: valueOrDefault(driver.code),
  source_key: getWcSourceKey(driver.driverId),
});

const mapLapsStatsToTable = lapStats => ({
  driver_id: lapStats.driverId,
  race_id: lapStats.raceId,
  lap: lapStats.lap,
  time_in_milliseconds: lapStats.milliseconds,
  source_key: getWcLapStatsSourceKey(lapStats),
});

const mapPitStopsStatsToTable = pitStopStats => ({
  driver_id: pitStopStats.driverId,
  race_id: pitStopStats.raceId,
  duration_in_milliseconds: pitStopStats.milliseconds,
  source_key: getWcSourceKey(`${pitStopStats.raceId}-${pitStopStats.driverId}-${pitStopStats.stop}`),
});

const mapRaceToTable = race => ({
  name: race.name,
  date: race.date,
  circuit_id: race.circuitId,
  source_key: getWcSourceKey(race.raceId),
});

const mapDriverRaceResultToTable = result => ({
  driver_id: result.driverId,
  race_id: result.raceId,
  team_id: result.constructorId,
  starting_position: valueOrDefault(result.grid),
  finishing_position: valueOrDefault(result.position),
  points: valueOrDefault(result.points),
  status_id: result.statusId,
  source_key: getWcSourceKey(result.resultId),
});

export class LoadToWcStaging {
  static async loadCircuitDimension() {
    const circuits = await extract('circuits');
    await insertIntoTable('stg_wc.circuit_dim', circuits.map(mapCircuitToTable));
  }

  static async loadDriverDimension() {
    const drivers = await extract('drivers');
    await insertIntoTable(WcStagingTable.DRIVER, drivers.map(mapDriverToTable));
  }

  static async loadLapsStatsDimension() {
    const lapsStats = await extract('lap_times');
    await insertIntoTable(WcStagingTable.LAP_STATS, lapsStats.map(mapLapsStatsToTable).filter(ls => ls.race_id < 1036));
  }

  static async loadDriverRaceResultFact() {
    const results = await extract('results');
    await insertIntoTable('stg_wc.driver_race_result', results.map(mapDriverRaceResultToTable).filter(q => q.race_id < 1036));
  }

  static async loadPitStopsStatsDimension() {
    const pitStopsStats = await extract('pit_stops');
    await insertIntoTable('stg_wc.pit_stops_stats_dim', pitStopsStats.map(mapPitStopsStatsToTable).filter(ps => ps.race_id < 1036));
  }

  static async loadRaceDimension() {
    const races = await extract('races');
    await insertIntoTable('stg_wc.race_dim', races.map(mapRaceToTable));
  }

  static async loadStatusDimension() {
    const statuses = await extract('status');
    await insertIntoTable(WcStagingTable.STATUS, statuses.map(mapStatusToTable));
  }

  static async loadTeamDimension() {
    const teams = await extract('constructors') as any[];
    await insertIntoTable(WcStagingTable.TEAM, teams.map(mapTeamToTable));
  }
}
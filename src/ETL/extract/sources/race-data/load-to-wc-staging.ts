import { extract } from './extract';
import { WcStaging } from './staging/staging';

const valueOrDefault = (value, def = null) => {
  if (value === '\\N') {
    return def;
  } else {
    return value;
  }
};

const mapTeamToTable = team => ({
  // id: team.constructorId,
  ref: valueOrDefault(team.constructorRef),
  name: team.name,
});

const mapStatusToTable = status => ({
  // id: status.statusId,
  name: status.status,
});

const mapCircuitToTable = circuit => ({
  // id: circuit.circuitId,
  ref: valueOrDefault(circuit.circuitRef),
  name: circuit.name,
});

const mapDriverToTable = driver => ({
  // id: driver.driverId,
  ref: valueOrDefault(driver.driverRef),
  code: driver.code,
});

const mapLapsStatsToTable = lapStats => ({
  // id: lapStats.driverId,
  driver_id: lapStats.driverId,
  race_id: lapStats.raceId,
  lap: lapStats.lap,
  time_in_milliseconds: lapStats.milliseconds,
});

const mapPitStopsStatsToTable = pitStopStats => ({
  driver_id: pitStopStats.driverId,
  race_id: pitStopStats.raceId,
  duration_in_milliseconds: pitStopStats.milliseconds,
});

const mapQualifyingToTable = qualifying => ({
  driver_id: qualifying.driverId,
  race_id: qualifying.raceId,
  team_id: qualifying.constructorId,
  position: qualifying.position,
});

const mapRaceToTable = race => ({
  name: race.name,
  date: race.date,
  circuit_id: race.circuitId,
});

const mapDriverRaceResultToTable = result => ({
    driver_id: result.driverId,
    race_id: result.raceId,
    team_id: result.constructorId,
    starting_position: valueOrDefault(result.grid),
    finishing_position: valueOrDefault(result.position),
    points: valueOrDefault(result.points),
    status_id: result.statusId,
});

// const isValid = value => {
//   if (Object.values(value).some(isNotExists)) return false;
//   else return true;
// }

export class LoadToWcStaging {
  static async load() {
    await this.loadCircuitDimension();
    await this.loadDriverDimension();
    await this.loadRaceDimension();
    await this.loadTeamDimension();
    await this.loadStatusDimension();
    await this.loadLapsStatsDimension();
    await this.loadPitStopsStatsDimension();
    await this.loadQualifyingDimension();
    await this.loadDriverRaceResultFact();
  }

  private static async loadCircuitDimension() {
    const circuits = await extract('circuits');
    await WcStaging.insertInto('stg_wc.circuit_dim', circuits.map(mapCircuitToTable));
  }

  private static async loadDriverDimension() {
    const drivers = await extract('drivers');
    await WcStaging.insertInto('stg_wc.driver_dim', drivers.map(mapDriverToTable));
  }

  private static async loadLapsStatsDimension() {
    const lapsStats = await extract('lap_times');
    await WcStaging.insertInto('stg_wc.laps_stats_dim', lapsStats.map(mapLapsStatsToTable).filter(ls => ls.race_id < 1036));
  }

  private static async loadDriverRaceResultFact() {
    const results = await extract('results');
    await WcStaging.insertInto('stg_wc.driver_race_result', results.map(mapDriverRaceResultToTable).filter(q => q.race_id < 1036));
  }

  private static async loadPitStopsStatsDimension() {
    const pitStopsStats = await extract('pit_stops');
    await WcStaging.insertInto('stg_wc.pit_stops_stats_dim', pitStopsStats.map(mapPitStopsStatsToTable).filter(ps => ps.race_id < 1036));
  }

  private static async loadQualifyingDimension() {
    const qualifying = await extract('qualifying');
    await WcStaging.insertInto('stg_wc.qualifying_dim', qualifying.map(mapQualifyingToTable).filter(q => q.race_id < 1036));
  }

  private static async loadRaceDimension() {
    const races = await extract('races');
    await WcStaging.insertInto('stg_wc.race_dim', races.map(mapRaceToTable));
  }

  private static async loadStatusDimension() {
    const statuses = await extract('status');
    await WcStaging.insertInto('stg_wc.status_dim', statuses.map(mapStatusToTable));
  }

  private static async loadTeamDimension() {
    const teams = await extract('constructors') as any[];
    await WcStaging.insertInto('stg_wc.team_dim', teams.map(mapTeamToTable));
  }
}
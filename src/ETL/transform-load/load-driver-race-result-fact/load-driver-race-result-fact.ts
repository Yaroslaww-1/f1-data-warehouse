import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { RdStagingTable } from '@src/ETL/extract/sources/rd/table-names.enum';
import { WcStagingTable } from '@src/ETL/extract/sources/wc/table-names.enum';
import { LoadLapsStats } from './load-lap-stats';
import { LoadPitStopsStats } from './load-pit-stops-stats';
import { LoadPointsStats } from './load-points-stats';
import { LoadPositionStats } from './load-position-stats';
import { getDimQualifySourceKey } from '../qualifying-dim/qualifying-dim';
import { DimTable, FaceTable } from '../table-names.enum';
import { getCurrentTimestamp } from '../utils/date';

const mapDriverRaceResultToTable = driverRaceResults => ({
  driver_id: driverRaceResults.driver_id,
  team_id: driverRaceResults.team_id,
  race_id: driverRaceResults.race_id,
  status_id: driverRaceResults.status_id,
  qualifying_id: driverRaceResults.qualifying_id,
  laps_count: driverRaceResults.laps_count,
  fastest_lap_time_in_milliseconds: driverRaceResults.fastest_lap_time_in_milliseconds,
  starting_position: driverRaceResults.starting_position,
  finishing_position: driverRaceResults.finishing_position,
  points: driverRaceResults.points,
  is_fastest_lap: driverRaceResults.is_fastest_lap,
  pit_stops_count: driverRaceResults.pit_stops_count,
  summary_pit_stops_time_in_milliseconds: driverRaceResults.summary_pit_stops_time_in_milliseconds,
});

interface IJoinable {
  id: number;
  source_key: string;
}

export class LoadDriverRaceResult {
  static async load() {
    const driverRaceResults = await this.getDriverRaceResults();
    await this.insertNewDriverRaceResults(driverRaceResults);
  }

  private static async getDriverRaceResults() {
    const currentTimestamp = getCurrentTimestamp();
    const lapsStats = await LoadLapsStats.getLapsStats();
    const pitStopsStats = await LoadPitStopsStats.getPitStopsStats();
    const pointsStats = await LoadPointsStats.getPointsStats();
    const positionsStats = await LoadPositionStats.getPositionsStats();
    const qualifying = await databaseAdapter.query<IJoinable>(`
      SELECT id, source_key FROM ${DimTable.QUALIFYING}
      WHERE valid_to > '${currentTimestamp}'
    `);

    const mapLapsStats = new Map<
    string, 
    {
      race_id: any;
      driver_id: any;
      laps_count: any;
      fastest_lap_time_in_milliseconds: any;
    }>();
    for (const lapStats of lapsStats) {
      const key = JSON.stringify({ raceId: lapStats.race_id, driverId: lapStats.driver_id });
      mapLapsStats.set(key, lapStats);
    }

    const mapPitStops = new Map<
    string,
    {
      race_id: any;
      driver_id: any;
      pit_stops_count: any;
      summary_pit_stops_time_in_milliseconds: any;
    }>();
    for (const pitStopStats of pitStopsStats) {
      const key = JSON.stringify({ raceId: pitStopStats.race_id, driverId: pitStopStats.driver_id });
      mapPitStops.set(key, pitStopStats);
    }

    const mapPointsStats = new Map<
    string,
    {
      race_id: any;
      driver_id: any;
      points: any;
      is_fastest_lap: any;
    }>();
    for (const pointStats of pointsStats) {
      const key = JSON.stringify({ raceId: pointStats.race_id, driverId: pointStats.driver_id });
      mapPointsStats.set(key, pointStats);
    }

    const mapPositionsStats = new Map<
    string,
    {
      driver_id: any;
      race_id: any;
      starting_position: any;
      finishing_position: any;
    }>();
    for (const positionStats of positionsStats) {
      const key = JSON.stringify({ raceId: positionStats.race_id, driverId: positionStats.driver_id });
      mapPositionsStats.set(key, positionStats);
    }

    const mapQualifyingSourceKeyToId = new Map<string, number>();
    for (const qualify of qualifying) {
      mapQualifyingSourceKeyToId.set(qualify.source_key, qualify.id);
    }

    const query = `
      SELECT 
        wc_stg_drr.*,
        driver.id AS driver_id,
        team.id AS team_id,
        status.id AS status_id,
        race.id AS race_id,
        wc_stg_driver.id AS wc_stg_driver_id,
        wc_stg_race.id AS wc_stg_race_id
      FROM ${WcStagingTable.DRIVER_RACE_RESULT} wc_stg_drr

      LEFT JOIN ${WcStagingTable.DRIVER} wc_stg_driver
      ON wc_stg_driver.id = wc_stg_drr.driver_id
      LEFT JOIN ${DimTable.DRIVER} driver
      ON
        driver.source_key = wc_stg_driver.source_key AND
        driver.valid_to > '${currentTimestamp}'

      LEFT JOIN ${WcStagingTable.STATUS} wc_stg_status
      ON wc_stg_status.id = wc_stg_drr.status_id
      LEFT JOIN ${DimTable.STATUS} status
      ON
        status.source_key = wc_stg_status.source_key AND
        status.valid_to > '${currentTimestamp}'

      LEFT JOIN ${WcStagingTable.RACE} wc_stg_race
      ON wc_stg_race.id = wc_stg_drr.race_id
      LEFT JOIN ${RdStagingTable.RACE} rd_stg_race
      ON
        rd_stg_race.name = wc_stg_race.name AND
        rd_stg_race.date = wc_stg_race.date
      LEFT JOIN ${DimTable.RACE} race
      ON
        race.source_key = rd_stg_race.source_key AND
        race.valid_to > '${currentTimestamp}'

      LEFT JOIN ${WcStagingTable.TEAM} wc_stg_team
      ON wc_stg_team.id = wc_stg_drr.team_id
      LEFT JOIN ${DimTable.TEAM} team
      ON
        get_team_name(team.name) = LOWER(wc_stg_team.name) AND
        team.valid_to > '${currentTimestamp}'

      WHERE race.id IS NOT NULL
    `;
    
    const driverRaceResults = await databaseAdapter.query<any>(query);

    return driverRaceResults.map(({
      driver_id,
      team_id,
      race_id,
      status_id,
      wc_stg_driver_id,
      wc_stg_race_id,
    }) => {
      const keyToJoinBy = JSON.stringify({ raceId: wc_stg_race_id, driverId: wc_stg_driver_id });
      return {
        driver_id,
        team_id,
        race_id,
        status_id,
        qualifying_id: mapQualifyingSourceKeyToId.get(
          getDimQualifySourceKey({ race_id: wc_stg_race_id, driver_id: wc_stg_driver_id })
        ),
        laps_count: mapLapsStats.get(keyToJoinBy)?.laps_count,
        fastest_lap_time_in_milliseconds:
          mapLapsStats.get(keyToJoinBy)?.fastest_lap_time_in_milliseconds,
        starting_position:
          mapPositionsStats.get(keyToJoinBy)?.starting_position,
        finishing_position:
          mapPositionsStats.get(keyToJoinBy)?.finishing_position,
        points: mapPointsStats.get(keyToJoinBy)?.points,
        is_fastest_lap: mapPointsStats.get(keyToJoinBy)?.is_fastest_lap,
        pit_stops_count: mapPitStops.get(keyToJoinBy)?.pit_stops_count,
        summary_pit_stops_time_in_milliseconds:
          mapPitStops.get(keyToJoinBy)?.summary_pit_stops_time_in_milliseconds,
      };
    });
  }

  private static async insertNewDriverRaceResults(driverRaceResults) {
    await insertIntoTable(FaceTable.DRIVER_RACE_RESULT, driverRaceResults.map(mapDriverRaceResultToTable));
  }
}
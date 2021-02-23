import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { FiaStagingTable } from '@src/ETL/extract/sources/fia/table-names.enum';
import { RdStagingTable } from '@src/ETL/extract/sources/rd/table-names.enum';
import { WcStagingTable } from '@src/ETL/extract/sources/wc/table-names.enum';
import { getDimLapStatsSourceKey } from '../load-lap-stats-dim/load-lap-stats-dim';
import { getDimPitStopsStatsSourceKey } from '../load-pit-stops-stats-dim/load-pit-stops-stats-dim';
import { getDimPointsStatsSourceKey } from '../load-points-stats-dim/load-points-stats-dim';
import { getDimPositionsStatsSourceKey } from '../load-position-stats-dim/load-position-stats-dim';
import { DimTable, FaceTable } from '../table-names.enum';
import { getCurrentTimestamp } from '../utils/date';

const mapDriverRaceResultToTable = driverRaceResults => ({
  driver_id: driverRaceResults.driver_id,
  team_id: driverRaceResults.team_id,
  race_id: driverRaceResults.race_id,
  status_id: driverRaceResults.status_id,
  qualifying_id: 1,
  laps_stats_id: driverRaceResults.laps_stats_id,
  position_stats_id: driverRaceResults.position_stats_id,
  points_stats_id: driverRaceResults.points_stats_id,
  pit_stops_stats_id: driverRaceResults.pit_stops_stats_id,
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
    const lapsStats = await databaseAdapter.query<IJoinable>(`
      SELECT id, source_key FROM ${DimTable.LAP_STATS}
      WHERE valid_to > '${currentTimestamp}'
    `);
    const pitStopsStats = await databaseAdapter.query<IJoinable>(`
      SELECT id, source_key FROM ${DimTable.PIT_STOPS_STATS}
      WHERE valid_to > '${currentTimestamp}'
    `);
    const pointsStats = await databaseAdapter.query<IJoinable>(`
      SELECT id, source_key FROM ${DimTable.POINTS_STATS}
      WHERE valid_to > '${currentTimestamp}'
    `);
    const positionsStats = await databaseAdapter.query<IJoinable>(`
      SELECT id, source_key FROM ${DimTable.POSITIONS_STATS}
      WHERE valid_to > '${currentTimestamp}'
    `);

    const mapLapsStatsSourceKeyToId = new Map<string, number>();
    for (const lapStats of lapsStats) {
      mapLapsStatsSourceKeyToId.set(lapStats.source_key, lapStats.id);
    }

    const mapPitStopsSourceKeyToId = new Map<string, number>();
    for (const pitStopStats of pitStopsStats) {
      mapPitStopsSourceKeyToId.set(pitStopStats.source_key, pitStopStats.id);
    }

    const mapPointsStatsSourceKeyToId = new Map<string, number>();
    for (const pointStats of pointsStats) {
      mapPointsStatsSourceKeyToId.set(pointStats.source_key, pointStats.id);
    }

    const mapPositionsSourceKeyToId = new Map<string, number>();
    for (const positionStats of positionsStats) {
      mapPositionsSourceKeyToId.set(positionStats.source_key, positionStats.id);
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

    const a = driverRaceResults.map(({
      driver_id,
      team_id,
      race_id,
      status_id,
      wc_stg_driver_id,
      wc_stg_race_id,
    }) => ({
      driver_id,
      team_id,
      race_id,
      status_id,
      qualifying_id: 0,
      laps_stats_id: mapLapsStatsSourceKeyToId.get(
        getDimLapStatsSourceKey({ race_id: wc_stg_race_id, driver_id: wc_stg_driver_id })
      ),
      pit_stops_stats_id: mapPitStopsSourceKeyToId.get(
        getDimPitStopsStatsSourceKey({ race_id: wc_stg_race_id, driver_id: wc_stg_driver_id })
      ),
      points_stats_id: mapPointsStatsSourceKeyToId.get(
        getDimPointsStatsSourceKey({ race_id: wc_stg_race_id, driver_id: wc_stg_driver_id })
      ),
      position_stats_id: mapPositionsSourceKeyToId.get(
        getDimPositionsStatsSourceKey({ race_id: wc_stg_race_id, driver_id: wc_stg_driver_id })
      ),
    }));

    return a;
  }

  private static async insertNewDriverRaceResults(driverRaceResults) {
    console.log(driverRaceResults.map(mapDriverRaceResultToTable)[0]);
    await insertIntoTable(FaceTable.DRIVER_RACE_RESULT, driverRaceResults.map(mapDriverRaceResultToTable));
  }
}
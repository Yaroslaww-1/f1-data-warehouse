import { databaseAdapter } from '@src/database/database-adapter';
import { WcStagingTable } from '@src/ETL/extract/sources/wc/table-names.enum';
import { LoadLapsStats } from './load-lap-stats';

const mapPointsStatsToTable = pointsStats => ({
  race_id: pointsStats.race_id,
  driver_id: pointsStats.driver_id,
  points: pointsStats.points,
  is_fastest_lap: pointsStats.is_fastest_lap,
});

export class LoadPointsStats {
  static async getPointsStats() {
    const lapsStatsForAllRaces = await LoadLapsStats.getLapsStats();
    lapsStatsForAllRaces.sort((s1, s2) => s1.race_id - s2.race_id);
    
    const fastestDriverByRaceMap = new Map<number, { driverId: number, timeInMilliseconds: number }>();

    for (const lapsStatsRaceDriver of lapsStatsForAllRaces) {
      const {
        race_id: raceId,
        driver_id: driverId,
        fastest_lap_time_in_milliseconds: timeInMilliseconds,
      } = lapsStatsRaceDriver;

      const isNewRace = !fastestDriverByRaceMap.has(raceId);
      const isNewDriverFaster = 
        fastestDriverByRaceMap.has(raceId) &&
        fastestDriverByRaceMap.get(raceId).timeInMilliseconds > timeInMilliseconds;

      if (isNewRace || isNewDriverFaster) {
        fastestDriverByRaceMap.set(raceId, { driverId, timeInMilliseconds });
      }
    }

    const query = `
      SELECT
        drr.driver_id,
        drr.race_id,
        drr.points
      FROM ${WcStagingTable.DRIVER_RACE_RESULT} drr
    `;
    const driverRaceResults = await databaseAdapter.query<any>(query);
    return driverRaceResults.map(result => {
      const isFastestLap =
        fastestDriverByRaceMap.has(result.race_id) &&
        fastestDriverByRaceMap.get(result.race_id).driverId === result.driver_id;
      return {
        race_id: result.race_id,
        driver_id: result.driver_id,
        points: Math.round(result.points),
        is_fastest_lap: isFastestLap,
      };
    }).map(mapPointsStatsToTable);
  }
}
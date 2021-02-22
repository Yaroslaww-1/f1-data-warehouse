import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { getWcSourceKey } from '@src/ETL/extract/sources/wc/load-to-wc-staging';
import { WcStagingTable } from '@src/ETL/extract/sources/wc/table-names.enum';
import { LoadLapsStatsDim } from '../load-lap-stats-dim/load-lap-stats-dim';
import { DimTable } from '../table-names.enum';
import { addMetaInformation } from '../utils/add-meta-information-to-table';
import { isIncrementalLoad } from '../utils/is-incremental-load';
import { updateDim } from '../utils/update-dim';

const mapPointsStatsToTable = (isIncrementalLoad: boolean) => pointsStats => ({
  points: pointsStats.points,
  is_fastest_lap: pointsStats.is_fastest_lap,
  ...addMetaInformation(pointsStats, isIncrementalLoad),
});

export class LoadPointsStatsDim {
  static async load() {
    const isIncremental = await isIncrementalLoad(DimTable.POINTS_STATS);
    const pointsStats = await this.getPointsStats();
    if (!isIncremental) {
      await this.insertNewPointsStats(pointsStats);
    } else {
      await this.updatePointsStats(pointsStats);
    }
  }

  private static async getPointsStats() {
    const lapsStatsForAllRaces = await LoadLapsStatsDim.getLapsStats();
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
        points: Math.round(result.points),
        is_fastest_lap: isFastestLap,
        source_key: getWcSourceKey(`${result.race_id}-${result.driver_id}`),
      };
    });
  }

  private static async insertNewPointsStats(pointsStats) {
    await insertIntoTable(DimTable.POINTS_STATS, pointsStats.map(mapPointsStatsToTable(false)));
  }

  private static async updatePointsStats(pointsStats: any[]) {
    await updateDim({
      dataFromStaging: pointsStats,
      dimTableName: DimTable.POINTS_STATS,
      mapDataItemToTableItemIncremental: mapPointsStatsToTable(true),
    });
  }
}
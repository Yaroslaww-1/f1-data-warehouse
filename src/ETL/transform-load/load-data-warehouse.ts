import { truncateTable } from '@src/database/utils/truncate-table';
import { FiaStagingTable } from '../extract/sources/fia/table-names.enum';
import { RdStagingTable } from '../extract/sources/rd/table-names.enum';
import { WcStaging } from '../extract/sources/wc/staging/staging';
import { WcStagingTable } from '../extract/sources/wc/table-names.enum';
import { LoadCircuitDim } from './load-circuit-dim/load-circuit-dim';
import { LoadDateDim } from './load-date-dim/load-date-dim';
import { LoadDriverDim } from './load-driver-dim/load-driver-dim';
import { LoadDriverRaceResult } from './load-driver-race-result-fact/load-driver-race-result-fact';
import { LoadLapsStatsDim } from './load-lap-stats-dim/load-lap-stats-dim';
import { LoadPitStopsStatsDim } from './load-pit-stops-stats-dim/load-pit-stops-stats-dim';
import { LoadPointsStatsDim } from './load-points-stats-dim/load-points-stats-dim';
import { LoadPositionStatsDim } from './load-position-stats-dim/load-position-stats-dim';
import { LoadRaceDim } from './load-race-dim/load-race-dim';
import { LoadStatusDim } from './load-status-dim/load-status-dim';
import { LoadTeamDim } from './load-team-dim/load-team-dim';
import { LoadQualifyingDim } from './qualifying-dim/qualifying-dim';

export class LoadDataWarehouse {
  static async load() {
    await LoadCircuitDim.load();
    console.log('Circuit dim loaded');
    await LoadDateDim.load();
    console.log('Date dim loaded');
    await LoadRaceDim.load();
    console.log('Race dim loaded');
    await LoadTeamDim.load();
    console.log('Team dim loaded');
    await LoadStatusDim.load();
    console.log('Status dim loaded');
    await LoadDriverDim.load();
    console.log('Driver dim loaded');
    await LoadLapsStatsDim.load();
    console.log('Laps Stats dim loaded');
    await LoadPitStopsStatsDim.load();
    console.log('Pit Stops Stats dim loaded');
    await LoadPointsStatsDim.load();
    console.log('Points Stats dim loaded');
    await LoadPositionStatsDim.load();
    console.log('Positions Stats dim loaded');
    await LoadQualifyingDim.load();
    console.log('Qualifying dim loaded');

    await LoadDriverRaceResult.load();
    console.log('Driver Race Results fact loaded');

    await this.truncateStaging();
  }

  private static async truncateStaging() {
    const truncates = [];
    for (const tableName of [
      ...Object.values(WcStagingTable),
      ...Object.values(RdStagingTable),
      ...Object.values(FiaStagingTable),
    ]) {
      truncates.push(truncateTable(tableName));
    }
    await Promise.all(truncates);
    console.log('All staging tables was truncated');
  }

  static async testIncrementalLoad() {
    await WcStaging.load();

    await LoadStatusDim.load();
    console.log('Status dim loaded');

    await this.truncateStaging();
  }
}
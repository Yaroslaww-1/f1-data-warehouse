import { truncateTable } from '@src/database/utils/truncate-table';
import { RdStagingTable } from '../extract/sources/rd/table-names.enum';
import { LoadCircuitDim } from './load-circuit-dim/load-circuit-dim';
import { LoadDateDim } from './load-date-dim/load-date-dim';
import { LoadDriverDim } from './load-driver-dim/load-driver-dim';
import { LoadRaceDim } from './load-race-dim/load-race-dim';
import { LoadStatusDim } from './load-status-dim/load-status-dim';
import { LoadTeamDim } from './load-team-dim/load-team-dim';

export class LoadDataWarehouse {
  static async load() {
    // await LoadCircuitDim.load();
    // console.log('Circuit dim loaded');
    // await LoadDateDim.load();
    // console.log('Date dim loaded');
    // await LoadRaceDim.load();
    // console.log('Race dim loaded');
    // await LoadTeamDim.load();
    // console.log('Team dim loaded');
    // await LoadStatusDim.load();
    // console.log('Status dim loaded');
    await LoadDriverDim.load();
    console.log('Driver dim loaded');

    // await truncateTable(RcStagingTable.CIRCUIT);
  }
}
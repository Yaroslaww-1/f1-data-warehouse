import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { WcStagingTable } from '@src/ETL/extract/sources/wc/table-names.enum';
import { DimTable } from '../table-names.enum';
import { addMetaInformation } from '../utils/add-meta-information-to-table';
import { isIncrementalLoad } from '../utils/is-incremental-load';
import { updateDim } from '../utils/update-dim';

const mapTeamToTable = (isIncrementalLoad: boolean) => team => ({
  ref: team.ref,
  name: team.name,
  ...addMetaInformation(team, isIncrementalLoad),
});

export class LoadTeamDim {
  static async load() {
    const isIncremental = await isIncrementalLoad(DimTable.TEAM);
    const teams = await this.getTeams();
    if (!isIncremental) {
      await this.insertNewTeams(teams);
    } else {
      await this.updateTeams(teams);
    }
  }

  private static async getTeams() {
    const teams = await databaseAdapter.query(`SELECT * FROM ${WcStagingTable.TEAM}`);
    return teams;
  }

  private static async insertNewTeams(teams) {
    await insertIntoTable(DimTable.TEAM, teams.map(mapTeamToTable(false)));
  }

  private static async updateTeams(teams: any[]) {
    await updateDim({
      dataFromStaging: teams,
      dimTableName: DimTable.TEAM,
      mapDataItemToTableItemIncremental: mapTeamToTable(true),
    });
  }
}
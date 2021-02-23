import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { FiaStagingTable } from '@src/ETL/extract/sources/fia/table-names.enum';
import { WcStagingTable } from '@src/ETL/extract/sources/wc/table-names.enum';
import { DimTable } from '../table-names.enum';
import { addMetaInformation } from '../utils/add-meta-information-to-table';
import { isIncrementalLoad } from '../utils/is-incremental-load';
import { updateDim } from '../utils/update-dim';

const mapTeamToTable = team => ({
  ref: team.ref,
  name: team.name,
  ...addMetaInformation(team),
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
    const query = `
      WITH fia_stg_team_new
      AS (
          SELECT
            fia_stg_team.*,
            get_team_name(fia_stg_team.name) as name_formatted
          FROM ${FiaStagingTable.TEAM} fia_stg_team
          )
      SELECT
        fia_stg_team_new.name as name,
        wc_stg_team.ref as ref,
        fia_stg_team_new.source_key as source_key
      FROM fia_stg_team_new
      LEFT JOIN ${WcStagingTable.TEAM} wc_stg_team
      ON
        fia_stg_team_new.name_formatted = LOWER(wc_stg_team.name)
      GROUP BY
        wc_stg_team.ref,
        fia_stg_team_new.name,
        fia_stg_team_new.source_key
    `;
    const teams = await databaseAdapter.query(query);
    return teams;
  }

  private static async insertNewTeams(teams) {
    await insertIntoTable(DimTable.TEAM, teams.map(mapTeamToTable));
  }

  private static async updateTeams(teams: any[]) {
    await updateDim({
      dataFromStaging: teams,
      dimTableName: DimTable.TEAM,
    });
  }
}
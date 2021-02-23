import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { extract } from './extract';
import { FiaStagingTable } from './table-names.enum';

export const getFiaSourceKey = itemId => `FIA|${itemId}`;

const mapTeamResultsToTable = teamResults => ({
  name: teamResults.Team,
  source_key: getFiaSourceKey(teamResults.Id),
});

export class LoadFiaRdStaging {
  static async loadTeamDimension() {
    const teamsResults = await extract('constructors_championship_1958-2020') as any[];
    const uniqueTeamsNames = new Set();
    const uniqueTeams = [];
    for (const teamResults of teamsResults) {
      const teamName = teamResults.Team;
      if (!uniqueTeamsNames.has(teamName)) {
        uniqueTeamsNames.add(teamName);
        uniqueTeams.push(teamResults);
      }
    }
    await insertIntoTable(FiaStagingTable.TEAM, uniqueTeams.map(mapTeamResultsToTable));
  }
}
import { extract } from './extract';
import { WcStaging } from './staging/staging';

const mapTeamToTable = team => {
  return {
    id: team.constructorId,
    ref: team.constructorRef,
    name: team.name,
  };
}; 

export class LoadToWcStaging {
  static async load() {
    await this.loadTeamDimension();
  }

  private static async loadTeamDimension() {
    const teams = await extract('constructors');
    await WcStaging.insertInto('stg_wc_team_dim', teams.map(mapTeamToTable));
  }
}
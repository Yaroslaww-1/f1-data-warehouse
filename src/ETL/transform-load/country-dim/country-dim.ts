import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { WcStagingTable } from '@src/ETL/extract/sources/wc/table-names.enum';
import { DimTable } from '../table-names.enum';
import { addMetaInformation } from '../utils/add-meta-information-to-table';
import { isIncrementalLoad } from '../utils/is-incremental-load';
import { updateDim } from '../utils/update-dim';

const mapCountryToTable = country => ({
  name: country.country,
  ...addMetaInformation(country),
});

export class LoadCountryDim {
  static async load() {
    const isIncremental = await isIncrementalLoad(DimTable.COUNTRY);
    const countries = await this.getCountries();
    if (!isIncremental) {
      await this.insertNewCountries(countries);
    } else {
      await this.updateCountries(countries);
    }
  }

  static async getCountries() {
    const query = `
      SELECT
        DISTINCT ON(country) country, source_key 
      FROM ${WcStagingTable.CIRCUIT}
    `;
    const countries = await databaseAdapter.query<any>(query);
    return countries.map(mapCountryToTable);
  }

  private static async insertNewCountries(countries) {
    await insertIntoTable(DimTable.COUNTRY, countries);
  }

  private static async updateCountries(countries: any[]) {
    await updateDim({
      dataFromStaging: countries,
      dimTableName: DimTable.COUNTRY,
    });
  }
}
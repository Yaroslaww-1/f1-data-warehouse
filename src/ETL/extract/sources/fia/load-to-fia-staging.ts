import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { extract } from './extract';
import { FiaStagingTable } from './table-names.enum';

export const getFiaSourceKey = itemId => `FIA|${itemId}`;

const escapeSymbols = (string = '') => {
  return string.split('\'').join('');
};

const valueOrDefault = (value, def = null) => {
  if (value === 'NC') {
    return def;
  } else {
    return value;
  }
};

const mapQualifyingToTable = qualifying => ({
  year: qualifying.Year,
  position: valueOrDefault(qualifying.Position),
  venue: qualifying.Venue,
  driver_name: escapeSymbols(qualifying.Name),
  driver_code: escapeSymbols(qualifying.NameTag),
  source_key: getFiaSourceKey(`${qualifying.Id}-${qualifying.Year}`),
});

export class LoadFiaRdStaging {
  static async loadQualifyingDimension() {
    const qualifyingBefore2006 = await extract('qualifying_times_1950-2005');
    const qualifyingAfter2006 = await extract('qualifying_times_2006-2020');
    const qualifying = [...qualifyingBefore2006, qualifyingAfter2006];
    await insertIntoTable(FiaStagingTable.QUALIFYING, qualifying.map(mapQualifyingToTable));
  }
}
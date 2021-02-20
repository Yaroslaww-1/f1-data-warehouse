import * as path from 'path';
import { parseCsv } from '../../utils/parse-csv';

export const extract = async (tableName: string) => {
  const filePath = path.join(__dirname, 'data', `${tableName}.csv`);

  const results = await parseCsv(filePath);

  return results;
};
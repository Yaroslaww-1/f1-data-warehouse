import * as parse from 'csv-parse';
import { promises as fs } from 'fs';

export const parseCsv = async <T>(filePath: string): Promise<T[]> => {
  const parseOptions = {
    columns: true,
    delimiter: ',',
  };

  const content = await fs.readFile(filePath);

  return new Promise<T[]>((resolve, reject) => {
    parse(content, parseOptions, (err, data) => {
      if (err) return reject(err);
      else return resolve(data);
    });
  });
};
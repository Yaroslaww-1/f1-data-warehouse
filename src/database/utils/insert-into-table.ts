import { databaseAdapter } from '../database-adapter';

export const insertIntoTable = async (tableName: string, data: unknown[]) => {
  const columnNames = Object.keys(data[0]).filter(name => name !== 'id');
  const values = data.map(value => {
    const valueProperties = columnNames.map(c => {
      if (value[c] === null || value[c] === undefined) return 'NULL';
      if (typeof value[c] === 'string' || value[c] instanceof Date) return `'${value[c]}'`;
      else return value[c];
    });
    return `(${valueProperties.join(',')})`;
  });
  const query = `
    INSERT INTO ${tableName} (${columnNames.join(',')})
    VALUES ${values.join(',')}
    RETURNING *
  `;
  // console.log(query.substring(0, 200));
  const result = await databaseAdapter.query<void>(query);
  return result;
};
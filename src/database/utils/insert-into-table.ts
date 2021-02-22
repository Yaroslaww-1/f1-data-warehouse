import { databaseAdapter } from '../database-adapter';

export const insertIntoTable = async (tableName: string, data: unknown[]) => {
  const columnNames = Object.keys(data[0]).filter(name => name !== 'id');
  const values = data.map(value => {
    const valueProperties = columnNames.map(c => {
      if (value[c] === null || value[c] === undefined) return 'NULL';
      if (typeof value[c] === 'string') return `'${value[c]}'`;
      else return value[c];
    });
    return `(${valueProperties.join(',')})`;
  });
  const query = `
    INSERT INTO ${tableName} (${columnNames.join(',')})
    VALUES ${values.join(',')}
    RETURNING *
  `;
  await databaseAdapter.query<void>(query);
};
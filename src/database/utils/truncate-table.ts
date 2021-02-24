import { databaseAdapter } from '../database-adapter';

export const truncateTable = async (tableName: string) => {
  const query = `TRUNCATE TABLE ${tableName} CASCADE`;
  await databaseAdapter.query<void>(query);
};
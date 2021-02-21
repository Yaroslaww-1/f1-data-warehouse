import { databaseAdapter } from '@src/database/database-adapter';

export const isIncrementalLoad = async (tableName: string): Promise<boolean> => {
  const query = `
    SELECT CASE 
      WHEN EXISTS (SELECT * FROM ${tableName} LIMIT 1) THEN 1
      ELSE 0 
    END
  `;
  const [{ case: isTableNotEmpty }] = await databaseAdapter.query(query);
  console.log(`isIncrementalLoad ${tableName} ${isTableNotEmpty}`);
  if (isTableNotEmpty) {
    return true;
  } else {
    return false;
  }
};
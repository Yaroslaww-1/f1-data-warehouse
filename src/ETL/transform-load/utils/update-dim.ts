import { databaseAdapter } from '@src/database/database-adapter';
import { getSets } from '@src/database/utils/get-sets';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { getCurrentTimestamp, subtractSeconds } from './date';

type WithSourceKey = {
  source_key: string;
}

type DuplicatingRow = {
  id: number;
  source_key: string;
}

const updateOldRows = async (
  { duplicatingRows, duplicatingRowsUpdates, tableName }:
  {
    duplicatingRows: DuplicatingRow[],
    duplicatingRowsUpdates: WithSourceKey[],
    tableName: string
  }
) => {
  const oldRowsReInserts = [];
  const updates = [];
  const currentTimestamp = getCurrentTimestamp();
  const subtractedTimestamp = subtractSeconds(currentTimestamp, 1);

  const getOldRowReInsertWithUpdatedValidTo = duplicatingRow => {
    const newRowToInsert = 
      [duplicatingRow]
      .map(d => ({
        ...d,
        valid_from: new Date(d.valid_from).toISOString(),
        valid_to: subtractedTimestamp }));
    return newRowToInsert;
  };

  for (const duplicatingRow of duplicatingRows) {
    const rowUpdate = duplicatingRowsUpdates.find(r => r.source_key === duplicatingRow.source_key);
    const updateQuery = `
      UPDATE ${tableName}
      SET 
        ${getSets({ ...rowUpdate, valid_from: `${currentTimestamp}` })}
      WHERE source_key = '${duplicatingRow.source_key}'
      AND valid_to > '${subtractedTimestamp}'
    `;
    updates.push(databaseAdapter.query(updateQuery));

    oldRowsReInserts.push(insertIntoTable(tableName, getOldRowReInsertWithUpdatedValidTo(duplicatingRow)));
  }
  await Promise.all(oldRowsReInserts);
  await Promise.all(updates);
};

export const updateDim = async (
  { dataFromStaging, dimTableName } :
  { dataFromStaging: WithSourceKey[], dimTableName: string }
) => {
  const newKeys = dataFromStaging.map(i => i.source_key);
  const currentTimestamp = getCurrentTimestamp();
  const query = `
    SELECT DISTINCT(source_key), *
    FROM ${dimTableName}
    WHERE
      source_key IN (${newKeys.map(key => `'${key}'`).join(',')}) AND
      valid_to > '${currentTimestamp}'
  `;
  const duplicatingRows = await databaseAdapter.query<DuplicatingRow>(query);
  const duplicatingKeys = duplicatingRows?.map(r => r.source_key);
  const newUniqueKeys = newKeys.filter(key => !duplicatingKeys.includes(key));
  const duplicatingRowsUpdates = dataFromStaging.filter(d => duplicatingKeys.includes(d.source_key));

  await updateOldRows({
    duplicatingRows,
    duplicatingRowsUpdates,
    tableName: dimTableName,
  });

  if (newUniqueKeys.length > 0) {
    const newData = dataFromStaging
      .filter(i => newUniqueKeys.includes(i.source_key));
    await insertIntoTable(dimTableName, newData);
  }
};
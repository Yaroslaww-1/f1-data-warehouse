import { databaseAdapter } from '@src/database/database-adapter';
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { getCurrentTimestamp, subtractSeconds } from './date';

type WithSourceKey = {
  source_key: string;
}

type MapDataItemIncremental = (data: unknown) => unknown;

const updateOldValues = async (
  { duplicatingKeys, tableName, dataFromStaging, mapDataItemToTableItemIncremental }:
  {
    duplicatingKeys: string[],
    tableName: string,
    dataFromStaging: WithSourceKey[],
    mapDataItemToTableItemIncremental: MapDataItemIncremental
  }
) => {
  const inserts = [];
  const updates = [];
  const currentTimestamp = getCurrentTimestamp();
  const subtractedTimestamp = subtractSeconds(currentTimestamp, 1);
  for (const duplicateKey of duplicatingKeys) {
    const updateQuery = `
      UPDATE ${tableName}
      SET valid_to = '${subtractedTimestamp}'
      WHERE source_key = '${duplicateKey}'
      AND valid_to > '${currentTimestamp}'
    `;

    updates.push(databaseAdapter.query(updateQuery));
    const newData = [dataFromStaging.find(item => item.source_key === duplicateKey)]
      .map(d => ({ ...d, valid_from: currentTimestamp }))
      .map(mapDataItemToTableItemIncremental);
    inserts.push(insertIntoTable(tableName, newData));
  }
  await Promise.all(updates);
  await Promise.all(inserts);
};

export const updateDim = async (
  { dataFromStaging, dimTableName, mapDataItemToTableItemIncremental } :
  { dataFromStaging: WithSourceKey[], dimTableName: string, mapDataItemToTableItemIncremental: MapDataItemIncremental }
) => {
  const sourceKeys = dataFromStaging.map(i => i.source_key);
  const query = `
    SELECT DISTINCT(source_key)
    FROM ${dimTableName}
    WHERE source_key IN (${sourceKeys.map(key => `'${key}'`).join(',')})
  `;
  const duplicatingKeys = (await databaseAdapter.query(query)).map((row: any) => row.source_key);
  const newKeys = sourceKeys.filter(key => !duplicatingKeys.includes(key));

  await updateOldValues({
    duplicatingKeys,
    tableName: dimTableName,
    dataFromStaging,
    mapDataItemToTableItemIncremental,
  });

  if (newKeys.length > 0) {
    const newData = dataFromStaging
      .filter(i => newKeys.includes(i.source_key))
      .map(mapDataItemToTableItemIncremental);
    await insertIntoTable(dimTableName, newData);
  }
};
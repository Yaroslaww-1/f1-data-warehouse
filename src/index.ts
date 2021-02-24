import * as dotenv from 'dotenv';
dotenv.config();

import * as promptSync from 'prompt-sync';
const prompt = promptSync({ sigint: true });

import { MainSchema } from './database/main-schema/main-schema';
import { FiaStaging } from './ETL/extract/sources/fia/staging/staging';
import { RdStaging } from './ETL/extract/sources/rd/staging/staging';
import { WcStaging } from './ETL/extract/sources/wc/staging/staging';
import { LoadDataWarehouse } from './ETL/transform-load/load-data-warehouse';

enum Option {
  DROP_MAIN_SCHEMA = '1',
  CREATE_MAIN_SCHEMA = '2',
  LOAD_WC_STAGING = '3',
  LOAD_RD_STAGING = '4',
  LOAD_FIA_STAGING = '5',
  LOAD_DATA_WAREHOUSE ='6',
  TEST_INCREMENTAL_LOAD = '7',
  EXIT = '99',
}

const processUserCommands = async () => {
  console.log(`Available options:
  Drop main schema = ${Option.DROP_MAIN_SCHEMA}
  Create main schema = ${Option.CREATE_MAIN_SCHEMA}
  [wc]Load wc staging = ${Option.LOAD_WC_STAGING}
  [rd]Load rd staging = ${Option.LOAD_RD_STAGING}
  [fia]Load fia staging = ${Option.LOAD_FIA_STAGING}
  [data-warehouse]Load Data Warehouse = ${Option.LOAD_DATA_WAREHOUSE}
  [data-warehouse]Test Incremental load = ${Option.TEST_INCREMENTAL_LOAD}
  Exit = ${Option.EXIT}
  `);
  while (true) {
    const optionId = prompt('Enter option ');
    switch (optionId) {
      // main
      case Option.DROP_MAIN_SCHEMA: {
        await MainSchema.drop();
        break;
      }
      case Option.CREATE_MAIN_SCHEMA: {
        await MainSchema.create();
        break;
      }

      // wc
      case Option.LOAD_WC_STAGING: {
        await WcStaging.load();
        break;
      }

      // rd
      case Option.LOAD_RD_STAGING: {
        await RdStaging.load();
        break;
      }

      // fia
      case Option.LOAD_FIA_STAGING: {
        await FiaStaging.load();
        break;
      }

      // data warehouse
      case Option.LOAD_DATA_WAREHOUSE: {
        await LoadDataWarehouse.load();
        break;
      }
      case Option.TEST_INCREMENTAL_LOAD: {
        await LoadDataWarehouse.testIncrementalLoad();
        break;
      }

      // exit
      case Option.EXIT: {
        return;
      }
    }
  }
};

(async () => {
  await processUserCommands();
})();
import * as dotenv from 'dotenv';
dotenv.config();

import * as promptSync from 'prompt-sync';
const prompt = promptSync({ sigint: true });

import { MainSchema } from './database/main-schema/main-schema';
import { LoadToWcStaging } from './ETL/extract/sources/wc/load-to-wc-staging';
import { WcStaging } from './ETL/extract/sources/wc/staging/staging';

const processUserCommands = async () => {
  const Option = {
    DROP_MAIN_SCHEMA: '1',
    CREATE_MAIN_SCHEMA: '2',
    CREATE_WC_STAGING_SCHEMA: '3',
    LOAD_WC_STAGING: '4',
    EXIT: '5',
  };
  console.log(`Available options:
  Drop main schema = ${Option.DROP_MAIN_SCHEMA}
  Create main schema = ${Option.CREATE_MAIN_SCHEMA}
  Create wc staging schema = ${Option.CREATE_WC_STAGING_SCHEMA}
  Load wc staging = ${Option.LOAD_WC_STAGING}
  Exit = ${Option.EXIT}
  `);
  while (true) {
    const optionId = prompt('Enter option ');
    switch (optionId) {
      case Option.DROP_MAIN_SCHEMA: {
        await MainSchema.drop();
        break;
      }
      case Option.CREATE_MAIN_SCHEMA: {
        await MainSchema.create();
        break;
      }
      case Option.CREATE_WC_STAGING_SCHEMA: {
        await WcStaging.create();
        break;
      }
      case Option.LOAD_WC_STAGING: {
        await LoadToWcStaging.load();
        break;
      }
      case Option.EXIT: {
        return;
      }
    }
  }
};

(async () => {
  await processUserCommands();
})();
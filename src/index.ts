import * as dotenv from 'dotenv';
dotenv.config();

import * as promptSync from 'prompt-sync';
const prompt = promptSync({ sigint: true });

import { MainSchema } from './database/main-schema/main-schema';
import { LoadToRdStaging } from './ETL/extract/sources/rd/load-to-rc-staging';
import { RdStaging } from './ETL/extract/sources/rd/staging/staging';
import { LoadToWcStaging } from './ETL/extract/sources/wc/load-to-wc-staging';
import { WcStaging } from './ETL/extract/sources/wc/staging/staging';

const processUserCommands = async () => {
  const Option = {
    DROP_MAIN_SCHEMA: '1',
    CREATE_MAIN_SCHEMA: '2',
    DROP_WC_STAGING_SCHEMA: '3',
    CREATE_WC_STAGING_SCHEMA: '4',
    LOAD_WC_STAGING: '5',
    DROP_RD_STAGING_SCHEMA: '6',
    CREATE_RD_STAGING_SCHEMA: '7',
    LOAD_RD_STAGING: '8',
    EXIT: '9',
  };
  console.log(`Available options:
  Drop main schema = ${Option.DROP_MAIN_SCHEMA}
  Create main schema = ${Option.CREATE_MAIN_SCHEMA}
  [wc]Drop wc staging schema = ${Option.DROP_WC_STAGING_SCHEMA}
  [wc]Create wc staging schema = ${Option.CREATE_WC_STAGING_SCHEMA}
  [wc]Load wc staging = ${Option.LOAD_WC_STAGING}
  [rd]Drop rd staging schema = ${Option.DROP_RD_STAGING_SCHEMA}
  [rd]Create rd staging schema = ${Option.CREATE_RD_STAGING_SCHEMA}
  [rd]Load rd staging = ${Option.LOAD_RD_STAGING}
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
      case Option.DROP_WC_STAGING_SCHEMA: {
        await WcStaging.drop();
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

      // rd
      case Option.DROP_RD_STAGING_SCHEMA: {
        await RdStaging.drop();
        break;
      }
      case Option.CREATE_RD_STAGING_SCHEMA: {
        await RdStaging.create();
        break;
      }
      case Option.LOAD_RD_STAGING: {
        await LoadToRdStaging.load();
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
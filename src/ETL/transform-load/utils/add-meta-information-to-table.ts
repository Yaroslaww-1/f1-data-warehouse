import { DEFAULT_VALID_FROM, DEFAULT_VALID_TO } from '../constants';

export const addMetaInformation = (item: any) => ({
  valid_from: item.valid_from || DEFAULT_VALID_FROM,
  valid_to: item.valid_to || DEFAULT_VALID_TO,
  source_key: item.source_key,
});
import { insertIntoTable } from '@src/database/utils/insert-into-table';
import { DimTable } from '../table-names.enum';
import { addDays, mapMonthNumberToMonthLabel } from '../utils/date';
import { isIncrementalLoad } from '../utils/is-incremental-load';

const mapDateToTable = (date: Date) => ({
  year: date.getFullYear(),
  month: mapMonthNumberToMonthLabel(date.getMonth() + 1),
  day: date.getDate(),
});

export class LoadDateDim {
  static async load() {
    const isIncremental = await isIncrementalLoad(DimTable.DATE);
    const dates = await this.getDates();
    if (!isIncremental) {
      await this.insertNewDates(dates);
    }
  }

  private static async getDates() {
    const dates = [];
    const START_DATE = new Date('01.01.1700');
    const END_DATE = new Date('01.01.2040');
    let date = START_DATE;
    while (date < END_DATE) {
      dates.push(date);
      date = addDays(date, 1);
    }
    return dates;
  }

  private static async insertNewDates(dates) {
    await insertIntoTable(DimTable.DATE, dates.map(mapDateToTable));
  }
}
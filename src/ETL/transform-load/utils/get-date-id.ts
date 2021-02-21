import { getDayMonthYear, mapMonthNumberToMonthLabel } from './date';

interface IDate {
  id: string;
  day: number;
  month: string;
  year: number;
}

export const getDateId = (dates: IDate[], date: Date) => {
  const { day, month, year } = getDayMonthYear(date);

  // console.log(date, day, mapMonthNumberToMonthLabel(month), year);

  const equalDate = dates.find(date => {
    return date.day === day && date.month === mapMonthNumberToMonthLabel(month) && date.year === year;
  });

  return equalDate.id;
};
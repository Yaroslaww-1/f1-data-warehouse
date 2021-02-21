export const getCurrentTimestamp = () => {
  return new Date().toISOString();
};

export const subtractOneSecond = (dateStr: string) => {
  const date = new Date(dateStr);
  return new Date(date.getTime() - 1000).toISOString();
};

export const addDays = (date: Date, days: number) => {
  const result = new Date(date);
  result.setDate(result.getDate() + days);
  return result;
};

export const getDayMonthYear = (date: Date) => {
  return {
    day: date.getDate(),
    month: date.getMonth() + 1,
    year: date.getFullYear(),
  };
};

export const mapMonthNumberToMonthLabel = (monthNumber: number) => {
  switch (monthNumber) {
    case 1: return 'January';
    case 2: return 'February';
    case 3: return 'March';
    case 4: return 'April';
    case 5: return 'May';
    case 6: return 'June';
    case 7: return 'July';
    case 8: return 'August';
    case 9: return 'September';
    case 10: return 'October';
    case 11: return 'November';
    case 12: return 'December';
  }
};

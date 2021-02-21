export const getCurrentTimestamp = () => {
  return new Date().toISOString();
};

export const subtractOneSecond = (dateStr: string) => {
  const date = new Date(dateStr);
  return new Date(date.getTime() - 1000).toISOString();
};
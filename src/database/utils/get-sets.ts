export const getSets = data => {
  const columnNames = Object.keys(data).filter(name => name !== 'id');
  const sets = columnNames.map(c => {
    if (data[c] === null || data[c] === undefined) return `${c} = NULL`;
    if (typeof data[c] === 'string') return `${c} = '${data[c]}'`;
    else return `${c} = ${data[c]}`;
  }).join(',\n');
  return sets;
};
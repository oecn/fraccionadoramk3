export function normalizeKey(value: string | null | undefined): string {
  return (value || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');
}

export function productPillClass(producto: string | null | undefined): string {
  const key = normalizeKey(producto);
  if (key.includes('arroz')) return 'pill-arroz';
  if (key.includes('poroto')) return 'pill-poroto';
  if (key.includes('pororo')) return 'pill-pororo';
  if (key.includes('locro') && !key.includes('locrillo')) return 'pill-locro';
  if (key.includes('locrillo')) return 'pill-locrillo';
  if (key.includes('azucar')) return 'pill-azucar';
  if (key.includes('lenteja')) return 'pill-lenteja';
  if (key.includes('limpieza')) return 'pill-limpieza';
  if (key.includes('domingo') || key.includes('feriado')) return 'pill-feriado';
  if (key.includes('gall') || key.includes('molida')) return 'pill-gall';
  return 'pill-default';
}

export function dateOffset(days: number): string {
  const date = new Date();
  date.setDate(date.getDate() + days);
  return date.toISOString().slice(0, 10);
}

export function todayIso(): string {
  return new Date().toISOString().slice(0, 10);
}

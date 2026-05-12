export function fmtGs(value: number | null | undefined): string {
  return Number(value || 0).toLocaleString('es-PY', { maximumFractionDigits: 0 });
}

export function fmtKg(value: number | null | undefined): string {
  return Number(value || 0).toLocaleString('es-PY', {
    minimumFractionDigits: 3,
    maximumFractionDigits: 3,
  });
}

export function fmtNumber(value: number | null | undefined, digits = 2): string {
  return Number(value || 0).toLocaleString('es-PY', {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  });
}

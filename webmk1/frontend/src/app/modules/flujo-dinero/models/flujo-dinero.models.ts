export type FlujoKpi = {
  title: string;
  value: string;
  subtitle: string;
};

export type FlujoMonthRow = {
  month: string;
  month_no: string;
  compras: number;
  ventas: number;
  ventas_retencion: number;
  notas_credito: number;
  gastos: number;
  flujo: number;
  margen: number;
  acumulado: number;
};

export type FlujoSummary = {
  year: number;
  from_date: string;
  to_date: string;
  quarter: string;
  retencion_mode: boolean;
  saldo_inicial: number;
  kpis: FlujoKpi[];
  rows: FlujoMonthRow[];
  updated_at: string;
  source: string;
};

export type FlujoFilters = {
  year: number;
  quarter: string;
  retencion_mode: boolean;
  from_date: string;
  to_date: string;
};


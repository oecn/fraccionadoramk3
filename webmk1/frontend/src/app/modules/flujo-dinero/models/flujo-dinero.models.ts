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

export type FlujoQuarterRow = {
  quarter: string;
  label: string;
  start_month: string;
  end_month: string;
  saldo_inicio: number;
  saldo_fin: number;
  ventas: number;
  compras: number;
  gastos: number;
  notas_credito: number;
  flujo: number;
  margen: number;
  profitable: boolean;
};

export type FlujoSummary = {
  year: number;
  from_date: string;
  to_date: string;
  quarter: string;
  retencion_mode: boolean;
  include_iva: boolean;
  saldo_inicial: number;
  kpis: FlujoKpi[];
  quarter_rows: FlujoQuarterRow[];
  rows: FlujoMonthRow[];
  updated_at: string;
  source: string;
};

export type FlujoFilters = {
  year: number;
  quarter: string;
  retencion_mode: boolean;
  include_iva: boolean;
  from_date: string;
  to_date: string;
};

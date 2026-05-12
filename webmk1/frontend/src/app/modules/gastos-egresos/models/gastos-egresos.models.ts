export type ExpenseCreate = {
  fecha: string;
  tipo: string;
  descripcion: string;
  monto_gs: number;
  nro_factura: string;
  forma_pago: string;
  referencia_pago: string;
};

export type ExpenseRow = {
  id: number;
  fecha: string;
  tipo: string;
  descripcion: string;
  monto_gs: number;
  nro_factura: string;
  forma_pago: string;
  referencia_pago: string;
};

export type ExpenseSummary = {
  rows: ExpenseRow[];
  total_gs: number;
  tipos: string[];
  formas_pago: string[];
};

export type CheckStatus = {
  available: boolean;
  found: boolean;
  used: boolean;
  message: string;
  chequera_id: string;
  cheque_no: string;
  serie: string;
  referencia: string;
};

export type IpsParseResult = {
  fecha: string;
  tipo: string;
  descripcion: string;
  monto_gs: number;
  nro_factura: string;
  forma_pago: string;
  referencia_pago: string;
  periodo: string;
  periodo_display: string;
  duplicate: boolean;
};

export type RrhhImportRow = {
  employee_id: number | null;
  documento: string;
  funcionario: string;
  cuenta_destino: string;
  concepto: string;
  monto: number;
  confirmado: boolean;
};

export type RrhhParseResult = {
  file_name: string;
  fecha: string;
  row_count: number;
  total_amount: number;
  conceptos: string[];
  unresolved_count: number;
  rows: RrhhImportRow[];
};

export type ImportResult = {
  message: string;
  inserted: number;
  skipped: number;
};

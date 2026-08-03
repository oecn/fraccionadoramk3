export type FacturaPendienteRow = {
  invoice_id: number;
  invoice_source: string;
  ts: string;
  invoice_no: string;
  customer: string;
  total_gs: number;
  cobrado_gs: number;
  saldo_gs: number;
};

export type CobroFacturaItemIn = {
  invoice_id: number;
  invoice_source: string;
  monto_gs: number;
};

export type CobroFacturaCreate = {
  fecha_cobro: string;
  cheque_no: string;
  boleta_deposito: string;
  banco: string;
  observacion: string;
  items: CobroFacturaItemIn[];
};

export type CobroFacturaItemRow = {
  id: number;
  invoice_id: number;
  invoice_source: string;
  invoice_no: string;
  customer: string;
  factura_total_gs: number;
  monto_gs: number;
};

export type CobroFacturaRow = {
  id: number;
  fecha_cobro: string;
  cheque_no: string;
  boleta_deposito: string;
  banco: string;
  observacion: string;
  total_gs: number;
  created_at: string;
  updated_at: string;
  items: CobroFacturaItemRow[];
};

export type CobrosSummary = {
  pendientes: FacturaPendienteRow[];
  cobros: CobroFacturaRow[];
};

export type Kpi = {
  title: string;
  value: string;
  subtitle: string;
};

export type OrderRow = {
  oc_id: number;
  numero: string;
  sucursal: string;
  fecha_pedido: string;
  fecha_compromiso: string;
  dias_atraso: number | null;
  estado: string;
  prioridad: string;
  pct_listo_entrega: number;
  monto_total: number;
};

export type OrderDeliveryResponse = {
  oc_id: number;
  updated: boolean;
  message: string;
};

export type PaymentRow = {
  factura_id: number;
  proveedor: string;
  numero_doc: string;
  fecha_emision: string;
  vencimiento: string;
  dias_para_vencer: number | null;
  monto: number;
  estado: string;
  sucursal: string;
};

export type PaymentCheckOption = {
  cheque_no: string;
  chequera_id: string;
  bank_id: string;
  bank_name: string;
  account_no: string;
  form_type: string;
  check_type: string;
  serie: string;
  reference_value: string;
  group_label: string;
  label: string;
};

export type PaymentCheckStatus = {
  available: boolean;
  found: boolean;
  used: boolean;
  message: string;
  check: PaymentCheckOption | null;
};

export type PaymentRegisterRequest = {
  lot_ids: number[];
  fecha_pago: string;
  medio: string;
  referencia: string;
  nro_deposito: string;
  nro_recibo_dinero: string;
  observacion: string;
  chequera_id: string;
  cheque_no: string;
  serie: string;
};

export type PaymentRegisterResponse = {
  payment_group_id: string;
  facturas: number;
  total_gs: number;
  medio: string;
  referencia: string;
};

export type PaymentDetailRow = {
  id: number;
  payment_group_id: string;
  lot_id: number;
  proveedor: string;
  factura: string;
  monto_gs: number;
  fecha_pago: string;
  medio: string;
  referencia: string;
  nro_recibo_dinero: string;
  observacion: string;
  total_grupo_gs: number;
  ts_registro: string;
};

export type PaymentReceiptUpdateRequest = {
  detail_ids: number[];
  nro_recibo_dinero: string;
};

export type PaymentReceiptUpdateResponse = {
  updated: number;
  nro_recibo_dinero: string;
};

export type CollectionRow = {
  invoice_id: number;
  ts: string;
  invoice_no: string;
  customer: string;
  gravada5_gs: number;
  iva5_gs: number;
  gravada10_gs: number;
  iva10_gs: number;
  total_gs: number;
  total_con_retencion: number;
  dias_sin_cobrar: number | null;
  dias_para_cobro: number | null;
};

export type DashboardSummary = {
  kpis: Kpi[];
  orders: OrderRow[];
  payments: PaymentRow[];
  collections: CollectionRow[];
  trend_7: number;
  trend_30: number;
  updated_at: string;
  source: string;
};

export type DashboardFilters = {
  sucursal: string;
  search: string;
  from_date: string;
  to_date: string;
};

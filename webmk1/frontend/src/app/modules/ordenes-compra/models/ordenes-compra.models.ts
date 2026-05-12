export type OrdenCompraItem = {
  linea: number;
  descripcion: string;
  cantidad: number | null;
  unidad: string | null;
  stock_disponible: number | null;
  stock_ok: boolean;
  producto_match: string;
  gramaje: number | null;
};

export type OrdenCompraImportResponse = {
  oc_id: number;
  nro_oc: string;
  sucursal: string;
  fecha_pedido: string;
  monto_total: number | null;
  items: OrdenCompraItem[];
  message: string;
};

export type OrdenCompraRow = {
  oc_id: number;
  nro_oc: string;
  sucursal: string;
  fecha_pedido: string;
  monto_total: number;
  items_count: number;
  completada: boolean;
  created_at: string;
  pct_listo_envio: number;
};

export type OrdenCompraDetail = OrdenCompraRow & {
  items: OrdenCompraItem[];
};

export type OrdenCompraDeleteResponse = {
  oc_id: number;
  deleted: boolean;
  message: string;
};

export type OrdenCompraStatusResponse = {
  oc_id: number;
  completada: boolean;
  message: string;
};

export type PendienteAcumuladoRow = {
  descripcion: string;
  necesario: number;
  stock_disponible: number | null;
  stock_ok: boolean;
  producto_match: string;
  gramaje: number | null;
};

export type PendientesAcumuladosSummary = {
  total_ocs: number;
  total_monto: number;
  total_items: number;
  items_ok: number;
  items_falta: number;
  pct_listo_envio: number;
  rows: PendienteAcumuladoRow[];
};

export type BolsasPendientesRow = {
  producto: string;
  bolsa_kg: number;
  necesarias: number;
  disponibles: number;
  stock_ok: boolean;
};

export type BolsasPendientesSummary = {
  rows: BolsasPendientesRow[];
};

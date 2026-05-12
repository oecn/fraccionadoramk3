export type ProductoItem = {
  id: number;
  name: string;
};

export type ReportesVentasOptions = {
  productos: ProductoItem[];
  meses: string[];
  gramajes: number[];
};

export type KpiCard = {
  key: string;
  label: string;
  value: number;
  delta_pct: number | null;
};

export type RankingRow = {
  label: string;
  paquetes: number;
  importe_gs: number;
};

export type ReporteVentaRow = {
  periodo: string;
  producto: string;
  gramaje: number;
  paquetes: number;
  importe_gs: number;
  base_gs: number;
  iva_gs: number;
  paq_luque: number;
  paq_aregua: number;
  paq_itaugua: number;
  share_pct: number;
  delta_paquetes_pct: number | null;
  delta_importe_pct: number | null;
};

export type ReportesVentasResumen = {
  periodo: string;
  filtros_label: string;
  kpis: KpiCard[];
  top_productos: RankingRow[];
  top_sucursales: RankingRow[];
  rows: ReporteVentaRow[];
};

export type FacturaDetalleRow = {
  fecha: string;
  nro_factura: string;
  cliente: string;
  producto: string;
  gramaje: number;
  paquetes: number;
  precio_unit: number;
  importe_gs: number;
  invoice_id: number | null;
};

export type FacturaItemsResponse = {
  rows: FacturaDetalleRow[];
};

export type ReportesVentasFilters = {
  periodo: string;
  product_id: number | null;
  gramaje: number | null;
  ym: string;
  desde: string;
  hasta: string;
  ranking_scope: string;
  ranking_ym: string;
};

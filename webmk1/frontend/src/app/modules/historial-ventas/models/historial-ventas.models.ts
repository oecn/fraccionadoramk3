export type VentaRow = {
  id: number;
  ts: string;
  invoice_no: string;
  customer: string;
  gravada5_gs: number;
  iva5_gs: number;
  gravada10_gs: number;
  iva10_gs: number;
  total_gs: number;
  total_con_retencion: number;
};

export type HistorialVentasSummary = {
  total_registros: number;
  total_gs: number;
  total_con_retencion: number;
  rows: VentaRow[];
};

export type HistorialVentasFilters = {
  search: string;
  from_date: string;
  to_date: string;
  retencion_mode: boolean;
};

export type TopProductoRow = {
  producto: string;
  gramaje: number;
  paquetes: number;
  total_gs: number;
};

export type ProductoItem = {
  id: number;
  name: string;
};

export type VentaDetalleRow = {
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
};

export type SucursalRow = {
  sucursal: string;
  importe_gs: number;
  paquetes: number;
};

export type VentasDetalleResponse = {
  rows: VentaDetalleRow[];
  sucursales: SucursalRow[];
  total_paquetes: number;
  total_importe_gs: number;
};

export type ReporteMensualData = {
  ym: string;
  empresa: string;
  d1: string;
  d2: string;
  ventas_facturas: number;
  ventas_bolsas: number;
  ventas_total: number;
  compras_total: number;
  gastos_total: number;
  margen_bruto: number;
  margen_bruto_pct: number;
  beneficio_operativo: number;
  beneficio_pct: number;
  cant_facturas: number;
  cant_ventas_bolsa: number;
  top_productos: TopProductoRow[];
  reporte_txt: string;
};

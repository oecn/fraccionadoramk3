export type ProductoItem = {
  id: number;
  name: string;
  raw_kg: number;
  ultimo_costo_kg_gs: number | null;
};

export type RawStockRow = {
  product_id: number;
  producto: string;
  kg: number;
};

export type LoteAbiertoRow = {
  id: number;
  product_id: number;
  producto: string;
  lote: string;
  proveedor: string;
  factura: string;
  kg_inicial: number;
  kg_saldo: number;
  costo_total_gs: number;
  costo_kg_gs: number;
  gravada5_gs: number;
  iva5_gs: number;
  gravada10_gs: number;
  iva10_gs: number;
  exenta_gs: number;
  costo_kg_anterior_gs: number | null;
  variacion_costo_pct: number | null;
  precio_cambio_detectado: boolean;
  precio_estado: string;
  precio_revisado: boolean;
  precio_revisado_por: string;
  precio_revisado_at: string;
  diferencia_costo_kg_gs: number;
  diferencia_costo_total_gs: number;
  ts: string;
};

export type CompraMateriaPrimaOptions = {
  productos: ProductoItem[];
  bolsa_kg_presets: number[];
};

export type CompraMateriaPrimaSummary = {
  raw_stock: RawStockRow[];
  lotes_abiertos: LoteAbiertoRow[];
  historial_revisiones_precio: LotePrecioReviewHistoryRow[];
};

export type CompraMateriaPrimaCreate = {
  product_id: number | null;
  fecha: string;
  lote: string;
  proveedor: string;
  factura: string;
  bolsa_kg: number | null;
  bolsas: number;
  costo_total_gs: number;
};

export type FacturaCompraItem = {
  linea: number;
  descripcion: string;
  kg: number;
  precio_unitario: number | null;
  total_linea: number;
  gravada5_gs: number;
  iva5_gs: number;
  gravada10_gs: number;
  iva10_gs: number;
  exenta_gs: number;
  product_id: number | null;
  producto: string;
  bolsa_kg: number;
  bolsas: number;
  costo_kg_anterior_gs: number | null;
  variacion_costo_pct: number | null;
  importable: boolean;
  message: string;
};

export type FacturaCompraPreview = {
  numero: string;
  proveedor: string;
  fecha_emision: string;
  total: number;
  items: FacturaCompraItem[];
};

export type FacturaCompraImportRequest = {
  numero: string;
  proveedor: string;
  fecha_emision: string;
  items: Array<{
    product_id: number;
    descripcion: string;
    kg: number;
    total_linea: number;
    gravada5_gs: number;
    iva5_gs: number;
    gravada10_gs: number;
    iva10_gs: number;
    exenta_gs: number;
    bolsa_kg: number;
    bolsas: number;
  }>;
};

export type FacturaCompraImportResponse = {
  inserted: number;
  skipped: number;
  message: string;
  lotes: LoteAbiertoRow[];
};

export type LoteDeleteRequest = {
  motivo: string;
};

export type LoteDeleteResponse = {
  deleted: boolean;
  lot_id: number;
  message: string;
};

export type LotePrecioReviewResponse = {
  lot_id: number;
  estado: string;
  revisado_por: string;
  revisado_at: string;
  message: string;
};

export type LotePrecioReviewRequest = {
  estado: string;
  revisado_por: string;
};

export type LotePrecioReviewHistoryRow = {
  id: number;
  lot_id: number;
  product_id: number;
  producto: string;
  estado: string;
  revisado_por: string;
  costo_kg_anterior_gs: number | null;
  costo_kg_gs: number;
  diferencia_costo_kg_gs: number;
  variacion_costo_pct: number | null;
  note: string;
  created_at: string;
};

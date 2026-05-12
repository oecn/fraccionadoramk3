export type ProductoItem = {
  id: number;
  name: string;
  raw_kg: number;
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
  ts: string;
};

export type CompraMateriaPrimaOptions = {
  productos: ProductoItem[];
  bolsa_kg_presets: number[];
};

export type CompraMateriaPrimaSummary = {
  raw_stock: RawStockRow[];
  lotes_abiertos: LoteAbiertoRow[];
};

export type CompraMateriaPrimaCreate = {
  product_id: number | null;
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
  product_id: number | null;
  producto: string;
  bolsa_kg: number;
  bolsas: number;
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

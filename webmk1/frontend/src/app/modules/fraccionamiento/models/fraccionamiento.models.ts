export type ProductoItem = {
  id: number;
  name: string;
  gramajes: number[];
  raw_kg: number;
};

export type LoteItem = {
  id: number;
  product_id: number;
  lote: string;
  kg_saldo: number;
  costo_kg_gs: number;
  proveedor: string;
  factura: string;
  ts: string;
};

export type RawStockRow = {
  product_id: number;
  producto: string;
  kg: number;
  bolsas_50: number;
  bolsas_25: number;
};

export type PackageStockRow = {
  product_id: number;
  producto: string;
  gramaje: number;
  paquetes: number;
};

export type FraccionamientoHistoryRow = {
  id: number;
  fecha: string;
  product_id: number;
  producto: string;
  gramaje: number;
  paquetes: number;
  kg_consumidos: number;
  bolsas_eq: string;
  lote: string;
};

export type FraccionamientoOptions = {
  productos: ProductoItem[];
  lotes: LoteItem[];
  hoy: string;
};

export type FraccionamientoSummary = {
  raw_stock: RawStockRow[];
  package_stock: PackageStockRow[];
  history: FraccionamientoHistoryRow[];
  total_raw_kg: number;
  total_paquetes: number;
  total_kg_mes: number;
  total_paquetes_mes: number;
};

export type FraccionamientoCreate = {
  product_id: number | null;
  gramaje: number | null;
  paquetes: number;
  fecha: string;
  lot_id: number | null;
};

export type ConsumoPreview = {
  kg_consumidos: number;
  unidades_por_paquete: number;
  bolsas_eq: string;
};

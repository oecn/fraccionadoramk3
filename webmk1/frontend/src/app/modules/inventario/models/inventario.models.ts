export type InventoryRawRow = {
  product_id: number;
  producto: string;
  kg: number;
  alerta_min_kg: number | null;
  alerta_min_bolsas: number | null;
  reposicion_bolsas: number | null;
  alerta_bolsa_kg: number;
  alerta_estado: 'normal' | 'cerca' | 'bajo';
  proveedor_whatsapp: string;
  bolsas_50: number;
  bolsas_25: number;
  lotes_abiertos: number;
  costo_promedio_gs: number;
  valor_stock_gs: number;
  conteo_planta_bolsas: number | null;
  conteo_planta_kg: number | null;
  conteo_diferencia_kg: number | null;
  conteo_fecha: string | null;
};

export type InventoryPackageRow = {
  product_id: number;
  producto: string;
  gramaje: number;
  paquetes: number;
  unidades: number;
  price_gs: number | null;
  iva: number | null;
  valor_venta_gs: number;
  conteo_planta_paquetes: number | null;
  conteo_diferencia_paquetes: number | null;
  conteo_fecha: string | null;
};

export type InventoryFilmRow = {
  product_id: number;
  producto: string;
  gramaje: number;
  rollos: number;
  alerta_min_rollos: number | null;
  alerta_estado: 'normal' | 'cerca' | 'bajo';
};

export type InventoryLotRow = {
  id: number;
  product_id: number;
  producto: string;
  lote: string;
  proveedor: string;
  factura: string;
  kg_saldo: number;
  costo_kg_gs: number;
  valor_saldo_gs: number;
  ts: string;
};

export type InventorySummary = {
  raw_stock: InventoryRawRow[];
  package_stock: InventoryPackageRow[];
  film_stock: InventoryFilmRow[];
  lotes_abiertos: InventoryLotRow[];
  raw_alerts_count: number;
  film_alerts_count: number;
  total_raw_kg: number;
  total_raw_valor_gs: number;
  total_paquetes: number;
  total_unidades: number;
  total_venta_gs: number;
  total_film_rollos: number;
};

export type RawStockAlertUpdate = {
  product_id: number;
  min_kg: number | null;
  reposicion_bolsas: number | null;
  proveedor_whatsapp: string;
};

export type RawStockAdjustment = {
  product_id: number;
  kg: number;
};

export type PackageStockAdjustment = {
  product_id: number;
  gramaje: number;
  paquetes: number;
};

export type FilmStockAdjustment = {
  product_id: number;
  gramaje: number;
  rollos: number;
};

export type FilmStockAlertUpdate = {
  product_id: number;
  gramaje: number;
  min_rollos: number | null;
};

export type InventoryAdjustmentUpdate = {
  raw_stock: RawStockAdjustment[];
  package_stock: PackageStockAdjustment[];
  film_stock: FilmStockAdjustment[];
  motivo: string;
};

export type RawStockWeeklyCount = {
  product_id: number;
  sistema_kg: number;
  planta_bolsas: number;
  bolsa_kg: number;
};

export type PackageStockWeeklyCount = {
  product_id: number;
  gramaje: number;
  sistema_paquetes: number;
  planta_paquetes: number;
};

export type WeeklyInventoryCountCreate = {
  motivo: string;
  raw_stock: RawStockWeeklyCount[];
  package_stock: PackageStockWeeklyCount[];
};

export type CompraSugeridaRow = {
  producto: string;
  stock_kg: number;
  consumo_diario: number;
  dias_restantes: number | null;
  consumo_total: number;
  dias_activos: number;
};

export type CompraSugeridaResponse = {
  ventana_dias: number;
  top_n: number | null;
  rows: CompraSugeridaRow[];
};

export type InventoryRawRow = {
  product_id: number;
  producto: string;
  kg: number;
  bolsas_50: number;
  bolsas_25: number;
  lotes_abiertos: number;
  costo_promedio_gs: number;
  valor_stock_gs: number;
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
  lotes_abiertos: InventoryLotRow[];
  total_raw_kg: number;
  total_raw_valor_gs: number;
  total_paquetes: number;
  total_unidades: number;
  total_venta_gs: number;
};

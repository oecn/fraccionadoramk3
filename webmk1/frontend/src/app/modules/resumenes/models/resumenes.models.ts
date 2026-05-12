export type ProductoOption = {
  id: number;
  name: string;
};

export type ResumenesOptions = {
  productos: ProductoOption[];
};

export type LoteResumenRow = {
  id: number;
  lote: string;
  producto: string;
  kg_total: number;
  kg_usado: number;
  kg_disponible: number;
  costo_total_gs: number;
  costo_kg_gs: number;
  proveedor: string;
  factura: string;
  fecha: string;
  cerrado: boolean;
};

export type LoteFraccionamientoRow = {
  fecha: string;
  gramaje: number;
  paquetes: number;
  kg_consumidos: number;
  bolsas_eq: number | null;
  costo_kg_gs: number;
  costo_total_gs: number;
  precio_venta_gs: number;
  beneficio_gs: number;
};

export type RecargoPresentacionRow = {
  gramaje: number;
  kg_por_paquete: number;
  precio_paquete_gs: number;
  venta_kg_gs: number;
  recargo_kg_gs: number;
  recargo_pct: number | null;
};

export type LoteDetalle = {
  id: number;
  lote: string;
  producto_id: number;
  producto: string;
  kg_total: number;
  kg_usado: number;
  kg_disponible: number;
  costo_total_gs: number;
  costo_kg_gs: number;
  proveedor: string;
  factura: string;
  fecha: string;
  cerrado: boolean;
  bolsas_total: number | null;
  bolsas_usadas: number | null;
  bolsas_disponibles: number | null;
  merma_kg: number;
  consumo_pct: number;
  merma_pct: number;
  venta_estimada_gs: number;
  venta_kg_gs: number;
  recargo_kg_gs: number;
  recargo_pct: number | null;
  beneficio_estimado_gs: number;
  beneficio_pct: number | null;
  recargos: RecargoPresentacionRow[];
  fraccionamientos: LoteFraccionamientoRow[];
};

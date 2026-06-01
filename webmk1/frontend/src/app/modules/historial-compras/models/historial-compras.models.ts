export type CompraRow = {
  id: number;
  ts: string;
  factura: string;
  proveedor: string;
  costo_total_gs: number;
  tipo: string;
  motivo: string;
  producto: string;
};

export type HistorialComprasSummary = {
  total_registros: number;
  total_gs: number;
  total_eliminado_gs: number;
  rows: CompraRow[];
};

export type HistorialComprasFilters = {
  search: string;
  from_date: string;
  to_date: string;
};

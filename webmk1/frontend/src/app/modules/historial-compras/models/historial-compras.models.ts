export type CompraRow = {
  id: number;
  ts: string;
  factura: string;
  proveedor: string;
  costo_total_gs: number;
};

export type HistorialComprasSummary = {
  total_registros: number;
  total_gs: number;
  rows: CompraRow[];
};

export type HistorialComprasFilters = {
  search: string;
  from_date: string;
  to_date: string;
};

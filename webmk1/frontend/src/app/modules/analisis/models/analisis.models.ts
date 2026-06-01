export type AnaliticaClienteRow = {
  cliente: string;
  ops: number;
  facturas: number;
  bolsas: number;
  total_gs: number;
  ticket_prom: number;
  last_ts: string | null;
};

export type AnaliticaClientesResponse = {
  top_n: number;
  total_clientes: number;
  total_gs: number;
  rows: AnaliticaClienteRow[];
};

export type ProyeccionCompraRow = {
  producto: string;
  stock_kg: number;
  consumo_diario: number;
  dias_restantes: number | null;
  consumo_total: number;
  dias_activos: number;
};

export type ProyeccionComprasResponse = {
  ventana_dias: number;
  top_n: number | null;
  rows: ProyeccionCompraRow[];
};


export type PrecioRow = {
  product_id: number;
  producto: string;
  gramaje: number;
  price_gs: number;
  iva: number;
  paquetes_stock: number;
};

export type ProductoPrecioSummary = {
  rows: PrecioRow[];
};

export type PrecioUpdate = {
  product_id: number;
  gramaje: number;
  price_gs: number;
  iva: number;
};

export type PrecioHistoryRow = {
  fecha: string;
  price_gs: number;
  iva: number;
};

export type PrecioHistoryResponse = {
  product_id: number;
  producto: string;
  gramaje: number;
  rows: PrecioHistoryRow[];
};

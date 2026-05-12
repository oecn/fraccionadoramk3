export type VentaStockItem = {
  product_id: number;
  producto: string;
  gramaje: number;
  paquetes: number;
  price_gs: number | null;
  iva: number | null;
};

export type VentaOptions = {
  stock: VentaStockItem[];
  hoy: string;
};

export type VentaItemCreate = {
  product_id: number;
  gramaje: number;
  cantidad: number;
};

export type VentaCreate = {
  invoice_no: string;
  customer: string;
  fecha: string;
  send_to_sheet: boolean;
  items: VentaItemCreate[];
};

export type VentaResumen = {
  invoice_id: number;
  gravada5_gs: number;
  iva5_gs: number;
  gravada10_gs: number;
  iva10_gs: number;
  total_gs: number;
  sheet_sent: boolean;
  sheet_error: string | null;
};

export type VentaCartItem = VentaStockItem & {
  cantidad: number;
};

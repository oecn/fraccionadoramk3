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
  price_gs?: number | null;
  iva?: number | null;
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

export type FacturaVentaParseItem = {
  linea: number;
  descripcion: string;
  cantidad: number;
  precio_unitario_gs: number;
  iva: number;
  total_linea_gs: number;
  gravada5_gs: number;
  gravada10_gs: number;
  exenta_gs: number;
};

export type FacturaVentaParsePreview = {
  numero: string;
  fecha_emision: string;
  cliente: string;
  ruc_cliente: string;
  condicion_venta: string;
  gravada5_gs: number;
  iva5_gs: number;
  gravada10_gs: number;
  iva10_gs: number;
  total_iva_gs: number;
  total_gs: number;
  items: FacturaVentaParseItem[];
};

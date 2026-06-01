export type EstadisticaKpi = {
  title: string;
  value: string;
  numeric: number;
  subtitle: string;
  tone: 'neutral' | 'good' | 'warn' | 'bad';
};

export type EstadisticaProductoRow = {
  producto: string;
  venta_gs: number;
  costo_estimado_gs: number;
  margen_gs: number;
  margen_pct: number;
  paquetes: number;
  unidades: number;
  kg_vendidos: number;
  merma_kg: number;
  merma_gs: number;
};

export type EstadisticaInventarioRow = {
  producto: string;
  stock_kg: number;
  valor_materia_prima_gs: number;
  valor_producto_terminado_gs: number;
  consumo_diario_kg: number;
  dias_cobertura: number | null;
  estado: string;
};

export type EstadisticaClienteRow = {
  cliente: string;
  facturas: number;
  total_gs: number;
  ticket_promedio_gs: number;
  ultima_compra: string | null;
  dias_sin_comprar: number | null;
};

export type EstadisticaCajaRow = {
  concepto: string;
  monto_gs: number;
  tipo: string;
  detalle: string;
};

export type EstadisticaAlertaDetalle = {
  label: string;
  value: string;
  monto_gs: number | null;
};

export type EstadisticaAlerta = {
  tipo: string;
  severidad: 'alta' | 'media' | 'baja';
  titulo: string;
  detalle: string;
  monto_gs: number | null;
  accion: string;
  detalles: EstadisticaAlertaDetalle[];
};

export type EstadisticasResumen = {
  from_date: string;
  to_date: string;
  kpis: EstadisticaKpi[];
  alertas: EstadisticaAlerta[];
  productos: EstadisticaProductoRow[];
  inventario: EstadisticaInventarioRow[];
  clientes: EstadisticaClienteRow[];
  caja: EstadisticaCajaRow[];
  updated_at: string;
  source: string;
};

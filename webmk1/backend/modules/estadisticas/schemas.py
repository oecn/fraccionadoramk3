from pydantic import BaseModel


class EstadisticaKpi(BaseModel):
    title: str
    value: str
    numeric: float = 0
    subtitle: str = ""
    tone: str = "neutral"


class EstadisticaProductoRow(BaseModel):
    producto: str
    venta_gs: float
    costo_estimado_gs: float
    margen_gs: float
    margen_pct: float
    paquetes: int
    unidades: int
    kg_vendidos: float
    merma_kg: float
    merma_gs: float


class EstadisticaInventarioRow(BaseModel):
    producto: str
    stock_kg: float
    valor_materia_prima_gs: float
    valor_producto_terminado_gs: float
    consumo_diario_kg: float
    dias_cobertura: float | None
    estado: str


class EstadisticaClienteRow(BaseModel):
    cliente: str
    facturas: int
    total_gs: float
    ticket_promedio_gs: float
    ultima_compra: str | None
    dias_sin_comprar: int | None


class EstadisticaCajaRow(BaseModel):
    concepto: str
    monto_gs: float
    tipo: str
    detalle: str = ""


class EstadisticaAlertaDetalle(BaseModel):
    label: str
    value: str
    monto_gs: float | None = None


class EstadisticaAlerta(BaseModel):
    tipo: str
    severidad: str
    titulo: str
    detalle: str
    monto_gs: float | None = None
    accion: str
    detalles: list[EstadisticaAlertaDetalle] = []


class EstadisticasResumen(BaseModel):
    from_date: str
    to_date: str
    kpis: list[EstadisticaKpi]
    alertas: list[EstadisticaAlerta]
    productos: list[EstadisticaProductoRow]
    inventario: list[EstadisticaInventarioRow]
    clientes: list[EstadisticaClienteRow]
    caja: list[EstadisticaCajaRow]
    updated_at: str
    source: str = "PostgreSQL"

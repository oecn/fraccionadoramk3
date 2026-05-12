from pydantic import BaseModel


class ProductoItem(BaseModel):
    id: int
    name: str


class ReportesVentasOptions(BaseModel):
    productos: list[ProductoItem]
    meses: list[str]
    gramajes: list[int]


class KpiCard(BaseModel):
    key: str
    label: str
    value: float
    delta_pct: float | None = None


class RankingRow(BaseModel):
    label: str
    paquetes: float
    importe_gs: float


class ReporteVentaRow(BaseModel):
    periodo: str
    producto: str
    gramaje: int
    paquetes: float
    importe_gs: float
    base_gs: float
    iva_gs: float
    paq_luque: float
    paq_aregua: float
    paq_itaugua: float
    share_pct: float
    delta_paquetes_pct: float | None = None
    delta_importe_pct: float | None = None


class ReportesVentasResumen(BaseModel):
    periodo: str
    filtros_label: str
    kpis: list[KpiCard]
    top_productos: list[RankingRow]
    top_sucursales: list[RankingRow]
    rows: list[ReporteVentaRow]


class FacturaDetalleRow(BaseModel):
    fecha: str
    nro_factura: str
    cliente: str
    producto: str
    gramaje: int
    paquetes: float
    precio_unit: float
    importe_gs: float
    invoice_id: int | None = None


class FacturaItemsResponse(BaseModel):
    rows: list[FacturaDetalleRow]

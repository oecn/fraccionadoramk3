from pydantic import BaseModel


class OrdenCompraItem(BaseModel):
    linea: int
    descripcion: str
    cantidad: int | float | None
    unidad: str | None = None
    stock_disponible: int | None = None
    stock_ok: bool = False
    producto_match: str = ""
    gramaje: int | None = None


class OrdenCompraImportResponse(BaseModel):
    oc_id: int
    nro_oc: str
    sucursal: str
    fecha_pedido: str
    monto_total: float | None = None
    items: list[OrdenCompraItem]
    message: str


class OrdenCompraRow(BaseModel):
    oc_id: int
    nro_oc: str
    sucursal: str
    fecha_pedido: str
    monto_total: float
    items_count: int
    completada: bool
    created_at: str
    pct_listo_envio: float


class OrdenCompraDetail(OrdenCompraRow):
    items: list[OrdenCompraItem]


class OrdenCompraDeleteResponse(BaseModel):
    oc_id: int
    deleted: bool
    message: str


class OrdenCompraStatusResponse(BaseModel):
    oc_id: int
    completada: bool
    message: str


class PendienteAcumuladoRow(BaseModel):
    descripcion: str
    necesario: int | float
    stock_disponible: int | None = None
    stock_ok: bool = False
    producto_match: str = ""
    gramaje: int | None = None


class PendientesAcumuladosSummary(BaseModel):
    total_ocs: int
    total_monto: float
    total_items: int
    items_ok: int
    items_falta: int
    pct_listo_envio: float
    rows: list[PendienteAcumuladoRow]


class BolsasPendientesRow(BaseModel):
    producto: str
    bolsa_kg: float
    necesarias: int
    disponibles: float
    stock_ok: bool


class BolsasPendientesSummary(BaseModel):
    rows: list[BolsasPendientesRow]

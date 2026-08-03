from pydantic import BaseModel, Field


class ProductoItem(BaseModel):
    id: int
    name: str
    raw_kg: float
    ultimo_costo_kg_gs: float | None = None


class RawStockRow(BaseModel):
    product_id: int
    producto: str
    kg: float


class LoteAbiertoRow(BaseModel):
    id: int
    product_id: int
    producto: str
    lote: str
    proveedor: str
    factura: str
    kg_inicial: float
    kg_saldo: float
    costo_total_gs: float
    costo_kg_gs: float
    gravada5_gs: float = 0
    iva5_gs: float = 0
    gravada10_gs: float = 0
    iva10_gs: float = 0
    exenta_gs: float = 0
    costo_kg_anterior_gs: float | None = None
    variacion_costo_pct: float | None = None
    precio_cambio_detectado: bool = False
    precio_estado: str = "Sin cambio"
    precio_revisado: bool = False
    precio_revisado_por: str = ""
    precio_revisado_at: str = ""
    diferencia_costo_kg_gs: float = 0
    diferencia_costo_total_gs: float = 0
    ts: str


class CompraMateriaPrimaOptions(BaseModel):
    productos: list[ProductoItem]
    bolsa_kg_presets: list[float]


class LotePrecioReviewHistoryRow(BaseModel):
    id: int
    lot_id: int
    product_id: int
    producto: str
    estado: str
    revisado_por: str
    costo_kg_anterior_gs: float | None = None
    costo_kg_gs: float
    diferencia_costo_kg_gs: float
    variacion_costo_pct: float | None = None
    note: str = ""
    created_at: str


class CompraMateriaPrimaSummary(BaseModel):
    raw_stock: list[RawStockRow]
    lotes_abiertos: list[LoteAbiertoRow]
    historial_revisiones_precio: list[LotePrecioReviewHistoryRow] = []


class CompraMateriaPrimaCreate(BaseModel):
    product_id: int
    fecha: str = ""
    lote: str = ""
    proveedor: str = ""
    factura: str = ""
    bolsa_kg: float = Field(gt=0)
    bolsas: float = Field(gt=0)
    costo_total_gs: float = Field(ge=0)
    iva: int = 5


class FacturaCompraItem(BaseModel):
    linea: int
    descripcion: str
    kg: float
    precio_unitario: float | None = None
    total_linea: float
    gravada5_gs: float = 0
    iva5_gs: float = 0
    gravada10_gs: float = 0
    iva10_gs: float = 0
    exenta_gs: float = 0
    product_id: int | None = None
    producto: str = ""
    bolsa_kg: float
    bolsas: int
    costo_kg_anterior_gs: float | None = None
    variacion_costo_pct: float | None = None
    importable: bool
    message: str = ""


class FacturaCompraPreview(BaseModel):
    numero: str
    proveedor: str
    fecha_emision: str
    total: float
    items: list[FacturaCompraItem]


class FacturaCompraImportItem(BaseModel):
    product_id: int
    descripcion: str
    kg: float = Field(gt=0)
    total_linea: float = Field(ge=0)
    gravada5_gs: float = 0
    iva5_gs: float = 0
    gravada10_gs: float = 0
    iva10_gs: float = 0
    exenta_gs: float = 0
    bolsa_kg: float = Field(gt=0)
    bolsas: int = Field(gt=0)


class FacturaCompraImportRequest(BaseModel):
    numero: str = ""
    proveedor: str = ""
    fecha_emision: str = ""
    items: list[FacturaCompraImportItem]


class FacturaCompraImportResponse(BaseModel):
    inserted: int
    skipped: int
    message: str
    lotes: list[LoteAbiertoRow]


class LoteDeleteRequest(BaseModel):
    motivo: str = Field(min_length=3)


class LoteDeleteResponse(BaseModel):
    deleted: bool
    lot_id: int
    message: str


class LotePrecioReviewRequest(BaseModel):
    estado: str = "Revisado y OK"
    revisado_por: str = ""


class LotePrecioReviewResponse(BaseModel):
    lot_id: int
    estado: str
    revisado_por: str
    revisado_at: str
    message: str

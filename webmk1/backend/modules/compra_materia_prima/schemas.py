from pydantic import BaseModel, Field


class ProductoItem(BaseModel):
    id: int
    name: str
    raw_kg: float


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
    ts: str


class CompraMateriaPrimaOptions(BaseModel):
    productos: list[ProductoItem]
    bolsa_kg_presets: list[float]


class CompraMateriaPrimaSummary(BaseModel):
    raw_stock: list[RawStockRow]
    lotes_abiertos: list[LoteAbiertoRow]


class CompraMateriaPrimaCreate(BaseModel):
    product_id: int
    lote: str = ""
    proveedor: str = ""
    factura: str = ""
    bolsa_kg: float = Field(gt=0)
    bolsas: float = Field(gt=0)
    costo_total_gs: float = Field(ge=0)


class FacturaCompraItem(BaseModel):
    linea: int
    descripcion: str
    kg: float
    precio_unitario: float | None = None
    total_linea: float
    product_id: int | None = None
    producto: str = ""
    bolsa_kg: float
    bolsas: int
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

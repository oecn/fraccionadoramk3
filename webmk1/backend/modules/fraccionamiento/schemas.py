from pydantic import BaseModel, Field


class ProductoItem(BaseModel):
    id: int
    name: str
    gramajes: list[int]
    raw_kg: float


class LoteItem(BaseModel):
    id: int
    product_id: int
    lote: str
    kg_saldo: float
    costo_kg_gs: float
    proveedor: str
    factura: str
    ts: str


class RawStockRow(BaseModel):
    product_id: int
    producto: str
    kg: float
    bolsas_50: float
    bolsas_25: float


class PackageStockRow(BaseModel):
    product_id: int
    producto: str
    gramaje: int
    paquetes: int


class FraccionamientoHistoryRow(BaseModel):
    id: int
    fecha: str
    product_id: int
    producto: str
    gramaje: int
    paquetes: int
    kg_consumidos: float
    bolsas_eq: str
    lote: str


class FraccionamientoOptions(BaseModel):
    productos: list[ProductoItem]
    lotes: list[LoteItem]
    hoy: str


class FraccionamientoSummary(BaseModel):
    raw_stock: list[RawStockRow]
    package_stock: list[PackageStockRow]
    history: list[FraccionamientoHistoryRow]
    total_raw_kg: float
    total_paquetes: int
    total_kg_mes: float
    total_paquetes_mes: int


class FraccionamientoCreate(BaseModel):
    product_id: int
    gramaje: int
    paquetes: int = Field(gt=0)
    fecha: str
    lot_id: int | None = None


class ConsumoPreview(BaseModel):
    kg_consumidos: float
    unidades_por_paquete: int
    bolsas_eq: str

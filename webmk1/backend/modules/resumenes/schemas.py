from pydantic import BaseModel


class ProductoOption(BaseModel):
    id: int
    name: str


class ResumenesOptions(BaseModel):
    productos: list[ProductoOption]


class LoteResumenRow(BaseModel):
    id: int
    lote: str
    producto: str
    kg_total: float
    kg_usado: float
    kg_disponible: float
    costo_total_gs: float
    costo_kg_gs: float
    proveedor: str
    factura: str
    fecha: str
    cerrado: bool


class LoteFraccionamientoRow(BaseModel):
    fecha: str
    gramaje: int
    paquetes: int
    kg_consumidos: float
    bolsas_eq: float | None
    costo_kg_gs: float
    costo_total_gs: float
    precio_venta_gs: float
    beneficio_gs: float


class RecargoPresentacionRow(BaseModel):
    gramaje: int
    kg_por_paquete: float
    precio_paquete_gs: float
    venta_kg_gs: float
    recargo_kg_gs: float
    recargo_pct: float | None


class LoteDetalle(BaseModel):
    id: int
    lote: str
    producto_id: int
    producto: str
    kg_total: float
    kg_usado: float
    kg_disponible: float
    costo_total_gs: float
    costo_kg_gs: float
    proveedor: str
    factura: str
    fecha: str
    cerrado: bool
    bolsas_total: float | None
    bolsas_usadas: float | None
    bolsas_disponibles: float | None
    merma_kg: float
    consumo_pct: float
    merma_pct: float
    venta_estimada_gs: float
    venta_kg_gs: float
    recargo_kg_gs: float
    recargo_pct: float | None
    beneficio_estimado_gs: float
    beneficio_pct: float | None
    recargos: list[RecargoPresentacionRow]
    fraccionamientos: list[LoteFraccionamientoRow]

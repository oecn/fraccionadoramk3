from pydantic import BaseModel


class CompraRow(BaseModel):
    id: int
    ts: str
    factura: str
    proveedor: str
    costo_total_gs: float


class HistorialComprasSummary(BaseModel):
    total_registros: int
    total_gs: float
    rows: list[CompraRow]

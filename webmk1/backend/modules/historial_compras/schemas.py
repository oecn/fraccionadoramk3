from pydantic import BaseModel


class CompraRow(BaseModel):
    id: int
    ts: str
    factura: str
    proveedor: str
    costo_total_gs: float
    tipo: str = "Compra"
    motivo: str = ""
    producto: str = ""


class HistorialComprasSummary(BaseModel):
    total_registros: int
    total_gs: float
    total_eliminado_gs: float = 0
    rows: list[CompraRow]

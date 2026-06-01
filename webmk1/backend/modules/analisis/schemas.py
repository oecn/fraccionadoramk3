from pydantic import BaseModel


class AnaliticaClienteRow(BaseModel):
    cliente: str
    ops: int
    facturas: int
    bolsas: int
    total_gs: float
    ticket_prom: float
    last_ts: str | None


class AnaliticaClientesResponse(BaseModel):
    top_n: int
    total_clientes: int
    total_gs: float
    rows: list[AnaliticaClienteRow]


class ProyeccionCompraRow(BaseModel):
    producto: str
    stock_kg: float
    consumo_diario: float
    dias_restantes: float | None
    consumo_total: float
    dias_activos: int


class ProyeccionComprasResponse(BaseModel):
    ventana_dias: int
    top_n: int | None
    rows: list[ProyeccionCompraRow]


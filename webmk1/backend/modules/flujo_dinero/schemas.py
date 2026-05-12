from pydantic import BaseModel


class FlujoKpi(BaseModel):
    title: str
    value: str
    subtitle: str = ""


class FlujoMonthRow(BaseModel):
    month: str
    month_no: str
    compras: float
    ventas: float
    ventas_retencion: float
    notas_credito: float
    gastos: float
    flujo: float
    margen: float
    acumulado: float


class FlujoSummary(BaseModel):
    year: int
    from_date: str
    to_date: str
    quarter: str
    retencion_mode: bool
    saldo_inicial: float
    kpis: list[FlujoKpi]
    rows: list[FlujoMonthRow]
    updated_at: str
    source: str = "PostgreSQL"


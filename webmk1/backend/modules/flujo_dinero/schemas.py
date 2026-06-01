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


class FlujoQuarterRow(BaseModel):
    quarter: str
    label: str
    start_month: str
    end_month: str
    saldo_inicio: float
    saldo_fin: float
    ventas: float
    compras: float
    gastos: float
    notas_credito: float
    flujo: float
    margen: float
    profitable: bool


class FlujoSummary(BaseModel):
    year: int
    from_date: str
    to_date: str
    quarter: str
    retencion_mode: bool
    include_iva: bool
    saldo_inicial: float
    kpis: list[FlujoKpi]
    quarter_rows: list[FlujoQuarterRow]
    rows: list[FlujoMonthRow]
    updated_at: str
    source: str = "PostgreSQL"

from pydantic import BaseModel, Field


class ExpenseCreate(BaseModel):
    fecha: str
    tipo: str = Field(min_length=1)
    descripcion: str = ""
    monto_gs: float = Field(gt=0)
    nro_factura: str = ""
    forma_pago: str = "Efectivo"
    referencia_pago: str = ""


class ExpenseRow(BaseModel):
    id: int
    fecha: str
    tipo: str
    descripcion: str
    monto_gs: float
    nro_factura: str
    forma_pago: str
    referencia_pago: str


class ExpenseSummary(BaseModel):
    rows: list[ExpenseRow]
    total_gs: float
    tipos: list[str]
    formas_pago: list[str]


class CheckStatus(BaseModel):
    available: bool
    found: bool
    used: bool
    message: str
    chequera_id: str = ""
    cheque_no: str = ""
    serie: str = ""
    referencia: str = ""


class IpsParseResult(BaseModel):
    fecha: str
    tipo: str = "IPS"
    descripcion: str
    monto_gs: float
    nro_factura: str
    forma_pago: str = "Homebanking"
    referencia_pago: str
    periodo: str = ""
    periodo_display: str = ""
    duplicate: bool = False


class RrhhImportRow(BaseModel):
    employee_id: int | None = None
    documento: str
    funcionario: str = ""
    cuenta_destino: str = ""
    concepto: str
    monto: float
    confirmado: bool = False


class RrhhParseResult(BaseModel):
    file_name: str
    fecha: str
    row_count: int
    total_amount: float
    conceptos: list[str]
    unresolved_count: int
    rows: list[RrhhImportRow]


class ImportResult(BaseModel):
    message: str
    inserted: int = 0
    skipped: int = 0

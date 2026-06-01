from pydantic import BaseModel, Field


class FacturaPendienteRow(BaseModel):
    invoice_id: int
    ts: str
    invoice_no: str
    customer: str
    total_gs: float
    cobrado_gs: float
    saldo_gs: float


class CobroFacturaItemIn(BaseModel):
    invoice_id: int
    monto_gs: float = Field(gt=0)


class CobroFacturaCreate(BaseModel):
    fecha_cobro: str = ""
    cheque_no: str = Field(min_length=1)
    boleta_deposito: str = Field(min_length=1)
    banco: str = ""
    observacion: str = ""
    items: list[CobroFacturaItemIn] = Field(min_length=1)


class CobroFacturaItemRow(BaseModel):
    id: int
    invoice_id: int
    invoice_no: str
    customer: str
    factura_total_gs: float
    monto_gs: float


class CobroFacturaRow(BaseModel):
    id: int
    fecha_cobro: str
    cheque_no: str
    boleta_deposito: str
    banco: str
    observacion: str
    total_gs: float
    created_at: str
    updated_at: str
    items: list[CobroFacturaItemRow] = []


class CobrosSummary(BaseModel):
    pendientes: list[FacturaPendienteRow]
    cobros: list[CobroFacturaRow]


from pydantic import BaseModel


class Kpi(BaseModel):
    title: str
    value: str
    subtitle: str = ""


class OrderRow(BaseModel):
    oc_id: int
    numero: str
    sucursal: str
    fecha_pedido: str
    fecha_compromiso: str
    dias_atraso: int | None
    estado: str
    prioridad: str
    pct_listo_entrega: float
    monto_total: float


class OrderDeliveryResponse(BaseModel):
    oc_id: int
    updated: bool
    message: str


class PaymentRow(BaseModel):
    factura_id: int
    proveedor: str
    numero_doc: str
    fecha_emision: str
    vencimiento: str
    dias_para_vencer: int | None
    monto: float
    estado: str
    sucursal: str


class PaymentCheckOption(BaseModel):
    cheque_no: str
    chequera_id: str
    bank_id: str
    bank_name: str
    account_no: str
    form_type: str
    check_type: str
    serie: str
    reference_value: str
    group_label: str
    label: str


class PaymentCheckStatus(BaseModel):
    available: bool
    found: bool
    used: bool
    message: str
    check: PaymentCheckOption | None = None


class PaymentRegisterRequest(BaseModel):
    lot_ids: list[int]
    fecha_pago: str
    medio: str = "Efectivo"
    referencia: str = ""
    nro_deposito: str = ""
    nro_recibo_dinero: str = ""
    observacion: str = ""
    chequera_id: str = ""
    cheque_no: str = ""
    serie: str = ""


class PaymentRegisterResponse(BaseModel):
    payment_group_id: str
    facturas: int
    total_gs: float
    medio: str
    referencia: str


class PaymentDetailRow(BaseModel):
    id: int
    payment_group_id: str
    lot_id: int
    proveedor: str
    factura: str
    monto_gs: float
    fecha_pago: str
    medio: str
    referencia: str
    nro_recibo_dinero: str
    observacion: str
    total_grupo_gs: float
    ts_registro: str


class PaymentReceiptUpdateRequest(BaseModel):
    detail_ids: list[int]
    nro_recibo_dinero: str


class PaymentReceiptUpdateResponse(BaseModel):
    updated: int
    nro_recibo_dinero: str


class CollectionRow(BaseModel):
    invoice_id: int
    ts: str
    invoice_no: str
    customer: str
    gravada5_gs: float
    iva5_gs: float
    gravada10_gs: float
    iva10_gs: float
    total_gs: float
    total_con_retencion: float
    dias_sin_cobrar: int | None
    dias_para_cobro: int | None


class DashboardSummary(BaseModel):
    kpis: list[Kpi]
    orders: list[OrderRow]
    payments: list[PaymentRow]
    collections: list[CollectionRow]
    trend_7: int
    trend_30: int
    updated_at: str
    source: str = "PostgreSQL"

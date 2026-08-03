from pydantic import BaseModel, Field


class VentaStockItem(BaseModel):
    product_id: int
    producto: str
    gramaje: int
    paquetes: int
    price_gs: float | None = None
    iva: int | None = None


class VentaOptions(BaseModel):
    stock: list[VentaStockItem]
    hoy: str


class VentaItemCreate(BaseModel):
    product_id: int
    gramaje: int
    cantidad: int = Field(gt=0)
    price_gs: float | None = None
    iva: int | None = None


class VentaCreate(BaseModel):
    invoice_no: str = ""
    customer: str = ""
    fecha: str = ""
    send_to_sheet: bool = True
    items: list[VentaItemCreate]


class VentaResumen(BaseModel):
    invoice_id: int
    gravada5_gs: float
    iva5_gs: float
    gravada10_gs: float
    iva10_gs: float
    total_gs: float
    sheet_sent: bool = False
    sheet_error: str | None = None


class FacturaVentaParseItem(BaseModel):
    linea: int
    descripcion: str
    cantidad: float
    precio_unitario_gs: float
    iva: int
    total_linea_gs: float
    gravada5_gs: float = 0
    gravada10_gs: float = 0
    exenta_gs: float = 0


class FacturaVentaParsePreview(BaseModel):
    numero: str
    fecha_emision: str
    cliente: str
    ruc_cliente: str
    condicion_venta: str
    gravada5_gs: float = 0
    iva5_gs: float = 0
    gravada10_gs: float = 0
    iva10_gs: float = 0
    total_iva_gs: float = 0
    total_gs: float = 0
    items: list[FacturaVentaParseItem]

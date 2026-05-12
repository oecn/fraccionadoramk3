from pydantic import BaseModel


class VentaRow(BaseModel):
    id: int
    ts: str
    invoice_no: str
    customer: str
    gravada5_gs: float
    iva5_gs: float
    gravada10_gs: float
    iva10_gs: float
    total_gs: float
    total_con_retencion: float


class HistorialVentasSummary(BaseModel):
    total_registros: int
    total_gs: float
    total_con_retencion: float
    rows: list[VentaRow]


class TopProductoRow(BaseModel):
    producto: str
    gramaje: int
    paquetes: int
    total_gs: float


class ProductoItem(BaseModel):
    id: int
    name: str


class VentaDetalleRow(BaseModel):
    periodo: str
    producto: str
    gramaje: int
    paquetes: float
    importe_gs: float
    base_gs: float
    iva_gs: float
    paq_luque: float
    paq_aregua: float
    paq_itaugua: float


class SucursalRow(BaseModel):
    sucursal: str
    importe_gs: float
    paquetes: float


class VentasDetalleResponse(BaseModel):
    rows: list[VentaDetalleRow]
    sucursales: list[SucursalRow]
    total_paquetes: float
    total_importe_gs: float


class ReporteMensualData(BaseModel):
    ym: str
    empresa: str
    d1: str
    d2: str
    ventas_facturas: float
    ventas_bolsas: float
    ventas_total: float
    compras_total: float
    gastos_total: float
    margen_bruto: float
    margen_bruto_pct: float
    beneficio_operativo: float
    beneficio_pct: float
    cant_facturas: int
    cant_ventas_bolsa: int
    top_productos: list[TopProductoRow]
    reporte_txt: str

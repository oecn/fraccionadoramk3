from pydantic import BaseModel


class InventoryRawRow(BaseModel):
    product_id: int
    producto: str
    kg: float
    alerta_min_kg: float | None = None
    alerta_min_bolsas: float | None = None
    reposicion_bolsas: float | None = None
    alerta_bolsa_kg: float = 50
    alerta_estado: str = "normal"
    proveedor_whatsapp: str = ""
    bolsas_50: float
    bolsas_25: float
    lotes_abiertos: int
    costo_promedio_gs: float
    valor_stock_gs: float


class InventoryPackageRow(BaseModel):
    product_id: int
    producto: str
    gramaje: int
    paquetes: int
    unidades: int
    price_gs: float | None
    iva: int | None
    valor_venta_gs: float


class InventoryLotRow(BaseModel):
    id: int
    product_id: int
    producto: str
    lote: str
    proveedor: str
    factura: str
    kg_saldo: float
    costo_kg_gs: float
    valor_saldo_gs: float
    ts: str


class InventorySummary(BaseModel):
    raw_stock: list[InventoryRawRow]
    package_stock: list[InventoryPackageRow]
    lotes_abiertos: list[InventoryLotRow]
    raw_alerts_count: int = 0
    total_raw_kg: float
    total_raw_valor_gs: float
    total_paquetes: int
    total_unidades: int
    total_venta_gs: float


class RawStockAlertUpdate(BaseModel):
    product_id: int
    min_kg: float | None = None
    reposicion_bolsas: float | None = None
    proveedor_whatsapp: str = ""


class RawStockAlertsUpdate(BaseModel):
    alerts: list[RawStockAlertUpdate]


class RawStockAdjustment(BaseModel):
    product_id: int
    kg: float


class PackageStockAdjustment(BaseModel):
    product_id: int
    gramaje: int
    paquetes: int


class InventoryAdjustmentUpdate(BaseModel):
    raw_stock: list[RawStockAdjustment] = []
    package_stock: list[PackageStockAdjustment] = []
    motivo: str = ""

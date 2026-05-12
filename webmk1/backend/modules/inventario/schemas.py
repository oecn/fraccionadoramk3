from pydantic import BaseModel


class InventoryRawRow(BaseModel):
    product_id: int
    producto: str
    kg: float
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
    total_raw_kg: float
    total_raw_valor_gs: float
    total_paquetes: int
    total_unidades: int
    total_venta_gs: float

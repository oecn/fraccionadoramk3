from fastapi import APIRouter

from modules.inventario.repository import InventarioRepository
from modules.inventario.schemas import InventoryAdjustmentUpdate, InventorySummary, RawStockAlertsUpdate


router = APIRouter(prefix="/inventario", tags=["inventario"])


@router.get("/summary", response_model=InventorySummary)
def summary() -> InventorySummary:
    return InventarioRepository().summary()


@router.put("/raw-alerts", response_model=InventorySummary)
def update_raw_alerts(payload: RawStockAlertsUpdate) -> InventorySummary:
    return InventarioRepository().update_alerts(payload)


@router.put("/adjustments", response_model=InventorySummary)
def adjust_inventory(payload: InventoryAdjustmentUpdate) -> InventorySummary:
    return InventarioRepository().adjust_inventory(payload)

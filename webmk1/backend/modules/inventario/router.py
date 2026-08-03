from fastapi import APIRouter

from modules.inventario.repository import InventarioRepository
from modules.inventario.schemas import (
    FilmStockAlertsUpdate,
    InventoryAdjustmentUpdate,
    InventoryControlAuditCreate,
    InventorySummary,
    RawStockAlertsUpdate,
)


router = APIRouter(prefix="/inventario", tags=["inventario"])


@router.get("/summary", response_model=InventorySummary)
def summary() -> InventorySummary:
    return InventarioRepository().summary()


@router.put("/raw-alerts", response_model=InventorySummary)
def update_raw_alerts(payload: RawStockAlertsUpdate) -> InventorySummary:
    return InventarioRepository().update_alerts(payload)


@router.put("/film-alerts", response_model=InventorySummary)
def update_film_alerts(payload: FilmStockAlertsUpdate) -> InventorySummary:
    return InventarioRepository().update_film_alerts(payload)


@router.put("/adjustments", response_model=InventorySummary)
def adjust_inventory(payload: InventoryAdjustmentUpdate) -> InventorySummary:
    return InventarioRepository().adjust_inventory(payload)


@router.post("/weekly-count", response_model=InventorySummary)
def weekly_count(payload: InventoryControlAuditCreate) -> InventorySummary:
    return InventarioRepository().record_weekly_count(payload)

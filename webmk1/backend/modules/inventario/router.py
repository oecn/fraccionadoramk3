from fastapi import APIRouter

from modules.inventario.repository import InventarioRepository
from modules.inventario.schemas import InventorySummary


router = APIRouter(prefix="/inventario", tags=["inventario"])


@router.get("/summary", response_model=InventorySummary)
def summary() -> InventorySummary:
    return InventarioRepository().summary()

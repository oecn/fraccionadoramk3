from fastapi import APIRouter, Query

from modules.historial_compras.repository import HistorialComprasRepository
from modules.historial_compras.schemas import HistorialComprasSummary


router = APIRouter(prefix="/historial-compras", tags=["historial-compras"])


@router.get("/summary", response_model=HistorialComprasSummary)
def historial_compras_summary(
    search: str = Query(default=""),
    from_date: str = Query(default=""),
    to_date: str = Query(default=""),
) -> HistorialComprasSummary:
    return HistorialComprasRepository().summary(
        search=search,
        from_date=from_date,
        to_date=to_date,
    )

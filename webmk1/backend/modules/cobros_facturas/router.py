from fastapi import APIRouter, HTTPException, Query

from modules.cobros_facturas.repository import CobrosFacturasRepository
from modules.cobros_facturas.schemas import CobroFacturaCreate, CobroFacturaRow, CobrosSummary


router = APIRouter(prefix="/cobros-facturas", tags=["cobros-facturas"])


@router.get("/summary", response_model=CobrosSummary)
def summary(
    from_date: str = Query(default=""),
    to_date: str = Query(default=""),
) -> CobrosSummary:
    return CobrosFacturasRepository().summary(from_date=from_date, to_date=to_date)


@router.post("", response_model=CobroFacturaRow)
def create(payload: CobroFacturaCreate) -> CobroFacturaRow:
    try:
        return CobrosFacturasRepository().create(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/{collection_id}", response_model=CobroFacturaRow)
def update(collection_id: int, payload: CobroFacturaCreate) -> CobroFacturaRow:
    try:
        return CobrosFacturasRepository().update(collection_id, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

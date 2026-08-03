from fastapi import APIRouter, File, HTTPException, Query, UploadFile

from modules.cobros_facturas.repository import CobrosFacturasRepository
from modules.cobros_facturas.schemas import CobroFacturaCreate, CobroFacturaRow, CobrosSummary, ReciboPreview


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


@router.post("/recibos/parse", response_model=ReciboPreview)
def parse_recibo(file: UploadFile = File(...)) -> ReciboPreview:
    try:
        return CobrosFacturasRepository().parse_recibo_pdf(file)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.put("/{collection_id}", response_model=CobroFacturaRow)
def update(collection_id: int, payload: CobroFacturaCreate) -> CobroFacturaRow:
    try:
        return CobrosFacturasRepository().update(collection_id, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

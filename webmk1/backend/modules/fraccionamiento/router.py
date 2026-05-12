from fastapi import APIRouter, HTTPException, Query

from modules.fraccionamiento.repository import FraccionamientoRepository
from modules.fraccionamiento.schemas import (
    ConsumoPreview,
    FraccionamientoCreate,
    FraccionamientoHistoryRow,
    FraccionamientoOptions,
    FraccionamientoSummary,
)


router = APIRouter(prefix="/fraccionamiento", tags=["fraccionamiento"])


@router.get("/options", response_model=FraccionamientoOptions)
def options() -> FraccionamientoOptions:
    return FraccionamientoRepository().options()


@router.get("/summary", response_model=FraccionamientoSummary)
def summary(
    product_id: int | None = Query(default=None),
    desde: str | None = Query(default=None),
    hasta: str | None = Query(default=None),
) -> FraccionamientoSummary:
    return FraccionamientoRepository().summary(product_id=product_id, desde=desde, hasta=hasta)


@router.get("/preview", response_model=ConsumoPreview)
def preview(product_id: int, gramaje: int, paquetes: int) -> ConsumoPreview:
    return FraccionamientoRepository().preview(product_id=product_id, gramaje=gramaje, paquetes=paquetes)


@router.post("/registrar", response_model=FraccionamientoHistoryRow)
def registrar(payload: FraccionamientoCreate) -> FraccionamientoHistoryRow:
    try:
        return FraccionamientoRepository().create(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

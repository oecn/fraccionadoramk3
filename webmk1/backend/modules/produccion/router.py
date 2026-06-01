from fastapi import APIRouter, HTTPException, Query

from modules.produccion.repository import ProduccionRepository
from modules.produccion.schemas import ProduccionOptions, ProduccionResumen


router = APIRouter(prefix="/produccion", tags=["produccion"])


@router.get("/options", response_model=ProduccionOptions)
def options() -> ProduccionOptions:
    return ProduccionRepository().options()


@router.get("/resumen", response_model=ProduccionResumen)
def resumen(
    year: int = Query(ge=2000, le=2100),
    month: int = Query(ge=1, le=12),
    range_months: int = Query(default=12, ge=1, le=36),
) -> ProduccionResumen:
    try:
        return ProduccionRepository().resumen(year=year, month=month, range_months=range_months)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

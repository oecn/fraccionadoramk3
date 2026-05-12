from fastapi import APIRouter, Query

from modules.flujo_dinero.repository import FlujoDineroRepository
from modules.flujo_dinero.schemas import FlujoSummary


router = APIRouter(prefix="/flujo-dinero", tags=["flujo-dinero"])


@router.get("/years", response_model=list[int])
def available_years() -> list[int]:
    return FlujoDineroRepository().available_years()


@router.get("/summary", response_model=FlujoSummary)
def flujo_summary(
    year: int | None = Query(default=None),
    quarter: str = Query(default="Todos"),
    retencion_mode: bool = Query(default=False),
    from_date: str = Query(default=""),
    to_date: str = Query(default=""),
) -> FlujoSummary:
    return FlujoDineroRepository().summary(
        year=year,
        quarter=quarter,
        retencion_mode=retencion_mode,
        from_date=from_date,
        to_date=to_date,
    )


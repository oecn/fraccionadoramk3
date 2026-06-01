from fastapi import APIRouter, Query

from modules.estadisticas.repository import EstadisticasRepository
from modules.estadisticas.schemas import EstadisticasResumen


router = APIRouter(prefix="/estadisticas", tags=["estadisticas"])


@router.get("/resumen", response_model=EstadisticasResumen)
def resumen(
    from_date: str = Query(default=""),
    to_date: str = Query(default=""),
    top_n: int = Query(default=12, ge=1, le=50),
) -> EstadisticasResumen:
    return EstadisticasRepository().resumen(from_date=from_date, to_date=to_date, top_n=top_n)

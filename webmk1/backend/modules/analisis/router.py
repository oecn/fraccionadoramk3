from fastapi import APIRouter, Query

from modules.analisis.repository import AnalisisRepository
from modules.analisis.schemas import AnaliticaClientesResponse, ProyeccionComprasResponse


router = APIRouter(prefix="/analisis", tags=["analisis"])


@router.get("/clientes", response_model=AnaliticaClientesResponse)
def analitica_clientes(top_n: int = Query(default=15, ge=1, le=200)) -> AnaliticaClientesResponse:
    return AnalisisRepository().analitica_clientes(top_n=top_n)


@router.get("/proyeccion-compras", response_model=ProyeccionComprasResponse)
def proyeccion_compras(
    ventana_dias: int = Query(default=30, ge=1, le=365),
    top_n: int | None = Query(default=None, ge=1, le=200),
) -> ProyeccionComprasResponse:
    return AnalisisRepository().proyeccion_compras(ventana_dias=ventana_dias, top_n=top_n)


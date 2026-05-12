from fastapi import APIRouter, Query

from modules.reportes_ventas.repository import ReportesVentasRepository
from modules.reportes_ventas.schemas import FacturaItemsResponse, ReportesVentasOptions, ReportesVentasResumen


router = APIRouter(prefix="/reportes-ventas", tags=["reportes-ventas"])


@router.get("/options", response_model=ReportesVentasOptions)
def options() -> ReportesVentasOptions:
    return ReportesVentasRepository().options()


@router.get("/resumen", response_model=ReportesVentasResumen)
def resumen(
    periodo: str = Query(default="mes"),
    product_id: int | None = Query(default=None),
    gramaje: int | None = Query(default=None),
    ym: str | None = Query(default=None),
    desde: str | None = Query(default=None),
    hasta: str | None = Query(default=None),
    ranking_scope: str = Query(default="month"),
    ranking_ym: str | None = Query(default=None),
) -> ReportesVentasResumen:
    return ReportesVentasRepository().resumen(
        periodo=periodo,
        product_id=product_id,
        gramaje=gramaje,
        ym=ym,
        desde=desde,
        hasta=hasta,
        ranking_scope=ranking_scope,
        ranking_ym=ranking_ym,
    )


@router.get("/detalle-facturas", response_model=FacturaItemsResponse)
def detalle_facturas(
    product_id: int | None = Query(default=None),
    gramaje: int | None = Query(default=None),
    ym: str | None = Query(default=None),
    desde: str | None = Query(default=None),
    hasta: str | None = Query(default=None),
) -> FacturaItemsResponse:
    return ReportesVentasRepository().detalle_facturas(
        product_id=product_id,
        gramaje=gramaje,
        ym=ym,
        desde=desde,
        hasta=hasta,
    )

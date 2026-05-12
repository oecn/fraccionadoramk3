from fastapi import APIRouter, Query

from modules.historial_ventas.repository import HistorialVentasRepository
from modules.historial_ventas.schemas import (
    HistorialVentasSummary,
    ProductoItem,
    ReporteMensualData,
    VentasDetalleResponse,
)


router = APIRouter(prefix="/historial-ventas", tags=["historial-ventas"])


@router.get("/summary", response_model=HistorialVentasSummary)
def historial_ventas_summary(
    search: str = Query(default=""),
    from_date: str = Query(default=""),
    to_date: str = Query(default=""),
    retencion_mode: bool = Query(default=False),
) -> HistorialVentasSummary:
    return HistorialVentasRepository().summary(
        search=search,
        from_date=from_date,
        to_date=to_date,
        retencion_mode=retencion_mode,
    )


@router.get("/reporte-mensual", response_model=ReporteMensualData)
def reporte_mensual(
    ym: str = Query(description="Periodo en formato YYYY-MM"),
    empresa: str = Query(default="Fraccionadora"),
) -> ReporteMensualData:
    return HistorialVentasRepository().reporte_mensual(ym=ym, empresa=empresa)


@router.get("/productos", response_model=list[ProductoItem])
def list_productos() -> list[ProductoItem]:
    return HistorialVentasRepository().list_productos()


@router.get("/meses", response_model=list[str])
def list_meses() -> list[str]:
    return HistorialVentasRepository().list_meses()


@router.get("/detalle", response_model=VentasDetalleResponse)
def ventas_detalle(
    periodo: str = Query(default="mes"),
    product_id: int | None = Query(default=None),
    gramaje: int | None = Query(default=None),
    ym: str | None = Query(default=None),
    desde: str | None = Query(default=None),
    hasta: str | None = Query(default=None),
) -> VentasDetalleResponse:
    return HistorialVentasRepository().ventas_detalle(
        periodo=periodo,
        product_id=product_id,
        gramaje=gramaje,
        ym=ym,
        desde=desde,
        hasta=hasta,
    )

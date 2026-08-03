from fastapi import APIRouter, File, HTTPException, UploadFile

from modules.ventas_paquetes.repository import VentasPaquetesRepository
from modules.ventas_paquetes.schemas import (
    FacturaVentaParsePreview,
    VentaCreate,
    VentaOptions,
    VentaResumen,
)


router = APIRouter(prefix="/ventas-paquetes", tags=["ventas-paquetes"])


@router.get("/options", response_model=VentaOptions)
def options() -> VentaOptions:
    return VentasPaquetesRepository().options()


@router.post("/facturas/parse", response_model=FacturaVentaParsePreview)
def parse_factura(file: UploadFile = File(...)) -> FacturaVentaParsePreview:
    try:
        return VentasPaquetesRepository().parse_factura_pdf(file)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/facturas", response_model=VentaResumen)
def create(payload: VentaCreate) -> VentaResumen:
    try:
        return VentasPaquetesRepository().create(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

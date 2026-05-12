from fastapi import APIRouter, HTTPException

from modules.ventas_paquetes.repository import VentasPaquetesRepository
from modules.ventas_paquetes.schemas import VentaCreate, VentaOptions, VentaResumen


router = APIRouter(prefix="/ventas-paquetes", tags=["ventas-paquetes"])


@router.get("/options", response_model=VentaOptions)
def options() -> VentaOptions:
    return VentasPaquetesRepository().options()


@router.post("/facturas", response_model=VentaResumen)
def create(payload: VentaCreate) -> VentaResumen:
    try:
        return VentasPaquetesRepository().create(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

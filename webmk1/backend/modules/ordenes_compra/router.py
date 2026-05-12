from fastapi import APIRouter, File, HTTPException, UploadFile

from modules.ordenes_compra.repository import OrdenesCompraRepository
from modules.ordenes_compra.schemas import (
    BolsasPendientesSummary,
    OrdenCompraDeleteResponse,
    OrdenCompraDetail,
    OrdenCompraImportResponse,
    OrdenCompraRow,
    OrdenCompraStatusResponse,
    PendientesAcumuladosSummary,
)


router = APIRouter(prefix="/ordenes-compra", tags=["ordenes-compra"])


@router.get("", response_model=list[OrdenCompraRow])
def list_imported() -> list[OrdenCompraRow]:
    return OrdenesCompraRepository().list_imported()


@router.get("/pendientes/acumulados", response_model=PendientesAcumuladosSummary)
def pendientes_acumulados() -> PendientesAcumuladosSummary:
    return OrdenesCompraRepository().pendientes_acumulados()


@router.get("/pendientes/bolsas", response_model=BolsasPendientesSummary)
def bolsas_pendientes() -> BolsasPendientesSummary:
    return OrdenesCompraRepository().bolsas_pendientes()


@router.get("/{oc_id}", response_model=OrdenCompraDetail)
def detail(oc_id: int) -> OrdenCompraDetail:
    try:
        return OrdenesCompraRepository().detail(oc_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/importar-pdf", response_model=OrdenCompraImportResponse)
def importar_pdf(file: UploadFile = File(...)) -> OrdenCompraImportResponse:
    try:
        return OrdenesCompraRepository().import_pdf(file)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.delete("/{oc_id}", response_model=OrdenCompraDeleteResponse)
def delete(oc_id: int) -> OrdenCompraDeleteResponse:
    try:
        return OrdenesCompraRepository().delete(oc_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{oc_id}/status", response_model=OrdenCompraStatusResponse)
def set_status(oc_id: int, completada: bool) -> OrdenCompraStatusResponse:
    try:
        return OrdenesCompraRepository().set_status(oc_id, completada)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

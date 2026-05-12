from fastapi import APIRouter, HTTPException, Query

from modules.resumenes.repository import ResumenesRepository
from modules.resumenes.schemas import LoteDetalle, LoteResumenRow, ResumenesOptions


router = APIRouter(prefix="/resumenes", tags=["resumenes"])


@router.get("/options", response_model=ResumenesOptions)
def options() -> ResumenesOptions:
    return ResumenesRepository().options()


@router.get("/lotes", response_model=list[LoteResumenRow])
def lotes(
    product_id: int | None = Query(default=None),
    solo_abiertos: bool = Query(default=False),
) -> list[LoteResumenRow]:
    return ResumenesRepository().lotes(product_id=product_id, solo_abiertos=solo_abiertos)


@router.get("/lotes/{lot_id}", response_model=LoteDetalle)
def detalle(lot_id: int) -> LoteDetalle:
    try:
        return ResumenesRepository().detalle(lot_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/lotes/{lot_id}/cerrar", response_model=LoteDetalle)
def cerrar(lot_id: int) -> LoteDetalle:
    repo = ResumenesRepository()
    try:
        repo.set_cerrado(lot_id, True)
        return repo.detalle(lot_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/lotes/{lot_id}/abrir", response_model=LoteDetalle)
def abrir(lot_id: int) -> LoteDetalle:
    repo = ResumenesRepository()
    try:
        repo.set_cerrado(lot_id, False)
        return repo.detalle(lot_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

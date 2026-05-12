from fastapi import APIRouter, HTTPException

from modules.productos.repository import ProductosRepository
from modules.productos.schemas import PrecioHistoryResponse, PrecioRow, PrecioUpdate, ProductoPrecioSummary


router = APIRouter(prefix="/productos", tags=["productos"])


@router.get("/precios", response_model=ProductoPrecioSummary)
def precios() -> ProductoPrecioSummary:
    return ProductosRepository().precios()


@router.put("/precios", response_model=PrecioRow)
def update_precio(payload: PrecioUpdate) -> PrecioRow:
    try:
        return ProductosRepository().update_precio(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/precios/{product_id}/{gramaje}/history", response_model=PrecioHistoryResponse)
def history(product_id: int, gramaje: int) -> PrecioHistoryResponse:
    try:
        return ProductosRepository().history(product_id, gramaje)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

from fastapi import APIRouter, File, HTTPException, Query, UploadFile

from modules.compra_materia_prima.repository import CompraMateriaPrimaRepository
from modules.compra_materia_prima.schemas import (
    CompraMateriaPrimaCreate,
    CompraMateriaPrimaOptions,
    CompraMateriaPrimaSummary,
    FacturaCompraImportRequest,
    FacturaCompraImportResponse,
    FacturaCompraPreview,
    LoteAbiertoRow,
)


router = APIRouter(prefix="/compra-materia-prima", tags=["compra-materia-prima"])


@router.get("/options", response_model=CompraMateriaPrimaOptions)
def options() -> CompraMateriaPrimaOptions:
    return CompraMateriaPrimaRepository().options()


@router.get("/summary", response_model=CompraMateriaPrimaSummary)
def summary(product_id: int | None = Query(default=None)) -> CompraMateriaPrimaSummary:
    return CompraMateriaPrimaRepository().summary(product_id=product_id)


@router.post("/compras", response_model=LoteAbiertoRow)
def create(payload: CompraMateriaPrimaCreate) -> LoteAbiertoRow:
    try:
        return CompraMateriaPrimaRepository().create(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/facturas/parse", response_model=FacturaCompraPreview)
def parse_factura(file: UploadFile = File(...)) -> FacturaCompraPreview:
    try:
        return CompraMateriaPrimaRepository().parse_factura_pdf(file)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/facturas/import", response_model=FacturaCompraImportResponse)
def import_factura(payload: FacturaCompraImportRequest) -> FacturaCompraImportResponse:
    try:
        return CompraMateriaPrimaRepository().import_factura(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

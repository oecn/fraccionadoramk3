import tempfile
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, Query, UploadFile

from modules.gastos_egresos.repository import GastosEgresosRepository
from modules.gastos_egresos.schemas import (
    CheckStatus,
    ExpenseCreate,
    ExpenseRow,
    ExpenseSummary,
    ImportResult,
    IpsParseResult,
    RrhhParseResult,
)


router = APIRouter(prefix="/gastos-egresos", tags=["gastos-egresos"])


@router.get("/summary", response_model=ExpenseSummary)
def summary(desde: str = Query(default=""), hasta: str = Query(default="")) -> ExpenseSummary:
    return GastosEgresosRepository().summary(desde=desde, hasta=hasta)


@router.get("/check-status", response_model=CheckStatus)
def check_status(serie: str = Query(default=""), cheque_no: str = Query(default="")) -> CheckStatus:
    return GastosEgresosRepository().check_status(serie=serie, cheque_no=cheque_no)


@router.post("/gastos", response_model=ExpenseRow)
def create(payload: ExpenseCreate) -> ExpenseRow:
    try:
        return GastosEgresosRepository().create(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/ips/parse", response_model=IpsParseResult)
async def parse_ips(file: UploadFile = File(...)) -> IpsParseResult:
    suffix = Path(file.filename or "ips.pdf").suffix or ".pdf"
    tmp_name = ""
    try:
        content = await file.read()
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(content)
            tmp_name = tmp.name
        return GastosEgresosRepository().parse_ips_pdf(tmp_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"No se pudo parsear el PDF IPS: {exc}") from exc
    finally:
        if tmp_name:
            Path(tmp_name).unlink(missing_ok=True)


@router.post("/ips/import", response_model=ImportResult)
def import_ips(payload: IpsParseResult) -> ImportResult:
    try:
        return GastosEgresosRepository().import_ips(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/rrhh/parse", response_model=RrhhParseResult)
async def parse_rrhh(file: UploadFile = File(...)) -> RrhhParseResult:
    suffix = Path(file.filename or "rrhh.txt").suffix or ".txt"
    tmp_name = ""
    try:
        content = await file.read()
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(content)
            tmp_name = tmp.name
        return GastosEgresosRepository().parse_rrhh_txt(tmp_name, file.filename or Path(tmp_name).name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"No se pudo parsear el TXT RRHH: {exc}") from exc
    finally:
        if tmp_name:
            Path(tmp_name).unlink(missing_ok=True)


@router.post("/rrhh/import", response_model=ImportResult)
def import_rrhh(payload: RrhhParseResult) -> ImportResult:
    try:
        return GastosEgresosRepository().import_rrhh(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

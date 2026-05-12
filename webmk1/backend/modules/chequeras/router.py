from fastapi import APIRouter, HTTPException

from modules.chequeras.repository import ChequerasRepository
from modules.chequeras.schemas import BankCreate, BankRow, CheckbookCreate, CheckbookRow, ChequerasSummary


router = APIRouter(prefix="/chequeras", tags=["chequeras"])


@router.get("/summary", response_model=ChequerasSummary)
def summary() -> ChequerasSummary:
    return ChequerasRepository().summary()


@router.post("/banks", response_model=BankRow)
def create_bank(payload: BankCreate) -> BankRow:
    try:
        return ChequerasRepository().create_bank(payload)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/checkbooks", response_model=CheckbookRow)
def create_checkbook(payload: CheckbookCreate) -> CheckbookRow:
    try:
        return ChequerasRepository().create_checkbook(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

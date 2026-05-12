from fastapi import APIRouter, HTTPException, Query

from modules.dashboard.repository import DashboardRepository
from modules.dashboard.schemas import (
    DashboardSummary,
    OrderDeliveryResponse,
    PaymentCheckOption,
    PaymentCheckStatus,
    PaymentDetailRow,
    PaymentReceiptUpdateRequest,
    PaymentReceiptUpdateResponse,
    PaymentRegisterRequest,
    PaymentRegisterResponse,
)


router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/sucursales", response_model=list[str])
def list_sucursales() -> list[str]:
    return DashboardRepository().list_sucursales()


@router.get("/summary", response_model=DashboardSummary)
def dashboard_summary(
    sucursal: str = Query(default=""),
    search: str = Query(default=""),
    from_date: str = Query(default=""),
    to_date: str = Query(default=""),
) -> DashboardSummary:
    return DashboardRepository().summary(
        sucursal=sucursal,
        search=search,
        from_date=from_date,
        to_date=to_date,
    )


@router.post("/orders/{oc_id}/delivered", response_model=OrderDeliveryResponse)
def mark_order_delivered(oc_id: int) -> OrderDeliveryResponse:
    try:
        return DashboardRepository().mark_order_delivered(oc_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/payment-checks", response_model=list[PaymentCheckOption])
def payment_checks() -> list[PaymentCheckOption]:
    return DashboardRepository().load_available_payment_checks()


@router.get("/payment-check-status", response_model=PaymentCheckStatus)
def payment_check_status(serie: str = Query(default=""), cheque_no: str = Query(default="")) -> PaymentCheckStatus:
    return DashboardRepository().payment_check_status(serie=serie, cheque_no=cheque_no)


@router.post("/payments/register", response_model=PaymentRegisterResponse)
def register_payment(payload: PaymentRegisterRequest) -> PaymentRegisterResponse:
    try:
        return DashboardRepository().register_payment(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/payments/details", response_model=list[PaymentDetailRow])
def payment_details() -> list[PaymentDetailRow]:
    return DashboardRepository().payment_details()


@router.post("/payments/receipt", response_model=PaymentReceiptUpdateResponse)
def update_payment_receipt(payload: PaymentReceiptUpdateRequest) -> PaymentReceiptUpdateResponse:
    try:
        return DashboardRepository().update_payment_receipt(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

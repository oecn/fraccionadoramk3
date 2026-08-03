from datetime import date
from io import BytesIO
import tempfile
from pathlib import Path
from typing import Any

from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from pydantic import BaseModel

from modules.gastos_egresos.repository import GastosEgresosRepository
from modules.gastos_egresos.schemas import ImportResult, IpsParseResult
from modules.historial_ventas.repository import HistorialVentasRepository
from modules.historial_ventas.schemas import ReporteMensualData
from modules.inventario.repository import InventarioRepository
from modules.inventario.schemas import InventoryPackageRow, InventoryRawRow, InventorySummary
from modules.ordenes_compra.repository import OrdenesCompraRepository
from modules.ordenes_compra.schemas import OrdenCompraImportResponse, OrdenCompraRow


router = APIRouter(prefix="/telegram", tags=["telegram"])


class TelegramPdfImportResponse(BaseModel):
    ok: bool
    tipo: str
    telegram_message: str
    orden: OrdenCompraImportResponse


class TelegramIpsImportResponse(BaseModel):
    ok: bool
    tipo: str
    telegram_message: str
    ips: IpsParseResult
    import_result: ImportResult


class TelegramAutoPdfImportResponse(BaseModel):
    ok: bool
    tipo: str
    telegram_message: str
    data: dict[str, Any]


class TelegramMessageResponse(BaseModel):
    ok: bool
    telegram_message: str


def _format_gs(value: float) -> str:
    return f"{value:,.0f} Gs".replace(",", ".")


def _format_telegram_message(result: OrdenCompraImportResponse) -> str:
    title = (
        "Pedido importado correctamente."
        if result.created
        else f"La OC {result.nro_oc} ya existia. Fue actualizada."
    )
    lines = [
        title,
        f"OC: {result.nro_oc}",
        f"Sucursal: {result.sucursal or '-'}",
        f"Fecha: {result.fecha_pedido or '-'}",
        f"Items: {len(result.items)}",
    ]
    if result.monto_total is not None:
        lines.append(f"Total: {result.monto_total:,.0f} Gs".replace(",", "."))

    preview_items = result.items[:8]
    if preview_items:
        lines.append("")
        lines.append("Detalle:")
        for item in preview_items:
            cantidad = "" if item.cantidad is None else f"{item.cantidad:g}"
            unidad = f" {item.unidad}" if item.unidad else ""
            lines.append(f"- {item.descripcion}: {cantidad}{unidad}".rstrip())
        if len(result.items) > len(preview_items):
            lines.append(f"- ... y {len(result.items) - len(preview_items)} items mas")

    return "\n".join(lines)


def _format_ips_message(parsed: IpsParseResult, imported: ImportResult) -> str:
    status = "ya existia. No se duplico." if imported.skipped else "importado correctamente."
    lines = [
        f"Gasto IPS {status}",
        f"Periodo: {parsed.periodo_display or parsed.periodo or '-'}",
        f"Fecha: {parsed.fecha or '-'}",
        f"Monto: {_format_gs(parsed.monto_gs)}",
        f"Referencia: {parsed.nro_factura or parsed.referencia_pago or '-'}",
        f"Forma de pago: {parsed.forma_pago or '-'}",
    ]
    if imported.message:
        lines.append(f"Resultado: {imported.message}")
    return "\n".join(lines)


def _upload_from_bytes(filename: str, content: bytes):
    class MemoryUpload:
        def __init__(self, name: str, raw: bytes) -> None:
            self.filename = name
            self.file = BytesIO(raw)

    return MemoryUpload(filename, content)


def _import_oc_from_bytes(filename: str, content: bytes) -> OrdenCompraImportResponse:
    upload = _upload_from_bytes(filename, content)
    return OrdenesCompraRepository().import_pdf(upload)  # type: ignore[arg-type]


def _import_ips_from_bytes(filename: str, content: bytes) -> tuple[IpsParseResult, ImportResult]:
    suffix = Path(filename or "ips.pdf").suffix or ".pdf"
    tmp_name = ""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(content)
            tmp_name = tmp.name
        repo = GastosEgresosRepository()
        parsed = repo.parse_ips_pdf(tmp_name)
        imported = repo.import_ips(parsed)
        return parsed, imported
    finally:
        if tmp_name:
            Path(tmp_name).unlink(missing_ok=True)


def _is_today(value: str) -> bool:
    return value[:10] == date.today().isoformat()


def _format_oc_line(oc: OrdenCompraRow) -> str:
    return (
        f"- OC {oc.nro_oc} {oc.sucursal or '-'}: "
        f"{oc.items_count} items, {oc.pct_listo_envio:.1f}% listo, {_format_gs(oc.monto_total)}"
    )


def _format_bags(value: float) -> str:
    if abs(value - round(value)) < 0.05:
        return str(int(round(value)))
    return f"{value:.1f}"


def _raw_bags(row: InventoryRawRow) -> tuple[float, str]:
    if row.alerta_bolsa_kg == 25:
        return row.bolsas_25, "bolsas 25kg"
    return row.bolsas_50, "bolsas 50kg"


def _format_inventory_line(row: InventoryRawRow) -> str:
    bags, label = _raw_bags(row)
    state = "PEDIR" if row.alerta_estado == "bajo" else "OK"
    min_text = ""
    if row.alerta_min_bolsas is not None:
        min_text = f" / min {_format_bags(row.alerta_min_bolsas)}"
    return f"- {row.producto}: {_format_bags(bags)} {label}{min_text} - {state}"


def _format_stock_alert(row: InventoryRawRow) -> str:
    bags, _label = _raw_bags(row)
    min_bags = row.alerta_min_bolsas or 0
    missing = max(0.0, min_bags - bags)
    suffix = "bolsa" if abs(missing - 1) < 0.05 else "bolsas"
    return f"- {row.producto} bajo minimo: faltan {_format_bags(missing)} {suffix}"


def _format_package_line(row: InventoryPackageRow) -> str:
    gramaje = "1kg" if row.gramaje == 1000 else f"{row.gramaje}g"
    return f"- {row.producto} {gramaje}: {row.paquetes} paquetes"


def _build_inventory_message(summary: InventorySummary) -> str:
    raw_stock = summary.raw_stock
    package_stock = summary.package_stock
    alerts = [row for row in raw_stock if row.alerta_estado == "bajo"]
    package_rows = [row for row in package_stock if row.paquetes > 0]

    lines = ["Inventario general", "", "Materia prima:"]
    if raw_stock:
        lines.extend(_format_inventory_line(row) for row in raw_stock)
    else:
        lines.append("- Sin materia prima cargada")

    lines.extend(["", "Paquetes:"])
    if package_rows:
        lines.extend(_format_package_line(row) for row in package_rows[:30])
        if len(package_rows) > 30:
            lines.append(f"- ... y {len(package_rows) - 30} lineas mas")
    else:
        lines.append("- Sin paquetes cargados")

    lines.extend(["", "Alertas:"])
    if alerts:
        lines.extend(_format_stock_alert(row) for row in alerts)
    else:
        lines.append("- Sin alertas de materia prima")

    inventory_total = float(summary.total_raw_valor_gs or 0) + float(summary.total_venta_gs or 0)
    lines.extend(
        [
            "",
            "Valor inventario:",
            f"- Materia prima: {_format_gs(summary.total_raw_valor_gs)}",
            f"- Producto terminado: {_format_gs(summary.total_venta_gs)}",
            f"- Total estimado: {_format_gs(inventory_total)}",
        ]
    )

    return "\n".join(lines)


def _format_gramaje(value: int) -> str:
    return "1kg" if int(value or 0) == 1000 else f"{int(value or 0)}g"


def _build_accounting_message(report: ReporteMensualData) -> str:
    egresos_total = float(report.compras_total or 0) + float(report.gastos_total or 0)
    lines = [
        f"Resumen contable - {report.ym}",
        "",
        "Ventas:",
        f"- Facturas: {_format_gs(report.ventas_facturas)}",
        f"- Bolsas: {_format_gs(report.ventas_bolsas)}",
        f"- Total ventas: {_format_gs(report.ventas_total)}",
        "",
        "Egresos:",
        f"- Materia prima: {_format_gs(report.compras_total)}",
        f"- Gastos varios: {_format_gs(report.gastos_total)}",
        f"- Egresos totales: {_format_gs(egresos_total)}",
        "",
        "Resultado:",
        f"- Margen bruto: {_format_gs(report.margen_bruto)} ({report.margen_bruto_pct:.1f}%)",
        f"- Beneficio operativo: {_format_gs(report.beneficio_operativo)} ({report.beneficio_pct:.1f}%)",
        "",
        "Operacion:",
        f"- Facturas emitidas: {report.cant_facturas}",
        f"- Ventas de bolsa: {report.cant_ventas_bolsa}",
    ]

    if report.top_productos:
        lines.extend(["", "Top productos:"])
        for idx, item in enumerate(report.top_productos[:5], start=1):
            lines.append(
                f"{idx}. {item.producto} {_format_gramaje(item.gramaje)} - {_format_gs(item.total_gs)}"
            )

    return "\n".join(lines)


def _build_daily_summary(rows: list[OrdenCompraRow], raw_stock: list[InventoryRawRow]) -> str:
    today = date.today().isoformat()
    pending = [oc for oc in rows if not oc.completada]
    low_stock = [oc for oc in pending if oc.pct_listo_envio < 100.0]
    critical = [oc for oc in pending if oc.pct_listo_envio < 50.0]
    pending_total = sum(oc.monto_total for oc in pending)
    raw_alerts = [row for row in raw_stock if row.alerta_estado == "bajo"]

    lines = [
        f"Resumen - {today}",
        "",
        f"OCs pendientes: {len(pending)}",
        f"Total pendiente: {_format_gs(pending_total)}",
        f"Con faltantes: {len(low_stock)}",
        f"Criticas: {len(critical)}",
    ]

    top_pending = sorted(pending, key=lambda oc: (oc.pct_listo_envio, -oc.monto_total))[:5]
    if top_pending:
        lines.extend(["", "Top pendientes:"])
        for idx, oc in enumerate(top_pending, start=1):
            lines.append(f"{idx}. OC {oc.nro_oc} {oc.sucursal or '-'} - {oc.pct_listo_envio:.1f}% listo")

    if raw_stock:
        lines.extend(["", f"Inventario bolsas: {len(raw_alerts)} en necesidad de pedir"])
        priority = sorted(raw_stock, key=lambda row: (row.alerta_estado != "bajo", row.producto))[:8]
        lines.extend(_format_inventory_line(row) for row in priority)

    return "\n".join(lines)


@router.post("/pedidos/pdf", response_model=TelegramPdfImportResponse)
def importar_pedido_pdf(file: UploadFile = File(...)) -> TelegramPdfImportResponse:
    try:
        result = OrdenesCompraRepository().import_pdf(file)
        return TelegramPdfImportResponse(
            ok=True,
            tipo="orden_compra",
            telegram_message=_format_telegram_message(result),
            orden=result,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"No se pudo importar el PDF enviado por Telegram: {exc}",
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/ips/pdf", response_model=TelegramIpsImportResponse)
async def importar_ips_pdf(file: UploadFile = File(...)) -> TelegramIpsImportResponse:
    filename = file.filename or ""
    if not filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Seleccione un archivo PDF IPS.")

    tmp_name = ""
    try:
        content = await file.read()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(content)
            tmp_name = tmp.name
        repo = GastosEgresosRepository()
        parsed = repo.parse_ips_pdf(tmp_name)
        imported = repo.import_ips(parsed)
        return TelegramIpsImportResponse(
            ok=True,
            tipo="ips",
            telegram_message=_format_ips_message(parsed, imported),
            ips=parsed,
            import_result=imported,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"No se pudo importar el PDF IPS: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"No se pudo parsear el PDF IPS: {exc}") from exc
    finally:
        if tmp_name:
            Path(tmp_name).unlink(missing_ok=True)


@router.post("/pdf", response_model=TelegramAutoPdfImportResponse)
async def importar_pdf_auto(file: UploadFile = File(...)) -> TelegramAutoPdfImportResponse:
    filename = file.filename or "telegram.pdf"
    if not filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Seleccione un archivo PDF.")

    content = await file.read()
    oc_error = ""
    ips_error = ""

    try:
        result = _import_oc_from_bytes(filename, content)
        return TelegramAutoPdfImportResponse(
            ok=True,
            tipo="orden_compra",
            telegram_message=_format_telegram_message(result),
            data={"orden": result.model_dump()},
        )
    except Exception as exc:
        oc_error = str(exc)

    try:
        parsed, imported = _import_ips_from_bytes(filename, content)
        return TelegramAutoPdfImportResponse(
            ok=True,
            tipo="ips",
            telegram_message=_format_ips_message(parsed, imported),
            data={"ips": parsed.model_dump(), "import_result": imported.model_dump()},
        )
    except Exception as exc:
        ips_error = str(exc)

    raise HTTPException(
        status_code=400,
        detail=(
            "No pude reconocer el PDF como OC ni como IPS. "
            f"OC: {oc_error or '-'} | IPS: {ips_error or '-'}"
        ),
    )


@router.get("/resumen-diario", response_model=TelegramMessageResponse)
def resumen_diario() -> TelegramMessageResponse:
    try:
        rows = OrdenesCompraRepository().list_imported()
        inventory = InventarioRepository().summary()
        return TelegramMessageResponse(ok=True, telegram_message=_build_daily_summary(rows, inventory.raw_stock))
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/inventario", response_model=TelegramMessageResponse)
def inventario() -> TelegramMessageResponse:
    try:
        summary = InventarioRepository().summary()
        return TelegramMessageResponse(
            ok=True,
            telegram_message=_build_inventory_message(summary),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/contable", response_model=TelegramMessageResponse)
def contable(ym: str = Query(default="")) -> TelegramMessageResponse:
    try:
        selected_ym = (ym or date.today().strftime("%Y-%m")).strip()
        report = HistorialVentasRepository().reporte_mensual(ym=selected_ym, empresa="Granos Central")
        return TelegramMessageResponse(ok=True, telegram_message=_build_accounting_message(report))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

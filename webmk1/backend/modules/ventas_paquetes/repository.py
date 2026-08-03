from __future__ import annotations

import datetime as dt
import re
import tempfile
import unicodedata
from pathlib import Path

from fastapi import UploadFile

from core.google_sheets import append_factura
from core.database import connection
from modules.ventas_paquetes.factura_venta_parser import parse_factura_venta_pdf
from modules.ventas_paquetes.schemas import (
    FacturaVentaParseItem,
    FacturaVentaParsePreview,
    VentaCreate,
    VentaOptions,
    VentaResumen,
    VentaStockItem,
)


PRODUCT_ORDER = [
    "arroz",
    "azucar",
    "pororo",
    "poroto pyta",
    "galleta molida",
    "locro",
    "locrillo",
]

VENTA_ORDER = [
    ("arroz", [250, 500, 1000]),
    ("azucar", [250, 500, 1000]),
    ("pororo", [200, 400, 800]),
    ("poroto pyta", [200, 400]),
    ("galleta molida", [200, 400, 800]),
    ("locro", [200, 400]),
    ("locrillo", [200, 400]),
]


def _normalize_product_key(name: str) -> str:
    text = (name or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.replace("\ufffd", "o").replace("ã", "a").replace("³", "o")
    if "azucar" in text or "azukita" in text:
        return "azucar"
    if "arroz" in text:
        return "arroz"
    if "pororo" in text:
        return "pororo"
    if "poroto" in text:
        return "poroto pyta"
    if "gallet" in text or "molida" in text:
        return "galleta molida"
    if "locrillo" in text:
        return "locrillo"
    if "locro" in text:
        return "locro"
    if "lenteja" in text:
        return "lenteja"
    return text


def _product_order_idx(name: str) -> int:
    key = _normalize_product_key(name)
    try:
        return PRODUCT_ORDER.index(key)
    except ValueError:
        return 10_000


def _gramajes_permitidos(product_name: str) -> list[int]:
    key = _normalize_product_key(product_name)
    if key in {"arroz", "azucar"}:
        return [250, 500, 1000]
    if key in {"poroto pyta", "locro", "locrillo"}:
        return [200, 400]
    return [200, 400, 800]


def _gram_order_idx(product_name: str, gramaje: int) -> int:
    try:
        return _gramajes_permitidos(product_name).index(int(gramaje))
    except ValueError:
        return 999


def _format_invoice_no(value: str) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))[:13]
    if not digits:
        return ""
    if len(digits) != 13:
        raise ValueError("El nro. de factura debe tener 13 digitos: 000-000-0000000.")
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"


def _tax_split_included(line_total: float, iva: int) -> tuple[float, float]:
    if iva == 5:
        return line_total, line_total / 21.0
    if iva == 10:
        return line_total, line_total / 11.0
    return line_total, 0.0


class VentasPaquetesRepository:
    def parse_factura_pdf(self, upload: UploadFile) -> FacturaVentaParsePreview:
        filename = upload.filename or ""
        if not filename.lower().endswith(".pdf"):
            raise ValueError("Seleccione un archivo PDF.")

        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                tmp.write(upload.file.read())
                tmp_path = tmp.name
            parsed = parse_factura_venta_pdf(tmp_path)
        finally:
            upload.file.close()
            if tmp_path:
                Path(tmp_path).unlink(missing_ok=True)

        return FacturaVentaParsePreview(
            numero=parsed["numero"],
            fecha_emision=parsed["fecha_emision"],
            cliente=parsed["cliente"],
            ruc_cliente=parsed["ruc_cliente"],
            condicion_venta=parsed["condicion_venta"],
            gravada5_gs=parsed["gravada5_gs"],
            iva5_gs=parsed["iva5_gs"],
            gravada10_gs=parsed["gravada10_gs"],
            iva10_gs=parsed["iva10_gs"],
            total_iva_gs=parsed["total_iva_gs"],
            total_gs=parsed["total_gs"],
            items=[
                FacturaVentaParseItem(linea=idx, **item)
                for idx, item in enumerate(parsed["items"], start=1)
            ],
        )

    def options(self) -> VentaOptions:
        with connection("fraccionadora") as cn:
            products = cn.execute("SELECT id, name FROM products ORDER BY name").fetchall()
            stock_rows = cn.execute("SELECT product_id, gramaje, paquetes FROM package_stock").fetchall()
            price_rows = cn.execute("SELECT product_id, gramaje, price_gs, iva FROM package_prices").fetchall()

        products_by_key: dict[str, object] = {}
        for product in products:
            key = _normalize_product_key(product["name"] or "")
            products_by_key.setdefault(key, product)

        stock_map = {
            (int(r["product_id"]), int(r["gramaje"])): int(r["paquetes"] or 0)
            for r in stock_rows
        }
        price_map = {
            (int(r["product_id"]), int(r["gramaje"])): r
            for r in price_rows
        }

        stock: list[VentaStockItem] = []
        for key, gramajes in VENTA_ORDER:
            product = products_by_key.get(key)
            if not product:
                continue
            product_id = int(product["id"])
            producto = product["name"] or ""
            for gramaje in gramajes:
                price = price_map.get((product_id, gramaje))
                stock.append(
                    VentaStockItem(
                        product_id=product_id,
                        producto=producto,
                        gramaje=gramaje,
                        paquetes=stock_map.get((product_id, gramaje), 0),
                        price_gs=float(price["price_gs"]) if price and price["price_gs"] is not None else None,
                        iva=int(price["iva"]) if price and price["iva"] is not None else None,
                    )
                )

        return VentaOptions(stock=stock, hoy=dt.date.today().isoformat())

    def create(self, payload: VentaCreate) -> VentaResumen:
        if not payload.items:
            raise ValueError("No hay items para facturar.")
        invoice_no = _format_invoice_no(payload.invoice_no)
        if not invoice_no:
            raise ValueError("Ingrese nro. de factura.")
        invoice_digits = re.sub(r"\D+", "", invoice_no)

        fecha = (payload.fecha or "").strip()
        if fecha:
            try:
                dt.datetime.strptime(fecha, "%Y-%m-%d")
            except ValueError as exc:
                raise ValueError("Formato de fecha invalido. Use YYYY-MM-DD.") from exc
            fecha_sql = f"{fecha} 00:00:00"
        else:
            fecha_sql = None

        with connection("fraccionadora") as cn:
            duplicate = cn.execute(
                """
                SELECT id
                FROM sales_invoices
                WHERE regexp_replace(COALESCE(invoice_no, ''), '[^0-9]', '', 'g') = %s
                LIMIT 1
                """,
                (invoice_digits,),
            ).fetchone()
            if duplicate:
                raise ValueError(f"El nro. de factura {invoice_no} ya existe.")

            lineas: list[tuple[int, int, int, float, int, float, float, float]] = []
            faltan_precios: list[str] = []
            sorted_items = sorted(
                payload.items,
                key=lambda item: (
                    _product_order_idx(self._product_name(cn, item.product_id)),
                    _gram_order_idx(self._product_name(cn, item.product_id), item.gramaje),
                ),
            )

            for item in sorted_items:
                stock = cn.execute(
                    """
                    SELECT COALESCE(ps.paquetes, 0) AS paquetes, p.name AS producto,
                           pp.price_gs, pp.iva
                    FROM products p
                    LEFT JOIN package_stock ps ON ps.product_id = p.id AND ps.gramaje = %s
                    LEFT JOIN package_prices pp ON pp.product_id = p.id AND pp.gramaje = %s
                    WHERE p.id = %s
                    """,
                    (item.gramaje, item.gramaje, item.product_id),
                ).fetchone()
                if not stock:
                    raise ValueError(f"Stock no encontrado para producto {item.product_id} {item.gramaje} g.")
                if int(stock["paquetes"] or 0) < item.cantidad:
                    raise ValueError(
                        f"Stock insuficiente para {stock['producto']} {item.gramaje} g. "
                        f"Disp: {int(stock['paquetes'] or 0)}, pide: {item.cantidad}."
                    )
                price = float(item.price_gs) if item.price_gs is not None else (
                    float(stock["price_gs"]) if stock["price_gs"] is not None else None
                )
                iva = int(item.iva) if item.iva is not None else int(stock["iva"] or 0)
                if price is None or iva not in (5, 10):
                    faltan_precios.append(f"{stock['producto']} {item.gramaje} g")
                    continue

                line_total = price * item.cantidad
                line_base, line_iva = _tax_split_included(line_total, iva)
                lineas.append((item.product_id, item.gramaje, item.cantidad, price, iva, line_total, line_base, line_iva))

            if faltan_precios:
                raise ValueError("Faltan precios/IVA para: " + ", ".join(faltan_precios))

            grav5 = iva5 = grav10 = iva10 = total = 0.0
            for _pid, _g, _qty, _price, iva, line_total, line_base, line_iva in lineas:
                total += line_total
                if iva == 5:
                    grav5 += line_base
                    iva5 += line_iva
                else:
                    grav10 += line_base
                    iva10 += line_iva

            if fecha_sql:
                invoice_row = cn.execute(
                    """
                    INSERT INTO sales_invoices(
                        ts, invoice_no, customer, gravada5_gs, iva5_gs, gravada10_gs, iva10_gs, total_gs
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (fecha_sql, invoice_no, payload.customer.strip(), grav5, iva5, grav10, iva10, total),
                ).fetchone()
            else:
                invoice_row = cn.execute(
                    """
                    INSERT INTO sales_invoices(
                        invoice_no, customer, gravada5_gs, iva5_gs, gravada10_gs, iva10_gs, total_gs
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (invoice_no, payload.customer.strip(), grav5, iva5, grav10, iva10, total),
                ).fetchone()

            invoice_id = int(invoice_row["id"])
            for pid, gramaje, qty, price, iva, line_total, line_base, line_iva in lineas:
                cn.execute(
                    """
                    INSERT INTO sales_invoice_items(
                        invoice_id, product_id, gramaje, cantidad, price_gs, iva,
                        line_total, line_base, line_iva
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (invoice_id, pid, gramaje, qty, price, iva, line_total, line_base, line_iva),
                )
                cn.execute(
                    "UPDATE package_stock SET paquetes = paquetes - %s WHERE product_id = %s AND gramaje = %s",
                    (qty, pid, gramaje),
                )
                cn.execute(
                    "INSERT INTO sales(product_id, gramaje, paquetes) VALUES(%s, %s, %s)",
                    (pid, gramaje, qty),
                )

        sheet_sent = False
        sheet_error = None
        if payload.send_to_sheet:
            try:
                append_factura(self._sheet_payload(invoice_id))
                sheet_sent = True
            except Exception as exc:
                sheet_error = str(exc)

        return VentaResumen(
            invoice_id=invoice_id,
            gravada5_gs=grav5,
            iva5_gs=iva5,
            gravada10_gs=grav10,
            iva10_gs=iva10,
            total_gs=total,
            sheet_sent=sheet_sent,
            sheet_error=sheet_error,
        )

    def _product_name(self, cn, product_id: int) -> str:
        row = cn.execute("SELECT name FROM products WHERE id = %s", (product_id,)).fetchone()
        return row["name"] if row else ""

    def _sheet_payload(self, invoice_id: int) -> dict[str, object]:
        with connection("fraccionadora") as cn:
            row = cn.execute(
                """
                SELECT invoice_no, customer, CAST(ts AS TEXT) AS ts,
                       COALESCE(iva5_gs, 0) AS iva5_gs,
                       COALESCE(iva10_gs, 0) AS iva10_gs,
                       COALESCE(total_gs, 0) AS total_gs
                FROM sales_invoices
                WHERE id = %s
                """,
                (invoice_id,),
            ).fetchone()
        if not row:
            raise ValueError("Factura no encontrada para enviar a Google Sheets.")

        meses = [
            "ENERO",
            "FEBRERO",
            "MARZO",
            "ABRIL",
            "MAYO",
            "JUNIO",
            "JULIO",
            "AGOSTO",
            "SEPTIEMBRE",
            "OCTUBRE",
            "NOVIEMBRE",
            "DICIEMBRE",
        ]
        fecha_iso = str(row["ts"] or "")
        fecha_fmt = fecha_iso
        mes_txt = ""
        try:
            parsed = dt.datetime.fromisoformat(fecha_iso.replace("Z", "").replace("T", " "))
            fecha_fmt = parsed.strftime("%d/%m/%Y")
            mes_txt = meses[parsed.month - 1]
        except Exception:
            pass

        customer = str(row["customer"] or "").strip().upper()
        cliente = customer if customer in {"LUQUE", "AREGUA", "ITAUGUA"} else "LUQUE"
        iva_total = float(row["iva5_gs"] or 0) + float(row["iva10_gs"] or 0)
        total = float(row["total_gs"] or 0)

        return {
            "mes": mes_txt,
            "cliente": cliente,
            "factura": str(row["invoice_no"] or "").strip(),
            "fecha": fecha_fmt,
            "remision": "Listo",
            "estado": "Entregado",
            "cobranza": "Sin OP",
            "recibo": "",
            "extra1": "",
            "total": total,
            "iva_total": iva_total,
            "extra2": total - 0.3 * iva_total,
        }

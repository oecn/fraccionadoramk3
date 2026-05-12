from __future__ import annotations

import datetime as dt
import unicodedata

from core.google_sheets import append_factura
from core.database import connection
from modules.ventas_paquetes.schemas import VentaCreate, VentaOptions, VentaResumen, VentaStockItem


PRODUCT_ORDER = [
    "arroz",
    "azucar",
    "pororo",
    "poroto rojo",
    "galleta molida",
    "locro",
    "locrillo",
    "lenteja",
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
        return "poroto rojo"
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
    if key in {"poroto rojo", "locro", "locrillo"}:
        return [200, 400]
    if key == "lenteja":
        return [250, 500]
    return [200, 400, 800]


def _gram_order_idx(product_name: str, gramaje: int) -> int:
    try:
        return _gramajes_permitidos(product_name).index(int(gramaje))
    except ValueError:
        return 999


class VentasPaquetesRepository:
    def options(self) -> VentaOptions:
        with connection("fraccionadora") as cn:
            rows = cn.execute(
                """
                SELECT ps.product_id, p.name AS producto, ps.gramaje, ps.paquetes,
                       pp.price_gs, pp.iva
                FROM package_stock ps
                JOIN products p ON p.id = ps.product_id
                LEFT JOIN package_prices pp ON pp.product_id = ps.product_id AND pp.gramaje = ps.gramaje
                WHERE ps.paquetes > 0
                ORDER BY p.name, ps.gramaje
                """
            ).fetchall()
        rows = sorted(
            rows,
            key=lambda r: (
                _product_order_idx(r["producto"] or ""),
                r["producto"] or "",
                _gram_order_idx(r["producto"] or "", int(r["gramaje"] or 0)),
                int(r["gramaje"] or 0),
            ),
        )
        return VentaOptions(
            stock=[
                VentaStockItem(
                    product_id=int(r["product_id"]),
                    producto=r["producto"] or "",
                    gramaje=int(r["gramaje"] or 0),
                    paquetes=int(r["paquetes"] or 0),
                    price_gs=float(r["price_gs"]) if r["price_gs"] is not None else None,
                    iva=int(r["iva"]) if r["iva"] is not None else None,
                )
                for r in rows
            ],
            hoy=dt.date.today().isoformat(),
        )

    def create(self, payload: VentaCreate) -> VentaResumen:
        if not payload.items:
            raise ValueError("No hay items para facturar.")

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
                    FROM package_stock ps
                    JOIN products p ON p.id = ps.product_id
                    LEFT JOIN package_prices pp ON pp.product_id = ps.product_id AND pp.gramaje = ps.gramaje
                    WHERE ps.product_id = %s AND ps.gramaje = %s
                    """,
                    (item.product_id, item.gramaje),
                ).fetchone()
                if not stock:
                    raise ValueError(f"Stock no encontrado para producto {item.product_id} {item.gramaje} g.")
                if int(stock["paquetes"] or 0) < item.cantidad:
                    raise ValueError(
                        f"Stock insuficiente para {stock['producto']} {item.gramaje} g. "
                        f"Disp: {int(stock['paquetes'] or 0)}, pide: {item.cantidad}."
                    )
                if stock["price_gs"] is None or int(stock["iva"] or 0) not in (5, 10):
                    faltan_precios.append(f"{stock['producto']} {item.gramaje} g")
                    continue

                price = float(stock["price_gs"])
                iva = int(stock["iva"])
                line_total = price * item.cantidad
                line_base = line_total / (1.0 + iva / 100.0)
                line_iva = line_total - line_base
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
                    (fecha_sql, payload.invoice_no.strip(), payload.customer.strip(), grav5, iva5, grav10, iva10, total),
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
                    (payload.invoice_no.strip(), payload.customer.strip(), grav5, iva5, grav10, iva10, total),
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

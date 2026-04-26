# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import webbrowser
import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import db
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from PySide6 import QtCore, QtWidgets
from reporte_mensual import _inventory_snapshot_values


DB_CANDIDATES = [
    ROOT_DIR / "GCMK8" / "fraccionadora.db",
    ROOT_DIR / "fraccionadora.db",
]
OUT_DIR_DEFAULT = ROOT_DIR / "reportes_trimestrales"
DATA_JSON_PATH = ROOT_DIR / "GCMK8" / "datos_iniciales.json"

QUARTER_MONTHS = {
    "T1": ("01", "02", "03"),
    "T2": ("04", "05", "06"),
    "T3": ("07", "08", "09"),
    "T4": ("10", "11", "12"),
}
MONTH_LABELS = {
    "01": "Enero",
    "02": "Febrero",
    "03": "Marzo",
    "04": "Abril",
    "05": "Mayo",
    "06": "Junio",
    "07": "Julio",
    "08": "Agosto",
    "09": "Septiembre",
    "10": "Octubre",
    "11": "Noviembre",
    "12": "Diciembre",
}
PDF_FONT = "Courier New"


def _apply_pdf_font():
    plt.rcParams["font.family"] = PDF_FONT
    plt.rcParams["axes.titlesize"] = 13
    plt.rcParams["axes.titleweight"] = "bold"
    plt.rcParams["axes.labelsize"] = 10
    plt.rcParams["xtick.labelsize"] = 9
    plt.rcParams["ytick.labelsize"] = 9


def _style_table(tbl, header_bg: str = "#eef2f7", body_bg: str = "#fafafa", edge: str = "#d1d5db"):
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor(edge)
        cell.set_linewidth(0.8)
        if r == 0:
            cell.set_facecolor(header_bg)
            cell.set_text_props(weight="bold", color="#0f172a", family=PDF_FONT)
        else:
            cell.set_facecolor(body_bg if r % 2 else "white")
            cell.set_text_props(color="#111827", family=PDF_FONT)


def _add_page_header(fig, title: str, subtitle: str):
    fig.text(0.06, 0.955, title, fontsize=18, fontweight="bold", color="#111827", family=PDF_FONT)
    fig.text(0.06, 0.927, subtitle, fontsize=10, color="#4b5563", family=PDF_FONT)
    fig.lines.append(plt.Line2D([0.06, 0.94], [0.905, 0.905], transform=fig.transFigure, color="#d1d5db", linewidth=0.9))


def _resolve_db(db_arg: str | None) -> Path:
    if db_arg:
        p = Path(db_arg).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"No existe la base: {p}")
        return p
    for p in DB_CANDIDATES:
        if p.exists():
            return p
    raise FileNotFoundError("No se encontro fraccionadora.db en rutas esperadas.")


def _fmt_gs(x: float | int | None) -> str:
    try:
        return f"{float(x or 0):,.0f}".replace(",", ".")
    except Exception:
        return "0"


def _fmt_num(x: float | int | None, digits: int = 1) -> str:
    try:
        return f"{float(x or 0):,.{digits}f}".replace(",", "_").replace(".", ",").replace("_", ".")
    except Exception:
        return "0"


def _parse_date(value: str | None) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except Exception:
        return None


def _load_initial_data() -> dict:
    if not DATA_JSON_PATH.exists():
        return {"ventas": [], "compras": [], "gastos": [], "saldo_inicial": []}
    try:
        raw = json.loads(DATA_JSON_PATH.read_text(encoding="utf-8"))
        return {
            "ventas": list(raw.get("ventas", []) or []),
            "compras": list(raw.get("compras", []) or []),
            "gastos": list(raw.get("gastos", []) or []),
            "saldo_inicial": list(raw.get("saldo_inicial", []) or []),
        }
    except Exception:
        return {"ventas": [], "compras": [], "gastos": [], "saldo_inicial": []}


def _json_sum_until(rows: list[dict], cutoff: date) -> float:
    total = 0.0
    for row in rows:
        f = _parse_date(row.get("fecha") or row.get("date"))
        if not f or f >= cutoff:
            continue
        try:
            total += float(row.get("monto", row.get("valor")) or 0.0)
        except Exception:
            continue
    return total


def _json_sum_between(rows: list[dict], start_exclusive: date | None, cutoff: date) -> float:
    total = 0.0
    for row in rows:
        f = _parse_date(row.get("fecha") or row.get("date"))
        if not f or f >= cutoff:
            continue
        if start_exclusive is not None and f <= start_exclusive:
            continue
        try:
            total += float(row.get("monto", row.get("valor")) or 0.0)
        except Exception:
            continue
    return total


def _saldo_inicial_base(rows: list[dict], cutoff: date) -> tuple[float, str]:
    selected_amount = 0.0
    selected_date = ""
    latest: date | None = None
    for row in rows:
        f = _parse_date(row.get("fecha") or row.get("date"))
        if not f or f > cutoff:
            continue
        if latest is None or f > latest:
            latest = f
            selected_date = f.isoformat()
            try:
                selected_amount = float(row.get("monto", row.get("valor")) or 0.0)
            except Exception:
                selected_amount = 0.0
    return selected_amount, selected_date


def _bank_estimate_between(cur, start_exclusive: date | None, cutoff_date: date, opening_base: float) -> float:
    cutoff = cutoff_date.isoformat()
    if start_exclusive is None:
        date_filter = "date(ts) < date(%s)"
        params = (cutoff,)
    else:
        date_filter = "date(ts) > date(%s) AND date(ts) < date(%s)"
        params = (start_exclusive.isoformat(), cutoff)
    ventas_ret = _scalar(
        cur,
        f"""
        SELECT COALESCE(SUM(total_gs - 0.30 * (COALESCE(iva5_gs, 0) + COALESCE(iva10_gs, 0))),0)
        FROM sales_invoices
        WHERE {date_filter};
        """,
        params,
    )
    bag_sales = _scalar(cur, f"SELECT COALESCE(SUM(total_gs),0) FROM bag_sales WHERE {date_filter};", params)
    notas_ret = _scalar(
        cur,
        f"""
        SELECT COALESCE(SUM(total_gs - 0.30 * (COALESCE(iva5_gs, 0) + COALESCE(iva10_gs, 0))),0)
        FROM credit_notes
        WHERE {date_filter};
        """,
        params,
    )
    compras = _scalar(cur, f"SELECT COALESCE(SUM(costo_total_gs),0) FROM raw_lots WHERE {date_filter};", params)
    gastos = _scalar(cur, f"SELECT COALESCE(SUM(monto_gs),0) FROM expenses WHERE {date_filter};", params)
    return opening_base + ventas_ret + bag_sales - notas_ret - compras - gastos


def _current_inventory_values(cur) -> tuple[float, float]:
    stock_terminado = _scalar(
        cur,
        """
        SELECT COALESCE(SUM(COALESCE(ps.paquetes,0) * COALESCE(pp.price_gs,0)),0)
        FROM package_stock ps
        LEFT JOIN package_prices pp
          ON pp.product_id = ps.product_id AND pp.gramaje = ps.gramaje;
        """,
    )

    cur.execute(
        """
        SELECT rs.product_id, COALESCE(rs.kg, 0) AS kg,
               (
                   SELECT rl.costo_kg_gs
                   FROM raw_lots rl
                   WHERE rl.product_id = rs.product_id
                   ORDER BY rl.ts DESC, rl.id DESC
                   LIMIT 1
               ) AS costo_kg_gs
        FROM raw_stock rs;
        """
    )
    stock_mp = 0.0
    for _pid, kg, costo_kg in cur.fetchall():
        try:
            stock_mp += float(kg or 0.0) * float(costo_kg or 0.0)
        except Exception:
            continue
    return stock_terminado, stock_mp


def _quarter_bounds(year: int, quarter: str) -> tuple[str, str, list[str]]:
    months = list(QUARTER_MONTHS[quarter])
    first_month = int(months[0])
    last_month = int(months[-1])
    d1 = date(year, first_month, 1)
    if last_month == 12:
        nxt = date(year + 1, 1, 1)
    else:
        nxt = date(year, last_month + 1, 1)
    d2 = nxt.fromordinal(nxt.toordinal() - 1)
    return d1.isoformat(), d2.isoformat(), months


def _scalar(cur, sql: str, params: tuple = ()) -> float:
    try:
        cur.execute(sql, params)
        row = cur.fetchone()
        return float((row[0] if row else 0) or 0)
    except Exception:
        return 0.0


def _count(cur, sql: str, params: tuple = ()) -> int:
    try:
        cur.execute(sql, params)
        row = cur.fetchone()
        return int((row[0] if row else 0) or 0)
    except Exception:
        return 0


@dataclass
class QuarterlyData:
    year: int
    quarter: str
    d1: str
    d2: str
    months: list[str]
    ventas_brutas: float
    ventas_bolsas: float
    notas_credito: float
    ventas_netas: float
    ventas_retencion: float
    compras: float
    gastos: float
    flujo_neto: float
    facturas: int
    ventas_bolsas_ops: int
    notas_credito_count: int
    lotes_comprados: int
    fraccionamientos: int
    kg_consumidos: float
    paquetes: int
    merma_kg: float
    dias_activos: int
    productos_activos: int
    stock_terminado_gs: float
    stock_mp_gs: float
    valor_en_planta_gs: float
    apertura_fecha: str
    valor_inicio_trimestre_gs: float
    saldo_base_fecha: str
    estimado_banco_inicio_trimestre_gs: float
    monthly_rows: list[dict]
    top_products_sales: list[tuple[str, float]]
    top_products_prod: list[tuple[str, float]]
    gastos_tipo: list[tuple[str, float]]
    proveedores: list[tuple[str, float]]


def _collect_quarterly_data(db_path: Path, year: int, quarter: str) -> QuarterlyData:
    d1, d2, months = _quarter_bounds(year, quarter)
    cn = db.connect("fraccionadora")
    cur = cn.cursor()
    params = (d1, d2)
    quarter_start = date.fromisoformat(d1)
    apertura_dt = quarter_start - timedelta(days=1)
    apertura_fecha = apertura_dt.isoformat()
    data_json = _load_initial_data()
    saldo_base, saldo_base_fecha = _saldo_inicial_base(data_json.get("saldo_inicial", []), quarter_start)
    saldo_base_dt = _parse_date(saldo_base_fecha) if saldo_base_fecha else None

    ventas_facturas = _scalar(cur, "SELECT COALESCE(SUM(total_gs),0) FROM sales_invoices WHERE ts::date >= %s AND ts::date <= %s;", params)
    ventas_bolsas = _scalar(cur, "SELECT COALESCE(SUM(total_gs),0) FROM bag_sales WHERE ts::date >= %s AND ts::date <= %s;", params)
    notas_credito = _scalar(cur, "SELECT COALESCE(SUM(total_gs),0) FROM credit_notes WHERE ts::date >= %s AND ts::date <= %s;", params)
    ventas_retencion_fact = _scalar(
        cur,
        """
        SELECT COALESCE(SUM(total_gs - 0.30 * (COALESCE(iva5_gs, 0) + COALESCE(iva10_gs, 0))),0)
        FROM sales_invoices
        WHERE ts::date >= %s AND ts::date <= %s;
        """,
        params,
    )
    notas_credito_ret = _scalar(
        cur,
        """
        SELECT COALESCE(SUM(total_gs - 0.30 * (COALESCE(iva5_gs, 0) + COALESCE(iva10_gs, 0))),0)
        FROM credit_notes
        WHERE ts::date >= %s AND ts::date <= %s;
        """,
        params,
    )
    compras = _scalar(cur, "SELECT COALESCE(SUM(costo_total_gs),0) FROM raw_lots WHERE ts::date >= %s AND ts::date <= %s;", params)
    gastos = _scalar(cur, "SELECT COALESCE(SUM(monto_gs),0) FROM expenses WHERE ts::date >= %s AND ts::date <= %s;", params)
    facturas = _count(cur, "SELECT COUNT(*) FROM sales_invoices WHERE ts::date >= %s AND ts::date <= %s;", params)
    ventas_bolsas_ops = _count(cur, "SELECT COUNT(*) FROM bag_sales WHERE ts::date >= %s AND ts::date <= %s;", params)
    notas_credito_count = _count(cur, "SELECT COUNT(*) FROM credit_notes WHERE ts::date >= %s AND ts::date <= %s;", params)
    lotes_comprados = _count(cur, "SELECT COUNT(*) FROM raw_lots WHERE ts::date >= %s AND ts::date <= %s;", params)
    fraccionamientos = _count(cur, "SELECT COUNT(*) FROM fractionations WHERE ts::date >= %s AND ts::date <= %s;", params)
    kg_consumidos = _scalar(cur, "SELECT COALESCE(SUM(kg_consumidos),0) FROM fractionations WHERE ts::date >= %s AND ts::date <= %s;", params)
    paquetes = int(round(_scalar(cur, "SELECT COALESCE(SUM(paquetes),0) FROM fractionations WHERE ts::date >= %s AND ts::date <= %s;", params)))
    merma_kg = _scalar(cur, "SELECT COALESCE(SUM(kg),0) FROM lot_mermas WHERE ts::date >= %s AND ts::date <= %s;", params)
    dias_activos = _count(cur, "SELECT COUNT(DISTINCT ts::date) FROM fractionations WHERE ts::date >= %s AND ts::date <= %s;", params)
    productos_activos = _count(cur, "SELECT COUNT(DISTINCT product_id) FROM fractionations WHERE ts::date >= %s AND ts::date <= %s;", params)

    stock_terminado_gs, stock_mp_gs = _current_inventory_values(cur)
    stock_terminado_ini_gs, stock_mp_ini_gs = _inventory_snapshot_values(cur, apertura_fecha)
    if saldo_base_dt == quarter_start:
        banco_inicio = saldo_base
    else:
        banco_inicio = _bank_estimate_between(cur, saldo_base_dt, quarter_start, saldo_base)
        banco_inicio += _json_sum_between(data_json.get("ventas", []), saldo_base_dt, quarter_start)
        banco_inicio += _json_sum_between(data_json.get("gastos", []), saldo_base_dt, quarter_start) * -1.0
        banco_inicio += _json_sum_between(data_json.get("compras", []), saldo_base_dt, quarter_start) * -1.0

    monthly_rows: list[dict] = []
    for month in months:
        p = (str(year), month)
        ventas_mes = _scalar(
            cur,
            """
            SELECT COALESCE(SUM(total_gs),0)
            FROM sales_invoices
            WHERE TO_CHAR((ts)::timestamp, 'YYYY')=%s AND TO_CHAR((ts)::timestamp, 'MM')=%s;
            """,
            p,
        ) + _scalar(
            cur,
            """
            SELECT COALESCE(SUM(total_gs),0)
            FROM bag_sales
            WHERE TO_CHAR((ts)::timestamp, 'YYYY')=%s AND TO_CHAR((ts)::timestamp, 'MM')=%s;
            """,
            p,
        )
        nc_mes = _scalar(
            cur,
            """
            SELECT COALESCE(SUM(total_gs),0)
            FROM credit_notes
            WHERE TO_CHAR((ts)::timestamp, 'YYYY')=%s AND TO_CHAR((ts)::timestamp, 'MM')=%s;
            """,
            p,
        )
        compras_mes = _scalar(
            cur,
            """
            SELECT COALESCE(SUM(costo_total_gs),0)
            FROM raw_lots
            WHERE TO_CHAR((ts)::timestamp, 'YYYY')=%s AND TO_CHAR((ts)::timestamp, 'MM')=%s;
            """,
            p,
        )
        gastos_mes = _scalar(
            cur,
            """
            SELECT COALESCE(SUM(monto_gs),0)
            FROM expenses
            WHERE TO_CHAR((ts)::timestamp, 'YYYY')=%s AND TO_CHAR((ts)::timestamp, 'MM')=%s;
            """,
            p,
        )
        ventas_netas_mes = ventas_mes - nc_mes
        flujo_mes = ventas_netas_mes - compras_mes - gastos_mes
        monthly_rows.append(
            {
                "mes": MONTH_LABELS[month],
                "ventas_brutas": ventas_mes,
                "notas_credito": nc_mes,
                "ventas_netas": ventas_netas_mes,
                "compras": compras_mes,
                "gastos": gastos_mes,
                "flujo": flujo_mes,
            }
        )

    cur.execute(
        """
        SELECT p.name, COALESCE(SUM(sii.line_total), 0) AS total
        FROM sales_invoice_items sii
        JOIN sales_invoices si ON si.id = sii.invoice_id
        JOIN products p ON p.id = sii.product_id
        WHERE si.ts::date >= %s AND si.ts::date <= %s
        GROUP BY p.name
        ORDER BY total DESC, p.name
        LIMIT 8;
        """,
        params,
    )
    top_products_sales = [(str(name), float(total or 0)) for name, total in cur.fetchall()]

    cur.execute(
        """
        SELECT p.name, COALESCE(SUM(f.kg_consumidos), 0) AS kg
        FROM fractionations f
        JOIN products p ON p.id = f.product_id
        WHERE f.ts::date >= %s AND f.ts::date <= %s
        GROUP BY p.name
        ORDER BY kg DESC, p.name
        LIMIT 8;
        """,
        params,
    )
    top_products_prod = [(str(name), float(total or 0)) for name, total in cur.fetchall()]

    cur.execute(
        """
        SELECT COALESCE(tipo, 'Sin tipo') AS tipo, COALESCE(SUM(monto_gs), 0) AS total
        FROM expenses
        WHERE ts::date >= %s AND ts::date <= %s
        GROUP BY COALESCE(tipo, 'Sin tipo')
        ORDER BY total DESC, tipo
        LIMIT 8;
        """,
        params,
    )
    gastos_tipo = [(str(name), float(total or 0)) for name, total in cur.fetchall()]

    cur.execute(
        """
        SELECT COALESCE(proveedor, 'Sin proveedor') AS proveedor, COALESCE(SUM(costo_total_gs), 0) AS total
        FROM raw_lots
        WHERE ts::date >= %s AND ts::date <= %s
        GROUP BY COALESCE(proveedor, 'Sin proveedor')
        ORDER BY total DESC, proveedor
        LIMIT 8;
        """,
        params,
    )
    proveedores = [(str(name), float(total or 0)) for name, total in cur.fetchall()]
    cn.close()

    ventas_brutas = ventas_facturas + ventas_bolsas
    ventas_netas = ventas_brutas - notas_credito
    ventas_retencion = ventas_retencion_fact + ventas_bolsas - notas_credito_ret
    flujo_neto = ventas_netas - compras - gastos

    return QuarterlyData(
        year=year,
        quarter=quarter,
        d1=d1,
        d2=d2,
        months=months,
        ventas_brutas=ventas_brutas,
        ventas_bolsas=ventas_bolsas,
        notas_credito=notas_credito,
        ventas_netas=ventas_netas,
        ventas_retencion=ventas_retencion,
        compras=compras,
        gastos=gastos,
        flujo_neto=flujo_neto,
        facturas=facturas,
        ventas_bolsas_ops=ventas_bolsas_ops,
        notas_credito_count=notas_credito_count,
        lotes_comprados=lotes_comprados,
        fraccionamientos=fraccionamientos,
        kg_consumidos=kg_consumidos,
        paquetes=paquetes,
        merma_kg=merma_kg,
        dias_activos=dias_activos,
        productos_activos=productos_activos,
        stock_terminado_gs=stock_terminado_gs,
        stock_mp_gs=stock_mp_gs,
        valor_en_planta_gs=stock_terminado_gs + stock_mp_gs,
        apertura_fecha=apertura_fecha,
        valor_inicio_trimestre_gs=stock_terminado_ini_gs + stock_mp_ini_gs,
        saldo_base_fecha=saldo_base_fecha,
        estimado_banco_inicio_trimestre_gs=banco_inicio,
        monthly_rows=monthly_rows,
        top_products_sales=top_products_sales,
        top_products_prod=top_products_prod,
        gastos_tipo=gastos_tipo,
        proveedores=proveedores,
    )


def _summary_lines(data: QuarterlyData, mode: str) -> list[tuple[str, str]]:
    quarter_label = data.saldo_base_fecha or data.apertura_fecha
    lines = [
        ("Ventas brutas", _fmt_gs(data.ventas_brutas)),
        ("Notas de credito", _fmt_gs(data.notas_credito)),
        ("Ventas netas", _fmt_gs(data.ventas_netas)),
        ("Ventas con retencion", _fmt_gs(data.ventas_retencion)),
        ("Compras", _fmt_gs(data.compras)),
        ("Gastos", _fmt_gs(data.gastos)),
        ("Flujo neto", _fmt_gs(data.flujo_neto)),
        ("Facturas", str(data.facturas)),
        ("Notas emitidas", str(data.notas_credito_count)),
    ]
    if mode in ("Administrativo", "Completo"):
        lines.extend(
            [
                ("Fraccionamientos", str(data.fraccionamientos)),
                ("Kg consumidos", _fmt_num(data.kg_consumidos, 2)),
                ("Paquetes", f"{data.paquetes:,}".replace(",", ".")),
                ("Merma kg", _fmt_num(data.merma_kg, 2)),
                ("Dias activos", str(data.dias_activos)),
                (f"Estimado banco inicio ({quarter_label})", _fmt_gs(data.estimado_banco_inicio_trimestre_gs)),
                (f"Valor al inicio ({data.apertura_fecha})", _fmt_gs(data.valor_inicio_trimestre_gs)),
                ("Valor en planta Gs", _fmt_gs(data.valor_en_planta_gs)),
                ("Stock terminado Gs", _fmt_gs(data.stock_terminado_gs)),
                ("Stock MP Gs", _fmt_gs(data.stock_mp_gs)),
            ]
        )
    return lines


def _build_cover_page(pdf: PdfPages, empresa: str, data: QuarterlyData, mode: str):
    fig = plt.figure(figsize=(11.69, 8.27))
    fig.patch.set_facecolor("white")
    _add_page_header(fig, f"Reporte Trimestral {mode}", f"{empresa} | {data.quarter} {data.year} | {data.d1} a {data.d2}")
    fig.text(0.06, 0.845, "Resumen ejecutivo", fontsize=14, fontweight="bold", color="#111827", family=PDF_FONT)
    fig.text(0.06, 0.818, "Sintesis de resultado contable y operativo del trimestre.", fontsize=10, color="#4b5563", family=PDF_FONT)

    rows = _summary_lines(data, mode)
    left_rows = rows[: (len(rows) + 1) // 2]
    right_rows = rows[(len(rows) + 1) // 2 :]

    def draw_summary_block(x_label: float, x_value: float, top_y: float, items: list[tuple[str, str]]):
        y = top_y
        for label, value in items:
            fig.text(x_label, y, label, fontsize=10, color="#374151", family=PDF_FONT)
            fig.text(x_value, y, value, fontsize=10.5, color="#111827", family=PDF_FONT, ha="right", fontweight="bold")
            fig.lines.append(
                plt.Line2D([x_label, x_value], [y - 0.012, y - 0.012], transform=fig.transFigure, color="#e5e7eb", linewidth=0.7)
            )
            y -= 0.043

    draw_summary_block(0.06, 0.45, 0.765, left_rows)
    draw_summary_block(0.54, 0.93, 0.765, right_rows)

    hallazgos = [
        f"Cierre del trimestre: ventas netas { _fmt_gs(data.ventas_netas) } Gs y flujo neto { _fmt_gs(data.flujo_neto) } Gs.",
        f"Estimado en banco al inicio del trimestre: { _fmt_gs(data.estimado_banco_inicio_trimestre_gs) } Gs.",
        f"Valor en planta al inicio ({data.apertura_fecha}): { _fmt_gs(data.valor_inicio_trimestre_gs) } Gs.",
        f"Notas de credito: { _fmt_num((data.notas_credito / data.ventas_brutas * 100.0) if data.ventas_brutas else 0.0, 1) }% sobre ventas brutas.",
        f"Actividad: {data.facturas} facturas, {data.notas_credito_count} notas de credito y {data.fraccionamientos} fraccionamientos.",
    ]
    fig.text(0.06, 0.33, "Comentarios ejecutivos", fontsize=14, fontweight="bold", color="#111827", family=PDF_FONT)
    fig.lines.append(plt.Line2D([0.06, 0.94], [0.318, 0.318], transform=fig.transFigure, color="#d1d5db", linewidth=0.8))
    yy = 0.282
    for line in hallazgos:
        fig.text(0.07, yy, f"- {line}", fontsize=10, color="#1f2937", family=PDF_FONT)
        yy -= 0.05

    fig.text(0.06, 0.08, "Documento generado automaticamente desde fraccionadora.db", fontsize=8.8, color="#6b7280", family=PDF_FONT)
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def _build_finance_page(pdf: PdfPages, data: QuarterlyData):
    fig = plt.figure(figsize=(11.69, 8.27))
    gs = fig.add_gridspec(2, 1, height_ratios=[1.4, 1.0], hspace=0.32)
    _add_page_header(fig, "Bloque contable", f"Resumen financiero trimestral | {data.quarter} {data.year}")
    ax = fig.add_subplot(gs[0])
    months = [row["mes"] for row in data.monthly_rows]
    ventas = [row["ventas_netas"] for row in data.monthly_rows]
    compras = [row["compras"] for row in data.monthly_rows]
    gastos = [row["gastos"] for row in data.monthly_rows]
    notas = [row["notas_credito"] for row in data.monthly_rows]
    flujo = [row["flujo"] for row in data.monthly_rows]
    x = list(range(len(months)))
    width = 0.18
    ax.bar([i - 1.5 * width for i in x], ventas, width=width, color="steelblue", label="Ventas netas")
    ax.bar([i - 0.5 * width for i in x], compras, width=width, color="indianred", label="Compras")
    ax.bar([i + 0.5 * width for i in x], gastos, width=width, color="goldenrod", label="Gastos")
    ax.bar([i + 1.5 * width for i in x], notas, width=width, color="mediumpurple", label="Notas de credito")
    ax.plot(x, flujo, color="gray", marker="o", linewidth=2.0, label="Flujo neto")
    ax.set_title(f"Resumen financiero {data.quarter} {data.year}", fontsize=14, fontweight="bold", family=PDF_FONT)
    ax.set_xticks(x)
    ax.set_xticklabels(months)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.set_axisbelow(True)
    ax.legend(loc="upper left", ncol=3, fontsize=9, frameon=False, prop={"family": PDF_FONT})

    ax_tbl = fig.add_subplot(gs[1])
    ax_tbl.axis("off")
    headers = ["Mes", "Ventas netas", "Notas", "Compras", "Gastos", "Flujo"]
    table_rows = [
        [
            row["mes"],
            _fmt_gs(row["ventas_netas"]),
            _fmt_gs(row["notas_credito"]),
            _fmt_gs(row["compras"]),
            _fmt_gs(row["gastos"]),
            _fmt_gs(row["flujo"]),
        ]
        for row in data.monthly_rows
    ]
    tbl = ax_tbl.table(cellText=table_rows, colLabels=headers, cellLoc="center", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.6)
    _style_table(tbl, header_bg="#f3f4f6", body_bg="#fbfbfb", edge="#d1d5db")
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def _build_commercial_page(pdf: PdfPages, data: QuarterlyData):
    fig = plt.figure(figsize=(11.69, 8.27))
    gs = fig.add_gridspec(1, 2, wspace=0.28)
    _add_page_header(fig, "Bloque comercial", f"Ventas y compras del trimestre | {data.quarter} {data.year}")
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1])

    sales_names = [name for name, _ in data.top_products_sales] or ["Sin datos"]
    sales_vals = [val for _, val in data.top_products_sales] or [0.0]
    ax1.barh(sales_names, sales_vals, color="#2563eb")
    ax1.invert_yaxis()
    ax1.set_title("Top productos por ventas", fontsize=13, fontweight="bold", family=PDF_FONT)
    ax1.grid(axis="x", linestyle="--", alpha=0.25)

    prov_names = [name for name, _ in data.proveedores] or ["Sin datos"]
    prov_vals = [val for _, val in data.proveedores] or [0.0]
    ax2.barh(prov_names, prov_vals, color="#dc2626")
    ax2.invert_yaxis()
    ax2.set_title("Top proveedores por compras", fontsize=13, fontweight="bold", family=PDF_FONT)
    ax2.grid(axis="x", linestyle="--", alpha=0.25)

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def _build_operational_page(pdf: PdfPages, data: QuarterlyData):
    fig = plt.figure(figsize=(11.69, 8.27))
    gs = fig.add_gridspec(2, 2, hspace=0.35, wspace=0.28)
    _add_page_header(fig, "Bloque administrativo", f"Produccion, gastos y stock | {data.quarter} {data.year}")
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])
    ax3 = fig.add_subplot(gs[1, :])

    prod_names = [name for name, _ in data.top_products_prod] or ["Sin datos"]
    prod_vals = [val for _, val in data.top_products_prod] or [0.0]
    ax1.barh(prod_names, prod_vals, color="#0f766e")
    ax1.invert_yaxis()
    ax1.set_title("Top productos por kg consumidos", fontsize=12, fontweight="bold", family=PDF_FONT)
    ax1.grid(axis="x", linestyle="--", alpha=0.25)

    gastos_names = [name for name, _ in data.gastos_tipo] or ["Sin datos"]
    gastos_vals = [val for _, val in data.gastos_tipo] or [0.0]
    ax2.bar(gastos_names, gastos_vals, color="#d97706")
    ax2.set_title("Gastos por tipo", fontsize=12, fontweight="bold", family=PDF_FONT)
    ax2.tick_params(axis="x", rotation=35)
    ax2.grid(axis="y", linestyle="--", alpha=0.25)

    ax3.axis("off")
    ops_rows = [
        ["Fraccionamientos", str(data.fraccionamientos)],
        ["Paquetes producidos", f"{data.paquetes:,}".replace(",", ".")],
        ["Kg consumidos", _fmt_num(data.kg_consumidos, 2)],
        ["Merma kg", _fmt_num(data.merma_kg, 2)],
        ["Dias activos", str(data.dias_activos)],
        ["Estimado banco al inicio", _fmt_gs(data.estimado_banco_inicio_trimestre_gs)],
        [f"Valor al inicio ({data.apertura_fecha})", _fmt_gs(data.valor_inicio_trimestre_gs)],
        ["Productos activos", str(data.productos_activos)],
        ["Valor en planta", _fmt_gs(data.valor_en_planta_gs)],
        ["Stock terminado valuado", _fmt_gs(data.stock_terminado_gs)],
        ["Stock MP valuado", _fmt_gs(data.stock_mp_gs)],
    ]
    tbl = ax3.table(cellText=ops_rows, colLabels=["Indicador", "Valor"], cellLoc="left", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(10)
    tbl.scale(1, 1.8)
    _style_table(tbl, header_bg="#f3f4f6", body_bg="#fbfbfb", edge="#d1d5db")
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def generate_quarterly_pdf(db_path: Path, year: int, quarter: str, empresa: str, mode: str, out_dir: Path | None = None) -> Path:
    _apply_pdf_font()
    data = _collect_quarterly_data(db_path, year, quarter)
    out = Path(out_dir or OUT_DIR_DEFAULT)
    out.mkdir(parents=True, exist_ok=True)
    slug = mode.lower().replace(" ", "_")
    pdf_path = out / f"reporte_trimestral_{slug}_{year}_{quarter.lower()}.pdf"
    with PdfPages(pdf_path) as pdf:
        _build_cover_page(pdf, empresa, data, mode)
        if mode in ("Contable", "Completo"):
            _build_finance_page(pdf, data)
            _build_commercial_page(pdf, data)
        if mode in ("Administrativo", "Completo"):
            _build_operational_page(pdf, data)
    return pdf_path


class ReporteTrimestralWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Reporte trimestral PDF")
        self.resize(760, 420)
        self.setMinimumSize(680, 360)
        self._build_ui()
        self._load_years()

    def _build_ui(self):
        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        root = QtWidgets.QVBoxLayout(central)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        title = QtWidgets.QLabel("Reporte trimestral PDF")
        title.setStyleSheet("font-size: 22px; font-weight: 700;")
        root.addWidget(title)

        sub = QtWidgets.QLabel("Genera un PDF contable, administrativo o completo y lo abre en el navegador.")
        sub.setWordWrap(True)
        sub.setStyleSheet("color: #475569;")
        root.addWidget(sub)

        form_card = QtWidgets.QFrame()
        form_card.setStyleSheet("QFrame { background: white; border: 1px solid #d9e2ef; border-radius: 12px; }")
        form = QtWidgets.QFormLayout(form_card)
        form.setContentsMargins(14, 14, 14, 14)
        form.setSpacing(10)

        self.cb_year = QtWidgets.QComboBox()
        self.cb_quarter = QtWidgets.QComboBox()
        self.cb_quarter.addItems(["T1", "T2", "T3", "T4"])
        self.cb_mode = QtWidgets.QComboBox()
        self.cb_mode.addItems(["Completo", "Contable", "Administrativo"])
        self.ent_empresa = QtWidgets.QLineEdit("Fraccionadora")
        self.ent_db = QtWidgets.QLineEdit(str(_resolve_db(None)))
        self.lbl_out = QtWidgets.QLabel(str(OUT_DIR_DEFAULT))
        self.lbl_out.setWordWrap(True)

        form.addRow("Ano", self.cb_year)
        form.addRow("Trimestre", self.cb_quarter)
        form.addRow("Tipo", self.cb_mode)
        form.addRow("Empresa", self.ent_empresa)
        form.addRow("Base de datos", self.ent_db)
        form.addRow("Salida", self.lbl_out)
        root.addWidget(form_card)

        btns = QtWidgets.QHBoxLayout()
        self.btn_generate = QtWidgets.QPushButton("Generar PDF y abrir")
        self.btn_generate.setStyleSheet("QPushButton { background:#1d4ed8; color:white; border-radius:10px; padding:10px 14px; font-weight:600; }")
        self.btn_open_dir = QtWidgets.QPushButton("Abrir carpeta")
        btns.addWidget(self.btn_generate)
        btns.addWidget(self.btn_open_dir)
        btns.addStretch(1)
        root.addLayout(btns)

        self.lbl_status = QtWidgets.QLabel("Listo.")
        self.lbl_status.setWordWrap(True)
        self.lbl_status.setStyleSheet("color: #334155;")
        root.addWidget(self.lbl_status)

        self.btn_generate.clicked.connect(self._generate_and_open)
        self.btn_open_dir.clicked.connect(self._open_output_dir)

    def _load_years(self):
        db_path = _resolve_db(self.ent_db.text().strip() or None)
        cn = db.connect("fraccionadora")
        cur = cn.cursor()
        years: set[str] = set()
        for table in ("sales_invoices", "bag_sales", "raw_lots", "fractionations", "expenses", "credit_notes"):
            try:
                cur.execute(f"SELECT DISTINCT TO_CHAR((ts)::timestamp, 'YYYY') FROM {table} ORDER BY 1;")
                years.update(str(row[0]) for row in cur.fetchall() if row[0])
            except Exception:
                continue
        cn.close()
        items = sorted(years) or [str(date.today().year)]
        self.cb_year.clear()
        self.cb_year.addItems(items)
        self.cb_year.setCurrentText(str(date.today().year))

    def _generate_and_open(self):
        try:
            db_path = _resolve_db(self.ent_db.text().strip() or None)
            year = int(self.cb_year.currentText())
            quarter = self.cb_quarter.currentText().strip()
            mode = self.cb_mode.currentText().strip()
            empresa = (self.ent_empresa.text() or "Fraccionadora").strip()
            self.lbl_status.setText("Generando PDF...")
            QtWidgets.QApplication.processEvents()
            pdf_path = generate_quarterly_pdf(db_path, year, quarter, empresa, mode)
            webbrowser.open_new_tab(pdf_path.resolve().as_uri())
            self.lbl_status.setText(f"PDF generado: {pdf_path}")
        except Exception as exc:
            self.lbl_status.setText(f"Error: {exc}")
            QtWidgets.QMessageBox.critical(self, "Reporte trimestral", f"No se pudo generar el PDF:\n{exc}")

    def _open_output_dir(self):
        OUT_DIR_DEFAULT.mkdir(parents=True, exist_ok=True)
        webbrowser.open_new_tab(OUT_DIR_DEFAULT.resolve().as_uri())


def main():
    app = QtWidgets.QApplication.instance() or QtWidgets.QApplication(sys.argv)
    win = ReporteTrimestralWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

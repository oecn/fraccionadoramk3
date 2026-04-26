# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, timedelta
import math
from pathlib import Path
from typing import Optional

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import db
from PySide6 import QtCharts, QtCore, QtGui, QtWidgets


DB_CANDIDATES = [
    ROOT_DIR / "GCMK8" / "fraccionadora.db",
    ROOT_DIR / "fraccionadora.db",
]
ACTIVE_HOURS_PER_DAY = 8.0
LINE_MAX_UNITS_PER_HOUR = 900.0


def resolve_db(db_arg: Optional[str] = None) -> Path:
    if db_arg:
        p = Path(db_arg).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"No existe la base: {p}")
        return p
    for p in DB_CANDIDATES:
        if p.exists():
            return p
    raise FileNotFoundError("No se encontró fraccionadora.db en rutas esperadas.")


def fmt_gs(x: float | int | None) -> str:
    try:
        return f"{float(x or 0):,.0f}".replace(",", ".")
    except Exception:
        return "0"


def fmt_num(x: float | int | None, digits: int = 2) -> str:
    try:
        return f"{float(x or 0):,.{digits}f}".replace(",", "_").replace(".", ",").replace("_", ".")
    except Exception:
        return "0"


def fmt_int(x: float | int | None) -> str:
    try:
        return f"{int(round(float(x or 0))):,}".replace(",", ".")
    except Exception:
        return "0"


def fmt_short_ts(ts: str | None) -> str:
    text = str(ts or "").strip()
    if not text:
        return ""
    try:
        dt_part, tm_part = text.split(" ", 1)
        yyyy, mm, dd = dt_part.split("-")
        hm = tm_part[:5]
        return f"{dd}/{mm} {hm}"
    except Exception:
        return text[:16]


def estimate_line_speed(unidades: int, dias_activos: int) -> tuple[float, float]:
    active_hours = float(dias_activos or 0) * ACTIVE_HOURS_PER_DAY
    units_per_hour = (float(unidades or 0) / active_hours) if active_hours > 0 else 0.0
    ratio = (units_per_hour / LINE_MAX_UNITS_PER_HOUR) if LINE_MAX_UNITS_PER_HOUR > 0 else 0.0
    return units_per_hour, max(0.0, min(1.0, ratio))


@dataclass
class KPIData:
    fraccionamientos: int = 0
    productos_activos: int = 0
    presentaciones_activas: int = 0
    paquetes: int = 0
    unidades: int = 0
    kg_consumidos: float = 0.0
    bolsas_usadas_eq: float = 0.0
    dias_activos: int = 0


class Repo:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.cn = db.connect("fraccionadora")

    def list_products(self):
        cur = self.cn.cursor()
        cur.execute("SELECT id, name FROM products ORDER BY name;")
        return cur.fetchall()

    def _prod_filter(self, product_id: Optional[int], alias: str = "f") -> tuple[str, list]:
        if product_id is None:
            return "", []
        return f" AND {alias}.product_id = ?", [int(product_id)]

    def production_kpis(self, d1: str, d2: str, product_id: Optional[int] = None) -> KPIData:
        where, params = self._prod_filter(product_id)
        cur = self.cn.cursor()
        cur.execute(
            f"""
            SELECT
                COUNT(*) AS fraccs,
                COUNT(DISTINCT f.product_id) AS productos,
                COUNT(DISTINCT f.product_id || '-' || f.gramaje) AS presentaciones,
                COALESCE(SUM(f.paquetes), 0) AS paquetes,
                COALESCE(SUM(CASE WHEN f.gramaje <= 250 THEN f.paquetes * 20 ELSE f.paquetes * 10 END), 0) AS unidades,
                COALESCE(SUM(f.kg_consumidos), 0) AS kg_consumidos,
                COALESCE(SUM(
                    CASE
                        WHEN lower(trim(p.name)) = 'arroz' THEN f.kg_consumidos / 50.0
                        WHEN lower(trim(p.name)) = 'galleta molida' THEN f.kg_consumidos / 25.0
                        ELSE f.kg_consumidos / 50.0
                    END
                ), 0) AS bolsas_usadas_eq,
                COUNT(DISTINCT f.ts::date) AS dias_activos
            FROM fractionations f
            JOIN products p ON p.id = f.product_id
            WHERE f.ts::date >= %s AND f.ts::date <= %s
            {where};
            """,
            [d1, d2] + params,
        )
        row = cur.fetchone()
        return KPIData(
            fraccionamientos=int(row["fraccs"] or 0),
            productos_activos=int(row["productos"] or 0),
            presentaciones_activas=int(row["presentaciones"] or 0),
            paquetes=int(row["paquetes"] or 0),
            unidades=int(row["unidades"] or 0),
            kg_consumidos=float(row["kg_consumidos"] or 0.0),
            bolsas_usadas_eq=float(row["bolsas_usadas_eq"] or 0.0),
            dias_activos=int(row["dias_activos"] or 0),
        )

    def daily_production(self, d1: str, d2: str, product_id: Optional[int] = None):
        where, params = self._prod_filter(product_id)
        cur = self.cn.cursor()
        cur.execute(
            f"""
            SELECT f.ts::date AS fecha,
                   COUNT(*) AS fraccs,
                   COALESCE(SUM(f.paquetes), 0) AS paquetes,
                   COALESCE(SUM(f.kg_consumidos), 0) AS kg
            FROM fractionations f
            WHERE f.ts::date >= %s AND f.ts::date <= %s
            {where}
            GROUP BY f.ts::date
            ORDER BY f.ts::date;
            """,
            [d1, d2] + params,
        )
        return [dict(r) for r in cur.fetchall()]

    def product_mix(self, d1: str, d2: str, product_id: Optional[int] = None):
        where, params = self._prod_filter(product_id)
        cur = self.cn.cursor()
        cur.execute(
            f"""
            SELECT p.name AS producto,
                   COALESCE(SUM(f.kg_consumidos), 0) AS kg,
                   COALESCE(SUM(f.paquetes), 0) AS paquetes
            FROM fractionations f
            JOIN products p ON p.id = f.product_id
            WHERE f.ts::date >= %s AND f.ts::date <= %s
            {where}
            GROUP BY p.name
            ORDER BY kg DESC, paquetes DESC;
            """,
            [d1, d2] + params,
        )
        return [dict(r) for r in cur.fetchall()]

    def gramaje_mix(self, d1: str, d2: str, product_id: Optional[int] = None):
        where, params = self._prod_filter(product_id)
        cur = self.cn.cursor()
        cur.execute(
            f"""
            SELECT f.gramaje,
                   COALESCE(SUM(f.paquetes), 0) AS paquetes,
                   COALESCE(SUM(f.kg_consumidos), 0) AS kg
            FROM fractionations f
            WHERE f.ts::date >= %s AND f.ts::date <= %s
            {where}
            GROUP BY f.gramaje
            ORDER BY kg DESC, f.gramaje ASC;
            """,
            [d1, d2] + params,
        )
        return [dict(r) for r in cur.fetchall()]

    def lot_metrics(self, d1: str, d2: str, product_id: Optional[int] = None):
        cur = self.cn.cursor()
        params = [d1, d2, d1, d2]
        where = ""
        if product_id is not None:
            where = " WHERE rl.product_id = ?"
            params.append(int(product_id))
        cur.execute(
            f"""
            WITH used_by_lot AS (
                SELECT lf.lot_id,
                       COUNT(DISTINCT lf.fractionation_id) AS fraccs,
                       COALESCE(SUM(lf.kg_consumidos), 0) AS kg_usados
                FROM lot_fractionations lf
                JOIN fractionations f ON f.id = lf.fractionation_id
                WHERE f.ts::date >= %s AND f.ts::date <= %s
                GROUP BY lf.lot_id
            ),
            merma_by_lot AS (
                SELECT lot_id, COALESCE(SUM(kg), 0) AS kg_merma
                FROM lot_mermas
                WHERE ts::date >= %s AND ts::date <= %s
                GROUP BY lot_id
            )
            SELECT rl.id AS lot_id, p.name AS producto, COALESCE(rl.lote, '') AS lote,
                   COALESCE(rl.kg_inicial, 0) AS kg_inicial,
                   COALESCE(ubl.kg_usados, 0) AS kg_usados,
                   COALESCE(mbl.kg_merma, 0) AS kg_merma,
                   COALESCE(rl.kg_saldo, 0) AS kg_saldo,
                   COALESCE(ubl.fraccs, 0) AS fraccs,
                   COALESCE(rl.costo_total_gs, 0) AS costo_total_gs
            FROM raw_lots rl
            JOIN products p ON p.id = rl.product_id
            LEFT JOIN used_by_lot ubl ON ubl.lot_id = rl.id
            LEFT JOIN merma_by_lot mbl ON mbl.lot_id = rl.id
            {where}
            ORDER BY kg_usados DESC, rl.id DESC;
            """,
            params,
        )
        return [dict(r) for r in cur.fetchall()]

    def production_vs_sales(self, d1: str, d2: str, product_id: Optional[int] = None):
        cur = self.cn.cursor()
        params = [d1, d2, d1, d2]
        prod_f = ""
        prod_s = ""
        if product_id is not None:
            prod_f = " AND f.product_id = ?"
            prod_s = " AND sii.product_id = ?"
            params.extend([int(product_id), int(product_id)])
        cur.execute(
            f"""
            WITH prod AS (
                SELECT f.product_id, f.gramaje, COALESCE(SUM(f.paquetes), 0) AS producidos,
                       COALESCE(SUM(f.kg_consumidos), 0) AS kg
                FROM fractionations f
                WHERE f.ts::date >= %s AND f.ts::date <= %s
                {prod_f}
                GROUP BY f.product_id, f.gramaje
            ),
            vent AS (
                SELECT sii.product_id, sii.gramaje,
                       COALESCE(SUM(sii.cantidad), 0) AS vendidos,
                       COALESCE(SUM(sii.line_total), 0) AS venta_gs
                FROM sales_invoice_items sii
                JOIN sales_invoices si ON si.id = sii.invoice_id
                WHERE si.ts::date >= %s AND si.ts::date <= %s
                {prod_s}
                GROUP BY sii.product_id, sii.gramaje
            ),
            keys AS (
                SELECT product_id, gramaje FROM prod
                UNION
                SELECT product_id, gramaje FROM vent
            )
            SELECT p.name AS producto, k.gramaje,
                   COALESCE(prod.producidos, 0) AS producidos,
                   COALESCE(prod.kg, 0) AS kg_consumidos,
                   COALESCE(vent.vendidos, 0) AS vendidos,
                   COALESCE(ps.paquetes, 0) AS stock_actual,
                   COALESCE(vent.venta_gs, 0) AS venta_gs
            FROM keys k
            JOIN products p ON p.id = k.product_id
            LEFT JOIN prod ON prod.product_id = k.product_id AND prod.gramaje = k.gramaje
            LEFT JOIN vent ON vent.product_id = k.product_id AND vent.gramaje = k.gramaje
            LEFT JOIN package_stock ps ON ps.product_id = k.product_id AND ps.gramaje = k.gramaje
            ORDER BY COALESCE(prod.producidos, 0) DESC, p.name, k.gramaje;
            """,
            params,
        )
        return [dict(r) for r in cur.fetchall()]

    def weekly_product_consumption(self, week_date: str, product_id: Optional[int] = None):
        cur = self.cn.cursor()
        params = [week_date, week_date]
        where = ""
        if product_id is not None:
            where = " WHERE p.id = ?"
            params.append(int(product_id))
        cur.execute(
            f"""
            WITH week_bounds AS (
                SELECT
                    %s, '-' || ((CAST(TO_CHAR((%s::date::timestamp, 'YYYY-MM-DD') AS INTEGER) + 6) % 7) || ' days') AS d1
            ),
            weekly AS (
                SELECT
                    f.product_id,
                    COALESCE(SUM(f.kg_consumidos), 0) AS kg_semana,
                    COUNT(*) AS fraccs_semana,
                    COUNT(DISTINCT f.ts::date) AS dias_activos_semana
                FROM fractionations f, week_bounds wb
                WHERE f.ts::date >= wb.d1 AND f.ts::date <= wb.d1, '+6 days'::date
                GROUP BY f.product_id
            ),
            weekly_history AS (
                SELECT
                    product_id,
                    AVG(kg_week) AS promedio_semanal_kg
                FROM (
                    SELECT
                        f.product_id,
                        f.ts, '-' || ((CAST(TO_CHAR((f.ts::date::timestamp, 'YYYY-MM-DD') AS INTEGER) + 6) % 7) || ' days') AS week_start,
                        SUM(f.kg_consumidos) AS kg_week
                    FROM fractionations f
                    GROUP BY f.product_id, week_start
                ) t
                GROUP BY product_id
            )
            SELECT
                p.id AS product_id,
                p.name AS producto,
                COALESCE(w.kg_semana, 0) AS kg_semana,
                COALESCE(w.fraccs_semana, 0) AS fraccs_semana,
                COALESCE(w.dias_activos_semana, 0) AS dias_activos_semana,
                COALESCE(wh.promedio_semanal_kg, 0) AS promedio_semanal_kg
            FROM products p
            LEFT JOIN weekly w ON w.product_id = p.id
            LEFT JOIN weekly_history wh ON wh.product_id = p.id
            {where}
            ORDER BY kg_semana DESC, producto;
            """,
            params,
        )
        return [dict(r) for r in cur.fetchall()]

    def search_lots(self, status: str = "Todos", search: str = "", product_id: Optional[int] = None):
        cur = self.cn.cursor()
        where = []
        params: list = []
        if status == "Abiertos":
            where.append("COALESCE(rl.cerrado, 0) = 0")
        elif status == "Cerrados":
            where.append("COALESCE(rl.cerrado, 0) = 1")
        if product_id is not None:
            where.append("rl.product_id = %s")
            params.append(int(product_id))
        txt = (search or "").strip()
        if txt:
            like = f"%{txt}%"
            where.append(
                "(COALESCE(rl.lote,'') ILIKE %s OR p.name ILIKE %s OR COALESCE(rl.proveedor,'') ILIKE %s OR COALESCE(rl.factura,'') ILIKE %s)"
            )
            params.extend([like, like, like, like])
        sql = """
            SELECT rl.id AS lot_id, p.name AS producto, COALESCE(rl.lote, '') AS lote,
                   COALESCE(rl.proveedor, '') AS proveedor, COALESCE(rl.factura, '') AS factura,
                   COALESCE(rl.kg_inicial, 0) AS kg_inicial, COALESCE(rl.kg_saldo, 0) AS kg_saldo,
                   COALESCE(rl.cerrado, 0) AS cerrado, rl.ts
            FROM raw_lots rl
            JOIN products p ON p.id = rl.product_id
        """
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY COALESCE(rl.cerrado, 0) ASC, rl.ts DESC, rl.id DESC;"
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

    def lot_header(self, lot_id: int):
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT rl.id AS lot_id, p.id AS product_id, p.name AS producto, COALESCE(rl.lote, '') AS lote,
                   COALESCE(rl.proveedor, '') AS proveedor, COALESCE(rl.factura, '') AS factura,
                   COALESCE(rl.kg_inicial, 0) AS kg_inicial, COALESCE(rl.kg_saldo, 0) AS kg_saldo,
                   COALESCE(rl.costo_total_gs, 0) AS costo_total_gs, COALESCE(rl.costo_kg_gs, 0) AS costo_kg_gs,
                   COALESCE(rl.cerrado, 0) AS cerrado, rl.ts
            FROM raw_lots rl
            JOIN products p ON p.id = rl.product_id
            WHERE rl.id = %s;
            """,
            (int(lot_id),),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def lot_kpis(self, lot_id: int) -> tuple[KPIData, dict]:
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT
                COUNT(DISTINCT f.id) AS fraccs,
                COUNT(DISTINCT f.product_id) AS productos,
                COUNT(DISTINCT f.product_id || '-' || f.gramaje) AS presentaciones,
                COALESCE(SUM(f.paquetes), 0) AS paquetes,
                COALESCE(SUM(CASE WHEN f.gramaje <= 250 THEN f.paquetes * 20 ELSE f.paquetes * 10 END), 0) AS unidades,
                COALESCE(SUM(lf.kg_consumidos), 0) AS kg_consumidos,
                COALESCE(SUM(
                    CASE
                        WHEN lower(trim(p.name)) = 'arroz' THEN lf.kg_consumidos / 50.0
                        WHEN lower(trim(p.name)) = 'galleta molida' THEN lf.kg_consumidos / 25.0
                        ELSE lf.kg_consumidos / 50.0
                    END
                ), 0) AS bolsas_usadas_eq,
                COUNT(DISTINCT f.ts::date) AS dias_activos
            FROM lot_fractionations lf
            JOIN fractionations f ON f.id = lf.fractionation_id
            JOIN products p ON p.id = f.product_id
            WHERE lf.lot_id = %s;
            """,
            (int(lot_id),),
        )
        row = cur.fetchone()
        cur.execute("SELECT COALESCE(SUM(kg), 0) AS kg_merma FROM lot_mermas WHERE lot_id=%s;", (int(lot_id),))
        merma_row = cur.fetchone()
        kpi = KPIData(
            fraccionamientos=int(row["fraccs"] or 0),
            productos_activos=int(row["productos"] or 0),
            presentaciones_activas=int(row["presentaciones"] or 0),
            paquetes=int(row["paquetes"] or 0),
            unidades=int(row["unidades"] or 0),
            kg_consumidos=float(row["kg_consumidos"] or 0.0),
            bolsas_usadas_eq=float(row["bolsas_usadas_eq"] or 0.0),
            dias_activos=int(row["dias_activos"] or 0),
        )
        extra = {"kg_merma": float(merma_row["kg_merma"] or 0.0)}
        return kpi, extra

    def lot_financial_kpis(self, lot_id: int) -> dict:
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT
                COALESCE(rl.costo_total_gs, 0) AS costo_total_gs,
                COALESCE(rl.costo_kg_gs, 0) AS costo_kg_gs,
                COALESCE(SUM(f.paquetes * COALESCE(pp.price_gs, 0)), 0) AS venta_estimada_gs
            FROM raw_lots rl
            LEFT JOIN lot_fractionations lf ON lf.lot_id = rl.id
            LEFT JOIN fractionations f ON f.id = lf.fractionation_id
            LEFT JOIN package_prices pp
                   ON pp.product_id = f.product_id
                  AND pp.gramaje = f.gramaje
            WHERE rl.id = %s
            GROUP BY rl.id, rl.costo_total_gs, rl.costo_kg_gs;
            """,
            (int(lot_id),),
        )
        row = cur.fetchone()
        if not row:
            return {"costo_kg_gs": 0.0, "venta_estimada_gs": 0.0, "beneficio_estimado_gs": 0.0, "margen_estimado_pct": 0.0}
        costo_total = float(row["costo_total_gs"] or 0.0)
        costo_kg = float(row["costo_kg_gs"] or 0.0)
        venta_estimada = float(row["venta_estimada_gs"] or 0.0)
        beneficio = venta_estimada - costo_total
        margen_pct = (beneficio / costo_total * 100.0) if costo_total else 0.0
        return {
            "costo_kg_gs": costo_kg,
            "venta_estimada_gs": venta_estimada,
            "beneficio_estimado_gs": beneficio,
            "margen_estimado_pct": margen_pct,
        }

    def lot_daily(self, lot_id: int):
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT f.ts::date AS fecha,
                   COUNT(*) AS fraccs,
                   COALESCE(SUM(f.paquetes), 0) AS paquetes,
                   COALESCE(SUM(lf.kg_consumidos), 0) AS kg
            FROM lot_fractionations lf
            JOIN fractionations f ON f.id = lf.fractionation_id
            WHERE lf.lot_id = %s
            GROUP BY f.ts::date
            ORDER BY f.ts::date;
            """,
            (int(lot_id),),
        )
        return [dict(r) for r in cur.fetchall()]

    def lot_gramaje_mix(self, lot_id: int):
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT f.gramaje,
                   COALESCE(SUM(f.paquetes), 0) AS paquetes,
                   COALESCE(SUM(lf.kg_consumidos), 0) AS kg
            FROM lot_fractionations lf
            JOIN fractionations f ON f.id = lf.fractionation_id
            WHERE lf.lot_id = %s
            GROUP BY f.gramaje
            ORDER BY kg DESC, f.gramaje ASC;
            """,
            (int(lot_id),),
        )
        return [dict(r) for r in cur.fetchall()]

    def lot_fractionation_rows(self, lot_id: int):
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT f.id AS frac_id, f.ts, p.name AS producto, f.gramaje, f.paquetes,
                   COALESCE(lf.kg_consumidos, 0) AS kg_consumidos
            FROM lot_fractionations lf
            JOIN fractionations f ON f.id = lf.fractionation_id
            JOIN products p ON p.id = f.product_id
            WHERE lf.lot_id = %s
            ORDER BY datetime(f.ts) DESC, f.id DESC;
            """,
            (int(lot_id),),
        )
        return [dict(r) for r in cur.fetchall()]


class KPIWidget(QtWidgets.QFrame):
    def __init__(self, title: str, accent: str):
        super().__init__()
        self.setObjectName("kpiCard")
        self.setMinimumHeight(86)
        self.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
        self._accent = accent
        lay = QtWidgets.QVBoxLayout(self)
        lay.setContentsMargins(12, 8, 12, 8)
        lay.setSpacing(1)
        self.bar = QtWidgets.QFrame()
        self.bar.setFixedHeight(3)
        self.bar.setStyleSheet(f"background:{accent}; border-radius:2px;")
        self.lbl_title = QtWidgets.QLabel(title)
        self.lbl_title.setObjectName("kpiTitle")
        self.lbl_value = QtWidgets.QLabel("-")
        self.lbl_value.setObjectName("kpiValue")
        self.lbl_sub = QtWidgets.QLabel("")
        self.lbl_sub.setObjectName("kpiSub")
        self.lbl_title.setWordWrap(True)
        self.lbl_value.setWordWrap(False)
        self.lbl_sub.setWordWrap(True)
        lay.addWidget(self.bar)
        lay.addWidget(self.lbl_title)
        lay.addWidget(self.lbl_value)
        lay.addWidget(self.lbl_sub)

    def set_data(self, value: str, sub: str = ""):
        self.lbl_value.setText(value)
        self.lbl_sub.setText(sub)

    def set_alert(self, enabled: bool):
        if enabled:
            self.setStyleSheet(
                "QFrame#kpiCard { background:#fff1f2; border:1px solid #fca5a5; border-radius:14px; }"
                " QLabel#kpiTitle { color:#b91c1c; }"
                " QLabel#kpiValue { color:#991b1b; font-size:16pt; font-weight:700; }"
                " QLabel#kpiSub { color:#b91c1c; }"
            )
            self.bar.setStyleSheet("background:#dc2626; border-radius:2px;")
        else:
            self.setStyleSheet("")
            self.bar.setStyleSheet(f"background:{self._accent}; border-radius:2px;")


class GaugeFaceWidget(QtWidgets.QWidget):
    def __init__(self, accent: str = "#7c2d12"):
        super().__init__()
        self._accent = QtGui.QColor(accent)
        self._ratio = 0.0
        self.setMinimumHeight(92)
        self.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)

    def set_ratio(self, ratio: float):
        self._ratio = max(0.0, min(1.0, float(ratio or 0.0)))
        self.update()

    def paintEvent(self, _event: QtGui.QPaintEvent):
        painter = QtGui.QPainter(self)
        painter.setRenderHint(QtGui.QPainter.Antialiasing)
        rect = self.rect().adjusted(10, 8, -10, -10)
        diameter = min(rect.width(), rect.height() * 2.0)
        radius = diameter / 2.0
        center = QtCore.QPointF(rect.center().x(), rect.bottom())
        arc_rect = QtCore.QRectF(center.x() - radius, center.y() - radius, radius * 2.0, radius * 2.0)

        base_pen = QtGui.QPen(QtGui.QColor("#dbe4f0"), 10)
        base_pen.setCapStyle(QtCore.Qt.RoundCap)
        painter.setPen(base_pen)
        painter.drawArc(arc_rect, 180 * 16, -180 * 16)

        if self._ratio > 0:
            if self._ratio < 0.55:
                color = QtGui.QColor("#16a34a")
            elif self._ratio < 0.85:
                color = QtGui.QColor("#d97706")
            else:
                color = QtGui.QColor("#dc2626")
            value_pen = QtGui.QPen(color, 10)
            value_pen.setCapStyle(QtCore.Qt.RoundCap)
            painter.setPen(value_pen)
            painter.drawArc(arc_rect, 180 * 16, int(-180 * 16 * self._ratio))

        angle_deg = 180.0 - (180.0 * self._ratio)
        angle = math.radians(angle_deg)
        needle_len = radius - 10.0
        needle_end = QtCore.QPointF(
            center.x() + math.cos(angle) * needle_len,
            center.y() - math.sin(angle) * needle_len,
        )
        painter.setPen(QtGui.QPen(QtGui.QColor("#0f172a"), 3))
        painter.drawLine(center, needle_end)
        painter.setBrush(QtGui.QColor("#0f172a"))
        painter.setPen(QtCore.Qt.NoPen)
        painter.drawEllipse(center, 4, 4)

        painter.setPen(QtGui.QColor("#64748b"))
        font = painter.font()
        font.setPointSize(8)
        painter.setFont(font)
        left_rect = QtCore.QRectF(arc_rect.left() - 8, arc_rect.bottom() - 22, 36, 18)
        right_rect = QtCore.QRectF(arc_rect.right() - 28, arc_rect.bottom() - 22, 36, 18)
        painter.drawText(left_rect, QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter, "0")
        painter.drawText(right_rect, QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter, "100%")


class SpeedGaugeWidget(QtWidgets.QFrame):
    def __init__(self, title: str, accent: str):
        super().__init__()
        self.setObjectName("kpiCard")
        self.setMinimumHeight(132)
        self.setMaximumHeight(148)
        self.setSizePolicy(QtWidgets.QSizePolicy.Preferred, QtWidgets.QSizePolicy.Fixed)
        lay = QtWidgets.QVBoxLayout(self)
        lay.setContentsMargins(10, 6, 10, 8)
        lay.setSpacing(1)
        bar = QtWidgets.QFrame()
        bar.setFixedHeight(3)
        bar.setStyleSheet(f"background:{accent}; border-radius:2px;")
        self.lbl_title = QtWidgets.QLabel(title)
        self.lbl_title.setObjectName("kpiTitle")
        self.gauge = GaugeFaceWidget(accent)
        self.lbl_value = QtWidgets.QLabel("-")
        self.lbl_value.setObjectName("kpiValue")
        self.lbl_value.setAlignment(QtCore.Qt.AlignCenter)
        self.lbl_sub = QtWidgets.QLabel("")
        self.lbl_sub.setObjectName("kpiSub")
        self.lbl_sub.setAlignment(QtCore.Qt.AlignCenter)
        self.lbl_sub.setWordWrap(True)
        lay.addWidget(bar)
        lay.addWidget(self.lbl_title)
        lay.addWidget(self.gauge)
        lay.addWidget(self.lbl_value)
        lay.addWidget(self.lbl_sub)

    def set_speed(self, units_per_hour: float, ratio: float, note: str = ""):
        self.gauge.set_ratio(ratio)
        self.lbl_value.setText(f"{fmt_int(units_per_hour)} u/h")
        self.lbl_sub.setText(note)

    def set_data(self, value: str, sub: str = ""):
        self.gauge.set_ratio(0.0)
        self.lbl_value.setText(value)
        self.lbl_sub.setText(sub)


class ChartDetailDialog(QtWidgets.QDialog):
    def __init__(self, title: str, chart: QtCharts.QChart, summary: list[tuple[str, str]], headers: list[str], rows: list[list[str]], parent: QtWidgets.QWidget | None = None, bar_values: list[float] | None = None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(1180, 760)

        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        top = QtWidgets.QLabel(title)
        top.setObjectName("titleLabel")
        root.addWidget(top)

        summary_grid = QtWidgets.QGridLayout()
        summary_grid.setHorizontalSpacing(10)
        summary_grid.setVerticalSpacing(8)
        for idx, (label, value) in enumerate(summary):
            card = QtWidgets.QFrame()
            card.setObjectName("kpiCard")
            lay = QtWidgets.QVBoxLayout(card)
            lay.setContentsMargins(12, 8, 12, 8)
            lay.setSpacing(2)
            lbl_title = QtWidgets.QLabel(label)
            lbl_title.setObjectName("kpiTitle")
            lbl_value = QtWidgets.QLabel(value)
            lbl_value.setObjectName("kpiValue")
            lay.addWidget(lbl_title)
            lay.addWidget(lbl_value)
            summary_grid.addWidget(card, idx // 4, idx % 4)
        root.addLayout(summary_grid)

        chart_view = QtCharts.QChartView()
        chart_view.setRenderHint(QtGui.QPainter.Antialiasing)
        chart_view.setChart(chart)
        root.addWidget(chart_view, 2)
        if bar_values:
            QtCore.QTimer.singleShot(0, lambda: self._add_bar_value_labels(chart_view, bar_values))

        table = QtWidgets.QTableWidget(len(rows), len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        table.setAlternatingRowColors(True)
        table.verticalHeader().setVisible(False)
        hdr = table.horizontalHeader()
        hdr.setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
        hdr.setStretchLastSection(True)
        for r, row in enumerate(rows):
            for c, val in enumerate(row):
                item = QtWidgets.QTableWidgetItem(str(val))
                if c > 0:
                    item.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                table.setItem(r, c, item)
        root.addWidget(table, 1)

    def _add_bar_value_labels(self, view: QtCharts.QChartView, values: list[float]):
        chart = view.chart()
        if chart is None or chart.scene() is None or not chart.series():
            return
        series = chart.series()[0]
        old_items = getattr(chart, "_value_label_items", [])
        for item in old_items:
            try:
                chart.scene().removeItem(item)
            except Exception:
                pass
        label_items: list[QtWidgets.QGraphicsTextItem] = []
        for idx, val in enumerate(values):
            item = QtWidgets.QGraphicsTextItem()
            item.setHtml(
                "<div style='background: rgba(255,255,255,0.92);"
                " color:#0f172a; font-weight:700; padding:1px 4px;"
                " border:1px solid #cbd5e1;'>"
                f"{int(round(float(val)))}"
                "</div>"
            )
            point = chart.mapToPosition(QtCore.QPointF(idx, float(val)), series)
            rect = item.boundingRect()
            item.setPos(point.x() - rect.width() / 2, point.y() - rect.height() - 8)
            chart.scene().addItem(item)
            label_items.append(item)
        chart._value_label_items = label_items


class LotPickerDialog(QtWidgets.QDialog):
    def __init__(self, repo: Repo, parent: QtWidgets.QWidget | None = None):
        super().__init__(parent)
        self.repo = repo
        self.selected_lot_id: int | None = None
        self.setWindowTitle("Buscar lote")
        self.resize(980, 560)

        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(8)

        filt = QtWidgets.QHBoxLayout()
        self.cb_status = QtWidgets.QComboBox()
        self.cb_status.addItems(["Todos", "Abiertos", "Cerrados"])
        self.cb_product = QtWidgets.QComboBox()
        self.cb_product.addItem("Todos", None)
        for row in self.repo.list_products():
            self.cb_product.addItem(str(row["name"]), int(row["id"]))
        self.ent_search = QtWidgets.QLineEdit()
        self.ent_search.setPlaceholderText("Buscar por lote, producto, proveedor o factura")
        self.btn_refresh = QtWidgets.QPushButton("Buscar")
        filt.addWidget(QtWidgets.QLabel("Estado"))
        filt.addWidget(self.cb_status)
        filt.addWidget(QtWidgets.QLabel("Producto"))
        filt.addWidget(self.cb_product)
        filt.addWidget(self.ent_search, 1)
        filt.addWidget(self.btn_refresh)
        root.addLayout(filt)

        self.table = QtWidgets.QTableWidget(0, 8)
        self.table.setHorizontalHeaderLabels([
            "ID", "Producto", "Lote", "Estado", "Kg inicial", "Kg saldo", "Proveedor", "Factura"
        ])
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
        header.setStretchLastSection(True)
        root.addWidget(self.table, 1)

        btns = QtWidgets.QHBoxLayout()
        btns.addStretch(1)
        self.btn_select = QtWidgets.QPushButton("Seleccionar")
        self.btn_cancel = QtWidgets.QPushButton("Cancelar")
        btns.addWidget(self.btn_select)
        btns.addWidget(self.btn_cancel)
        root.addLayout(btns)

        self.btn_refresh.clicked.connect(self._load_rows)
        self.cb_status.currentTextChanged.connect(lambda _x: self._load_rows())
        self.cb_product.currentIndexChanged.connect(lambda _x: self._load_rows())
        self.ent_search.returnPressed.connect(self._load_rows)
        self.btn_select.clicked.connect(self._accept_selected)
        self.btn_cancel.clicked.connect(self.reject)
        self.table.clicked.connect(lambda idx: self.table.selectRow(idx.row()))
        self.table.doubleClicked.connect(lambda _idx: self._accept_selected())

        self._load_rows()

    def _load_rows(self):
        product_id = self.cb_product.currentData()
        rows = self.repo.search_lots(
            self.cb_status.currentText(),
            self.ent_search.text(),
            None if product_id in (None, "") else int(product_id),
        )
        self.table.setRowCount(len(rows))
        for r, row in enumerate(rows):
            estado = "Cerrado" if int(row["cerrado"] or 0) else "Abierto"
            vals = [
                str(row["lot_id"]),
                row["producto"],
                row["lote"] or f'L{row["lot_id"]}',
                estado,
                fmt_num(row["kg_inicial"], 3),
                fmt_num(row["kg_saldo"], 3),
                row["proveedor"],
                row["factura"],
            ]
            for c, val in enumerate(vals):
                item = QtWidgets.QTableWidgetItem(str(val))
                if c in (0, 4, 5):
                    item.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                self.table.setItem(r, c, item)

    def _accept_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        item = self.table.item(row, 0)
        if not item:
            return
        self.selected_lot_id = int(item.text())
        self.accept()


class MetricaWindow(QtWidgets.QMainWindow):
    def __init__(self, initial_lot_id: Optional[int] = None):
        super().__init__()
        self.repo = Repo(resolve_db())
        self._selected_lot_id: int | None = None
        self._chart_payloads: dict[str, dict] = {}
        self._initial_lot_id = int(initial_lot_id) if initial_lot_id is not None else None
        self.setWindowTitle("Métrica de Producción")
        self.resize(1360, 900)
        self.setMinimumSize(1100, 740)

        self.scroll = QtWidgets.QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.scroll.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
        self.scroll.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
        self.setCentralWidget(self.scroll)

        central = QtWidgets.QWidget()
        central.setMinimumWidth(1180)
        self.scroll.setWidget(central)
        root = QtWidgets.QVBoxLayout(central)
        root.setContentsMargins(14, 14, 14, 10)
        root.setSpacing(10)

        self._build_header(root)
        self._build_main_pages(root)
        self._apply_style()
        self._load_products()
        self._set_current_month()
        if self._initial_lot_id is not None:
            self._open_lot_by_id(self._initial_lot_id)

    def _build_main_pages(self, root: QtWidgets.QVBoxLayout):
        self.main_tabs = QtWidgets.QTabWidget()
        root.addWidget(self.main_tabs, 1)

        self.tab_general = QtWidgets.QWidget()
        lay_general = QtWidgets.QVBoxLayout(self.tab_general)
        lay_general.setContentsMargins(0, 0, 0, 0)
        lay_general.setSpacing(10)
        self._build_filters(lay_general)
        self._build_kpis(lay_general)
        self._build_tabs(lay_general)
        self.main_tabs.addTab(self.tab_general, "General")

    def _build_header(self, root: QtWidgets.QVBoxLayout):
        frame = QtWidgets.QFrame()
        frame.setObjectName("headerCard")
        lay = QtWidgets.QHBoxLayout(frame)
        lay.setContentsMargins(16, 14, 16, 14)
        box = QtWidgets.QVBoxLayout()
        title = QtWidgets.QLabel("Métrica de producción")
        title.setObjectName("titleLabel")
        sub = QtWidgets.QLabel("Análisis operativo del fraccionamiento, lotes y balance producción vs ventas.")
        sub.setObjectName("subTitleLabel")
        box.addWidget(title)
        box.addWidget(sub)
        lay.addLayout(box, 1)
        self.lbl_status = QtWidgets.QLabel("Listo")
        self.lbl_status.setObjectName("statusPill")
        lay.addWidget(self.lbl_status)
        root.addWidget(frame)

    def _build_filters(self, root: QtWidgets.QVBoxLayout):
        frame = QtWidgets.QFrame()
        frame.setObjectName("panelCard")
        grid = QtWidgets.QGridLayout(frame)
        grid.setContentsMargins(12, 12, 12, 12)
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(8)

        self.ent_db = QtWidgets.QLineEdit(str(self.repo.db_path))
        self.ent_db.setReadOnly(True)
        self.dt_from = QtWidgets.QDateEdit(calendarPopup=True, displayFormat="yyyy-MM-dd")
        self.dt_to = QtWidgets.QDateEdit(calendarPopup=True, displayFormat="yyyy-MM-dd")
        self.dt_week = QtWidgets.QDateEdit(calendarPopup=True, displayFormat="yyyy-MM-dd")
        self.cb_product = QtWidgets.QComboBox()
        self.cb_product.setMinimumWidth(220)
        self.btn_month = QtWidgets.QPushButton("Este mes")
        self.btn_30d = QtWidgets.QPushButton("Últimos 30 días")
        self.btn_week = QtWidgets.QPushButton("Semana de fecha")
        self.btn_refresh = QtWidgets.QPushButton("Refrescar")
        self.btn_refresh.setProperty("variant", "primary")

        for col, (label, widget) in enumerate([
            ("Base", self.ent_db),
            ("Desde", self.dt_from),
            ("Hasta", self.dt_to),
            ("Fecha semana", self.dt_week),
            ("Producto", self.cb_product),
        ]):
            grid.addWidget(QtWidgets.QLabel(label), 0, col)
            grid.addWidget(widget, 1, col)

        btns = QtWidgets.QHBoxLayout()
        btns.addWidget(self.btn_month)
        btns.addWidget(self.btn_30d)
        btns.addWidget(self.btn_week)
        btns.addWidget(self.btn_refresh)
        btns.addStretch(1)
        grid.addLayout(btns, 1, 5)
        root.addWidget(frame)

        self.btn_month.clicked.connect(self._set_current_month)
        self.btn_30d.clicked.connect(self._set_last_30_days)
        self.btn_week.clicked.connect(self._apply_selected_week)
        self.btn_refresh.clicked.connect(self.refresh_all)

    def _build_kpis(self, root: QtWidgets.QVBoxLayout):
        grid = QtWidgets.QGridLayout()
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(8)
        self.kpi_kg = KPIWidget("Kg", "#1d4ed8")
        self.kpi_paq = KPIWidget("Paquetes", "#0f766e")
        self.kpi_unid = KPIWidget("Unidades", "#7c3aed")
        self.kpi_fracc = KPIWidget("Fracc.", "#c2410c")
        self.kpi_bags = KPIWidget("Bolsas", "#2563eb")
        self.kpi_speed = SpeedGaugeWidget("Velocidad de linea", "#7c2d12")
        self.kpi_rate = KPIWidget("Kg/día", "#15803d")
        grid.addWidget(self.kpi_kg, 0, 0)
        grid.addWidget(self.kpi_paq, 0, 1)
        grid.addWidget(self.kpi_unid, 0, 2)
        grid.addWidget(self.kpi_fracc, 0, 3)
        grid.addWidget(self.kpi_bags, 1, 0)
        grid.addWidget(self.kpi_speed, 1, 1)
        grid.addWidget(self.kpi_rate, 1, 2)
        self.lbl_period_info = QtWidgets.QLabel("Período: -")
        self.lbl_period_info.setObjectName("rangeInfo")
        grid.addWidget(self.lbl_period_info, 1, 3)
        for col in range(4):
            grid.setColumnStretch(col, 1)
        root.addLayout(grid)

    def _build_tabs(self, root: QtWidgets.QVBoxLayout):
        self.tabs = QtWidgets.QTabWidget()
        root.addWidget(self.tabs, 1)

        tab_prod = QtWidgets.QWidget()
        lay_prod = QtWidgets.QVBoxLayout(tab_prod)
        self.chart_daily = QtCharts.QChartView()
        self.chart_product = QtCharts.QChartView()
        self.chart_gram = QtCharts.QChartView()
        for chart in (self.chart_daily, self.chart_product, self.chart_gram):
            chart.setRenderHint(QtGui.QPainter.Antialiasing)
        lay_prod.addWidget(self.chart_daily, 1)
        split = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        split.addWidget(self.chart_product)
        split.addWidget(self.chart_gram)
        split.setSizes([520, 520])
        lay_prod.addWidget(split, 1)
        self.tabs.addTab(tab_prod, "Producción")

        tab_week = QtWidgets.QWidget()
        lay_week = QtWidgets.QVBoxLayout(tab_week)
        self.lbl_week_info = QtWidgets.QLabel("Semana: -")
        self.lbl_week_info.setObjectName("insightCard")
        self.tbl_week = QtWidgets.QTableWidget(0, 6)
        self.tbl_week.setHorizontalHeaderLabels([
            "Producto", "Kg semana", "Fraccs semana", "Días activos", "Promedio semanal", "Promedio diario semana"
        ])
        self._setup_table(self.tbl_week)
        lay_week.addWidget(self.lbl_week_info)
        lay_week.addWidget(self.tbl_week)
        self.tabs.addTab(tab_week, "Consumo semanal")

        self.tab_lot_specific = QtWidgets.QWidget()
        lay_lot = QtWidgets.QVBoxLayout(self.tab_lot_specific)
        top_lot = QtWidgets.QHBoxLayout()
        self.lbl_lot_selected = QtWidgets.QLabel("Lote: sin seleccionar")
        self.lbl_lot_selected.setObjectName("rangeInfo")
        self.btn_pick_lot = QtWidgets.QPushButton("Buscar lote")
        top_lot.addWidget(self.lbl_lot_selected, 1)
        top_lot.addWidget(self.btn_pick_lot)
        lay_lot.addLayout(top_lot)

        lot_grid = QtWidgets.QGridLayout()
        lot_grid.setHorizontalSpacing(10)
        lot_grid.setVerticalSpacing(8)
        self.lot_kpi_kg = KPIWidget("Kg", "#1d4ed8")
        self.lot_kpi_paq = KPIWidget("Paquetes", "#0f766e")
        self.lot_kpi_unid = KPIWidget("Unidades", "#7c3aed")
        self.lot_kpi_fracc = KPIWidget("Fracc.", "#c2410c")
        self.lot_kpi_bags = KPIWidget("Bolsas", "#2563eb")
        self.lot_kpi_rate = KPIWidget("Kg/día", "#15803d")
        self.lot_kpi_merma = KPIWidget("Merma", "#b91c1c")
        self.lot_kpi_cost = KPIWidget("Costo por kg", "#475569")
        self.lot_kpi_sale = KPIWidget("Venta estimada", "#0f766e")
        self.lot_kpi_profit = KPIWidget("Beneficio estimado", "#b91c1c")
        self.lot_kpi_margin = KPIWidget("Margen estimado %", "#7c3aed")
        lot_cards = [
            self.lot_kpi_kg, self.lot_kpi_paq, self.lot_kpi_unid, self.lot_kpi_fracc,
            self.lot_kpi_bags, self.lot_kpi_rate, self.lot_kpi_merma,
            self.lot_kpi_cost, self.lot_kpi_sale, self.lot_kpi_profit, self.lot_kpi_margin,
        ]
        for idx, card in enumerate(lot_cards):
            lot_grid.addWidget(card, idx // 4, idx % 4)
            card.show()
        for col in range(4):
            lot_grid.setColumnStretch(col, 1)
        lay_lot.addLayout(lot_grid)

        split_lot = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        self.chart_lot_daily = QtCharts.QChartView()
        self.chart_lot_gram = QtCharts.QChartView()
        self.chart_lot_daily.setRenderHint(QtGui.QPainter.Antialiasing)
        self.chart_lot_gram.setRenderHint(QtGui.QPainter.Antialiasing)
        split_lot.addWidget(self.chart_lot_daily)
        split_lot.addWidget(self.chart_lot_gram)
        split_lot.setSizes([520, 520])
        lay_lot.addWidget(split_lot, 1)

        bottom_lot = QtWidgets.QHBoxLayout()
        bottom_lot.setSpacing(10)
        self.tbl_lot_hist = QtWidgets.QTableWidget(0, 5)
        self.tbl_lot_hist.setHorizontalHeaderLabels(["Fecha", "Producto", "Gramaje", "Paquetes", "Kg"])
        self._setup_table(self.tbl_lot_hist)
        self.tbl_lot_hist.setSizePolicy(QtWidgets.QSizePolicy.Maximum, QtWidgets.QSizePolicy.Maximum)
        hist_header = self.tbl_lot_hist.horizontalHeader()
        hist_header.setStretchLastSection(False)
        hist_header.setSectionResizeMode(0, QtWidgets.QHeaderView.ResizeToContents)
        hist_header.setSectionResizeMode(1, QtWidgets.QHeaderView.Fixed)
        hist_header.setSectionResizeMode(2, QtWidgets.QHeaderView.ResizeToContents)
        hist_header.setSectionResizeMode(3, QtWidgets.QHeaderView.ResizeToContents)
        hist_header.setSectionResizeMode(4, QtWidgets.QHeaderView.ResizeToContents)
        self.tbl_lot_hist.setColumnWidth(1, 150)
        self.tbl_lot_hist.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        bottom_lot.addWidget(self.tbl_lot_hist, 0)
        bottom_lot.addStretch(1)
        self.lot_side_panel = QtWidgets.QFrame()
        self.lot_side_panel.setObjectName("panelCard")
        self.lot_side_panel.setFixedWidth(220)
        lot_side = QtWidgets.QVBoxLayout(self.lot_side_panel)
        lot_side.setContentsMargins(8, 8, 8, 8)
        lot_side.setSpacing(10)
        self.lot_kpi_days = KPIWidget("Dias restantes estimados", "#16a34a")
        self.lot_kpi_days.setMinimumWidth(0)
        self.lot_kpi_days.setMaximumWidth(204)
        self.lot_kpi_days.setMinimumHeight(78)
        self.lot_kpi_days.lbl_value.setWordWrap(True)
        self.lbl_lot_projection = QtWidgets.QLabel("Sin datos del lote.")
        self.lbl_lot_projection.setObjectName("insightCard")
        self.lbl_lot_projection.setWordWrap(True)
        self.lbl_lot_projection.setMinimumWidth(0)
        self.lbl_lot_projection.setMaximumWidth(204)
        lot_side.addWidget(self.lot_kpi_days)
        lot_side.addWidget(self.lbl_lot_projection, 1)
        bottom_lot.addWidget(self.lot_side_panel, 0)
        lay_lot.addLayout(bottom_lot, 1)
        self._update_lot_hist_max_height()
        self.tabs.addTab(self.tab_lot_specific, "Lote específico")
        self.tabs.removeTab(self.tabs.indexOf(self.tab_lot_specific))
        self.main_tabs.addTab(self.tab_lot_specific, "Lote específico")

        tab_lots = QtWidgets.QWidget()
        lay_lots = QtWidgets.QVBoxLayout(tab_lots)
        self.tbl_lots = QtWidgets.QTableWidget(0, 9)
        self.tbl_lots.setHorizontalHeaderLabels([
            "Producto", "Lote", "Kg inicial", "Kg usados", "Kg merma",
            "Kg saldo", "% uso", "% merma", "Fraccs"
        ])
        self._setup_table(self.tbl_lots)
        lay_lots.addWidget(self.tbl_lots)
        self.tabs.addTab(tab_lots, "Lotes y merma")

        tab_bal = QtWidgets.QWidget()
        lay_bal = QtWidgets.QVBoxLayout(tab_bal)
        self.tbl_balance = QtWidgets.QTableWidget(0, 7)
        self.tbl_balance.setHorizontalHeaderLabels([
            "Producto", "Gramaje", "Producidos", "Vendidos", "Stock actual", "Kg consumidos", "Venta (Gs)"
        ])
        self._setup_table(self.tbl_balance)
        lay_bal.addWidget(self.tbl_balance)
        self.tabs.addTab(tab_bal, "Producción vs ventas")

        self.lbl_insight = QtWidgets.QLabel("")
        self.lbl_insight.setObjectName("insightCard")
        self.lbl_insight.setWordWrap(True)
        root.addWidget(self.lbl_insight)
        self.btn_pick_lot.clicked.connect(self._open_lot_picker)
        self.tbl_lots.doubleClicked.connect(self._open_lot_from_summary)
        self._bind_chart_view(self.chart_daily, "daily_general")
        self._bind_chart_view(self.chart_product, "mix_product")
        self._bind_chart_view(self.chart_gram, "mix_gramaje")
        self._bind_chart_view(self.chart_lot_daily, "daily_lot")
        self._bind_chart_view(self.chart_lot_gram, "mix_lot_gramaje")

    def _bind_chart_view(self, view: QtCharts.QChartView, key: str):
        view.setProperty("chart_key", key)
        view.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        view.installEventFilter(self)

    def eventFilter(self, watched, event):
        if isinstance(watched, QtCharts.QChartView) and event.type() == QtCore.QEvent.MouseButtonDblClick:
            key = watched.property("chart_key")
            if key:
                self._open_chart_detail(str(key))
                return True
        return super().eventFilter(watched, event)

    def _setup_table(self, table: QtWidgets.QTableWidget):
        table.setAlternatingRowColors(True)
        table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        table.verticalHeader().setVisible(False)
        table.verticalHeader().setDefaultSectionSize(24)
        table.verticalHeader().setMinimumSectionSize(22)
        header = table.horizontalHeader()
        header.setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
        header.setStretchLastSection(True)

    def _update_lot_hist_max_height(self):
        if not hasattr(self, "tbl_lot_hist"):
            return
        screen = self.screen()
        screen_h = screen.availableGeometry().height() if screen else 900
        max_h = max(180, int(screen_h * 0.45))
        self.tbl_lot_hist.setMaximumHeight(max_h)

    def _update_lot_hist_width(self):
        if not hasattr(self, "tbl_lot_hist"):
            return
        table = self.tbl_lot_hist
        header = table.horizontalHeader()
        width = table.frameWidth() * 2
        if table.verticalScrollBar().isVisible():
            width += table.verticalScrollBar().sizeHint().width()
        for col in range(table.columnCount()):
            if not table.isColumnHidden(col):
                width += header.sectionSize(col)
        width = max(420, min(width + 2, 900))
        table.setFixedWidth(width)

    def resizeEvent(self, event: QtGui.QResizeEvent):
        super().resizeEvent(event)
        self._update_lot_hist_max_height()
        self._update_lot_hist_width()

    def _apply_style(self):
        self.setStyleSheet(
            """
            QWidget { background: #f4f7fb; color: #17202b; font: 10pt 'Segoe UI'; }
            QFrame#headerCard, QFrame#panelCard, QFrame#kpiCard { background: white; border: 1px solid #d9e2ef; border-radius: 14px; }
            QLabel#titleLabel { font-size: 22px; font-weight: 700; }
            QLabel#subTitleLabel { color: #627287; }
            QLabel#statusPill { background: #e8f0ff; color: #1d4ed8; border-radius: 12px; padding: 6px 10px; font-weight: 600; }
            QLabel#kpiTitle { color: #64748b; font-size: 9pt; }
            QLabel#kpiValue { font-size: 16pt; font-weight: 700; color: #0f172a; }
            QLabel#kpiSub { color: #64748b; font-size: 8.5pt; }
            QLabel#insightCard { background: white; border: 1px solid #d9e2ef; border-radius: 14px; padding: 12px; }
            QLabel#rangeInfo { background: white; border: 1px solid #d9e2ef; border-radius: 14px; padding: 12px; color: #334155; font-weight: 600; }
            QPushButton[variant="primary"] { background: #1d4ed8; color: white; border: 0; border-radius: 10px; padding: 8px 14px; font-weight: 600; }
            QPushButton { background: white; border: 1px solid #cdd7e4; border-radius: 10px; padding: 7px 12px; }
            QLineEdit, QComboBox, QDateEdit { background: white; border: 1px solid #cdd7e4; border-radius: 9px; padding: 6px 8px; }
            QTableWidget { background: white; border: 1px solid #d9e2ef; border-radius: 12px; gridline-color: #edf2f7; alternate-background-color: #f8fbff; selection-background-color: #dbeafe; selection-color: #0f172a; }
            QTableWidget::item { padding: 3px 6px; }
            QTableWidget::item:selected { background: #dbeafe; color: #0f172a; }
            QTableWidget::item:selected:active { background: #bfdbfe; color: #0f172a; }
            QHeaderView::section { background: #eef4fb; color: #334155; border: 0; border-bottom: 1px solid #d9e2ef; padding: 6px; font-weight: 600; }
            """
        )

    def _load_products(self):
        self.cb_product.clear()
        self.cb_product.addItem("Todos", None)
        for row in self.repo.list_products():
            self.cb_product.addItem(str(row["name"]), int(row["id"]))

    def _set_current_month(self):
        today = date.today()
        first = today.replace(day=1)
        self.dt_from.setDate(QtCore.QDate(first.year, first.month, first.day))
        self.dt_to.setDate(QtCore.QDate(today.year, today.month, today.day))
        self.dt_week.setDate(QtCore.QDate(today.year, today.month, today.day))
        self.refresh_all()

    def _set_last_30_days(self):
        today = date.today()
        start = today - timedelta(days=29)
        self.dt_from.setDate(QtCore.QDate(start.year, start.month, start.day))
        self.dt_to.setDate(QtCore.QDate(today.year, today.month, today.day))
        self.dt_week.setDate(QtCore.QDate(today.year, today.month, today.day))
        self.refresh_all()

    def _apply_selected_week(self):
        ref = self.dt_week.date().toPython()
        week_start = ref - timedelta(days=ref.weekday())
        week_end = week_start + timedelta(days=6)
        self.dt_from.setDate(QtCore.QDate(week_start.year, week_start.month, week_start.day))
        self.dt_to.setDate(QtCore.QDate(week_end.year, week_end.month, week_end.day))
        self.refresh_all()

    def _filters(self) -> tuple[str, str, Optional[int]]:
        d1 = self.dt_from.date().toPython().isoformat()
        d2 = self.dt_to.date().toPython().isoformat()
        pid = self.cb_product.currentData()
        return d1, d2, None if pid in (None, "") else int(pid)

    def _set_top_kpis(self, k: KPIData, scope_text: str, merma_text: str | None = None):
        kg_fracc = k.kg_consumidos / k.fraccionamientos if k.fraccionamientos else 0.0
        kg_dia = k.kg_consumidos / k.dias_activos if k.dias_activos else 0.0
        units_per_hour, speed_ratio = estimate_line_speed(k.unidades, k.dias_activos)
        self.kpi_kg.set_data(f"{fmt_num(k.kg_consumidos, 3)} kg", f"{fmt_num(kg_fracc, 3)} kg/fracc.")
        self.kpi_paq.set_data(fmt_int(k.paquetes), f"{fmt_int(k.presentaciones_activas)} pres.")
        self.kpi_unid.set_data(fmt_int(k.unidades), "según gramaje")
        self.kpi_fracc.set_data(fmt_int(k.fraccionamientos), f"{fmt_int(k.productos_activos)} prod.")
        self.kpi_bags.set_data(f"{fmt_num(k.bolsas_usadas_eq, 2)}", "bolsas eq.")
        self.kpi_speed.set_speed(
            units_per_hour,
            speed_ratio,
            f"{fmt_num(speed_ratio * 100.0, 1)}% de {fmt_int(LINE_MAX_UNITS_PER_HOUR)} u/h max. | {fmt_num(ACTIVE_HOURS_PER_DAY, 1)} h/dia est.",
        )
        self.kpi_rate.set_data(f"{fmt_num(kg_dia, 3)}", merma_text or f"{fmt_int(k.dias_activos)} días act.")
        self.lbl_period_info.setText(scope_text)

    def refresh_all(self):
        d1, d2, product_id = self._filters()
        week_date = self.dt_week.date().toPython()
        week_start = week_date - timedelta(days=week_date.weekday())
        week_end = week_start + timedelta(days=6)
        self.lbl_status.setText(f"Período {d1} a {d2}")

        k = self.repo.production_kpis(d1, d2, product_id)
        daily = self.repo.daily_production(d1, d2, product_id)
        mix_prod = self.repo.product_mix(d1, d2, product_id)
        mix_gram = self.repo.gramaje_mix(d1, d2, product_id)
        weekly = self.repo.weekly_product_consumption(week_date.isoformat(), product_id)
        lots = self.repo.lot_metrics(d1, d2, product_id)
        balance = self.repo.production_vs_sales(d1, d2, product_id)

        kg_fracc = k.kg_consumidos / k.fraccionamientos if k.fraccionamientos else 0.0
        kg_dia = k.kg_consumidos / k.dias_activos if k.dias_activos else 0.0
        units_per_hour, speed_ratio = estimate_line_speed(k.unidades, k.dias_activos)
        d1_short = self.dt_from.date().toString("dd/MM/yy")
        d2_short = self.dt_to.date().toString("dd/MM/yy")
        self.kpi_kg.set_data(f"{fmt_num(k.kg_consumidos, 3)} kg", f"{fmt_num(kg_fracc, 3)} kg/fracc.")
        self.kpi_paq.set_data(fmt_int(k.paquetes), f"{fmt_int(k.presentaciones_activas)} pres.")
        self.kpi_unid.set_data(fmt_int(k.unidades), "según gramaje")
        self.kpi_fracc.set_data(fmt_int(k.fraccionamientos), f"{fmt_int(k.productos_activos)} prod.")
        self.kpi_bags.set_data(f"{fmt_num(k.bolsas_usadas_eq, 2)}", "bolsas eq.")
        self.kpi_speed.set_speed(
            units_per_hour,
            speed_ratio,
            f"{fmt_num(speed_ratio * 100.0, 1)}% de {fmt_int(LINE_MAX_UNITS_PER_HOUR)} u/h max. | {fmt_num(ACTIVE_HOURS_PER_DAY, 1)} h/dia est.",
        )
        self.kpi_rate.set_data(f"{fmt_num(kg_dia, 3)}", f"{fmt_int(k.dias_activos)} días act.")
        self.lbl_period_info.setText(f"Período: {d1_short} → {d2_short}")

        self._render_daily_chart(daily)
        self._render_bar_chart(self.chart_product, "Mix por producto (kg)", [r["producto"] for r in mix_prod[:8]], [float(r["kg"] or 0) for r in mix_prod[:8]], "#1d4ed8")
        self._render_bar_chart(self.chart_gram, "Mix por gramaje (paquetes)", [f'{int(r["gramaje"])} g' for r in mix_gram[:8]], [float(r["paquetes"] or 0) for r in mix_gram[:8]], "#0f766e")
        self._load_weekly(weekly, week_start, week_end)
        self._load_lots(lots)
        self._load_balance(balance)
        self._load_insights(k, mix_prod, mix_gram, lots, balance)
        self._refresh_lot_specific()

    def _render_daily_chart(self, rows: list[dict]):
        self._render_daily_chart_for(self.chart_daily, rows)

    def _make_daily_chart(self, rows: list[dict], title: str, color: str = "#1d4ed8"):
        chart = QtCharts.QChart()
        chart.setTitle(title)
        chart.legend().hide()
        series = QtCharts.QLineSeries()
        pen = QtGui.QPen(QtGui.QColor(color))
        pen.setWidth(3)
        series.setPen(pen)
        axis_x = QtCharts.QCategoryAxis()
        axis_x.setLabelsPosition(QtCharts.QCategoryAxis.AxisLabelsPositionOnValue)
        axis_x.setStartValue(0)
        axis_y = QtCharts.QValueAxis()
        axis_y.setLabelFormat("%.1f")
        max_y = 1.0
        if rows:
            for idx, row in enumerate(rows):
                val = float(row["kg"] or 0)
                max_y = max(max_y, val)
                series.append(idx, val)
                axis_x.append(str(row["fecha"])[5:], idx)
            axis_x.setRange(0, max(0, len(rows) - 1))
        else:
            series.append(0, 0)
            axis_x.append("sin datos", 0)
            axis_x.setRange(0, 1)
        axis_y.setRange(0, max_y * 1.15)
        chart.addSeries(series)
        chart.addAxis(axis_x, QtCore.Qt.AlignBottom)
        chart.addAxis(axis_y, QtCore.Qt.AlignLeft)
        series.attachAxis(axis_x)
        series.attachAxis(axis_y)
        return chart

    def _render_daily_chart_for(self, view: QtCharts.QChartView, rows: list[dict], title: str = "Producción diaria (kg consumidos)"):
        chart = self._make_daily_chart(rows, title)
        view.setChart(chart)
        key = view.property("chart_key")
        if key:
            self._chart_payloads[str(key)] = {"kind": "daily", "title": title, "rows": [dict(r) for r in rows]}

    def _make_bar_chart(self, title: str, labels: list[str], values: list[float], color: str, show_labels: bool = False):
        chart = QtCharts.QChart()
        chart.setTitle(title)
        bar = QtCharts.QBarSet(title)
        bar.setColor(QtGui.QColor(color))
        if values:
            for val in values:
                bar.append(float(val))
        else:
            labels = ["sin datos"]
            bar.append(0.0)
        series = QtCharts.QBarSeries()
        series.append(bar)
        if show_labels:
            series.setLabelsVisible(True)
            series.setLabelsPosition(QtCharts.QAbstractBarSeries.LabelsOutsideEnd)
            series.setLabelsFormat("@value")
            series.setLabelsPrecision(0)
        chart.addSeries(series)
        axis_x = QtCharts.QBarCategoryAxis()
        axis_x.append(labels)
        axis_y = QtCharts.QValueAxis()
        axis_y.setRange(0, max(1.0, (max(values) if values else 1.0) * (1.32 if show_labels else 1.18)))
        chart.addAxis(axis_x, QtCore.Qt.AlignBottom)
        chart.addAxis(axis_y, QtCore.Qt.AlignLeft)
        series.attachAxis(axis_x)
        series.attachAxis(axis_y)
        chart.legend().hide()
        return chart

    def _render_bar_chart(self, view: QtCharts.QChartView, title: str, labels: list[str], values: list[float], color: str):
        chart = self._make_bar_chart(title, labels, values, color, show_labels=True)
        view.setChart(chart)
        if values:
            QtCore.QTimer.singleShot(0, lambda v=view, vals=list(values): self._add_bar_value_labels(v, vals))
        key = view.property("chart_key")
        if key:
            self._chart_payloads[str(key)] = {"kind": "bar", "title": title, "labels": list(labels), "values": list(values), "color": color}

    def _add_bar_value_labels(self, view: QtCharts.QChartView, values: list[float]):
        chart = view.chart()
        if chart is None or chart.scene() is None or not chart.series():
            return
        series = chart.series()[0]
        old_items = getattr(chart, "_value_label_items", [])
        for item in old_items:
            try:
                chart.scene().removeItem(item)
            except Exception:
                pass
        label_items: list[QtWidgets.QGraphicsTextItem] = []
        for idx, val in enumerate(values):
            item = QtWidgets.QGraphicsTextItem()
            item.setHtml(
                "<div style='background: rgba(255,255,255,0.92);"
                " color:#0f172a; font-weight:700; padding:1px 4px;"
                " border:1px solid #cbd5e1;'>"
                f"{int(round(float(val)))}"
                "</div>"
            )
            point = chart.mapToPosition(QtCore.QPointF(idx, float(val)), series)
            rect = item.boundingRect()
            item.setPos(point.x() - rect.width() / 2, point.y() - rect.height() - 8)
            chart.scene().addItem(item)
            label_items.append(item)
        chart._value_label_items = label_items

    def _open_chart_detail(self, key: str):
        payload = self._chart_payloads.get(key)
        if not payload:
            return
        if payload["kind"] == "daily":
            rows = payload["rows"]
            chart = self._make_daily_chart(rows, payload["title"])
            total = sum(float(r.get("kg", 0) or 0) for r in rows)
            peak_row = max(rows, key=lambda r: float(r.get("kg", 0) or 0), default=None)
            avg = total / len(rows) if rows else 0.0
            summary = [
                ("Total kg", fmt_num(total, 3)),
                ("Días", fmt_int(len(rows))),
                ("Promedio/día", fmt_num(avg, 3)),
                ("Pico", "-" if not peak_row else f'{str(peak_row["fecha"])} | {fmt_num(float(peak_row.get("kg", 0) or 0), 3)} kg'),
            ]
            table_rows = [
                [str(r.get("fecha", "")), fmt_num(float(r.get("kg", 0) or 0), 3)]
                for r in rows
            ]
            dlg = ChartDetailDialog(payload["title"], chart, summary, ["Fecha", "Kg"], table_rows, self)
            dlg.exec()
            return

        labels = payload["labels"]
        values = [float(v or 0) for v in payload["values"]]
        chart = self._make_bar_chart(payload["title"], labels, values, payload["color"], show_labels=True)
        total = sum(values)
        pairs = list(zip(labels, values))
        top = max(pairs, key=lambda x: x[1], default=None)
        avg = total / len(values) if values else 0.0
        summary = [
            ("Total", fmt_num(total, 3)),
            ("Ítems", fmt_int(len(labels))),
            ("Promedio", fmt_num(avg, 3)),
            ("Mayor aporte", "-" if not top else f"{top[0]} | {fmt_num(top[1], 3)}"),
        ]
        table_rows = [[label, fmt_num(value, 3)] for label, value in pairs]
        dlg = ChartDetailDialog(payload["title"], chart, summary, ["Etiqueta", "Valor"], table_rows, self, bar_values=values)
        dlg.exec()

    def _load_lots(self, rows: list[dict]):
        self.tbl_lots.setRowCount(len(rows))
        for r, row in enumerate(rows):
            kg_inicial = float(row["kg_inicial"] or 0)
            kg_usados = float(row["kg_usados"] or 0)
            kg_merma = float(row["kg_merma"] or 0)
            uso_pct = kg_usados / kg_inicial * 100.0 if kg_inicial else 0.0
            merma_pct = kg_merma / kg_inicial * 100.0 if kg_inicial else 0.0
            vals = [
                row["producto"],
                row["lote"] or f'L{row["lot_id"]}',
                fmt_num(kg_inicial, 3),
                fmt_num(kg_usados, 3),
                fmt_num(kg_merma, 3),
                fmt_num(float(row["kg_saldo"] or 0), 3),
                f"{fmt_num(uso_pct, 1)}%",
                f"{fmt_num(merma_pct, 1)}%",
                fmt_int(row["fraccs"]),
            ]
            for c, val in enumerate(vals):
                item = QtWidgets.QTableWidgetItem(str(val))
                if c == 0:
                    item.setData(QtCore.Qt.UserRole, int(row["lot_id"]))
                if c >= 2:
                    item.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                self.tbl_lots.setItem(r, c, item)

    def _load_weekly(self, rows: list[dict], week_start: date, week_end: date):
        self.lbl_week_info.setText(
            f"Semana seleccionada: {week_start.isoformat()} a {week_end.isoformat()} | "
            "Consumo por producto en kg."
        )
        self.tbl_week.setRowCount(len(rows))
        for r, row in enumerate(rows):
            dias = int(row["dias_activos_semana"] or 0)
            kg_semana = float(row["kg_semana"] or 0)
            prom_sem = float(row["promedio_semanal_kg"] or 0)
            prom_dia = kg_semana / dias if dias else 0.0
            vals = [
                row["producto"],
                fmt_num(kg_semana, 3),
                fmt_int(row["fraccs_semana"]),
                fmt_int(dias),
                fmt_num(prom_sem, 3),
                fmt_num(prom_dia, 3),
            ]
            for c, val in enumerate(vals):
                item = QtWidgets.QTableWidgetItem(str(val))
                if c >= 1:
                    item.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                self.tbl_week.setItem(r, c, item)

    def _open_lot_from_summary(self, index: QtCore.QModelIndex):
        if not index.isValid():
            return
        item = self.tbl_lots.item(index.row(), 0)
        if not item:
            return
        lot_id = item.data(QtCore.Qt.UserRole)
        if lot_id is None:
            return
        self._open_lot_by_id(int(lot_id))

    def _open_lot_picker(self):
        dlg = LotPickerDialog(self.repo, self)
        if dlg.exec() == QtWidgets.QDialog.Accepted and dlg.selected_lot_id:
            self._open_lot_by_id(int(dlg.selected_lot_id))

    def _open_lot_by_id(self, lot_id: int):
        self._selected_lot_id = int(lot_id)
        if hasattr(self, "main_tabs") and hasattr(self, "tab_lot_specific"):
            self.main_tabs.setCurrentWidget(self.tab_lot_specific)
        self._refresh_lot_specific()

    def _refresh_lot_specific(self):
        if not hasattr(self, "tbl_lot_hist"):
            return

        if not self._selected_lot_id:
            self.lbl_lot_selected.setText("Lote: sin seleccionar")
            for card in (
                self.lot_kpi_kg, self.lot_kpi_paq, self.lot_kpi_unid, self.lot_kpi_fracc,
                self.lot_kpi_bags, self.lot_kpi_rate, self.lot_kpi_merma,
                self.lot_kpi_cost, self.lot_kpi_sale, self.lot_kpi_profit, self.lot_kpi_margin,
                self.lot_kpi_days,
            ):
                card.set_data("-", "")
            self.lot_kpi_days.set_alert(False)
            self.lbl_lot_projection.setText("Sin datos del lote.")
            self.tbl_lot_hist.setRowCount(0)
            self._update_lot_hist_width()
            self._render_daily_chart_for(self.chart_lot_daily, [])
            self._render_bar_chart(self.chart_lot_gram, "Mix por gramaje del lote", [], [], "#0f766e")
            return

        info = self.repo.lot_header(self._selected_lot_id)
        if not info:
            self._selected_lot_id = None
            self._refresh_lot_specific()
            return

        estado = "Cerrado" if int(info["cerrado"] or 0) else "Abierto"
        lote_txt = info["lote"] or f'L{info["lot_id"]}'
        self.lbl_lot_selected.setText(
            f"Lote {lote_txt} | {info['producto']} | {estado} | saldo {fmt_num(info['kg_saldo'], 3)} kg"
        )

        k, extra = self.repo.lot_kpis(self._selected_lot_id)
        fin = self.repo.lot_financial_kpis(self._selected_lot_id)
        kg_fracc = k.kg_consumidos / k.fraccionamientos if k.fraccionamientos else 0.0
        kg_dia = k.kg_consumidos / k.dias_activos if k.dias_activos else 0.0
        kg_merma = float(extra.get("kg_merma", 0.0) or 0.0)
        kg_saldo = float(info["kg_saldo"] or 0.0)
        merma_pct = (kg_merma / float(info["kg_inicial"] or 0) * 100.0) if float(info["kg_inicial"] or 0) else 0.0
        dias_restantes = (kg_saldo / kg_dia) if kg_dia > 0 else None

        self.lot_kpi_kg.set_data(f"{fmt_num(k.kg_consumidos, 3)} kg", f"{fmt_num(kg_fracc, 3)} kg/fracc.")
        self.lot_kpi_paq.set_data(fmt_int(k.paquetes), f"{fmt_int(k.presentaciones_activas)} pres.")
        self.lot_kpi_unid.set_data(fmt_int(k.unidades), "según gramaje")
        self.lot_kpi_fracc.set_data(fmt_int(k.fraccionamientos), f"{fmt_int(k.productos_activos)} prod.")
        self.lot_kpi_bags.set_data(f"{fmt_num(k.bolsas_usadas_eq, 2)}", "bolsas eq.")
        self.lot_kpi_rate.set_data(f"{fmt_num(kg_dia, 3)}", f"{fmt_int(k.dias_activos)} días act.")
        self.lot_kpi_merma.set_data(f"{fmt_num(kg_merma, 3)} kg", f"{fmt_num(merma_pct, 1)}% merma")
        self.lot_kpi_cost.set_data(fmt_gs(fin["costo_kg_gs"]), "Gs/kg")
        self.lot_kpi_sale.set_data(fmt_gs(fin["venta_estimada_gs"]), "Gs estimados")
        self.lot_kpi_profit.set_data(fmt_gs(fin["beneficio_estimado_gs"]), "Gs estimados")
        self.lot_kpi_margin.set_data(f"{fmt_num(fin['margen_estimado_pct'], 1)}%", "sobre costo")
        if kg_saldo <= 0:
            self.lot_kpi_days.set_data("0 dias", "lote sin saldo")
            self.lot_kpi_days.set_alert(True)
            self.lbl_lot_projection.setText("El lote ya no tiene saldo disponible para produccion.")
        elif dias_restantes is None:
            self.lot_kpi_days.set_data("-", "sin consumo promedio")
            self.lot_kpi_days.set_alert(False)
            self.lbl_lot_projection.setText(
                f"Saldo actual: {fmt_num(kg_saldo, 3)} kg. Aun no hay suficiente historial de consumo para estimar cuantos dias mas podra producir."
            )
        else:
            dias_techo = int(math.ceil(dias_restantes))
            fecha_fin = date.today() + timedelta(days=max(dias_techo - 1, 0))
            self.lot_kpi_days.set_data(
                f"{fmt_num(dias_restantes, 1)} dias",
                f"fin estimado {fecha_fin.isoformat()}",
            )
            self.lot_kpi_days.set_alert(dias_restantes < 3.0)
            self.lbl_lot_projection.setText(
                f"Con un consumo promedio de {fmt_num(kg_dia, 3)} kg/dia y un saldo de {fmt_num(kg_saldo, 3)} kg, este lote alcanzaria para aproximadamente {fmt_num(dias_restantes, 1)} dias mas de produccion."
            )

        daily = self.repo.lot_daily(self._selected_lot_id)
        gram = self.repo.lot_gramaje_mix(self._selected_lot_id)
        rows = self.repo.lot_fractionation_rows(self._selected_lot_id)
        self._render_daily_chart_for(self.chart_lot_daily, daily, "Consumo diario del lote (kg)")
        self._render_bar_chart(
            self.chart_lot_gram,
            "Mix por gramaje del lote",
            [f'{int(r["gramaje"])} g' for r in gram[:8]],
            [float(r["paquetes"] or 0) for r in gram[:8]],
            "#2563eb",
        )
        self.tbl_lot_hist.setRowCount(len(rows))
        for r, row in enumerate(rows):
            vals = [
                fmt_short_ts(row["ts"]),
                row["producto"],
                f'{int(row["gramaje"])} g',
                fmt_int(row["paquetes"]),
                fmt_num(row["kg_consumidos"], 3),
            ]
            for c, val in enumerate(vals):
                item = QtWidgets.QTableWidgetItem(str(val))
                if c in (3, 4):
                    item.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                self.tbl_lot_hist.setItem(r, c, item)
        self.tbl_lot_hist.resizeColumnsToContents()
        self.tbl_lot_hist.setColumnWidth(1, min(max(self.tbl_lot_hist.columnWidth(1), 120), 150))
        self._update_lot_hist_width()

    def _load_balance(self, rows: list[dict]):
        self.tbl_balance.setRowCount(len(rows))
        for r, row in enumerate(rows):
            vals = [
                row["producto"],
                f'{int(row["gramaje"])} g',
                fmt_int(row["producidos"]),
                fmt_int(row["vendidos"]),
                fmt_int(row["stock_actual"]),
                fmt_num(float(row["kg_consumidos"] or 0), 3),
                fmt_gs(row["venta_gs"]),
            ]
            for c, val in enumerate(vals):
                item = QtWidgets.QTableWidgetItem(str(val))
                if c >= 2:
                    item.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                self.tbl_balance.setItem(r, c, item)

    def _load_insights(self, k: KPIData, mix_prod: list[dict], mix_gram: list[dict], lots: list[dict], balance: list[dict]):
        lines: list[str] = []
        if mix_prod:
            top = mix_prod[0]
            share = float(top["kg"] or 0) / k.kg_consumidos * 100.0 if k.kg_consumidos else 0.0
            lines.append(f"Producto dominante: {top['producto']} con {fmt_num(top['kg'], 3)} kg ({fmt_num(share, 1)}% del consumo).")
        if mix_gram:
            topg = mix_gram[0]
            lines.append(f"Gramaje líder: {int(topg['gramaje'])} g con {fmt_int(topg['paquetes'])} paquetes.")
        if lots:
            best = max(lots, key=lambda r: float(r["kg_usados"] or 0))
            lines.append(f"Lote más aprovechado: {best['producto']} / {(best['lote'] or ('L' + str(best['lot_id'])))} con {fmt_num(best['kg_usados'], 3)} kg usados.")
        if balance:
            gap_row = max(balance, key=lambda r: float(r["producidos"] or 0) - float(r["vendidos"] or 0))
            gap = float(gap_row["producidos"] or 0) - float(gap_row["vendidos"] or 0)
            lines.append(f"Mayor brecha producción vs venta: {gap_row['producto']} {int(gap_row['gramaje'])} g con {fmt_int(gap)} paquetes.")
        if not lines:
            lines.append("No hay suficiente movimiento en el período seleccionado para generar observaciones.")
        self.lbl_insight.setText("\n".join(lines))


def _parse_initial_lot_id(argv: list[str]) -> Optional[int]:
    for idx, arg in enumerate(argv[1:], start=1):
        if arg == "--lot-id" and idx + 1 < len(argv):
            try:
                return int(argv[idx + 1])
            except Exception:
                return None
        if arg.startswith("--lot-id="):
            try:
                return int(arg.split("=", 1)[1])
            except Exception:
                return None
    return None


def main():
    app = QtWidgets.QApplication(sys.argv)
    win = MetricaWindow(initial_lot_id=_parse_initial_lot_id(sys.argv))
    win.showMaximized()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

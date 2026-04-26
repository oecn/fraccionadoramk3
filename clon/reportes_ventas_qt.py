# -*- coding: utf-8 -*-
import sys
import csv
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import db
from PySide6 import QtWidgets, QtGui, QtCore, QtCharts


DB_PATH = ROOT_DIR / "GCMK8" / "fraccionadora.db"

TREND_COLORS = {
    "up": QtGui.QColor("#1a9c47"),
    "down": QtGui.QColor("#d64541"),
    "flat": QtGui.QColor("#6d7a88"),
}

PRODUCT_ANALYSIS_ROLE = QtCore.Qt.UserRole + 20

MONTH_NAMES = [
    "enero", "febrero", "marzo", "abril", "mayo", "junio",
    "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"
]

class Repo:
    def __init__(self, path: str | Path = DB_PATH):
        self.cn = db.connect("fraccionadora")

    def list_products(self):
        cur = self.cn.cursor()
        cur.execute("SELECT id, name FROM products ORDER BY name;")
        return cur.fetchall()

    def get_product_id_by_name(self, name: str):
        cur = self.cn.cursor()
        cur.execute("SELECT id FROM products WHERE name=%s;", (name,))
        row = cur.fetchone()
        return row[0] if row else None

    def list_gramajes_for_product(self, product_id: int):
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT DISTINCT gramaje FROM (
                SELECT gramaje FROM package_stock WHERE product_id=%s
                UNION ALL
                SELECT gramaje FROM sales_invoice_items WHERE product_id=%s
                UNION ALL
                SELECT CAST(kg_por_bolsa*1000 AS INTEGER) FROM bag_sales WHERE product_id=%s
            ) t
            ORDER BY gramaje;
            """,
            (product_id, product_id, product_id),
        )
        return [r[0] for r in cur.fetchall()]

    def list_available_year_months(self):
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT DISTINCT ym FROM (
                SELECT TO_CHAR((ts)::timestamp, 'YYYY-MM') AS ym FROM sales_invoices
                UNION
                SELECT TO_CHAR((ts)::timestamp, 'YYYY-MM') AS ym FROM bag_sales
            ) m
            ORDER BY ym DESC;
            """
        )
        return [r[0] for r in cur.fetchall()]

    def resumen_ventas(self, periodo: str = "mes", product_id: Optional[int] = None,
                       gramaje: Optional[int] = None,
                       ym: Optional[str] = None,
                       desde: Optional[str] = None, hasta: Optional[str] = None):
        cur = self.cn.cursor()

        if periodo == "semana":
            key_si = "TO_CHAR((si.ts)::timestamp, 'IYYY-\"W\"IW')"
            key_bs = "TO_CHAR((bs.ts)::timestamp, 'IYYY-\"W\"IW')"
            order = "periodo ASC, producto, gramaje"
        else:
            key_si = "TO_CHAR((si.ts)::timestamp, 'YYYY-MM')"
            key_bs = "TO_CHAR((bs.ts)::timestamp, 'YYYY-MM')"
            # Listar de más nuevo a más viejo para que los años recientes queden arriba
            order = "periodo DESC, importe_gs DESC, producto, gramaje"

        where_si = ["1=1"]
        params_si = []
        if product_id is not None:
            where_si.append("sii.product_id=%s")
            params_si.append(product_id)
        if gramaje is not None:
            where_si.append("sii.gramaje=%s")
            params_si.append(gramaje)
        if ym:
            where_si.append("TO_CHAR((si.ts)::timestamp, 'YYYY-MM')=%s")
            params_si.append(ym)
        if desde:
            where_si.append("date(si.ts) >= date(%s)")
            params_si.append(desde)
        if hasta:
            where_si.append("date(si.ts) <= date(%s)")
            params_si.append(hasta)

        where_bs = ["1=1"]
        params_bs = []
        if product_id is not None:
            where_bs.append("bs.product_id=%s")
            params_bs.append(product_id)
        if gramaje is not None:
            where_bs.append("CAST(bs.kg_por_bolsa*1000 AS INTEGER)=%s")
            params_bs.append(gramaje)
        if ym:
            where_bs.append("TO_CHAR((bs.ts)::timestamp, 'YYYY-MM')=%s")
            params_bs.append(ym)
        if desde:
            where_bs.append("date(bs.ts) >= date(%s)")
            params_bs.append(desde)
        if hasta:
            where_bs.append("date(bs.ts) <= date(%s)")
            params_bs.append(hasta)

        sql = f"""
            SELECT * FROM (
                SELECT {key_si} AS periodo,
                       p.name      AS producto,
                       sii.gramaje AS gramaje,
                       SUM(sii.cantidad)          AS paquetes,
                       SUM(sii.line_total)        AS importe_gs,
                       SUM(sii.line_base)         AS base_gs,
                       SUM(sii.line_iva)          AS iva_gs,
                       SUM(CASE WHEN position('luque' in lower(coalesce(si.customer,''))) > 0 THEN sii.cantidad ELSE 0 END) AS paq_luque,
                       SUM(CASE WHEN position('aregua' in lower(coalesce(si.customer,''))) > 0 THEN sii.cantidad ELSE 0 END) AS paq_aregua,
                       SUM(CASE WHEN position('itaugua' in lower(coalesce(si.customer,''))) > 0 THEN sii.cantidad ELSE 0 END) AS paq_itaugua
                FROM sales_invoice_items sii
                JOIN sales_invoices si ON si.id = sii.invoice_id
                JOIN products p       ON p.id = sii.product_id
                WHERE {' AND '.join(where_si)}
                GROUP BY 1, 2, 3
                UNION ALL
                SELECT {key_bs} AS periodo,
                       p.name AS producto,
                       CAST(bs.kg_por_bolsa*1000 AS INTEGER) AS gramaje,
                       SUM(bs.bolsas) AS paquetes,
                       SUM(bs.total_gs) AS importe_gs,
                       0 AS base_gs,
                       0 AS iva_gs,
                       SUM(CASE WHEN position('luque' in lower(coalesce(bs.customer,''))) > 0 THEN bs.bolsas ELSE 0 END) AS paq_luque,
                       SUM(CASE WHEN position('aregua' in lower(coalesce(bs.customer,''))) > 0 THEN bs.bolsas ELSE 0 END) AS paq_aregua,
                       SUM(CASE WHEN position('itaugua' in lower(coalesce(bs.customer,''))) > 0 THEN bs.bolsas ELSE 0 END) AS paq_itaugua
                FROM bag_sales bs
                JOIN products p ON p.id = bs.product_id
                WHERE {' AND '.join(where_bs)}
                GROUP BY 1, 2, 3
            ) resumen
            ORDER BY {order};
        """
        cur.execute(sql, params_si + params_bs)
        return [dict(r) for r in cur.fetchall()]

    def facturas_por_periodo(self, product_id: Optional[int] = None,
                             gramaje: Optional[int] = None,
                             ym: Optional[str] = None,
                             desde: Optional[str] = None, hasta: Optional[str] = None):
        cur = self.cn.cursor()

        where_si = ["1=1"]
        params_si = []
        if product_id is not None:
            where_si.append("sii.product_id=%s")
            params_si.append(product_id)
        if gramaje is not None:
            where_si.append("sii.gramaje=%s")
            params_si.append(gramaje)
        if ym:
            where_si.append("TO_CHAR((si.ts)::timestamp, 'YYYY-MM')=%s")
            params_si.append(ym)
        if desde:
            where_si.append("date(si.ts) >= date(%s)")
            params_si.append(desde)
        if hasta:
            where_si.append("date(si.ts) <= date(%s)")
            params_si.append(hasta)

        where_bs = ["1=1"]
        params_bs = []
        if product_id is not None:
            where_bs.append("bs.product_id=%s")
            params_bs.append(product_id)
        if gramaje is not None:
            where_bs.append("CAST(bs.kg_por_bolsa*1000 AS INTEGER)=%s")
            params_bs.append(gramaje)
        if ym:
            where_bs.append("TO_CHAR((bs.ts)::timestamp, 'YYYY-MM')=%s")
            params_bs.append(ym)
        if desde:
            where_bs.append("date(bs.ts) >= date(%s)")
            params_bs.append(desde)
        if hasta:
            where_bs.append("date(bs.ts) <= date(%s)")
            params_bs.append(hasta)

        sql = f"""
            SELECT periodo, COUNT(*) AS facturas
            FROM (
                SELECT DISTINCT TO_CHAR((si.ts)::timestamp, 'YYYY-MM') AS periodo, 'SI-' || si.id AS doc_key
                FROM sales_invoice_items sii
                JOIN sales_invoices si ON si.id = sii.invoice_id
                WHERE {' AND '.join(where_si)}
                UNION
                SELECT DISTINCT TO_CHAR((bs.ts)::timestamp, 'YYYY-MM') AS periodo, 'BS-' || bs.id AS doc_key
                FROM bag_sales bs
                WHERE {' AND '.join(where_bs)}
            ) docs
            GROUP BY periodo
            ORDER BY periodo DESC;
        """
        cur.execute(sql, params_si + params_bs)
        return [dict(r) for r in cur.fetchall()]

    def resumen_sucursales(self, product_id: Optional[int] = None,
                           gramaje: Optional[int] = None,
                           ym: Optional[str] = None,
                           desde: Optional[str] = None, hasta: Optional[str] = None):
        cur = self.cn.cursor()

        where_si = ["1=1"]
        params_si = []
        if product_id is not None:
            where_si.append("sii.product_id=%s")
            params_si.append(product_id)
        if gramaje is not None:
            where_si.append("sii.gramaje=%s")
            params_si.append(gramaje)
        if ym:
            where_si.append("TO_CHAR((si.ts)::timestamp, 'YYYY-MM')=%s")
            params_si.append(ym)
        if desde:
            where_si.append("date(si.ts) >= date(%s)")
            params_si.append(desde)
        if hasta:
            where_si.append("date(si.ts) <= date(%s)")
            params_si.append(hasta)

        where_bs = ["1=1"]
        params_bs = []
        if product_id is not None:
            where_bs.append("bs.product_id=%s")
            params_bs.append(product_id)
        if gramaje is not None:
            where_bs.append("CAST(bs.kg_por_bolsa*1000 AS INTEGER)=%s")
            params_bs.append(gramaje)
        if ym:
            where_bs.append("TO_CHAR((bs.ts)::timestamp, 'YYYY-MM')=%s")
            params_bs.append(ym)
        if desde:
            where_bs.append("date(bs.ts) >= date(%s)")
            params_bs.append(desde)
        if hasta:
            where_bs.append("date(bs.ts) <= date(%s)")
            params_bs.append(hasta)

        sql = f"""
            SELECT sucursal, SUM(importe_gs) AS importe_gs, SUM(paquetes) AS paquetes
            FROM (
                SELECT
                    CASE
                        WHEN position('luque' in lower(coalesce(si.customer,''))) > 0 THEN 'Luque'
                        WHEN position('aregua' in lower(coalesce(si.customer,''))) > 0 THEN 'Aregua'
                        WHEN position('itaugua' in lower(coalesce(si.customer,''))) > 0 THEN 'Itaugua'
                        ELSE 'Otras'
                    END AS sucursal,
                    SUM(sii.line_total) AS importe_gs,
                    SUM(sii.cantidad) AS paquetes
                FROM sales_invoice_items sii
                JOIN sales_invoices si ON si.id = sii.invoice_id
                WHERE {' AND '.join(where_si)}
                GROUP BY 1
                UNION ALL
                SELECT
                    CASE
                        WHEN position('luque' in lower(coalesce(bs.customer,''))) > 0 THEN 'Luque'
                        WHEN position('aregua' in lower(coalesce(bs.customer,''))) > 0 THEN 'Aregua'
                        WHEN position('itaugua' in lower(coalesce(bs.customer,''))) > 0 THEN 'Itaugua'
                        ELSE 'Otras'
                    END AS sucursal,
                    SUM(bs.total_gs) AS importe_gs,
                    SUM(bs.bolsas) AS paquetes
                FROM bag_sales bs
                WHERE {' AND '.join(where_bs)}
                GROUP BY 1
            ) suc
            GROUP BY sucursal
            ORDER BY importe_gs DESC, paquetes DESC;
        """
        cur.execute(sql, params_si + params_bs)
        return [dict(r) for r in cur.fetchall()]

    def product_branch_history(self, product_id: int, gramaje: int):
        cur = self.cn.cursor()
        sql = """
            SELECT periodo,
                   SUM(luque) AS luque,
                   SUM(aregua) AS aregua,
                   SUM(itaugua) AS itaugua,
                   SUM(otras) AS otras,
                   SUM(importe_gs) AS importe_gs
            FROM (
                SELECT TO_CHAR((si.ts)::timestamp, 'YYYY-MM') AS periodo,
                       SUM(CASE WHEN position('luque' in lower(coalesce(si.customer,''))) > 0 THEN sii.cantidad ELSE 0 END) AS luque,
                       SUM(CASE WHEN position('aregua' in lower(coalesce(si.customer,''))) > 0 THEN sii.cantidad ELSE 0 END) AS aregua,
                       SUM(CASE WHEN position('itaugua' in lower(coalesce(si.customer,''))) > 0 THEN sii.cantidad ELSE 0 END) AS itaugua,
                       SUM(CASE
                               WHEN position('luque' in lower(coalesce(si.customer,''))) > 0 THEN 0
                               WHEN position('aregua' in lower(coalesce(si.customer,''))) > 0 THEN 0
                               WHEN position('itaugua' in lower(coalesce(si.customer,''))) > 0 THEN 0
                               ELSE sii.cantidad
                           END) AS otras,
                       SUM(sii.line_total) AS importe_gs
                FROM sales_invoice_items sii
                JOIN sales_invoices si ON si.id = sii.invoice_id
                WHERE sii.product_id=%s AND sii.gramaje=%s
                GROUP BY 1
                UNION ALL
                SELECT TO_CHAR((bs.ts)::timestamp, 'YYYY-MM') AS periodo,
                       SUM(CASE WHEN position('luque' in lower(coalesce(bs.customer,''))) > 0 THEN bs.bolsas ELSE 0 END) AS luque,
                       SUM(CASE WHEN position('aregua' in lower(coalesce(bs.customer,''))) > 0 THEN bs.bolsas ELSE 0 END) AS aregua,
                       SUM(CASE WHEN position('itaugua' in lower(coalesce(bs.customer,''))) > 0 THEN bs.bolsas ELSE 0 END) AS itaugua,
                       SUM(CASE
                               WHEN position('luque' in lower(coalesce(bs.customer,''))) > 0 THEN 0
                               WHEN position('aregua' in lower(coalesce(bs.customer,''))) > 0 THEN 0
                               WHEN position('itaugua' in lower(coalesce(bs.customer,''))) > 0 THEN 0
                               ELSE bs.bolsas
                           END) AS otras,
                       SUM(bs.total_gs) AS importe_gs
                FROM bag_sales bs
                WHERE bs.product_id=%s AND CAST(bs.kg_por_bolsa*1000 AS INTEGER)=%s
                GROUP BY 1
            ) hist
            WHERE periodo IS NOT NULL AND periodo <> ''
            GROUP BY periodo
            ORDER BY periodo ASC;
        """
        cur.execute(sql, (product_id, gramaje, product_id, gramaje))
        return [dict(r) for r in cur.fetchall()]

    def branch_history_until(self, upto_ym: str,
                             product_id: Optional[int] = None,
                             gramaje: Optional[int] = None):
        cur = self.cn.cursor()
        where_si = ["TO_CHAR((si.ts)::timestamp, 'YYYY-MM') <= %s"]
        params_si = [upto_ym]
        if product_id is not None:
            where_si.append("sii.product_id=%s")
            params_si.append(product_id)
        if gramaje is not None:
            where_si.append("sii.gramaje=%s")
            params_si.append(gramaje)

        where_bs = ["TO_CHAR((bs.ts)::timestamp, 'YYYY-MM') <= %s"]
        params_bs = [upto_ym]
        if product_id is not None:
            where_bs.append("bs.product_id=%s")
            params_bs.append(product_id)
        if gramaje is not None:
            where_bs.append("CAST(bs.kg_por_bolsa*1000 AS INTEGER)=%s")
            params_bs.append(gramaje)

        sql = f"""
            SELECT periodo,
                   SUM(luque) AS luque,
                   SUM(aregua) AS aregua,
                   SUM(itaugua) AS itaugua,
                   SUM(otras) AS otras,
                   SUM(paquetes) AS paquetes,
                   SUM(importe_gs) AS importe_gs
            FROM (
                SELECT TO_CHAR((si.ts)::timestamp, 'YYYY-MM') AS periodo,
                       SUM(CASE WHEN position('luque' in lower(coalesce(si.customer,''))) > 0 THEN sii.cantidad ELSE 0 END) AS luque,
                       SUM(CASE WHEN position('aregua' in lower(coalesce(si.customer,''))) > 0 THEN sii.cantidad ELSE 0 END) AS aregua,
                       SUM(CASE WHEN position('itaugua' in lower(coalesce(si.customer,''))) > 0 THEN sii.cantidad ELSE 0 END) AS itaugua,
                       SUM(CASE
                               WHEN position('luque' in lower(coalesce(si.customer,''))) > 0 THEN 0
                               WHEN position('aregua' in lower(coalesce(si.customer,''))) > 0 THEN 0
                               WHEN position('itaugua' in lower(coalesce(si.customer,''))) > 0 THEN 0
                               ELSE sii.cantidad
                           END) AS otras,
                       SUM(sii.cantidad) AS paquetes,
                       SUM(sii.line_total) AS importe_gs
                FROM sales_invoice_items sii
                JOIN sales_invoices si ON si.id = sii.invoice_id
                WHERE {' AND '.join(where_si)}
                GROUP BY 1
                UNION ALL
                SELECT TO_CHAR((bs.ts)::timestamp, 'YYYY-MM') AS periodo,
                       SUM(CASE WHEN position('luque' in lower(coalesce(bs.customer,''))) > 0 THEN bs.bolsas ELSE 0 END) AS luque,
                       SUM(CASE WHEN position('aregua' in lower(coalesce(bs.customer,''))) > 0 THEN bs.bolsas ELSE 0 END) AS aregua,
                       SUM(CASE WHEN position('itaugua' in lower(coalesce(bs.customer,''))) > 0 THEN bs.bolsas ELSE 0 END) AS itaugua,
                       SUM(CASE
                               WHEN position('luque' in lower(coalesce(bs.customer,''))) > 0 THEN 0
                               WHEN position('aregua' in lower(coalesce(bs.customer,''))) > 0 THEN 0
                               WHEN position('itaugua' in lower(coalesce(bs.customer,''))) > 0 THEN 0
                               ELSE bs.bolsas
                           END) AS otras,
                       SUM(bs.bolsas) AS paquetes,
                       SUM(bs.total_gs) AS importe_gs
                FROM bag_sales bs
                WHERE {' AND '.join(where_bs)}
                GROUP BY 1
            ) hist
            WHERE periodo IS NOT NULL AND periodo <> ''
            GROUP BY periodo
            ORDER BY periodo ASC;
        """
        cur.execute(sql, params_si + params_bs)
        return [dict(r) for r in cur.fetchall()]

    def kpi_history_series(self, granularity: str = "month",
                           product_id: Optional[int] = None,
                           gramaje: Optional[int] = None):
        cur = self.cn.cursor()
        key_si = "TO_CHAR((si.ts)::timestamp, 'YYYY-MM')"
        key_bs = "TO_CHAR((bs.ts)::timestamp, 'YYYY-MM')"
        order_clause = "periodo ASC"

        where_si = ["1=1"]
        params_si = []
        if product_id is not None:
            where_si.append("sii.product_id=%s")
            params_si.append(product_id)
        if gramaje is not None:
            where_si.append("sii.gramaje=%s")
            params_si.append(gramaje)

        where_bs = ["1=1"]
        params_bs = []
        if product_id is not None:
            where_bs.append("bs.product_id=%s")
            params_bs.append(product_id)
        if gramaje is not None:
            where_bs.append("CAST(bs.kg_por_bolsa*1000 AS INTEGER)=%s")
            params_bs.append(gramaje)

        sql = f"""
            SELECT periodo,
                   SUM(paquetes) AS paquetes,
                   SUM(importe_gs) AS importe_gs,
                   SUM(facturas) AS facturas
            FROM (
                SELECT {key_si} AS periodo,
                       SUM(sii.cantidad) AS paquetes,
                       SUM(sii.line_total) AS importe_gs,
                       COUNT(DISTINCT si.id) AS facturas
                FROM sales_invoice_items sii
                JOIN sales_invoices si ON si.id = sii.invoice_id
                WHERE {' AND '.join(where_si)}
                GROUP BY 1
                UNION ALL
                SELECT {key_bs} AS periodo,
                       SUM(bs.bolsas) AS paquetes,
                       SUM(bs.total_gs) AS importe_gs,
                       COUNT(*) AS facturas
                FROM bag_sales bs
                WHERE {' AND '.join(where_bs)}
                GROUP BY 1
            ) serie
            WHERE periodo IS NOT NULL AND periodo <> ''
            GROUP BY periodo
            ORDER BY {order_clause};
        """
        cur.execute(sql, params_si + params_bs)
        rows = []
        for r in cur.fetchall():
            paquetes = int(r["paquetes"] or 0)
            importe = float(r["importe_gs"] or 0.0)
            facturas = int(r["facturas"] or 0)
            ticket = importe / facturas if facturas > 0 else 0.0
            rows.append({
                "periodo": str(r["periodo"]),
                "paquetes": paquetes,
                "importe": importe,
                "facturas": facturas,
                "ticket_promedio": ticket,
            })
        return rows

    def detalle_por_factura(self, product_id: Optional[int] = None,
                             gramaje: Optional[int] = None,
                             ym: Optional[str] = None,
                             desde: Optional[str] = None, hasta: Optional[str] = None):
        cur = self.cn.cursor()
        where_si = ["1=1"]
        params_si = []
        if product_id is not None:
            where_si.append("sii.product_id=%s"); params_si.append(product_id)
        if gramaje is not None:
            where_si.append("sii.gramaje=%s"); params_si.append(gramaje)
        if ym:
            where_si.append("TO_CHAR((si.ts)::timestamp, 'YYYY-MM')=%s"); params_si.append(ym)
        if desde:
            where_si.append("date(si.ts) >= date(%s)"); params_si.append(desde)
        if hasta:
            where_si.append("date(si.ts) <= date(%s)"); params_si.append(hasta)

        where_bs = ["1=1"]
        params_bs = []
        if product_id is not None:
            where_bs.append("bs.product_id=%s"); params_bs.append(product_id)
        if gramaje is not None:
            where_bs.append("CAST(bs.kg_por_bolsa*1000 AS INTEGER)=%s"); params_bs.append(gramaje)
        if ym:
            where_bs.append("TO_CHAR((bs.ts)::timestamp, 'YYYY-MM')=%s"); params_bs.append(ym)
        if desde:
            where_bs.append("date(bs.ts) >= date(%s)"); params_bs.append(desde)
        if hasta:
            where_bs.append("date(bs.ts) <= date(%s)"); params_bs.append(hasta)

        sql = f"""
            SELECT * FROM (
                SELECT si.ts AS fecha,
                       coalesce(si.invoice_no,'') AS nro_factura,
                       coalesce(si.customer,'')   AS cliente,
                       p.name AS producto,
                       sii.gramaje AS gramaje,
                       sii.cantidad AS paquetes,
                       sii.price_gs AS precio_unit,
                       sii.line_total AS importe,
                       si.id AS invoice_id
                FROM sales_invoice_items sii
                JOIN sales_invoices si ON si.id = sii.invoice_id
                JOIN products p       ON p.id = sii.product_id
                WHERE {' AND '.join(where_si)}
                UNION ALL
                SELECT bs.ts AS fecha,
                       coalesce(bs.invoice_no,'') AS nro_factura,
                       coalesce(bs.customer,'')   AS cliente,
                       p.name AS producto,
                       CAST(bs.kg_por_bolsa*1000 AS INTEGER) AS gramaje,
                       bs.bolsas AS paquetes,
                       bs.price_bolsa_gs AS precio_unit,
                       bs.total_gs AS importe,
                       NULL AS invoice_id
                FROM bag_sales bs
                JOIN products p ON p.id = bs.product_id
                WHERE {' AND '.join(where_bs)}
            ) detalle
            ORDER BY fecha ASC, producto ASC, gramaje ASC;
        """
        cur.execute(sql, params_si + params_bs)
        return [dict(r) for r in cur.fetchall()]

    def factura_items(self, invoice_id: int):
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT si.ts AS fecha, coalesce(si.invoice_no,'') AS nro_factura, coalesce(si.customer,'') AS cliente,
                   p.name AS producto, sii.gramaje, sii.cantidad, sii.price_gs, sii.line_total
            FROM sales_invoice_items sii
            JOIN sales_invoices si ON si.id = sii.invoice_id
            JOIN products p       ON p.id = sii.product_id
            WHERE si.id=%s
            ORDER BY p.name, sii.gramaje;
            """,
            (invoice_id,),
        )
        return [dict(r) for r in cur.fetchall()]

@dataclass
class Filters:
    prod_cb: QtWidgets.QComboBox
    gram_cb: QtWidgets.QComboBox
    mes_cb: QtWidgets.QComboBox
    desde_edit: QtWidgets.QLineEdit
    hasta_edit: QtWidgets.QLineEdit

class PercentBarDelegate(QtWidgets.QStyledItemDelegate):
    """
    Renderiza una barra de avance ligera en la celda según el porcentaje (UserRole).
    """
    NO_BAR_ROLE = QtCore.Qt.UserRole + 1

    def paint(self, painter, option, index):
        # No barras en filas marcadas o en los nodos de nivel superior (títulos de mes)
        if index.data(self.NO_BAR_ROLE) or not index.parent().isValid():
            # Para filas que no deben mostrar barra (ej. títulos de mes)
            super().paint(painter, option, index)
            return
        pct = index.data(QtCore.Qt.UserRole)
        try:
            pct_val = float(pct)
        except Exception:
            pct_val = 0.0
        pct_val = max(0.0, min(100.0, pct_val))

        # Fondo de selección respetando el highlight
        if option.state & QtWidgets.QStyle.State_Selected:
            painter.fillRect(option.rect, option.palette.highlight())
            text_color = option.palette.highlightedText().color()
        else:
            painter.fillRect(option.rect, option.palette.base())
            text_color = option.palette.text().color()

        # Barra
        bar_rect = option.rect.adjusted(2, 6, -2, -6)
        fill_width = int(bar_rect.width() * (pct_val / 100.0))
        bar_color = QtGui.QColor("#d9e5ff")
        painter.fillRect(QtCore.QRect(bar_rect.left(), bar_rect.top(), fill_width, bar_rect.height()), bar_color)
        painter.setPen(QtGui.QPen(QtGui.QColor("#0d4ba0")))
        painter.drawRect(bar_rect)

        # Texto centrado
        painter.setPen(text_color)
        text = index.data(QtCore.Qt.DisplayRole) or ""
        painter.drawText(option.rect, QtCore.Qt.AlignCenter, text)

    def helpEvent(self, event, view, option, index):
        # Propaga para tooltips por defecto
        return super().helpEvent(event, view, option, index)

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Reportes de Ventas - PySide6")
        self.resize(1280, 820)
        self._settings = QtCore.QSettings("GCPDFMK10", "ReportesVentasQt")
        self._dark_mode = self._load_theme_preference()
        self.repo = Repo()
        self._help_labels: list[QtWidgets.QLabel] = []
        self._kpi_cards: dict[str, QtWidgets.QFrame] = {}
        self._kpi_labels: dict[str, dict[str, QtWidgets.QLabel]] = {}
        self._ranking_cards: dict[str, QtWidgets.QFrame] = {}
        self._kpi_history_dialog: QtWidgets.QDialog | None = None
        self._btn_expand: QtWidgets.QPushButton | None = None
        self._btn_collapse: QtWidgets.QPushButton | None = None
        self._tune_palette()
        self.status_label = QtWidgets.QLabel("Listo")
        self.statusBar().addWidget(self.status_label)

        central = QtWidgets.QWidget()
        central.setObjectName("root")
        self.setCentralWidget(central)
        layout = QtWidgets.QVBoxLayout(central)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        top_bar = QtWidgets.QHBoxLayout()
        top_bar.setSpacing(8)
        title = QtWidgets.QLabel("Reporte de ventas")
        title.setObjectName("pageTitle")
        top_bar.addWidget(title)
        top_bar.addStretch(1)
        top_bar.addWidget(QtWidgets.QLabel("Tema:"))
        self.theme_cb = QtWidgets.QComboBox()
        self.theme_cb.addItem("Claro", "light")
        self.theme_cb.addItem("Oscuro", "dark")
        self.theme_cb.setFixedWidth(130)
        top_bar.addWidget(self.theme_cb)
        layout.addLayout(top_bar)

        self.tabs = QtWidgets.QTabWidget()
        layout.addWidget(self.tabs)

        self._build_tab_mes()
        self._build_tab_semana()
        self._build_tab_detalle()

        self.theme_cb.currentIndexChanged.connect(self._on_theme_changed)
        self.theme_cb.blockSignals(True)
        self.theme_cb.setCurrentIndex(1 if self._dark_mode else 0)
        self.theme_cb.blockSignals(False)
        self._apply_theme()

    def eventFilter(self, obj, event):
        if event.type() == QtCore.QEvent.MouseButtonDblClick:
            kpi_key = obj.property("kpi_key")
            if kpi_key:
                self._open_kpi_history_dialog(str(kpi_key))
                return True
        return super().eventFilter(obj, event)

    # --------- helpers de UI ---------
    def _build_filters(self, parent: QtWidgets.QWidget, help_text: str = ""):
        box = QtWidgets.QFrame(parent)
        v = QtWidgets.QVBoxLayout(box)
        v.setContentsMargins(6, 4, 6, 4)
        v.setSpacing(4)
        row = QtWidgets.QHBoxLayout()
        row.setSpacing(6)
        v.addLayout(row)

        row.addWidget(QtWidgets.QLabel("Producto:"))
        cb_prod = QtWidgets.QComboBox(); cb_prod.setFixedWidth(260)
        row.addWidget(cb_prod)

        row.addWidget(QtWidgets.QLabel("Gramaje:"))
        cb_gram = QtWidgets.QComboBox(); cb_gram.setFixedWidth(100)
        row.addWidget(cb_gram)

        row.addWidget(QtWidgets.QLabel("Mes (YYYY-MM):"))
        cb_mes = QtWidgets.QComboBox(); cb_mes.setFixedWidth(120)
        row.addWidget(cb_mes)

        row.addWidget(QtWidgets.QLabel("o Desde:"))
        ent_desde = QtWidgets.QLineEdit(); ent_desde.setPlaceholderText("YYYY-MM-DD")
        ent_desde.setFixedWidth(110); row.addWidget(ent_desde)
        row.addWidget(QtWidgets.QLabel("Hasta:"))
        ent_hasta = QtWidgets.QLineEdit(); ent_hasta.setPlaceholderText("YYYY-MM-DD")
        ent_hasta.setFixedWidth(110); row.addWidget(ent_hasta)

        row.addStretch(1)

        btn_refresh = QtWidgets.QPushButton("Refrescar")
        row.addWidget(btn_refresh)
        btn_export = QtWidgets.QPushButton("Exportar CSV")
        row.addWidget(btn_export)
        btn_clear = QtWidgets.QPushButton("Limpiar filtros")
        row.addWidget(btn_clear)

        if help_text:
            help_lbl = QtWidgets.QLabel(help_text)
            self._help_labels.append(help_lbl)
            help_lbl.setWordWrap(True)
            v.addWidget(help_lbl)

        filters = Filters(cb_prod, cb_gram, cb_mes, ent_desde, ent_hasta)

        btn_clear.clicked.connect(lambda: self._clear_filters(filters))
        btn_export.clicked.connect(self._export_current_view)
        btn_refresh.clicked.connect(self._refresh_all)

        return box, filters

    def _tune_palette(self):
        """
        Ajusta el color de selección a un azul claro para TreeView/TableView.
        """
        app = QtWidgets.QApplication.instance()
        if not app:
            return
        pal = app.palette()
        if self._dark_mode:
            pal.setColor(QtGui.QPalette.Highlight, QtGui.QColor("#1e3a8a"))
            pal.setColor(QtGui.QPalette.HighlightedText, QtGui.QColor("#e5e7eb"))
            pal.setColor(QtGui.QPalette.Base, QtGui.QColor("#0f172a"))
            pal.setColor(QtGui.QPalette.AlternateBase, QtGui.QColor("#111c31"))
            pal.setColor(QtGui.QPalette.Window, QtGui.QColor("#0b1220"))
            pal.setColor(QtGui.QPalette.WindowText, QtGui.QColor("#e5e7eb"))
            pal.setColor(QtGui.QPalette.Text, QtGui.QColor("#e5e7eb"))
            pal.setColor(QtGui.QPalette.Button, QtGui.QColor("#162033"))
            pal.setColor(QtGui.QPalette.ButtonText, QtGui.QColor("#e5e7eb"))
        else:
            pal.setColor(QtGui.QPalette.Highlight, QtGui.QColor("#d9e5ff"))
            pal.setColor(QtGui.QPalette.HighlightedText, QtGui.QColor("#12326b"))
            pal.setColor(QtGui.QPalette.Base, QtGui.QColor("#ffffff"))
            pal.setColor(QtGui.QPalette.AlternateBase, QtGui.QColor("#f8fafc"))
            pal.setColor(QtGui.QPalette.Window, QtGui.QColor("#f4f7fb"))
            pal.setColor(QtGui.QPalette.WindowText, QtGui.QColor("#1f2937"))
            pal.setColor(QtGui.QPalette.Text, QtGui.QColor("#1f2937"))
            pal.setColor(QtGui.QPalette.Button, QtGui.QColor("#ffffff"))
            pal.setColor(QtGui.QPalette.ButtonText, QtGui.QColor("#0f172a"))
        app.setPalette(pal)

    def _build_kpi_bar(self, parent: QtWidgets.QWidget):
        frame = QtWidgets.QFrame(parent)
        grid = QtWidgets.QGridLayout(frame)
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setSpacing(10)

        cards_info = [
            ("Paquetes", "paquetes"),
            ("Importe (Gs)", "importe"),
            ("Cantidad de facturas", "facturas"),
            ("Ticket promedio", "ticket_promedio"),
        ]
        for i, (title, key) in enumerate(cards_info):
            card = QtWidgets.QFrame()
            card.setObjectName(f"kpi_{key}")
            card.setProperty("kpi_key", key)
            card.installEventFilter(self)
            card.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
            card.setToolTip("Doble clic para ver el historico")
            self._kpi_cards[key] = card
            # Efecto de sombra para hover, ya que Qt no soporta box-shadow en estilos
            shadow = QtWidgets.QGraphicsDropShadowEffect(card)
            shadow.setBlurRadius(12)
            shadow.setOffset(0, 4)
            shadow.setColor(QtGui.QColor(0, 0, 0, 30))
            card.setGraphicsEffect(shadow)
            lay = QtWidgets.QVBoxLayout(card)
            lay.setContentsMargins(12, 10, 12, 10)
            lay.setSpacing(4)

            top = QtWidgets.QHBoxLayout()
            top.setContentsMargins(0, 0, 0, 0)
            top.setSpacing(6)
            lbl_title = QtWidgets.QLabel(title)
            lbl_title.setProperty("kpi_key", key)
            lbl_title.installEventFilter(self)
            lbl_title.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
            badge = QtWidgets.QLabel("→")
            badge.setAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
            badge.setProperty("kpi_key", key)
            badge.installEventFilter(self)
            badge.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
            top.addWidget(lbl_title, 1)
            top.addWidget(badge, 0)

            lbl_val = QtWidgets.QLabel("0")
            lbl_delta = QtWidgets.QLabel("(s/d)")
            lbl_val.setProperty("kpi_key", key)
            lbl_delta.setProperty("kpi_key", key)
            lbl_val.installEventFilter(self)
            lbl_delta.installEventFilter(self)
            lbl_val.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
            lbl_delta.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))

            lay.addLayout(top)
            lay.addWidget(lbl_val)
            lay.addWidget(lbl_delta)

            grid.addWidget(card, 0, i)
            self._kpi_labels[key] = {
                "title": lbl_title,
                "value": lbl_val,
                "badge": badge,
                "delta": lbl_delta,
            }
            grid.setColumnStretch(i, 1)

        self.kpi_period = QtWidgets.QLabel("")
        grid.addWidget(self.kpi_period, 0, len(cards_info), 1, 1, QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
        grid.setColumnStretch(len(cards_info), 0)
        return frame

    def _build_rankings_bar(self, parent: QtWidgets.QWidget):
        frame = QtWidgets.QFrame(parent)
        frame.setObjectName("rankingsBar")
        outer = QtWidgets.QVBoxLayout(frame)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(8)

        controls = QtWidgets.QHBoxLayout()
        controls.setSpacing(8)
        self.lbl_rankings = QtWidgets.QLabel("Rankings")
        self.lbl_rankings.setObjectName("rankingSectionTitle")
        controls.addWidget(self.lbl_rankings)
        controls.addStretch(1)
        controls.addWidget(QtWidgets.QLabel("Alcance:"))
        self.rank_scope_cb = QtWidgets.QComboBox()
        self.rank_scope_cb.addItem("Todo el tiempo", "all")
        self.rank_scope_cb.addItem("Mes concreto", "month")
        self.rank_scope_cb.setFixedWidth(150)
        controls.addWidget(self.rank_scope_cb)
        self.rank_month_cb = QtWidgets.QComboBox()
        self.rank_month_cb.setFixedWidth(130)
        for ym in self.repo.list_available_year_months():
            self.rank_month_cb.addItem(ym)
        now_ym = datetime.now().strftime("%Y-%m")
        if self.rank_month_cb.count() == 0:
            self.rank_month_cb.addItem(now_ym)
        month_idx = self.rank_month_cb.findText(now_ym)
        if month_idx >= 0:
            self.rank_month_cb.setCurrentIndex(month_idx)
        else:
            self.rank_month_cb.insertItem(0, now_ym)
            self.rank_month_cb.setCurrentIndex(0)
        self.rank_scope_cb.setCurrentIndex(self.rank_scope_cb.findData("month"))
        self.rank_month_cb.setEnabled(True)
        controls.addWidget(self.rank_month_cb)
        outer.addLayout(controls)

        layout = QtWidgets.QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)
        outer.addLayout(layout)

        def _make_card(title: str, key: str):
            card = QtWidgets.QFrame()
            card.setObjectName(f"ranking_{key}")
            lay = QtWidgets.QVBoxLayout(card)
            lay.setContentsMargins(12, 10, 12, 10)
            lay.setSpacing(6)
            lbl_title = QtWidgets.QLabel(title)
            lbl_title.setObjectName("rankingTitle")
            lbl_body = QtWidgets.QLabel("Sin datos")
            lbl_body.setObjectName("rankingBody")
            lbl_body.setWordWrap(True)
            lay.addWidget(lbl_title)
            lay.addWidget(lbl_body)
            layout.addWidget(card, 1)
            self._ranking_cards[key] = card
            return lbl_body

        self.lbl_top_productos = _make_card("Top 3 productos", "productos")
        self.lbl_top_sucursales = _make_card("Top 3 sucursales", "sucursales")
        return frame

    def _build_tab_mes(self):
        tab = QtWidgets.QWidget()
        v = QtWidgets.QVBoxLayout(tab)
        v.setContentsMargins(6, 6, 6, 6)
        v.setSpacing(6)

        self.kpi_bar = self._build_kpi_bar(tab)
        v.addWidget(self.kpi_bar)

        self.rankings_bar = self._build_rankings_bar(tab)
        v.addWidget(self.rankings_bar)

        filt_box, filters = self._build_filters(tab, "Tip: Combina el selector de mes con fechas para comparar cierres.")
        v.addWidget(filt_box)
        self.filters_mes = filters

        act_row = QtWidgets.QHBoxLayout()
        act_row.setSpacing(6)
        btn_expand = QtWidgets.QPushButton("Desplegar todo")
        btn_collapse = QtWidgets.QPushButton("Contraer todo")
        btn_style = (
            "QPushButton {"
            "background-color: #1b5fbf;"
            "color: white;"
            "border: none;"
            "border-radius: 4px;"
            "padding: 6px 12px;"
            "font-weight: 600;"
            "}"
            "QPushButton:hover {"
            "background-color: #1f6ed8;"
            "}"
            "QPushButton:pressed {"
            "background-color: #1958ad;"
            "}"
        )
        self._btn_expand = btn_expand
        self._btn_collapse = btn_collapse
        act_row.addWidget(btn_expand)
        act_row.addWidget(btn_collapse)
        act_row.addSpacing(10)
        act_row.addWidget(QtWidgets.QLabel("Grafico mes:"))
        self.month_chart_cb = QtWidgets.QComboBox()
        self.month_chart_cb.setMinimumWidth(150)
        act_row.addWidget(self.month_chart_cb)
        btn_month_chart = QtWidgets.QPushButton("Abrir grafico")
        act_row.addWidget(btn_month_chart)
        act_row.addStretch(1)
        v.addLayout(act_row)

        self.view_mes = QtWidgets.QTreeView()
        self.view_mes.setAlternatingRowColors(True)
        self.view_mes.setRootIsDecorated(True)
        self.view_mes.setSortingEnabled(False)
        self.view_mes.setIndentation(18)
        self.view_mes.setIconSize(QtCore.QSize(12, 12))
        self.view_mes.clicked.connect(self._handle_month_row_click)
        self.view_mes.doubleClicked.connect(self._open_product_history_popup)
        v.addWidget(self.view_mes, 1)
        self.share_delegate = PercentBarDelegate(self.view_mes)

        btn_expand.clicked.connect(lambda: self.view_mes.expandAll())
        btn_collapse.clicked.connect(lambda: self.view_mes.collapseAll())
        btn_month_chart.clicked.connect(self._open_selected_month_chart)

        self._populate_filters(filters)
        self._fill_resumen("mes")
        self.rank_scope_cb.currentIndexChanged.connect(self._on_ranking_scope_changed)
        self.rank_month_cb.currentIndexChanged.connect(self._on_ranking_month_changed)

        self.tabs.addTab(tab, "Resumen por Mes")

    def _build_tab_semana(self):
        tab = QtWidgets.QWidget()
        v = QtWidgets.QVBoxLayout(tab)
        v.setContentsMargins(6, 6, 6, 6)
        v.setSpacing(6)

        filt_box, filters = self._build_filters(tab, "Tip: Ideal para validar metas semanales.")
        v.addWidget(filt_box)
        self.filters_sem = filters

        self.view_sem = QtWidgets.QTableView()
        self.view_sem.setAlternatingRowColors(True)
        self.view_sem.setSortingEnabled(True)
        v.addWidget(self.view_sem, 1)

        self._populate_filters(filters)
        self._fill_resumen("semana")

        self.tabs.addTab(tab, "Resumen por Semana")

    def _build_tab_detalle(self):
        tab = QtWidgets.QWidget()
        v = QtWidgets.QVBoxLayout(tab)
        v.setContentsMargins(6, 6, 6, 6)
        v.setSpacing(6)

        filt_box, filters = self._build_filters(tab, "Tip: Doble clic abre la factura con todos sus items.")
        v.addWidget(filt_box)
        self.filters_det = filters

        self.view_det = QtWidgets.QTableView()
        self.view_det.setAlternatingRowColors(True)
        self.view_det.setSortingEnabled(True)
        self.view_det.doubleClicked.connect(self._open_invoice_popup)
        v.addWidget(self.view_det, 1)

        self._populate_filters(filters)
        self._fill_detalle()

        self.tabs.addTab(tab, "Detalle y Facturas")

    def _detect_system_dark_mode(self) -> bool:
        app = QtWidgets.QApplication.instance()
        if not app:
            return False
        pal = app.palette()
        return pal.color(QtGui.QPalette.Window).lightness() < 128

    def _load_theme_preference(self) -> bool:
        theme = str(self._settings.value("theme_mode", "") or "").strip().lower()
        if theme == "dark":
            return True
        if theme == "light":
            return False
        return self._detect_system_dark_mode()

    def _save_theme_preference(self):
        self._settings.setValue("theme_mode", "dark" if self._dark_mode else "light")

    def _on_theme_changed(self, *_args):
        mode = self.theme_cb.currentData()
        self._dark_mode = mode == "dark"
        self._save_theme_preference()
        self._apply_theme()

    def _on_ranking_scope_changed(self, *_args):
        is_month = self.rank_scope_cb.currentData() == "month"
        self.rank_month_cb.setEnabled(is_month)
        self._fill_resumen("mes")

    def _on_ranking_month_changed(self, *_args):
        if self.rank_scope_cb.currentData() == "month":
            self._fill_resumen("mes")

    def _prev_month(self, ym: str) -> Optional[str]:
        try:
            year, month = ym.split("-")
            year_i = int(year)
            month_i = int(month)
            month_i -= 1
            if month_i == 0:
                year_i -= 1
                month_i = 12
            return f"{year_i:04d}-{month_i:02d}"
        except Exception:
            return None

    def _ranking_params(self, pid: Optional[int], gram: Optional[int]):
        scope = self.rank_scope_cb.currentData() if hasattr(self, "rank_scope_cb") else "all"
        if scope == "month":
            ym = self.rank_month_cb.currentText().strip() or None
            return pid, gram, ym, None, None
        return pid, gram, None, None, None

    def _kpi_metric_items(self):
        return [
            ("Todos", "all"),
            ("Paquetes", "paquetes"),
            ("Importe (Gs)", "importe"),
            ("Cantidad de facturas", "facturas"),
            ("Ticket promedio", "ticket_promedio"),
        ]

    def _open_kpi_history_dialog(self, initial_key: str):
        if self._kpi_history_dialog is None:
            dlg = QtWidgets.QDialog(self)
            dlg.setWindowTitle("Historico de KPI")
            dlg.resize(980, 620)
            layout = QtWidgets.QVBoxLayout(dlg)

            controls = QtWidgets.QHBoxLayout()
            controls.setSpacing(8)
            controls.addWidget(QtWidgets.QLabel("KPI:"))
            metric_cb = QtWidgets.QComboBox()
            for label, key in self._kpi_metric_items():
                metric_cb.addItem(label, key)
            controls.addWidget(metric_cb)
            controls.addWidget(QtWidgets.QLabel("Vista:"))
            granularity_cb = QtWidgets.QComboBox()
            granularity_cb.addItem("Mensual desde el primer registro", "month")
            granularity_cb.addItem("Por años", "years")
            controls.addWidget(granularity_cb)
            controls.addStretch(1)
            layout.addLayout(controls)

            info_lbl = QtWidgets.QLabel("")
            info_lbl.setWordWrap(True)
            layout.addWidget(info_lbl)

            year_selector_widget = QtWidgets.QWidget()
            year_selector_layout = QtWidgets.QHBoxLayout(year_selector_widget)
            year_selector_layout.setContentsMargins(0, 0, 0, 0)
            year_selector_layout.setSpacing(8)
            year_selector_widget.hide()
            layout.addWidget(year_selector_widget)

            chart_stack = QtWidgets.QStackedWidget()

            single_chart_widget = QtWidgets.QWidget()
            single_chart_layout = QtWidgets.QVBoxLayout(single_chart_widget)
            single_chart_layout.setContentsMargins(0, 0, 0, 0)
            single_chart_layout.setSpacing(6)
            chart_view = QtCharts.QChartView()
            chart_view.setRenderHint(QtGui.QPainter.Antialiasing, True)
            single_chart_layout.addWidget(chart_view, 1)
            single_detail_lbl = QtWidgets.QLabel("Haz clic en una barra o en la linea para ver el detalle.")
            single_detail_lbl.setWordWrap(True)
            single_detail_lbl.setObjectName("chartSelectionInfo")
            single_chart_layout.addWidget(single_detail_lbl)
            chart_stack.addWidget(single_chart_widget)

            all_charts_widget = QtWidgets.QWidget()
            all_grid = QtWidgets.QGridLayout(all_charts_widget)
            all_grid.setContentsMargins(0, 0, 0, 0)
            all_grid.setSpacing(8)
            all_chart_views = {}
            all_chart_detail_labels = {}
            for idx, (label, key) in enumerate(self._kpi_metric_items()):
                if key == "all":
                    continue
                chart_box = QtWidgets.QWidget()
                chart_box_layout = QtWidgets.QVBoxLayout(chart_box)
                chart_box_layout.setContentsMargins(0, 0, 0, 0)
                chart_box_layout.setSpacing(4)
                view = QtCharts.QChartView()
                view.setRenderHint(QtGui.QPainter.Antialiasing, True)
                detail_lbl = QtWidgets.QLabel("Haz clic para ver el detalle.")
                detail_lbl.setWordWrap(True)
                detail_lbl.setObjectName("chartSelectionInfo")
                chart_box_layout.addWidget(view, 1)
                chart_box_layout.addWidget(detail_lbl)
                all_grid.addWidget(chart_box, (idx - 1) // 2, (idx - 1) % 2)
                all_chart_views[key] = view
                all_chart_detail_labels[key] = detail_lbl
            chart_stack.addWidget(all_charts_widget)
            layout.addWidget(chart_stack, 1)

            close_btn = QtWidgets.QPushButton("Cerrar")
            close_btn.clicked.connect(dlg.close)
            layout.addWidget(close_btn, 0, QtCore.Qt.AlignRight)

            self._kpi_history_dialog = dlg
            self._kpi_hist_metric_cb = metric_cb
            self._kpi_hist_granularity_cb = granularity_cb
            self._kpi_hist_chart_view = chart_view
            self._kpi_hist_chart_stack = chart_stack
            self._kpi_hist_all_chart_views = all_chart_views
            self._kpi_hist_single_detail_lbl = single_detail_lbl
            self._kpi_hist_all_detail_labels = all_chart_detail_labels
            self._kpi_hist_info_lbl = info_lbl
            self._kpi_hist_year_selector_widget = year_selector_widget
            self._kpi_hist_year_selector_layout = year_selector_layout
            self._kpi_hist_year_buttons: list[QtWidgets.QPushButton] = []
            self._kpi_hist_selected_year: str | None = None

            metric_cb.currentIndexChanged.connect(self._refresh_kpi_history_chart)
            granularity_cb.currentIndexChanged.connect(self._refresh_kpi_history_chart)

        metric_idx = self._kpi_hist_metric_cb.findData(initial_key)
        if metric_idx >= 0:
            self._kpi_hist_metric_cb.setCurrentIndex(metric_idx)
        self._refresh_kpi_history_chart()
        if self._kpi_hist_metric_cb.currentData() == "all":
            self._maximize_kpi_history_dialog()
        else:
            self._restore_kpi_history_dialog()
        self._kpi_history_dialog.raise_()
        self._kpi_history_dialog.activateWindow()

    def _maximize_kpi_history_dialog(self):
        if self._kpi_history_dialog is None:
            return
        self._kpi_history_dialog.show()
        self._kpi_history_dialog.setWindowState(
            self._kpi_history_dialog.windowState() | QtCore.Qt.WindowMaximized
        )

    def _restore_kpi_history_dialog(self):
        if self._kpi_history_dialog is None:
            return
        self._kpi_history_dialog.show()
        self._kpi_history_dialog.setWindowState(
            self._kpi_history_dialog.windowState() & ~QtCore.Qt.WindowMaximized
        )
        self._kpi_history_dialog.resize(980, 620)

    def _build_kpi_chart(self, labels, values, metric_key: str, metric_label: str):
        chart = QtCharts.QChart()
        chart.setAnimationOptions(QtCharts.QChart.SeriesAnimations)
        chart.legend().setVisible(True)
        chart.setTitle(f"{metric_label} vs tiempo")

        bar_set = QtCharts.QBarSet(metric_label)
        for value in values:
            bar_set.append(float(value))
        bar_series = QtCharts.QBarSeries()
        bar_series.append(bar_set)
        chart.addSeries(bar_series)

        line_series = QtCharts.QLineSeries()
        line_series.setName("Tendencia")
        pen = QtGui.QPen(TREND_COLORS["up"])
        pen.setWidth(2)
        line_series.setPen(pen)
        for idx, value in enumerate(values):
            line_series.append(idx, float(value))
        chart.addSeries(line_series)

        axis_x = QtCharts.QBarCategoryAxis()
        axis_x.append(labels)
        chart.addAxis(axis_x, QtCore.Qt.AlignBottom)
        bar_series.attachAxis(axis_x)
        line_series.attachAxis(axis_x)

        axis_y = QtCharts.QValueAxis()
        axis_y.setLabelFormat("%.0f")
        max_val = max(values) if values else 0.0
        axis_y.setRange(0, max_val * 1.15 if max_val > 0 else 1)
        if metric_key in {"importe", "ticket_promedio"}:
            axis_y.setTitleText("Gs")
        elif metric_key == "facturas":
            axis_y.setTitleText("Facturas")
        else:
            axis_y.setTitleText("Paquetes")
        chart.addAxis(axis_y, QtCore.Qt.AlignLeft)
        bar_series.attachAxis(axis_y)
        line_series.attachAxis(axis_y)
        return chart, bar_set, line_series

    def _format_kpi_value(self, metric_key: str, value: float) -> str:
        if metric_key in {"importe", "ticket_promedio"}:
            return f"{self._fmt_gs(value)} Gs"
        if metric_key == "facturas":
            return f"{int(round(value)):,} facturas".replace(",", ".")
        return f"{int(round(value)):,} paquetes".replace(",", ".")

    def _bind_chart_selection(self, bar_set, line_series, labels, values, metric_key: str, metric_label: str, target_label: QtWidgets.QLabel):
        def _show_index(index: int):
            if 0 <= index < len(labels):
                period = labels[index]
                value = values[index]
                target_label.setText(
                    f"Seleccionado: {period} | {metric_label}: {self._format_kpi_value(metric_key, value)}"
                )

        def _show_point(point):
            idx = int(round(point.x()))
            _show_index(idx)

        bar_set.clicked.connect(_show_index)
        line_series.clicked.connect(_show_point)

    def _set_kpi_history_year(self, year: str):
        self._kpi_hist_selected_year = year
        for btn in getattr(self, "_kpi_hist_year_buttons", []):
            btn.blockSignals(True)
            btn.setChecked(btn.property("year_value") == year)
            btn.blockSignals(False)
        self._refresh_kpi_history_chart()

    def _refresh_kpi_history_year_buttons(self, rows):
        years = sorted({str(r.get("periodo", "")).split("-")[0] for r in rows if r.get("periodo")}, reverse=True)
        while self._kpi_hist_year_selector_layout.count():
            item = self._kpi_hist_year_selector_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        self._kpi_hist_year_buttons = []
        if not years:
            self._kpi_hist_selected_year = None
            self._kpi_hist_year_selector_widget.hide()
            return
        self._kpi_hist_year_selector_layout.addWidget(QtWidgets.QLabel("Años:"))
        if self._kpi_hist_selected_year not in years:
            self._kpi_hist_selected_year = years[0]
        for year in years:
            btn = QtWidgets.QPushButton(year)
            btn.setCheckable(True)
            btn.setProperty("yearChip", True)
            btn.setProperty("year_value", year)
            btn.setChecked(year == self._kpi_hist_selected_year)
            btn.clicked.connect(lambda _checked=False, y=year: self._set_kpi_history_year(y))
            self._kpi_hist_year_selector_layout.addWidget(btn)
            self._kpi_hist_year_buttons.append(btn)
        self._kpi_hist_year_selector_layout.addStretch(1)
        self._kpi_hist_year_selector_widget.show()

    def _refresh_kpi_history_chart(self, *_args):
        if self._kpi_history_dialog is None:
            return
        metric_key = self._kpi_hist_metric_cb.currentData()
        metric_label = self._kpi_hist_metric_cb.currentText()
        granularity = self._kpi_hist_granularity_cb.currentData()
        pid, gram, _ym, _d1, _d2 = self._get_filters(self.filters_mes)
        rows = self.repo.kpi_history_series(granularity=granularity, product_id=pid, gramaje=gram)
        if not rows:
            chart = QtCharts.QChart()
            chart.setTitle(f"{metric_label} vs tiempo")
            self._kpi_hist_chart_view.setChart(chart)
            self._kpi_hist_year_selector_widget.hide()
            self._kpi_hist_single_detail_lbl.setText("Haz clic en una barra o en la linea para ver el detalle.")
            for key, view in self._kpi_hist_all_chart_views.items():
                empty_chart = QtCharts.QChart()
                label = next((lbl for lbl, k in self._kpi_metric_items() if k == key), key)
                empty_chart.setTitle(f"{label} vs tiempo")
                view.setChart(empty_chart)
                self._kpi_hist_all_detail_labels[key].setText("Haz clic para ver el detalle.")
            self._kpi_hist_info_lbl.setText("No hay datos historicos para los filtros actuales.")
            return
        if granularity == "years":
            self._refresh_kpi_history_year_buttons(rows)
            selected_year = self._kpi_hist_selected_year
            rows = [r for r in rows if str(r.get("periodo", "")).startswith(f"{selected_year}-")]
        else:
            self._kpi_hist_year_selector_widget.hide()
        labels = [str(r.get("periodo", "")) for r in rows]
        if metric_key == "all":
            self._kpi_hist_chart_stack.setCurrentIndex(1)
            self._maximize_kpi_history_dialog()
            for label, key in self._kpi_metric_items():
                if key == "all":
                    continue
                values = [float(r.get(key, 0.0) or 0.0) for r in rows]
                chart, bar_set, line_series = self._build_kpi_chart(labels, values, key, label)
                self._kpi_hist_all_chart_views[key].setChart(chart)
                self._kpi_hist_all_detail_labels[key].setText("Haz clic para ver el detalle.")
                self._bind_chart_selection(
                    bar_set, line_series, labels, values, key, label, self._kpi_hist_all_detail_labels[key]
                )
        else:
            self._kpi_hist_chart_stack.setCurrentIndex(0)
            self._restore_kpi_history_dialog()
            values = [float(r.get(metric_key, 0.0) or 0.0) for r in rows]
            chart, bar_set, line_series = self._build_kpi_chart(labels, values, metric_key, metric_label)
            self._kpi_hist_chart_view.setChart(chart)
            self._kpi_hist_single_detail_lbl.setText("Haz clic en una barra o en la linea para ver el detalle.")
            self._bind_chart_selection(
                bar_set, line_series, labels, values, metric_key, metric_label, self._kpi_hist_single_detail_lbl
            )
        product_txt = self.filters_mes.prod_cb.currentText().strip() or "Todos"
        gram_txt = self.filters_mes.gram_cb.currentText().strip() or "Todos"
        alcance = "mensual desde el primer registro" if granularity == "month" else f"por años, mostrando meses de {self._kpi_hist_selected_year}"
        self._kpi_hist_info_lbl.setText(
            f"Filtro actual: producto={product_txt}, gramaje={gram_txt}. Vista {alcance}."
        )

    def _ranked_products_from_rows(self, rows):
        product_totals: dict[tuple[str, int], dict[str, float]] = {}
        for r in rows:
            prod_name = str(r.get("producto", "") or "").strip() or "Sin nombre"
            gramaje = int(r.get("gramaje", 0) or 0)
            stats = product_totals.setdefault((prod_name, gramaje), {"importe": 0.0, "paquetes": 0})
            stats["importe"] += float(r.get("importe_gs", 0.0) or 0.0)
            stats["paquetes"] += int(r.get("paquetes", 0) or 0)
        ranked = sorted(
            product_totals.items(),
            key=lambda item: (item[1].get("importe", 0.0), item[1].get("paquetes", 0)),
            reverse=True,
        )[:3]
        result = []
        for (prod_name, gramaje), stats in ranked:
            label = f"{prod_name} {gramaje} g" if gramaje > 0 else prod_name
            result.append({
                "producto": label,
                "importe_gs": stats.get("importe", 0.0),
                "paquetes": stats.get("paquetes", 0),
            })
        return result

    def _ranked_branches_from_rows(self, rows):
        return sorted(
            rows,
            key=lambda item: (float(item.get("importe_gs", 0.0) or 0.0), int(item.get("paquetes", 0) or 0)),
            reverse=True,
        )[:3]

    def _ranking_arrow_html(self, current_pos: int, previous_pos: Optional[int]):
        if previous_pos is None:
            color = TREND_COLORS["up"].name()
            return f"<span style='color:{color}; font-weight:700;'>▲N</span>"
        if current_pos < previous_pos:
            color = TREND_COLORS["up"].name()
            diff = previous_pos - current_pos
            return f"<span style='color:{color}; font-weight:700;'>▲{diff}</span>"
        if current_pos > previous_pos:
            color = TREND_COLORS["down"].name()
            diff = current_pos - previous_pos
            return f"<span style='color:{color}; font-weight:700;'>▼{diff}</span>"
        color = TREND_COLORS["flat"].name()
        return f"<span style='color:{color}; font-weight:700;'>▶0</span>"

    def _build_ranking_html(self, rows, previous_positions, name_key: str):
        if not rows:
            return "Sin datos para el filtro actual."
        lines = []
        for idx, item in enumerate(rows, start=1):
            name = str(item.get(name_key, "Sin nombre"))
            importe = float(item.get("importe_gs", 0.0) or 0.0)
            paquetes = int(item.get("paquetes", 0) or 0)
            arrow = self._ranking_arrow_html(idx, previous_positions.get(name))
            lines.append(
                f"{idx}. {arrow} {name}: <b>{self._fmt_gs(importe)} Gs</b> | {paquetes:,} paq".replace(",", ".")
            )
        return "<br>".join(lines)

    def _populate_filters(self, filters: Filters):
        nombres = [r[1] for r in self.repo.list_products()]
        filters.prod_cb.clear()
        filters.prod_cb.addItem("Todos")
        for n in nombres:
            filters.prod_cb.addItem(n)
        filters.prod_cb.currentTextChanged.connect(lambda _=None, f=filters: self._on_product_change(f))

        filters.gram_cb.clear()
        filters.gram_cb.addItem("Todos")

        filters.mes_cb.clear()
        filters.mes_cb.addItem("")
        for ym in self.repo.list_available_year_months():
            filters.mes_cb.addItem(ym)

    def _on_product_change(self, filters: Filters):
        name = filters.prod_cb.currentText().strip()
        filters.gram_cb.blockSignals(True)
        filters.gram_cb.clear()
        filters.gram_cb.addItem("Todos")
        if name and name != "Todos":
            pid = self.repo.get_product_id_by_name(name)
            grams = self.repo.list_gramajes_for_product(pid) or []
            for g in grams:
                filters.gram_cb.addItem(str(g))
        filters.gram_cb.blockSignals(False)

    # --------- llenadores ---------
    def _get_filters(self, filters: Filters):
        prod = filters.prod_cb.currentText().strip()
        pid = None if (not prod or prod == "Todos") else self.repo.get_product_id_by_name(prod)
        gram_txt = filters.gram_cb.currentText().strip()
        gram = None
        try:
            if gram_txt and gram_txt != "Todos":
                gram = int(gram_txt)
        except Exception:
            gram = None
        ym = filters.mes_cb.currentText().strip() or None
        d1 = filters.desde_edit.text().strip() or None
        d2 = filters.hasta_edit.text().strip() or None
        return pid, gram, ym, d1, d2

    def _fmt_gs(self, x):
        try:
            return f"{float(x):,.0f}".replace(",", ".")
        except Exception:
            return "0"

    def _fmt_int(self, x):
        try:
            return f"{int(round(float(x))):,}".replace(",", ".")
        except Exception:
            return "0"

    def _fmt_fecha(self, ts):
        try:
            dt = datetime.fromisoformat(str(ts))
            return dt.strftime('%Y-%m-%d %H:%M')
        except Exception:
            return str(ts or '')

    def _format_month_label(self, periodo: Optional[str]):
        try:
            periodo = periodo or ""
            year, month = periodo.split("-")
            idx = int(month) - 1
            if 0 <= idx < len(MONTH_NAMES):
                return f"{MONTH_NAMES[idx].capitalize()} {year}"
        except Exception:
            pass
        return periodo or ""

    def _format_trend_pct(self, prev_val: Optional[float], current_val: float):
        if prev_val is None:
            return "(s/d)"
        if prev_val == 0:
            # Si el periodo anterior fue 0 y ahora hay valor, muestra 100%
            return "(+100%)" if current_val > 0 else "(0%)"
        delta = (current_val - prev_val) / prev_val * 100.0
        return f"({delta:+.0f}%)"

    def _ensure_trend_icons(self):
        """
        Genera QPixmaps simples para flechas up/down/flat y los cachea.
        """
        if hasattr(self, "_trend_icons"):
            return self._trend_icons
        icons = {}
        size = 14
        for name, color in TREND_COLORS.items():
            pix = QtGui.QPixmap(size, size)
            pix.fill(QtCore.Qt.transparent)
            painter = QtGui.QPainter(pix)
            painter.setRenderHint(QtGui.QPainter.Antialiasing, True)
            painter.setBrush(QtGui.QBrush(color))
            painter.setPen(QtGui.QPen(color))
            if name == "up":
                points = [QtCore.QPointF(size/2, 2), QtCore.QPointF(2, size-3), QtCore.QPointF(size-2, size-3)]
            elif name == "down":
                points = [QtCore.QPointF(2, 3), QtCore.QPointF(size-2, 3), QtCore.QPointF(size/2, size-2)]
            else:  # flat
                painter.drawRect(2, size/2 - 2, size-4, 4)
                points = None
            if points:
                painter.drawPolygon(QtGui.QPolygonF(points))
            painter.end()
            icons[name] = QtGui.QIcon(pix)
        self._trend_icons = icons
        return icons

    def _fill_resumen(self, periodo: str):
        filters = self.filters_mes if periodo == "mes" else self.filters_sem
        pid, gram, ym, d1, d2 = self._get_filters(filters)
        rows = self.repo.resumen_ventas(periodo=periodo, product_id=pid, gramaje=gram, ym=ym, desde=d1, hasta=d2)

        total_paq = total_luque = total_aregua = total_itaugua = 0
        total_importe = total_base = total_iva = 0.0

        if periodo == "mes":
            fact_rows = self.repo.facturas_por_periodo(product_id=pid, gramaje=gram, ym=ym, desde=d1, hasta=d2)
            headers = [
                "Periodo", "Producto", "g", "Paquetes",
                "Importe (Gs)", "Base (Gs)", "IVA (Gs)",
                "Luque", "Aregua", "Itaugua",
                "Share %",
            ]
            model = QtGui.QStandardItemModel(0, len(headers), self)
            model.setHorizontalHeaderLabels(headers)
            trend_icons = self._ensure_trend_icons()
            branch_cols = [("Luque", "paq_luque"), ("Aregua", "paq_aregua"), ("Itaugua", "paq_itaugua")]

            month_totals = {}
            month_paquetes = {}
            month_base = {}
            month_iva = {}
            month_luque = {}
            month_aregua = {}
            month_itaugua = {}
            month_facturas = {str(r.get("periodo", "")): int(r.get("facturas", 0) or 0) for r in fact_rows}
            for r in rows:
                imp_val = float(r.get("importe_gs", 0.0) or 0.0)
                month_key = r.get("periodo", "")
                month_totals[month_key] = month_totals.get(month_key, 0.0) + imp_val
                month_paquetes[month_key] = month_paquetes.get(month_key, 0) + int(r.get("paquetes", 0) or 0)
                month_base[month_key] = month_base.get(month_key, 0.0) + float(r.get("base_gs", 0.0) or 0.0)
                month_iva[month_key] = month_iva.get(month_key, 0.0) + float(r.get("iva_gs", 0.0) or 0.0)
                month_luque[month_key] = month_luque.get(month_key, 0) + int(r.get("paq_luque", 0) or 0)
                month_aregua[month_key] = month_aregua.get(month_key, 0) + int(r.get("paq_aregua", 0) or 0)
                month_itaugua[month_key] = month_itaugua.get(month_key, 0) + int(r.get("paq_itaugua", 0) or 0)

            # Tendencias por mes (comparando con el mes anterior cronológicamente)
            month_trend_map: dict[str, tuple[str, str]] = {}
            month_fact_trend_map: dict[str, tuple[str, str]] = {}
            month_ticket_trend_map: dict[str, tuple[str, str]] = {}
            prev_total = None
            prev_facturas = None
            prev_ticket = None
            for pk in sorted(month_totals.keys()):  # ascendente: viejo -> nuevo
                curr = month_totals.get(pk, 0.0)
                if prev_total is None:
                    trend = "flat"
                elif curr > prev_total:
                    trend = "up"
                elif curr < prev_total:
                    trend = "down"
                else:
                    trend = "flat"
                pct_label = self._format_trend_pct(prev_total, curr)
                month_trend_map[pk] = (trend, pct_label)
                prev_total = curr

                curr_facturas = float(month_facturas.get(pk, 0) or 0)
                if prev_facturas is None:
                    fact_trend = "flat"
                elif curr_facturas > prev_facturas:
                    fact_trend = "up"
                elif curr_facturas < prev_facturas:
                    fact_trend = "down"
                else:
                    fact_trend = "flat"
                month_fact_trend_map[pk] = (fact_trend, self._format_trend_pct(prev_facturas, curr_facturas))
                prev_facturas = curr_facturas

                curr_ticket = curr / curr_facturas if curr_facturas > 0 else 0.0
                if prev_ticket is None:
                    ticket_trend = "flat"
                elif curr_ticket > prev_ticket:
                    ticket_trend = "up"
                elif curr_ticket < prev_ticket:
                    ticket_trend = "down"
                else:
                    ticket_trend = "flat"
                month_ticket_trend_map[pk] = (ticket_trend, self._format_trend_pct(prev_ticket, curr_ticket))
                prev_ticket = curr_ticket

            # Tendencias por producto/gramaje comparando contra el mes anterior
            prod_trend_map: dict[tuple[str, str, int], tuple[str, str]] = {}
            prev_paq_by_key: dict[tuple[str, int], int] = {}
            rows_sorted = sorted(
                rows,
                key=lambda r: (
                    r.get("periodo", ""),  # ascendente (YYYY-MM)
                    r.get("producto", "") or "",
                    int(r.get("gramaje", 0) or 0),
                ),
            )
            for r in rows_sorted:  # viejo -> nuevo
                key_prod = (r.get("producto", ""), int(r.get("gramaje", 0) or 0))
                paq_curr = int(r.get("paquetes", 0) or 0)
                prev_paq = prev_paq_by_key.get(key_prod)
                if prev_paq is None:
                    trend = "flat"
                elif paq_curr > prev_paq:
                    trend = "up"
                elif paq_curr < prev_paq:
                    trend = "down"
                else:
                    trend = "flat"
                pct_label = self._format_trend_pct(prev_paq, paq_curr)
                prod_trend_map[(r.get("periodo", ""), key_prod[0], key_prod[1])] = (trend, pct_label)
                prev_paq_by_key[key_prod] = paq_curr

            # Tendencias por sucursal (por producto/gramaje)
            branch_trend_map: dict[tuple[str, str, int, str], tuple[str, str]] = {}
            prev_branch_by_key: dict[tuple[str, int, str], int] = {}
            for r in rows_sorted:  # viejo -> nuevo
                base_key = (r.get("producto", ""), int(r.get("gramaje", 0) or 0))
                for branch_name, field in branch_cols:
                    curr_val = int(r.get(field, 0) or 0)
                    prev_val = prev_branch_by_key.get((*base_key, branch_name))
                    if prev_val is None:
                        trend = "flat"
                    elif curr_val > prev_val:
                        trend = "up"
                    elif curr_val < prev_val:
                        trend = "down"
                    else:
                        trend = "flat"
                    pct_label = self._format_trend_pct(prev_val, curr_val)
                    branch_trend_map[(r.get("periodo", ""), base_key[0], base_key[1], branch_name)] = (trend, pct_label)
                    prev_branch_by_key[(*base_key, branch_name)] = curr_val

            current_month = None
            month_parent = None

            for r in rows:
                paq = int(r.get("paquetes", 0) or 0)
                paq_luque = int(r.get("paq_luque", 0) or 0)
                paq_aregua = int(r.get("paq_aregua", 0) or 0)
                paq_itaugua = int(r.get("paq_itaugua", 0) or 0)
                imp = float(r.get("importe_gs", 0.0) or 0.0)
                base = float(r.get("base_gs", 0.0) or 0.0)
                iva = float(r.get("iva_gs", 0.0) or 0.0)

                total_paq += paq
                total_luque += paq_luque
                total_aregua += paq_aregua
                total_itaugua += paq_itaugua
                total_importe += imp
                total_base += base
                total_iva += iva

                period_txt = r.get("periodo", "")
                if period_txt != current_month:
                    month_total = month_totals.get(period_txt, 0.0)
                    month_trend, pct_month = month_trend_map.get(period_txt, ("flat", "(s/d)"))
                    importe_label = f"{self._fmt_gs(month_total)} {pct_month}".strip()
                    month_label = self._format_month_label(period_txt)

                    items = []
                    for idx, val in enumerate((period_txt, month_label, "", "", importe_label, "", "")):
                        it = QtGui.QStandardItem(str(val))
                        if idx in (3, 4, 5, 6):
                            it.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                        items.append(it)
                    items[1].setIcon(trend_icons.get(month_trend))
                    share_placeholder = QtGui.QStandardItem("")
                    share_placeholder.setData(True, PercentBarDelegate.NO_BAR_ROLE)
                    items.append(share_placeholder)
                    color = TREND_COLORS.get(month_trend, TREND_COLORS["flat"])
                    for it in items:
                        it.setForeground(QtGui.QBrush(color))
                        font = it.font(); font.setBold(True); it.setFont(font)
                    month_parent = QtGui.QStandardItem()
                    items[0] = month_parent
                    items[0].setText(period_txt)
                    items[1].setText(month_label or period_txt)
                    month_meta = {
                        "type": "month",
                        "periodo": str(period_txt),
                        "month_label": str(month_label or period_txt),
                        "importe_total": month_total,
                        "paquetes_total": int(month_paquetes.get(period_txt, 0) or 0),
                        "share_pct": 100.0,
                    }
                    for item in items:
                        item.setData(month_meta, PRODUCT_ANALYSIS_ROLE)
                    model.appendRow(items)
                    current_month = period_txt

                key = (r.get("producto", ""), int(r.get("gramaje", 0) or 0))
                trend, pct_label = prod_trend_map.get((period_txt, key[0], key[1]), ("flat", "(s/d)"))
                product_label = f"{r.get('producto', '')} {pct_label}".strip()

                row_items = [
                    QtGui.QStandardItem(str(r.get("periodo", ""))),
                    QtGui.QStandardItem(product_label),
                    QtGui.QStandardItem(str(r.get("gramaje", 0))),
                    QtGui.QStandardItem(str(paq)),
                    QtGui.QStandardItem(self._fmt_gs(imp)),
                    QtGui.QStandardItem(self._fmt_gs(base)),
                    QtGui.QStandardItem(self._fmt_gs(iva)),
                    QtGui.QStandardItem(str(paq_luque)),
                    QtGui.QStandardItem(str(paq_aregua)),
                    QtGui.QStandardItem(str(paq_itaugua)),
                    QtGui.QStandardItem(""),
                ]
                for idx in (3, 4, 5, 6, 7, 8, 9):
                    row_items[idx].setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                color = TREND_COLORS.get(trend, TREND_COLORS["flat"])
                # Icono de tendencia por producto para lectura rápida
                row_items[1].setIcon(trend_icons.get(trend))
                for it in row_items:
                    it.setForeground(QtGui.QBrush(color))
                # share vs total del mes
                share_pct = 0.0
                if month_totals.get(period_txt, 0.0) > 0:
                    share_pct = imp / month_totals.get(period_txt, 0.0) * 100.0
                row_items[-1].setData(f"{share_pct:.0f}%", QtCore.Qt.DisplayRole)
                row_items[-1].setData(share_pct, QtCore.Qt.UserRole)
                row_items[-1].setTextAlignment(QtCore.Qt.AlignCenter)
                row_meta = {
                    "type": "product",
                    "periodo": str(period_txt),
                    "producto": str(r.get("producto", "") or ""),
                    "gramaje": int(r.get("gramaje", 0) or 0),
                    "product_id": self.repo.get_product_id_by_name(str(r.get("producto", "") or "")),
                    "share_pct": share_pct,
                }
                for item in row_items:
                    item.setData(row_meta, PRODUCT_ANALYSIS_ROLE)
                # Colorear y etiquetar sucursales de forma independiente
                for idx_col, (branch_name, field) in zip((7, 8, 9), branch_cols):
                    val = int(r.get(field, 0) or 0)
                    b_trend, b_pct = branch_trend_map.get((period_txt, key[0], key[1], branch_name), ("flat", "(s/d)"))
                    row_items[idx_col].setText(f"{val} {b_pct}")
                    row_items[idx_col].setIcon(trend_icons.get(b_trend))
                    row_items[idx_col].setForeground(QtGui.QBrush(TREND_COLORS.get(b_trend, TREND_COLORS["flat"])))
                if month_parent:
                    month_parent.appendRow(row_items)
                else:
                    model.appendRow(row_items)

            total_items = [
                QtGui.QStandardItem("TOTAL"),
                QtGui.QStandardItem(""),
                QtGui.QStandardItem(""),
                QtGui.QStandardItem(str(total_paq)),
                QtGui.QStandardItem(self._fmt_gs(total_importe)),
                QtGui.QStandardItem(self._fmt_gs(total_base)),
                QtGui.QStandardItem(self._fmt_gs(total_iva)),
                QtGui.QStandardItem(str(total_luque)),
                QtGui.QStandardItem(str(total_aregua)),
                QtGui.QStandardItem(str(total_itaugua)),
                QtGui.QStandardItem(""),
            ]
            for idx in (3, 4, 5, 6, 7, 8, 9):
                total_items[idx].setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
            total_items[-1].setTextAlignment(QtCore.Qt.AlignCenter)
            for it in total_items:
                font = it.font(); font.setBold(True); it.setFont(font)
                it.setForeground(QtGui.QBrush(QtGui.QColor("#12326b")))
            # marcador para que el delegate no dibuje barra en TOTAL
            total_items[-1].setData(True, PercentBarDelegate.NO_BAR_ROLE)
            model.appendRow(total_items)

            self.view_mes.setModel(model)
            header = self.view_mes.header()
            header.setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
            header.setStretchLastSection(True)
            self.view_mes.expandAll()
            # delegate para barra de participación
            share_idx = headers.index("Share %")
            self.view_mes.setItemDelegateForColumn(share_idx, self.share_delegate)
            self._populate_month_chart_combo(model)

            # KPIs muestran el mes más reciente disponible
            latest_month = max(month_totals.keys()) if month_totals else None
            trend_badge, trend_pct = month_trend_map.get(latest_month, ("flat", "(s/d)"))
            def _set_kpi(key: str, val: str, trend_key: str, pct: str):
                widgets = self._kpi_labels.get(key, {})
                if widgets:
                    val_lbl = widgets.get("value")
                    badge = widgets.get("badge")
                    delta_lbl = widgets.get("delta")
                    if val_lbl:
                        val_lbl.setText(val)
                    arrow = {"up": "▲", "down": "▼", "flat": "→"}.get(trend_key, "→")
                    color = TREND_COLORS.get(trend_key, TREND_COLORS["flat"])
                    if badge:
                        badge.setText(arrow)
                        badge.setStyleSheet(f"color: {color.name()}; font-size: 12px; font-weight: 700;")
                    if delta_lbl:
                        delta_lbl.setText(pct)
                        delta_lbl.setStyleSheet(f"color: {color.name()}; font-size: 11px;")

            if latest_month:
                _set_kpi("paquetes", f"{int(month_paquetes.get(latest_month, 0)):,}".replace(",", "."), trend_badge, trend_pct)
                _set_kpi("importe", self._fmt_gs(month_totals.get(latest_month, 0.0)), trend_badge, trend_pct)
                fact_trend_badge, fact_trend_pct = month_fact_trend_map.get(latest_month, ("flat", "(s/d)"))
                ticket_trend_badge, ticket_trend_pct = month_ticket_trend_map.get(latest_month, ("flat", "(s/d)"))
                latest_facturas = int(month_facturas.get(latest_month, 0) or 0)
                latest_ticket = month_totals.get(latest_month, 0.0) / latest_facturas if latest_facturas > 0 else 0.0
                _set_kpi("facturas", f"{latest_facturas:,}".replace(",", "."), fact_trend_badge, fact_trend_pct)
                _set_kpi("ticket_promedio", self._fmt_gs(latest_ticket), ticket_trend_badge, ticket_trend_pct)
                if "base" in self._kpi_labels:
                    _set_kpi("base", self._fmt_gs(month_base.get(latest_month, 0.0)), trend_badge, trend_pct)
                if "iva" in self._kpi_labels:
                    _set_kpi("iva", self._fmt_gs(month_iva.get(latest_month, 0.0)), trend_badge, trend_pct)
                self.kpi_period.setText(self._format_month_label(latest_month))
            elif ym:
                self.kpi_period.setText(self._format_month_label(ym))
            elif d1 or d2:
                self.kpi_period.setText("Rango personalizado")
            else:
                self.kpi_period.setText("Todos los meses")

            rank_pid, rank_gram, rank_ym, rank_d1, rank_d2 = self._ranking_params(pid, gram)
            rank_rows = self.repo.resumen_ventas(periodo="mes", product_id=rank_pid, gramaje=rank_gram, ym=rank_ym, desde=rank_d1, hasta=rank_d2)
            rank_branch_rows = self.repo.resumen_sucursales(product_id=rank_pid, gramaje=rank_gram, ym=rank_ym, desde=rank_d1, hasta=rank_d2)
            top_productos = self._ranked_products_from_rows(rank_rows)
            top_sucursales = self._ranked_branches_from_rows(rank_branch_rows)

            prev_product_positions = {}
            prev_branch_positions = {}
            if self.rank_scope_cb.currentData() == "month":
                prev_ym = self._prev_month(rank_ym or "")
                if prev_ym:
                    prev_prod_rows = self.repo.resumen_ventas(periodo="mes", product_id=rank_pid, gramaje=rank_gram, ym=prev_ym)
                    prev_branch_rows = self.repo.resumen_sucursales(product_id=rank_pid, gramaje=rank_gram, ym=prev_ym)
                    prev_product_positions = {
                        str(item.get("producto", "Sin nombre")): idx
                        for idx, item in enumerate(self._ranked_products_from_rows(prev_prod_rows), start=1)
                    }
                    prev_branch_positions = {
                        str(item.get("sucursal", "Sin nombre")): idx
                        for idx, item in enumerate(self._ranked_branches_from_rows(prev_branch_rows), start=1)
                    }

            self.lbl_top_productos.setTextFormat(QtCore.Qt.RichText)
            self.lbl_top_sucursales.setTextFormat(QtCore.Qt.RichText)
            self.lbl_top_productos.setText(self._build_ranking_html(top_productos, prev_product_positions, "producto"))
            self.lbl_top_sucursales.setText(self._build_ranking_html(top_sucursales, prev_branch_positions, "sucursal"))

            self.status_label.setText(f"{len(rows)} filas - {self._fmt_gs(total_importe)} Gs")

        else:
            headers = [
                "Semana", "Producto", "g", "Paquetes",
                "Importe (Gs)", "Base (Gs)", "IVA (Gs)",
                "Luque", "Aregua", "Itaugua",
            ]
            model = QtGui.QStandardItemModel(0, len(headers), self)
            model.setHorizontalHeaderLabels(headers)

            for r in rows:
                paq = int(r.get("paquetes", 0) or 0)
                paq_luque = int(r.get("paq_luque", 0) or 0)
                paq_aregua = int(r.get("paq_aregua", 0) or 0)
                paq_itaugua = int(r.get("paq_itaugua", 0) or 0)
                imp = float(r.get("importe_gs", 0.0) or 0.0)
                base = float(r.get("base_gs", 0.0) or 0.0)
                iva = float(r.get("iva_gs", 0.0) or 0.0)

                total_paq += paq
                total_luque += paq_luque
                total_aregua += paq_aregua
                total_itaugua += paq_itaugua
                total_importe += imp
                total_base += base
                total_iva += iva

                row_items = [
                    QtGui.QStandardItem(str(r.get("periodo", ""))),
                    QtGui.QStandardItem(str(r.get("producto", ""))),
                    QtGui.QStandardItem(str(r.get("gramaje", 0))),
                    QtGui.QStandardItem(str(paq)),
                    QtGui.QStandardItem(self._fmt_gs(imp)),
                    QtGui.QStandardItem(self._fmt_gs(base)),
                    QtGui.QStandardItem(self._fmt_gs(iva)),
                    QtGui.QStandardItem(str(paq_luque)),
                    QtGui.QStandardItem(str(paq_aregua)),
                    QtGui.QStandardItem(str(paq_itaugua)),
                ]
                for idx in (3, 4, 5, 6, 7, 8, 9):
                    row_items[idx].setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                model.appendRow(row_items)

            self.view_sem.setModel(model)
            header = self.view_sem.horizontalHeader()
            header.setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
            header.setStretchLastSection(True)
            self.status_label.setText(f"{len(rows)} filas - {self._fmt_gs(total_importe)} Gs")

    def _populate_month_chart_combo(self, model: QtGui.QStandardItemModel):
        if not hasattr(self, "month_chart_cb"):
            return
        current_period = self.month_chart_cb.currentData()
        self.month_chart_cb.blockSignals(True)
        self.month_chart_cb.clear()
        for row in range(model.rowCount()):
            meta = model.index(row, 0).data(PRODUCT_ANALYSIS_ROLE)
            if not isinstance(meta, dict) or meta.get("type") != "month":
                continue
            self.month_chart_cb.addItem(str(meta.get("month_label", meta.get("periodo", ""))), str(meta.get("periodo", "")))
        if self.month_chart_cb.count() > 0:
            idx = self.month_chart_cb.findData(current_period)
            self.month_chart_cb.setCurrentIndex(idx if idx >= 0 else 0)
        self.month_chart_cb.blockSignals(False)

    def _fill_detalle(self):
        pid, gram, ym, d1, d2 = self._get_filters(self.filters_det)
        rows = self.repo.detalle_por_factura(product_id=pid, gramaje=gram, ym=ym, desde=d1, hasta=d2)

        headers = ["Fecha", "Nro Factura", "Cliente", "Producto", "g", "Paquetes", "Precio (Gs)", "Importe (Gs)", "FacturaID"]
        model = QtGui.QStandardItemModel(0, len(headers), self)
        model.setHorizontalHeaderLabels(headers)

        total_importe = 0.0
        for r in rows:
            total_importe += float(r.get("importe", 0.0) or 0.0)
            items = [
                QtGui.QStandardItem(self._fmt_fecha(r.get("fecha"))),
                QtGui.QStandardItem(str(r.get("nro_factura", ""))),
                QtGui.QStandardItem(str(r.get("cliente", ""))),
                QtGui.QStandardItem(str(r.get("producto", ""))),
                QtGui.QStandardItem(str(r.get("gramaje", 0))),
                QtGui.QStandardItem(str(int(r.get("paquetes", 0) or 0))),
                QtGui.QStandardItem(self._fmt_gs(r.get("precio_unit", 0.0))),
                QtGui.QStandardItem(self._fmt_gs(r.get("importe", 0.0))),
                QtGui.QStandardItem(str(r.get("invoice_id", 0))),
            ]
            for idx in (5, 6, 7):
                items[idx].setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
            model.appendRow(items)

        self.view_det.setModel(model)
        header = self.view_det.horizontalHeader()
        header.setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
        header.setStretchLastSection(True)

        if rows:
            self.status_label.setText(f"{len(rows)} facturas - {self._fmt_gs(total_importe)} Gs")
        else:
            self.status_label.setText("Detalle vacio: ajusta los filtros.")

    def _open_product_history_popup(self, index: QtCore.QModelIndex):
        if not index.isValid():
            return
        model = self.view_mes.model()
        if model is None:
            return
        meta_index = model.index(index.row(), 0, index.parent())
        meta = meta_index.data(PRODUCT_ANALYSIS_ROLE)
        if not isinstance(meta, dict):
            return
        if meta.get("type") == "month":
            return
        product_id = meta.get("product_id")
        gramaje = int(meta.get("gramaje", 0) or 0)
        producto = str(meta.get("producto", "") or "").strip()
        periodo = str(meta.get("periodo", "") or "").strip()
        if not product_id or not producto or not periodo:
            return

        history = self.repo.product_branch_history(int(product_id), gramaje)
        if not history:
            QtWidgets.QMessageBox.information(self, "Producto", "No hay historial para este producto.")
            return
        self._show_product_history_dialog(producto, gramaje, periodo, float(meta.get("share_pct", 0.0) or 0.0), history)

    def _handle_month_row_click(self, index: QtCore.QModelIndex):
        if not index.isValid() or index.parent().isValid():
            return
        model = self.view_mes.model()
        if model is None:
            return
        meta = model.index(index.row(), 0).data(PRODUCT_ANALYSIS_ROLE)
        if not isinstance(meta, dict) or meta.get("type") != "month":
            return
        self._show_month_summary_dialog(model, index.row(), meta)

    def _open_selected_month_chart(self):
        model = self.view_mes.model()
        if model is None or not hasattr(self, "month_chart_cb"):
            return
        periodo = str(self.month_chart_cb.currentData() or "").strip()
        if not periodo:
            QtWidgets.QMessageBox.information(self, "Grafico mes", "No hay meses disponibles para graficar.")
            return
        for row in range(model.rowCount()):
            meta = model.index(row, 0).data(PRODUCT_ANALYSIS_ROLE)
            if isinstance(meta, dict) and meta.get("type") == "month" and str(meta.get("periodo", "")) == periodo:
                self._show_month_summary_dialog(model, row, meta)
                return
        QtWidgets.QMessageBox.information(self, "Grafico mes", "No se encontro el mes seleccionado en la vista actual.")

    def _show_month_summary_dialog(self, model, month_row: int, meta: dict):
        periodo = str(meta.get("periodo", "") or "").strip()
        month_label = str(meta.get("month_label", periodo) or periodo)
        if not periodo:
            return

        product_rows = []
        branch_totals = {"Luque": 0.0, "Aregua": 0.0, "Itaugua": 0.0}
        for child_row in range(model.rowCount(model.index(month_row, 0))):
            parent_idx = model.index(month_row, 0)
            producto = str(model.index(child_row, 1, parent_idx).data() or "").strip()
            gramaje = str(model.index(child_row, 2, parent_idx).data() or "").strip()
            paquetes = float(model.index(child_row, 3, parent_idx).data() or 0)
            importe_txt = str(model.index(child_row, 4, parent_idx).data() or "0").replace(".", "")
            luque_txt = str(model.index(child_row, 7, parent_idx).data() or "0").split(" ")[0].replace(".", "")
            aregua_txt = str(model.index(child_row, 8, parent_idx).data() or "0").split(" ")[0].replace(".", "")
            itaugua_txt = str(model.index(child_row, 9, parent_idx).data() or "0").split(" ")[0].replace(".", "")
            try:
                importe = float(importe_txt)
            except Exception:
                importe = 0.0
            try:
                luque = float(luque_txt)
            except Exception:
                luque = 0.0
            try:
                aregua = float(aregua_txt)
            except Exception:
                aregua = 0.0
            try:
                itaugua = float(itaugua_txt)
            except Exception:
                itaugua = 0.0

            product_rows.append({
                "label": f"{producto} {gramaje}g".strip(),
                "paquetes": paquetes,
                "importe": importe,
            })
            branch_totals["Luque"] += luque
            branch_totals["Aregua"] += aregua
            branch_totals["Itaugua"] += itaugua

        if not product_rows:
            QtWidgets.QMessageBox.information(self, "Mes", "No hay detalle disponible para este mes.")
            return

        product_rows.sort(key=lambda r: r["importe"], reverse=True)
        top_rows = product_rows[:8]

        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("Analisis del mes")
        dlg.setWindowState(dlg.windowState() | QtCore.Qt.WindowMaximized)
        lay = QtWidgets.QVBoxLayout(dlg)
        lay.setContentsMargins(14, 14, 14, 14)
        lay.setSpacing(10)

        title = QtWidgets.QLabel(
            f"{month_label}  |  Importe total {self._fmt_gs(meta.get('importe_total', 0.0))}  |  Paquetes {self._fmt_int(meta.get('paquetes_total', 0))}"
        )
        title.setStyleSheet("font-size: 16px; font-weight: 700;")
        lay.addWidget(title)

        controls = QtWidgets.QHBoxLayout()
        controls.addWidget(QtWidgets.QLabel("Vista:"))
        view_cb = QtWidgets.QComboBox()
        view_cb.addItem("Resumen del mes", "month_summary")
        view_cb.addItem("Historico 6 meses", "history_6")
        view_cb.addItem("Historico 1 año", "history_12")
        controls.addWidget(view_cb)
        controls.addStretch(1)
        lay.addLayout(controls)

        subtitle = QtWidgets.QLabel()
        subtitle.setStyleSheet("font-size: 11px; color: #64748b;")
        lay.addWidget(subtitle)

        charts_row = QtWidgets.QHBoxLayout()
        charts_row.setSpacing(12)
        lay.addLayout(charts_row, 1)
        left_chart_view = QtCharts.QChartView()
        left_chart_view.setRenderHint(QtGui.QPainter.Antialiasing)
        charts_row.addWidget(left_chart_view, 1)
        right_chart_view = QtCharts.QChartView()
        right_chart_view.setRenderHint(QtGui.QPainter.Antialiasing)
        charts_row.addWidget(right_chart_view, 1)

        pid, gram, _ym, _d1, _d2 = self._get_filters(self.filters_mes)
        history_rows = self.repo.branch_history_until(periodo, product_id=pid, gramaje=gram)

        def _render_month_summary():
            subtitle.setText("Vista actual: mix de productos y distribucion por sucursal del mes seleccionado.")
            product_chart = QtCharts.QChart()
            product_chart.setTitle("Top productos por importe")
            product_chart.legend().hide()
            product_bar = QtCharts.QHorizontalBarSeries()
            product_set = QtCharts.QBarSet("Importe")
            product_labels = []
            max_importe = 0.0
            for row in reversed(top_rows):
                product_set.append(float(row["importe"]))
                product_labels.append(str(row["label"]))
                max_importe = max(max_importe, float(row["importe"]))
            product_set.setColor(QtGui.QColor("#2563eb"))
            product_bar.append(product_set)
            product_chart.addSeries(product_bar)
            product_axis_y = QtCharts.QBarCategoryAxis()
            product_axis_y.append(product_labels)
            product_chart.addAxis(product_axis_y, QtCore.Qt.AlignLeft)
            product_bar.attachAxis(product_axis_y)
            product_axis_x = QtCharts.QValueAxis()
            product_axis_x.setTitleText("Importe (Gs)")
            product_axis_x.setLabelFormat("%.0f")
            product_axis_x.setRange(0, max(1.0, max_importe * 1.15))
            product_chart.addAxis(product_axis_x, QtCore.Qt.AlignBottom)
            product_bar.attachAxis(product_axis_x)
            left_chart_view.setChart(product_chart)

            branch_chart = QtCharts.QChart()
            branch_chart.setTitle("Distribucion por sucursal")
            pie_series = QtCharts.QPieSeries()
            pie_colors = {
                "Luque": QtGui.QColor("#2563eb"),
                "Aregua": QtGui.QColor("#16a34a"),
                "Itaugua": QtGui.QColor("#dc2626"),
            }
            for branch_name, value in branch_totals.items():
                if value <= 0:
                    continue
                slice_ = pie_series.append(f"{branch_name} ({self._fmt_int(value)})", value)
                slice_.setLabelVisible(True)
                slice_.setLabel(f"{branch_name} {self._fmt_int(value)}")
                slice_.setBrush(pie_colors.get(branch_name, QtGui.QColor("#64748b")))
            pie_series.setHoleSize(0.35)
            branch_chart.addSeries(pie_series)
            branch_chart.legend().setVisible(True)
            branch_chart.legend().setAlignment(QtCore.Qt.AlignBottom)
            right_chart_view.setChart(branch_chart)

        def _render_history(limit_months: int):
            visible_rows = history_rows[-limit_months:] if limit_months > 0 else history_rows
            subtitle.setText(
                f"Historico hasta {month_label}: evolucion mensual y sucursales para {view_cb.currentText().lower()}."
            )
            if not visible_rows:
                empty_chart = QtCharts.QChart()
                empty_chart.setTitle("Sin datos historicos")
                left_chart_view.setChart(empty_chart)
                right_chart_view.setChart(QtCharts.QChart())
                return

            labels = [self._format_month_label(str(row.get("periodo", ""))) for row in visible_rows]
            luque_set = QtCharts.QBarSet("Luque")
            aregua_set = QtCharts.QBarSet("Aregua")
            itaugua_set = QtCharts.QBarSet("Itaugua")
            otras_set = QtCharts.QBarSet("Otras")
            line_series = QtCharts.QLineSeries()
            total_luque = 0.0
            total_aregua = 0.0
            total_itaugua = 0.0
            total_otras = 0.0
            total_importe = 0.0
            line_series.setPen(QtGui.QPen(QtGui.QColor("#f59e0b"), 3))
            max_branch = 0.0
            max_importe = 0.0

            trend_chart = QtCharts.QChart()
            trend_chart.setTitle("Historico mensual por sucursal")

            for idx, row in enumerate(visible_rows):
                luque = float(row.get("luque", 0) or 0)
                aregua = float(row.get("aregua", 0) or 0)
                itaugua = float(row.get("itaugua", 0) or 0)
                otras = float(row.get("otras", 0) or 0)
                importe = float(row.get("importe_gs", 0.0) or 0.0)
                luque_set.append(luque)
                aregua_set.append(aregua)
                itaugua_set.append(itaugua)
                otras_set.append(otras)
                line_series.append(idx, importe)
                total_luque += luque
                total_aregua += aregua
                total_itaugua += itaugua
                total_otras += otras
                total_importe += importe
                max_branch = max(max_branch, luque, aregua, itaugua, otras)
                max_importe = max(max_importe, importe)

            luque_set.setLabel(f"Luque ({self._fmt_int(total_luque)})")
            aregua_set.setLabel(f"Aregua ({self._fmt_int(total_aregua)})")
            itaugua_set.setLabel(f"Itaugua ({self._fmt_int(total_itaugua)})")
            otras_set.setLabel(f"Otras ({self._fmt_int(total_otras)})")
            line_series.setName(f"Importe total ({self._fmt_gs(total_importe)})")
            luque_set.setColor(QtGui.QColor("#2563eb"))
            aregua_set.setColor(QtGui.QColor("#16a34a"))
            itaugua_set.setColor(QtGui.QColor("#dc2626"))
            otras_set.setColor(QtGui.QColor("#64748b"))
            bar_series = QtCharts.QBarSeries()
            for bar_set in (luque_set, aregua_set, itaugua_set, otras_set):
                bar_series.append(bar_set)

            trend_chart.addSeries(bar_series)
            trend_chart.addSeries(line_series)
            trend_chart.legend().setVisible(True)
            trend_chart.legend().setAlignment(QtCore.Qt.AlignBottom)
            axis_x = QtCharts.QBarCategoryAxis()
            axis_x.append(labels)
            trend_chart.addAxis(axis_x, QtCore.Qt.AlignBottom)
            bar_series.attachAxis(axis_x)
            line_series.attachAxis(axis_x)
            axis_y_left = QtCharts.QValueAxis()
            axis_y_left.setTitleText("Paquetes")
            axis_y_left.setLabelFormat("%.0f")
            axis_y_left.setRange(0, max(1.0, max_branch * 1.2))
            trend_chart.addAxis(axis_y_left, QtCore.Qt.AlignLeft)
            bar_series.attachAxis(axis_y_left)
            axis_y_right = QtCharts.QValueAxis()
            axis_y_right.setTitleText("Importe (Gs)")
            axis_y_right.setLabelFormat("%.0f")
            axis_y_right.setRange(0, max(1.0, max_importe * 1.15))
            trend_chart.addAxis(axis_y_right, QtCore.Qt.AlignRight)
            line_series.attachAxis(axis_y_right)
            left_chart_view.setChart(trend_chart)

            import_chart = QtCharts.QChart()
            import_chart.setTitle("Importe mensual")
            import_series = QtCharts.QBarSeries()
            import_set = QtCharts.QBarSet("Importe")
            max_import_only = 0.0
            for row in visible_rows:
                importe = float(row.get("importe_gs", 0.0) or 0.0)
                import_set.append(importe)
                max_import_only = max(max_import_only, importe)
            import_set.setColor(QtGui.QColor("#0f766e"))
            import_series.append(import_set)
            import_chart.addSeries(import_series)
            import_chart.legend().hide()
            import_axis_x = QtCharts.QBarCategoryAxis()
            import_axis_x.append(labels)
            import_chart.addAxis(import_axis_x, QtCore.Qt.AlignBottom)
            import_series.attachAxis(import_axis_x)
            import_axis_y = QtCharts.QValueAxis()
            import_axis_y.setTitleText("Importe (Gs)")
            import_axis_y.setLabelFormat("%.0f")
            import_axis_y.setRange(0, max(1.0, max_import_only * 1.15))
            import_chart.addAxis(import_axis_y, QtCore.Qt.AlignLeft)
            import_series.attachAxis(import_axis_y)
            right_chart_view.setChart(import_chart)

        def _refresh_view():
            mode = str(view_cb.currentData() or "")
            if mode == "history_6":
                _render_history(6)
            elif mode == "history_12":
                _render_history(12)
            else:
                _render_month_summary()

        view_cb.currentIndexChanged.connect(_refresh_view)
        _refresh_view()

        close_btn = QtWidgets.QPushButton("Cerrar")
        close_btn.clicked.connect(dlg.close)
        btn_row = QtWidgets.QHBoxLayout()
        btn_row.addStretch(1)
        btn_row.addWidget(close_btn)
        lay.addLayout(btn_row)

        dlg.exec()

    def _show_product_history_dialog(self, producto: str, gramaje: int, periodo: str, share_pct: float, history_rows):
        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("Analisis del producto")
        dlg.setWindowState(dlg.windowState() | QtCore.Qt.WindowMaximized)
        lay = QtWidgets.QVBoxLayout(dlg)
        lay.setContentsMargins(14, 14, 14, 14)
        lay.setSpacing(10)

        title = QtWidgets.QLabel()
        title.setStyleSheet("font-size: 16px; font-weight: 700;")
        lay.addWidget(title)

        controls = QtWidgets.QHBoxLayout()
        controls.addWidget(QtWidgets.QLabel("Periodo:"))
        range_cb = QtWidgets.QComboBox()
        range_cb.addItem("6 meses", 6)
        range_cb.addItem("1 año", 12)
        range_cb.addItem("2 años", 24)
        range_cb.addItem("Todo", 0)
        controls.addWidget(range_cb)
        controls.addStretch(1)
        lay.addLayout(controls)

        subtitle = QtWidgets.QLabel()
        subtitle.setStyleSheet("font-size: 11px; color: #64748b;")
        lay.addWidget(subtitle)

        chart_view = QtCharts.QChartView()
        chart_view.setRenderHint(QtGui.QPainter.Antialiasing)
        lay.addWidget(chart_view, 1)

        summary = QtWidgets.QLabel()
        summary.setAlignment(QtCore.Qt.AlignRight)
        summary.setStyleSheet("font-size: 11px; font-weight: 600;")
        lay.addWidget(summary)

        def _refresh_chart():
            months = int(range_cb.currentData() or 0)
            visible_rows = history_rows[-months:] if months > 0 else history_rows
            if not visible_rows:
                empty_chart = QtCharts.QChart()
                empty_chart.setTitle("Sin datos para el periodo seleccionado")
                chart_view.setChart(empty_chart)
                summary.setText("")
                return

            title.setText(
                f"{producto} {gramaje} g  |  {self._format_month_label(periodo)}  |  Share {share_pct:.0f}%"
            )
            subtitle.setText(
                f"{range_cb.currentText()}: barras agrupadas por sucursal y linea de importe total."
            )

            labels = [
                self._format_month_label(str(row.get("periodo", ""))) or str(row.get("periodo", ""))
                for row in visible_rows
            ]
            luque_set = QtCharts.QBarSet("Luque")
            aregua_set = QtCharts.QBarSet("Aregua")
            itaugua_set = QtCharts.QBarSet("Itaugua")
            otras_set = QtCharts.QBarSet("Otras")
            line_series = QtCharts.QLineSeries()
            total_luque = 0.0
            total_aregua = 0.0
            total_itaugua = 0.0
            total_otras = 0.0
            total_importe_legend = 0.0
            line_series.setPen(QtGui.QPen(QtGui.QColor("#f59e0b"), 3))

            max_branch_value = 0.0
            max_importe = 0.0
            total_importe = 0.0
            best_month = ""
            best_month_importe = -1.0

            for idx, row in enumerate(visible_rows):
                luque = float(row.get("luque", 0) or 0)
                aregua = float(row.get("aregua", 0) or 0)
                itaugua = float(row.get("itaugua", 0) or 0)
                otras = float(row.get("otras", 0) or 0)
                importe = float(row.get("importe_gs", 0.0) or 0.0)
                luque_set.append(luque)
                aregua_set.append(aregua)
                itaugua_set.append(itaugua)
                otras_set.append(otras)
                line_series.append(idx, importe)
                total_luque += luque
                total_aregua += aregua
                total_itaugua += itaugua
                total_otras += otras
                total_importe_legend += importe
                max_branch_value = max(max_branch_value, luque, aregua, itaugua, otras)
                max_importe = max(max_importe, importe)
                total_importe += importe
                if importe > best_month_importe:
                    best_month_importe = importe
                    best_month = str(row.get("periodo", ""))

            luque_set.setLabel(f"Luque ({self._fmt_int(total_luque)})")
            aregua_set.setLabel(f"Aregua ({self._fmt_int(total_aregua)})")
            itaugua_set.setLabel(f"Itaugua ({self._fmt_int(total_itaugua)})")
            otras_set.setLabel(f"Otras ({self._fmt_int(total_otras)})")
            line_series.setName(f"Importe total ({self._fmt_gs(total_importe_legend)})")
            luque_set.setColor(QtGui.QColor("#2563eb"))
            aregua_set.setColor(QtGui.QColor("#16a34a"))
            itaugua_set.setColor(QtGui.QColor("#dc2626"))
            otras_set.setColor(QtGui.QColor("#64748b"))

            bar_series = QtCharts.QBarSeries()
            for bar_set in (luque_set, aregua_set, itaugua_set, otras_set):
                bar_series.append(bar_set)

            chart = QtCharts.QChart()
            chart.addSeries(bar_series)
            chart.addSeries(line_series)
            chart.setAnimationOptions(QtCharts.QChart.SeriesAnimations)
            chart.legend().setVisible(True)
            chart.legend().setAlignment(QtCore.Qt.AlignBottom)

            axis_x = QtCharts.QBarCategoryAxis()
            axis_x.append(labels)
            chart.addAxis(axis_x, QtCore.Qt.AlignBottom)
            bar_series.attachAxis(axis_x)
            line_series.attachAxis(axis_x)

            axis_y_left = QtCharts.QValueAxis()
            axis_y_left.setTitleText("Paquetes por sucursal")
            axis_y_left.setLabelFormat("%.0f")
            axis_y_left.setRange(0, max(1.0, max_branch_value * 1.2))
            chart.addAxis(axis_y_left, QtCore.Qt.AlignLeft)
            bar_series.attachAxis(axis_y_left)

            axis_y_right = QtCharts.QValueAxis()
            axis_y_right.setTitleText("Importe (Gs)")
            axis_y_right.setLabelFormat("%.0f")
            axis_y_right.setRange(0, max(1.0, max_importe * 1.15))
            chart.addAxis(axis_y_right, QtCore.Qt.AlignRight)
            line_series.attachAxis(axis_y_right)

            chart_view.setChart(chart)
            summary.setText(
                f"Total periodo: {self._fmt_gs(total_importe)}   |   Mejor mes: {self._format_month_label(best_month)} ({self._fmt_gs(best_month_importe)})"
            )

        range_cb.currentIndexChanged.connect(_refresh_chart)
        _refresh_chart()

        close_btn = QtWidgets.QPushButton("Cerrar")
        close_btn.clicked.connect(dlg.close)
        btn_row = QtWidgets.QHBoxLayout()
        btn_row.addStretch(1)
        btn_row.addWidget(close_btn)
        lay.addLayout(btn_row)

        dlg.exec()

    # --------- acciones ---------
    def _clear_filters(self, filters: Filters):
        filters.prod_cb.setCurrentText("Todos")
        filters.gram_cb.setCurrentText("Todos")
        filters.mes_cb.setCurrentIndex(0)
        filters.desde_edit.clear()
        filters.hasta_edit.clear()
        self.status_label.setText("Filtros restablecidos. Refresca para ver todo.")

    def _refresh_all(self):
        self._fill_resumen("mes")
        self._fill_resumen("semana")
        self._fill_detalle()

    def _apply_theme(self):
        self._tune_palette()
        if self._dark_mode:
            help_color = "#94a3b8"
            kpi_title = "#94a3b8"
            kpi_value = "#93c5fd"
            kpi_delta = "#cbd5e1"
            kpi_period = "#cbd5e1"
            status_color = "#cbd5e1"
            card_bg = "#0f172a"
            card_border = "#334155"
            card_hover = "#475569"
            root_style = """
                QWidget#root { background: #0b1220; color: #e5e7eb; }
                QLabel#pageTitle { color: #dbeafe; font-size: 20px; font-weight: 700; }
                QLabel#rankingSectionTitle { color: #cbd5e1; font-size: 13px; font-weight: 700; }
                QTabWidget::pane {
                    border: 1px solid #334155;
                    background: #0f172a;
                    border-radius: 8px;
                }
                QTabBar::tab {
                    background: #162033;
                    color: #cbd5e1;
                    padding: 8px 12px;
                    border: 1px solid #334155;
                    border-bottom: none;
                    border-top-left-radius: 6px;
                    border-top-right-radius: 6px;
                    margin-right: 2px;
                }
                QTabBar::tab:selected {
                    background: #1e293b;
                    color: #ffffff;
                }
                QFrame {
                    background: #0f172a;
                    border: 1px solid #334155;
                    border-radius: 8px;
                }
                QFrame[id^="ranking_"] {
                    background: #0f172a;
                    border: 1px solid #334155;
                    border-radius: 8px;
                }
                QLabel#rankingTitle {
                    color: #cbd5e1;
                    font-size: 12px;
                    font-weight: 700;
                }
                QLabel#rankingBody {
                    color: #e5e7eb;
                    font-size: 12px;
                    line-height: 1.4;
                }
                QLineEdit, QComboBox, QTableView, QTreeView {
                    background: #111c31;
                    color: #e5e7eb;
                    border: 1px solid #3b4d74;
                    border-radius: 6px;
                    padding: 4px 6px;
                }
                QHeaderView::section {
                    background: #162033;
                    color: #cbd5e1;
                    border: 1px solid #334155;
                    padding: 6px;
                }
                QComboBox QAbstractItemView {
                    background: #0f172a;
                    color: #e5e7eb;
                    selection-background-color: #1e3a8a;
                    selection-color: #ffffff;
                }
                QPushButton {
                    background: #162033;
                    color: #e5e7eb;
                    border: 1px solid #334155;
                    border-radius: 6px;
                    padding: 6px 10px;
                    font-weight: 600;
                }
                QPushButton:hover { background: #1e293b; }
                QPushButton[yearChip="true"] {
                    padding: 6px 14px;
                    border-radius: 12px;
                    font-weight: 700;
                }
                QPushButton[yearChip="true"]:checked {
                    background: #1d4ed8;
                    color: #ffffff;
                    border-color: #2563eb;
                }
                QStatusBar { color: #cbd5e1; }
            """
            expand_style = (
                "QPushButton {"
                "background-color: #1d4ed8;"
                "color: white;"
                "border: none;"
                "border-radius: 4px;"
                "padding: 6px 12px;"
                "font-weight: 600;"
                "}"
                "QPushButton:hover {"
                "background-color: #2563eb;"
                "}"
                "QPushButton:pressed {"
                "background-color: #1e40af;"
                "}"
            )
        else:
            help_color = "#4e5b68"
            kpi_title = "#4e5b68"
            kpi_value = "#0d4ba0"
            kpi_delta = "#6d7a88"
            kpi_period = "#4e5b68"
            status_color = "#334155"
            card_bg = "#ffffff"
            card_border = "#d9e2ec"
            card_hover = "#aac5ef"
            root_style = """
                QWidget#root { background: #f4f7fb; color: #1f2937; }
                QLabel#pageTitle { color: #0f172a; font-size: 20px; font-weight: 700; }
                QLabel#rankingSectionTitle { color: #1e3a8a; font-size: 13px; font-weight: 700; }
                QTabWidget::pane {
                    border: 1px solid #d1d9e6;
                    background: #ffffff;
                    border-radius: 8px;
                }
                QTabBar::tab {
                    background: #eef3f9;
                    color: #334155;
                    padding: 8px 12px;
                    border: 1px solid #d1d9e6;
                    border-bottom: none;
                    border-top-left-radius: 6px;
                    border-top-right-radius: 6px;
                    margin-right: 2px;
                }
                QTabBar::tab:selected {
                    background: #ffffff;
                    color: #0f172a;
                }
                QFrame {
                    background: #ffffff;
                    border: 1px solid #d9e2ec;
                    border-radius: 8px;
                }
                QFrame[id^="ranking_"] {
                    background: #ffffff;
                    border: 1px solid #d9e2ec;
                    border-radius: 8px;
                }
                QLabel#rankingTitle {
                    color: #1e3a8a;
                    font-size: 12px;
                    font-weight: 700;
                }
                QLabel#rankingBody {
                    color: #334155;
                    font-size: 12px;
                    line-height: 1.4;
                }
                QLineEdit, QComboBox, QTableView, QTreeView {
                    background: #ffffff;
                    color: #0f172a;
                    border: 1px solid #bfccdf;
                    border-radius: 6px;
                    padding: 4px 6px;
                }
                QHeaderView::section {
                    background: #f8fafc;
                    color: #334155;
                    border: 1px solid #d1d9e6;
                    padding: 6px;
                }
                QComboBox QAbstractItemView {
                    background: #ffffff;
                    color: #0f172a;
                    selection-background-color: #dbeafe;
                    selection-color: #1e3a8a;
                }
                QPushButton {
                    background: #eef2f7;
                    color: #0f172a;
                    border: 1px solid #cbd5e1;
                    border-radius: 6px;
                    padding: 6px 10px;
                    font-weight: 600;
                }
                QPushButton:hover { background: #e2e8f0; }
                QPushButton[yearChip="true"] {
                    padding: 6px 14px;
                    border-radius: 12px;
                    font-weight: 700;
                }
                QPushButton[yearChip="true"]:checked {
                    background: #1b5fbf;
                    color: #ffffff;
                    border-color: #1f6ed8;
                }
                QStatusBar { color: #334155; }
            """
            expand_style = (
                "QPushButton {"
                "background-color: #1b5fbf;"
                "color: white;"
                "border: none;"
                "border-radius: 4px;"
                "padding: 6px 12px;"
                "font-weight: 600;"
                "}"
                "QPushButton:hover {"
                "background-color: #1f6ed8;"
                "}"
                "QPushButton:pressed {"
                "background-color: #1958ad;"
                "}"
            )

        self.setStyleSheet(root_style)
        self.status_label.setStyleSheet(f"color: {status_color};")
        for help_lbl in self._help_labels:
            help_lbl.setStyleSheet(f"color: {help_color}; font-size: 11px;")
        for key, card in self._kpi_cards.items():
            card.setStyleSheet(
                f"""
                QFrame#{card.objectName()} {{
                    background: {card_bg};
                    border: 1px solid {card_border};
                    border-radius: 8px;
                }}
                QFrame#{card.objectName()}:hover {{
                    border-color: {card_hover};
                }}
                """
            )
            widgets = self._kpi_labels.get(key, {})
            if widgets.get("value"):
                widgets["value"].setStyleSheet(f"color: {kpi_value}; font-size: 20px; font-weight: 700;")
            if widgets.get("badge"):
                widgets["badge"].setStyleSheet(f"color: {kpi_delta}; font-size: 11px;")
            if widgets.get("delta"):
                widgets["delta"].setStyleSheet(f"color: {kpi_delta}; font-size: 11px;")
            if widgets.get("title"):
                widgets["title"].setStyleSheet(f"color: {kpi_title}; font-size: 11px; font-weight: 600;")
        self.kpi_period.setStyleSheet(f"color: {kpi_period}; font-size: 12px; font-weight: 600;")
        if self._btn_expand is not None:
            self._btn_expand.setStyleSheet(expand_style)
        if self._btn_collapse is not None:
            self._btn_collapse.setStyleSheet(expand_style)

    def _export_current_view(self):
        idx = self.tabs.currentIndex()
        view = None
        if idx == 0:
            view = self.view_mes
        elif idx == 1:
            view = self.view_sem
        elif idx == 2:
            view = self.view_det
        if view is None or view.model() is None:
            QtWidgets.QMessageBox.information(self, "Exportar", "No hay tabla activa para exportar.")
            return
        fname, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Exportar a CSV", "", "CSV (*.csv)")
        if not fname:
            self.status_label.setText("Exportacion cancelada.")
            return
        try:
            model = view.model()
            with open(fname, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=";")
                headers = [model.headerData(i, QtCore.Qt.Horizontal) for i in range(model.columnCount())]
                w.writerow(headers)
                def write_rows(parent=QtCore.QModelIndex()):
                    for r in range(model.rowCount(parent)):
                        idxs = [model.index(r, c, parent) for c in range(model.columnCount())]
                        w.writerow([model.data(i) for i in idxs])
                        if model.hasChildren(idxs[0]):
                            write_rows(idxs[0])
                write_rows()
            QtWidgets.QMessageBox.information(self, "Exportado", f"Archivo guardado: {fname}")
            self.status_label.setText(f"Exportado correctamente a {fname}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", str(e))
            self.status_label.setText("No se pudo exportar el CSV.")

    def _open_invoice_popup(self, index: QtCore.QModelIndex):
        if not index.isValid():
            return
        model = self.view_det.model()
        row = index.row()
        try:
            invoice_id = int(model.index(row, 8).data())
        except Exception:
            return
        rows = self.repo.factura_items(invoice_id)
        if not rows:
            QtWidgets.QMessageBox.information(self, "Factura", "No se encontraron items para esta factura.")
            return

        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle(f"Factura #{invoice_id}")
        dlg.resize(780, 420)
        lay = QtWidgets.QVBoxLayout(dlg)

        top = QtWidgets.QHBoxLayout()
        first = rows[0]
        top.addWidget(QtWidgets.QLabel(f"Fecha: {self._fmt_fecha(first.get('fecha'))}"))
        top.addWidget(QtWidgets.QLabel(f"Nro: {first.get('nro_factura','')}"))
        top.addWidget(QtWidgets.QLabel(f"Cliente: {first.get('cliente','')}"))
        top.addStretch(1)
        lay.addLayout(top)

        headers = ["Producto", "g", "Paquetes", "Precio (Gs)", "Importe (Gs)"]
        model_tv = QtGui.QStandardItemModel(0, len(headers), self)
        model_tv.setHorizontalHeaderLabels(headers)

        total = 0.0
        for r in rows:
            total += float(r.get('line_total', 0.0) or 0.0)
            items = [
                QtGui.QStandardItem(str(r.get('producto',''))),
                QtGui.QStandardItem(str(r.get('gramaje',0))),
                QtGui.QStandardItem(str(int(r.get('cantidad',0) or 0))),
                QtGui.QStandardItem(self._fmt_gs(r.get('price_gs',0.0))),
                QtGui.QStandardItem(self._fmt_gs(r.get('line_total',0.0))),
            ]
            for idx in (2, 3, 4):
                items[idx].setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
            model_tv.appendRow(items)

        tv = QtWidgets.QTableView()
        tv.setModel(model_tv)
        tv.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
        tv.horizontalHeader().setStretchLastSection(True)
        lay.addWidget(tv, 1)

        total_lbl = QtWidgets.QLabel(f"TOTAL: {self._fmt_gs(total)}")
        font = total_lbl.font(); font.setBold(True); total_lbl.setFont(font)
        total_lbl.setAlignment(QtCore.Qt.AlignRight)
        lay.addWidget(total_lbl)

        dlg.exec()


def main():
    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow()
    win.showMaximized()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

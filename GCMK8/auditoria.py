# -*- coding: utf-8 -*-
"""
Auditoria visual (PySide6).
- Balance materia prima, paquetes y lotes.
- Incluye ajustes en el balance.
- Exporta CSV.
"""
from __future__ import annotations

import csv
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db
from PySide6 import QtCore, QtWidgets

DB_PATH = Path(__file__).resolve().parent / "fraccionadora.db"

AUDIT_IDEAS = [
    "Registrar TODA merma y ajustes de inventario con motivo y fecha.",
    "Hacer cierre semanal o mensual con conteo fisico y ajustar con motivo.",
    "Verificar que todas las ventas se registren (incluye ventas rapidas).",
    "Auditar por lote: compra -> fraccionado -> merma -> saldo.",
    "Evitar cambios manuales de stock sin dejar rastro.",
]


@dataclass
class BalanceRowRaw:
    producto: str
    compras: float
    fracc: float
    venta_bolsa: float
    merma: float
    ajustes: float
    esperado: float
    actual: float
    delta: float


@dataclass
class BalanceRowPkg:
    producto: str
    gram: int
    fracc_paq: int
    venta_paq: int
    ajustes: int
    esperado: int
    actual: int
    delta: int


@dataclass
class BalanceRowLot:
    lote_id: int
    producto: str
    lote: str
    kg_ini: float
    kg_usado: float
    kg_merma: float
    kg_saldo: float
    delta: float


def _date_clause(field: str, desde: str | None, hasta: str | None) -> tuple[str, list[str]]:
    where = []
    params: list[str] = []
    if desde:
        where.append(f"date({field}) >= date(%s)")
        params.append(desde)
    if hasta:
        where.append(f"date({field}) <= date(%s)")
        params.append(hasta)
    if not where:
        return "", []
    return " AND " + " AND ".join(where), params


class Repo:
    def __init__(self, path: Path = DB_PATH):
        self.cn = db.connect("fraccionadora")

    def first_purchase_date(self) -> str | None:
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT MIN(ts) AS min_ts FROM (
                SELECT ts FROM purchases
                UNION ALL
                SELECT ts FROM raw_lots
            );
            """
        )
        row = cur.fetchone()
        return row["min_ts"] if row and row["min_ts"] else None

    def list_products(self):
        cur = self.cn.cursor()
        cur.execute("SELECT id, name FROM products ORDER BY name;")
        return cur.fetchall()

    def raw_stock(self):
        cur = self.cn.cursor()
        cur.execute("SELECT product_id, kg FROM raw_stock;")
        return {r["product_id"]: float(r["kg"] or 0.0) for r in cur.fetchall()}

    def package_stock(self):
        cur = self.cn.cursor()
        cur.execute("SELECT product_id, gramaje, paquetes FROM package_stock;")
        return {(r["product_id"], int(r["gramaje"])): int(r["paquetes"] or 0) for r in cur.fetchall()}

    def sum_purchases(self, desde: str | None, hasta: str | None):
        cur = self.cn.cursor()
        clause, params = _date_clause("ts", desde, hasta)
        cur.execute(
            "SELECT product_id, COALESCE(SUM(kg),0) AS kg FROM purchases WHERE 1=1" + clause + " GROUP BY product_id;",
            params,
        )
        return {r["product_id"]: float(r["kg"] or 0.0) for r in cur.fetchall()}

    def sum_raw_lots(self, desde: str | None, hasta: str | None):
        cur = self.cn.cursor()
        clause, params = _date_clause("ts", desde, hasta)
        cur.execute(
            "SELECT product_id, COALESCE(SUM(kg_inicial),0) AS kg FROM raw_lots WHERE 1=1" + clause + " GROUP BY product_id;",
            params,
        )
        return {r["product_id"]: float(r["kg"] or 0.0) for r in cur.fetchall()}

    def sum_fractionations_kg(self, desde: str | None, hasta: str | None):
        cur = self.cn.cursor()
        clause, params = _date_clause("ts", desde, hasta)
        cur.execute(
            "SELECT product_id, COALESCE(SUM(kg_consumidos),0) AS kg FROM fractionations WHERE 1=1" + clause + " GROUP BY product_id;",
            params,
        )
        return {r["product_id"]: float(r["kg"] or 0.0) for r in cur.fetchall()}

    def sum_bag_sales_kg(self, desde: str | None, hasta: str | None):
        cur = self.cn.cursor()
        clause, params = _date_clause("ts", desde, hasta)
        cur.execute(
            "SELECT product_id, COALESCE(SUM(kg_total),0) AS kg FROM bag_sales WHERE 1=1" + clause + " GROUP BY product_id;",
            params,
        )
        return {r["product_id"]: float(r["kg"] or 0.0) for r in cur.fetchall()}

    def sum_merma_kg(self, desde: str | None, hasta: str | None):
        cur = self.cn.cursor()
        clause, params = _date_clause("lm.ts", desde, hasta)
        cur.execute(
            """
            SELECT rl.product_id, COALESCE(SUM(lm.kg),0) AS kg
            FROM lot_mermas lm
            JOIN raw_lots rl ON rl.id = lm.lot_id
            WHERE 1=1
            """ + clause +
            " GROUP BY rl.product_id;",
            params,
        )
        return {r["product_id"]: float(r["kg"] or 0.0) for r in cur.fetchall()}

    def sum_adjustments_raw(self, desde: str | None, hasta: str | None):
        cur = self.cn.cursor()
        clause, params = _date_clause("ts", desde, hasta)
        cur.execute(
            """SELECT product_id, COALESCE(SUM(delta),0) AS delta
               FROM stock_adjustments
               WHERE kind='raw'""" + clause + " GROUP BY product_id;",
            params,
        )
        return {r["product_id"]: float(r["delta"] or 0.0) for r in cur.fetchall()}

    def sum_fractionations_paq(self, desde: str | None, hasta: str | None):
        cur = self.cn.cursor()
        clause, params = _date_clause("ts", desde, hasta)
        cur.execute(
            """
            SELECT product_id, gramaje, COALESCE(SUM(paquetes),0) AS paq
            FROM fractionations WHERE 1=1
            """ + clause + " GROUP BY product_id, gramaje;",
            params,
        )
        return {(r["product_id"], int(r["gramaje"])): int(r["paq"] or 0) for r in cur.fetchall()}

    def sum_sales_paq(self, desde: str | None, hasta: str | None):
        cur = self.cn.cursor()
        clause, params = _date_clause("ts", desde, hasta)
        cur.execute(
            """
            SELECT product_id, gramaje, COALESCE(SUM(paquetes),0) AS paq
            FROM sales WHERE 1=1
            """ + clause + " GROUP BY product_id, gramaje;",
            params,
        )
        return {(r["product_id"], int(r["gramaje"])): int(r["paq"] or 0) for r in cur.fetchall()}

    def sum_adjustments_paq(self, desde: str | None, hasta: str | None):
        cur = self.cn.cursor()
        clause, params = _date_clause("ts", desde, hasta)
        cur.execute(
            """SELECT product_id, gramaje, COALESCE(SUM(delta),0) AS delta
               FROM stock_adjustments
               WHERE kind='package'""" + clause + " GROUP BY product_id, gramaje;",
            params,
        )
        return {(r["product_id"], int(r["gramaje"])): int(r["delta"] or 0) for r in cur.fetchall()}

    def lot_audit(self):
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT rl.id AS lot_id,
                   p.name AS producto,
                   COALESCE(rl.lote, '') AS lote,
                   rl.kg_inicial AS kg_inicial,
                   rl.kg_saldo AS kg_saldo,
                   COALESCE(SUM(lf.kg_consumidos),0) AS kg_usado,
                   COALESCE(SUM(lm.kg),0) AS kg_merma
            FROM raw_lots rl
            JOIN products p ON p.id = rl.product_id
            LEFT JOIN lot_fractionations lf ON lf.lot_id = rl.id
            LEFT JOIN lot_mermas lm ON lm.lot_id = rl.id
            GROUP BY rl.id, p.name, rl.lote, rl.kg_inicial, rl.kg_saldo
            ORDER BY p.name, rl.id;
            """
        )
        return cur.fetchall()


class AuditoriaWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Auditoria de Stock")
        self.resize(1200, 760)
        self.repo = Repo()

        root = QtWidgets.QWidget()
        self.setCentralWidget(root)
        main_layout = QtWidgets.QVBoxLayout(root)

        filtros = QtWidgets.QHBoxLayout()
        main_layout.addLayout(filtros)

        min_ts = self.repo.first_purchase_date()
        if min_ts:
            try:
                min_dt = datetime.fromisoformat(str(min_ts))
                self.date_min = QtCore.QDate(min_dt.year, min_dt.month, min_dt.day)
            except Exception:
                self.date_min = QtCore.QDate.currentDate()
        else:
            self.date_min = QtCore.QDate.currentDate()
        self.date_today = QtCore.QDate.currentDate()
        filtros.addLayout(self._build_date_field("Desde:", "desde"))
        filtros.addLayout(self._build_date_field("Hasta:", "hasta"))

        self.btn_refresh = QtWidgets.QPushButton("Refrescar")
        self.btn_refresh.clicked.connect(self.refresh)
        filtros.addWidget(self.btn_refresh)

        self.btn_export = QtWidgets.QPushButton("Exportar CSV")
        self.btn_export.clicked.connect(self.export_csv)
        filtros.addWidget(self.btn_export)

        filtros.addStretch(1)

        self.tabs = QtWidgets.QTabWidget()
        main_layout.addWidget(self.tabs)

        self.tab_raw = QtWidgets.QWidget()
        self.tab_pkg = QtWidgets.QWidget()
        self.tab_lot = QtWidgets.QWidget()
        self.tab_ideas = QtWidgets.QWidget()
        self.tabs.addTab(self.tab_raw, "Materia Prima")
        self.tabs.addTab(self.tab_pkg, "Paquetes")
        self.tabs.addTab(self.tab_lot, "Lotes")
        self.tabs.addTab(self.tab_ideas, "Ideas")

        self.tbl_raw = self._build_table(self.tab_raw, [
            "Producto", "Compras", "Fracc", "Venta Bolsa", "Merma", "Ajustes",
            "Esperado", "Actual", "Delta",
        ])
        self.tbl_pkg = self._build_table(self.tab_pkg, [
            "Producto", "g", "Fracc Paq", "Venta Paq", "Ajustes",
            "Esperado", "Actual", "Delta",
        ])
        self.tbl_lot = self._build_table(self.tab_lot, [
            "Lote ID", "Producto", "Lote", "Kg Ini", "Kg Usado",
            "Kg Merma", "Kg Saldo", "Delta",
        ])

        ideas_layout = QtWidgets.QVBoxLayout(self.tab_ideas)
        ideas = QtWidgets.QTextEdit()
        ideas.setReadOnly(True)
        ideas.setPlainText("\n".join(f"- {i}" for i in AUDIT_IDEAS))
        ideas_layout.addWidget(ideas)

        self.status = QtWidgets.QLabel("Listo")
        main_layout.addWidget(self.status)

        self._apply_styles()
        self.refresh()

    def _build_date_field(self, label_text: str, kind: str) -> QtWidgets.QHBoxLayout:
        layout = QtWidgets.QHBoxLayout()
        layout.addWidget(QtWidgets.QLabel(label_text))

        date_edit = QtWidgets.QDateEdit()
        date_edit.setDisplayFormat("yyyy-MM-dd")
        date_edit.setMinimumDate(self.date_min)
        if kind == "desde":
            date_edit.setDate(self.date_min)
        else:
            date_edit.setDate(self.date_today)
        date_edit.setFixedWidth(140)

        btn_calendar = QtWidgets.QToolButton()
        btn_calendar.setToolTip("Abrir calendario")
        btn_calendar.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_FileDialogDetailedView))
        menu = QtWidgets.QMenu(self)
        cal = QtWidgets.QCalendarWidget(menu)
        cal.setGridVisible(True)
        action = QtWidgets.QWidgetAction(menu)
        action.setDefaultWidget(cal)
        menu.addAction(action)

        def on_date_clicked(date):
            date_edit.setDate(date)
            menu.hide()

        def show_menu():
            cal.setSelectedDate(date_edit.date())
            menu.popup(btn_calendar.mapToGlobal(QtCore.QPoint(0, btn_calendar.height())))

        cal.clicked.connect(on_date_clicked)
        btn_calendar.clicked.connect(show_menu)

        layout.addWidget(date_edit)
        layout.addWidget(btn_calendar)

        if kind == "desde":
            self.date_desde = date_edit
        else:
            self.date_hasta = date_edit

        return layout

    def _apply_styles(self):
        self.setStyleSheet(
            """
            QWidget { background: #f5f8ff; color: #12325b; }
            QTabWidget::pane { border: 1px solid #c6d6f2; }
            QTabBar::tab {
                background: #e6eefc; padding: 6px 12px; border: 1px solid #c6d6f2;
                border-bottom-color: #c6d6f2; border-top-left-radius: 4px; border-top-right-radius: 4px;
            }
            QTabBar::tab:selected { background: #ffffff; color: #0d4ba0; }
            QHeaderView::section {
                background: #e1eafc; color: #0d4ba0; padding: 6px; border: 1px solid #c6d6f2;
            }
            QTableWidget { background: #ffffff; gridline-color: #d6e2f7; }
            QTableWidget::item:selected { background: #cfe0ff; color: #12325b; }
            QTableView::item:selected { background: #cfe0ff; color: #12325b; }
            QTableView::item:selected:active { background: #cfe0ff; color: #12325b; }
            QPushButton {
                background: #1f5fbf; color: #ffffff; padding: 6px 12px; border-radius: 4px;
            }
            QPushButton:hover { background: #2a6ed6; }
            QToolButton { background: #e6eefc; border: 1px solid #c6d6f2; padding: 4px; }
            QLineEdit, QDateEdit {
                background: #ffffff; border: 1px solid #c6d6f2; padding: 4px; border-radius: 4px;
            }
            QLabel { color: #12325b; }
            QMenu { background: #ffffff; border: 1px solid #c6d6f2; }
            QCalendarWidget QWidget { background: #ffffff; color: #12325b; }
            QCalendarWidget QToolButton { background: #e6eefc; border: 1px solid #c6d6f2; }
            QCalendarWidget QAbstractItemView {
                selection-background-color: #cfe0ff;
                selection-color: #12325b;
                background: #ffffff;
                gridline-color: #d6e2f7;
            }
            """
        )

    def _build_table(self, parent: QtWidgets.QWidget, headers: list[str]) -> QtWidgets.QTableWidget:
        layout = QtWidgets.QVBoxLayout(parent)
        table = QtWidgets.QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.horizontalHeader().setStretchLastSection(True)
        table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        table.setAlternatingRowColors(True)
        layout.addWidget(table)
        return table

    def _get_dates(self) -> tuple[str | None, str | None]:
        d1 = self.date_desde.date().toString("yyyy-MM-dd")
        d2 = self.date_hasta.date().toString("yyyy-MM-dd")
        return d1, d2

    def refresh(self):
        d1, d2 = self._get_dates()
        products = self.repo.list_products()
        raw_stock = self.repo.raw_stock()
        pkg_stock = self.repo.package_stock()

        compras = self.repo.sum_purchases(d1, d2)
        lotes = self.repo.sum_raw_lots(d1, d2)
        fracc_kg = self.repo.sum_fractionations_kg(d1, d2)
        venta_bolsa = self.repo.sum_bag_sales_kg(d1, d2)
        merma = self.repo.sum_merma_kg(d1, d2)
        ajustes_raw = self.repo.sum_adjustments_raw(d1, d2)

        fracc_paq = self.repo.sum_fractionations_paq(d1, d2)
        ventas_paq = self.repo.sum_sales_paq(d1, d2)
        ajustes_paq = self.repo.sum_adjustments_paq(d1, d2)

        raw_rows: list[BalanceRowRaw] = []
        for pid, name in products:
            c = compras.get(pid, 0.0) + lotes.get(pid, 0.0)
            f = fracc_kg.get(pid, 0.0)
            v = venta_bolsa.get(pid, 0.0)
            m = merma.get(pid, 0.0)
            a = ajustes_raw.get(pid, 0.0)
            esperado = c + a - f - v - m
            actual = raw_stock.get(pid, 0.0)
            delta = actual - esperado
            raw_rows.append(BalanceRowRaw(name, c, f, v, m, a, esperado, actual, delta))

        pkg_rows: list[BalanceRowPkg] = []
        keys = set(pkg_stock.keys()) | set(fracc_paq.keys()) | set(ventas_paq.keys())
        prod_map = {pid: name for pid, name in products}
        for (pid, gram) in sorted(keys, key=lambda x: (x[0], x[1])):
            name = prod_map.get(pid, str(pid))
            fp = fracc_paq.get((pid, gram), 0)
            vp = ventas_paq.get((pid, gram), 0)
            ap = ajustes_paq.get((pid, gram), 0)
            esperado = fp + ap - vp
            actual = pkg_stock.get((pid, gram), 0)
            delta = actual - esperado
            pkg_rows.append(BalanceRowPkg(name, gram, fp, vp, ap, esperado, actual, delta))

        lot_rows: list[BalanceRowLot] = []
        for r in self.repo.lot_audit():
            esperado = float(r["kg_inicial"] or 0.0) - float(r["kg_usado"] or 0.0) - float(r["kg_merma"] or 0.0)
            delta = float(r["kg_saldo"] or 0.0) - esperado
            lot_rows.append(BalanceRowLot(
                int(r["lot_id"]),
                r["producto"],
                r["lote"],
                float(r["kg_inicial"] or 0.0),
                float(r["kg_usado"] or 0.0),
                float(r["kg_merma"] or 0.0),
                float(r["kg_saldo"] or 0.0),
                float(delta),
            ))

        self._fill_raw(raw_rows)
        self._fill_pkg(pkg_rows)
        self._fill_lot(lot_rows)

        rango = f"desde={d1 or '-'} hasta={d2 or '-'}"
        self.status.setText(f"Listo ({rango})")

    def _fill_raw(self, rows: list[BalanceRowRaw]):
        self.tbl_raw.setRowCount(0)
        for row in rows:
            r = self.tbl_raw.rowCount()
            self.tbl_raw.insertRow(r)
            values = [
                row.producto,
                f"{row.compras:.3f}",
                f"{row.fracc:.3f}",
                f"{row.venta_bolsa:.3f}",
                f"{row.merma:.3f}",
                f"{row.ajustes:.3f}",
                f"{row.esperado:.3f}",
                f"{row.actual:.3f}",
                f"{row.delta:.3f}",
            ]
            for c, v in enumerate(values):
                self.tbl_raw.setItem(r, c, QtWidgets.QTableWidgetItem(v))

    def _fill_pkg(self, rows: list[BalanceRowPkg]):
        self.tbl_pkg.setRowCount(0)
        for row in rows:
            r = self.tbl_pkg.rowCount()
            self.tbl_pkg.insertRow(r)
            values = [
                row.producto,
                str(row.gram),
                str(row.fracc_paq),
                str(row.venta_paq),
                str(row.ajustes),
                str(row.esperado),
                str(row.actual),
                str(row.delta),
            ]
            for c, v in enumerate(values):
                self.tbl_pkg.setItem(r, c, QtWidgets.QTableWidgetItem(v))

    def _fill_lot(self, rows: list[BalanceRowLot]):
        self.tbl_lot.setRowCount(0)
        for row in rows:
            r = self.tbl_lot.rowCount()
            self.tbl_lot.insertRow(r)
            values = [
                str(row.lote_id),
                row.producto,
                row.lote,
                f"{row.kg_ini:.3f}",
                f"{row.kg_usado:.3f}",
                f"{row.kg_merma:.3f}",
                f"{row.kg_saldo:.3f}",
                f"{row.delta:.3f}",
            ]
            for c, v in enumerate(values):
                self.tbl_lot.setItem(r, c, QtWidgets.QTableWidgetItem(v))

    def export_csv(self):
        base, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Exportar CSV",
            f"auditoria_{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            "CSV (*.csv)",
        )
        if not base:
            return
        base_path = Path(base)
        if base_path.suffix.lower() != ".csv":
            base_path = base_path.with_suffix(".csv")
        raw_path = base_path.with_name(base_path.stem + "_raw.csv")
        pkg_path = base_path.with_name(base_path.stem + "_pkg.csv")
        lot_path = base_path.with_name(base_path.stem + "_lotes.csv")

        def write_table(path: Path, headers: list[str], table: QtWidgets.QTableWidget):
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=";")
                w.writerow(headers)
                for r in range(table.rowCount()):
                    w.writerow([table.item(r, c).text() if table.item(r, c) else "" for c in range(table.columnCount())])

        write_table(raw_path, [
            "producto", "compras_kg", "fracc_kg", "venta_bolsa_kg", "merma_kg", "ajustes_kg",
            "esperado_kg", "actual_kg", "delta_kg",
        ], self.tbl_raw)
        write_table(pkg_path, [
            "producto", "gramaje", "fracc_paq", "venta_paq", "ajustes_paq",
            "esperado_paq", "actual_paq", "delta_paq",
        ], self.tbl_pkg)
        write_table(lot_path, [
            "lot_id", "producto", "lote", "kg_inicial", "kg_usado", "kg_merma", "kg_saldo", "delta_kg",
        ], self.tbl_lot)

        self.status.setText(f"Exportado: {raw_path.name}, {pkg_path.name}, {lot_path.name}")


def main():
    app = QtWidgets.QApplication(sys.argv)
    win = AuditoriaWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
"""
Version ligera en PySide6 de fraccionadora.
- Lectura de stock (paquetes y materia prima) desde fraccionadora.db
- Accesos rapidos a Importador OC (Tk) y Reportes de ventas (Qt)
"""
from __future__ import annotations
import sys
import subprocess
from pathlib import Path
from typing import Iterable, Sequence

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import db
from PySide6 import QtWidgets, QtGui, QtCore


DB_PATH = ROOT_DIR / "GCMK8" / "fraccionadora.db"


class Repo:
    """Capa minima de lectura sobre la base existente."""

    def __init__(self, path: str = DB_PATH):
        self.cn = db.connect("fraccionadora")

    def package_stock(self) -> Sequence:
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT p.name AS producto, ps.gramaje AS gramaje, ps.paquetes AS paquetes
            FROM package_stock ps
            JOIN products p ON p.id = ps.product_id
            ORDER BY p.name ASC, ps.gramaje ASC;
            """
        )
        return cur.fetchall()

    def raw_stock(self) -> Sequence:
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT p.name AS producto, rs.kg AS kg
            FROM raw_stock rs
            JOIN products p ON p.id = rs.product_id
            ORDER BY p.name ASC;
            """
        )
        return cur.fetchall()

    def resumen_paquetes(self) -> tuple[int, float]:
        cur = self.cn.cursor()
        cur.execute("SELECT COALESCE(SUM(paquetes),0), COUNT(*) FROM package_stock;")
        row = cur.fetchone()
        total_paq = int(row[0] or 0)
        items = int(row[1] or 0)
        return total_paq, items

    def resumen_raw(self) -> tuple[float, int]:
        cur = self.cn.cursor()
        cur.execute("SELECT COALESCE(SUM(kg),0), COUNT(*) FROM raw_stock;")
        row = cur.fetchone()
        total_kg = float(row[0] or 0.0)
        items = int(row[1] or 0)
        return total_kg, items

    def close(self) -> None:
        try:
            self.cn.close()
        except Exception:
            pass


class StockTable(QtWidgets.QTableView):
    def __init__(self, headers: Iterable[str], parent: QtWidgets.QWidget | None = None):
        super().__init__(parent)
        self.setAlternatingRowColors(True)
        self.setSortingEnabled(True)
        self.horizontalHeader().setStretchLastSection(True)
        self.verticalHeader().setVisible(False)
        self._headers = list(headers)
        self.model_table = QtGui.QStandardItemModel(0, len(self._headers), self)
        self.model_table.setHorizontalHeaderLabels(self._headers)
        self.setModel(self.model_table)

    def load_rows(self, rows: Iterable[Sequence], aligns: Iterable[QtCore.Qt.AlignmentFlag] | None = None):
        self.model_table.removeRows(0, self.model_table.rowCount())
        aligns = list(aligns or [])
        for r in rows:
            items = []
            for idx, val in enumerate(r):
                it = QtGui.QStandardItem(str(val))
                if idx < len(aligns):
                    it.setTextAlignment(aligns[idx] | QtCore.Qt.AlignVCenter)
                items.append(it)
            self.model_table.appendRow(items)
        self.resizeColumnsToContents()


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.repo = Repo()
        self.setWindowTitle("Fraccionadora (Qt)")
        self.resize(1180, 820)

        self.status_label = QtWidgets.QLabel("Listo")
        self.statusBar().addWidget(self.status_label)

        self._build_toolbar()
        self._build_tabs()
        self._refresh_all()

    # UI builders
    def _build_toolbar(self) -> None:
        bar = self.addToolBar("Acciones")
        bar.setMovable(False)

        act_refresh = QtGui.QAction("Refrescar", self)
        act_refresh.triggered.connect(self._refresh_all)
        act_refresh.setShortcut("F5")
        bar.addAction(act_refresh)

        bar.addSeparator()

        act_import = QtGui.QAction("Importador OC (Tk)", self)
        act_import.triggered.connect(self._launch_importador)
        bar.addAction(act_import)

        act_reportes = QtGui.QAction("Reportes de ventas (Qt)", self)
        act_reportes.triggered.connect(self._launch_reportes)
        bar.addAction(act_reportes)

        act_reporte_mensual = QtGui.QAction("Reporte mensual (Qt)", self)
        act_reporte_mensual.triggered.connect(self._launch_reporte_mensual)
        bar.addAction(act_reporte_mensual)

        act_bancos = QtGui.QAction("Bancos / Chequeras (Qt)", self)
        act_bancos.triggered.connect(self._launch_bancos_chequeras)
        bar.addAction(act_bancos)

    def _build_tabs(self) -> None:
        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        layout = QtWidgets.QVBoxLayout(central)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        self.tabs = QtWidgets.QTabWidget()
        layout.addWidget(self.tabs, 1)

        self._build_stock_tab()

    def _build_stock_tab(self) -> None:
        tab = QtWidgets.QWidget()
        v = QtWidgets.QVBoxLayout(tab)
        v.setContentsMargins(6, 6, 6, 6)
        v.setSpacing(8)

        header = QtWidgets.QHBoxLayout()
        header.addWidget(QtWidgets.QLabel("Lectura directa de fraccionadora.db"))
        header.addStretch(1)
        btn_refresh = QtWidgets.QPushButton("Refrescar")
        btn_refresh.clicked.connect(self._refresh_all)
        header.addWidget(btn_refresh)
        v.addLayout(header)

        # Paquetes
        v.addWidget(QtWidgets.QLabel("Stock en paquetes"))
        self.tbl_pack = StockTable(["Producto", "g", "Paquetes"], tab)
        v.addWidget(self.tbl_pack)
        self.lbl_pack_tot = QtWidgets.QLabel("Total paquetes: 0")
        v.addWidget(self.lbl_pack_tot)

        # Materia prima
        v.addWidget(QtWidgets.QLabel("Stock materia prima (kg)") )
        self.tbl_raw = StockTable(["Producto", "Kg"], tab)
        v.addWidget(self.tbl_raw)
        self.lbl_raw_tot = QtWidgets.QLabel("Total kg: 0")
        v.addWidget(self.lbl_raw_tot)

        v.addStretch(1)
        self.tabs.addTab(tab, "Stock")

    # Actions
    def _launch_importador(self) -> None:
        self._launch_script(
            Path(__file__).resolve().parent.parent / "PDFMK10" / "app_tk.py",
            "Importador OC",
        )

    def _launch_reportes(self) -> None:
        self._launch_script(
            Path(__file__).resolve().parent.parent / "reportes_ventas_qt.py",
            "Reportes de ventas",
        )

    def _launch_reporte_mensual(self) -> None:
        self._launch_script(
            Path(__file__).resolve().parent / "reporte_mensual_qt.py",
            "Reporte mensual",
        )

    def _launch_bancos_chequeras(self) -> None:
        self._launch_script(
            Path(__file__).resolve().parent / "bancos_chequeras_qt.py",
            "Bancos y chequeras",
        )

    def _launch_script(self, script: Path, title: str) -> None:
        if not script.exists():
            QtWidgets.QMessageBox.critical(self, title, f"No se encontro {script}.")
            return
        try:
            subprocess.Popen([sys.executable, str(script)], cwd=str(script.parent))
            self.status_label.setText(f"Abriendo {script.name}...")
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, title, f"No se pudo abrir: {exc}")

    # Data fillers
    def _refresh_all(self) -> None:
        self._fill_package_stock()
        self._fill_raw_stock()
        self.status_label.setText("Datos actualizados")

    def _fill_package_stock(self) -> None:
        rows = self.repo.package_stock()
        data = [(r["producto"], r["gramaje"], r["paquetes"]) for r in rows]
        self.tbl_pack.load_rows(
            data,
            aligns=[QtCore.Qt.AlignLeft, QtCore.Qt.AlignCenter, QtCore.Qt.AlignRight],
        )
        total_paq, items = self.repo.resumen_paquetes()
        self.lbl_pack_tot.setText(f"Total paquetes: {total_paq} (items: {items})")

    def _fill_raw_stock(self) -> None:
        rows = self.repo.raw_stock()
        data = [(r["producto"], f"{float(r['kg'] or 0):.2f}") for r in rows]
        self.tbl_raw.load_rows(data, aligns=[QtCore.Qt.AlignLeft, QtCore.Qt.AlignRight])
        total_kg, items = self.repo.resumen_raw()
        self.lbl_raw_tot.setText(f"Total kg: {total_kg:.2f} (items: {items})")

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:  # type: ignore[override]
        try:
            self.repo.close()
        finally:
            super().closeEvent(event)


def main() -> None:
    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

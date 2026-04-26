# -*- coding: utf-8 -*-

"""

Demo de dos pestañas (PySide6) para facturas:

- Pestaña 1: parsea la factura PDF y guarda en SQLite con las funciones existentes.

- Al guardar, llena la pestaña 2 con los campos del formulario de compras (lote):

    Producto, Proveedor, Factura, N° Lote, Kg por bolsa (aprox), Bolsas, Monto total.



Ejecutar desde esta carpeta:

    python app_qt_tabs.py



Requiere: pip install PySide6 pdfplumber

"""

import importlib

import math

import sys

from pathlib import Path

from typing import Dict, Any, List



BASE_DIR = Path(__file__).resolve().parent

PROJECT_ROOT = BASE_DIR.parent

if str(PROJECT_ROOT) not in sys.path:

    sys.path.insert(0, str(PROJECT_ROOT))

if str(BASE_DIR) not in sys.path:

    sys.path.insert(0, str(BASE_DIR))

# Para importar la l¢gica de compras/lotes de la fraccionadora

FRACC_DIR = PROJECT_ROOT / "GCMK8"

if str(FRACC_DIR) not in sys.path:

    sys.path.insert(0, str(FRACC_DIR))



import db
from PySide6 import QtWidgets, QtCore



import IFACTURA  # ensure_db, insert_factura, BOLSA_KG, peso_bolsa_estandar

from factura_parser import parse_invoice

from config_factura import PDF_PATH, DB_PATH as FACT_DB_PATH

def bolsas_equivalentes(kg: float | None, bolsa_kg: float) -> int | None:

    if kg is None or kg <= 0 or bolsa_kg <= 0:

        return None

    return int(math.ceil(kg / bolsa_kg))





class MainTabs(QtWidgets.QMainWindow):

    def __init__(self):

        super().__init__()

        self.setWindowTitle("Importador con vista de lote")

        self.resize(1000, 650)



        self.meta: Dict[str, Any] = {}

        self.items: List[Dict[str, Any]] = []

        self._fracc_repo = None

        self._normalize_product_key = lambda s: (s or "").strip().lower()

        self._product_map_cache: Dict[str, tuple[int, str]] | None = None



        tabs = QtWidgets.QTabWidget()
        self.tabs = tabs
        self.setCentralWidget(tabs)



        # --- Tab 1: factura ---

        tab1 = QtWidgets.QWidget()

        tabs.addTab(tab1, "Factura")

        l1 = QtWidgets.QVBoxLayout(tab1)



        row = QtWidgets.QHBoxLayout()

        l1.addLayout(row)

        row.addWidget(QtWidgets.QLabel("PDF:"))

        self.le_pdf = QtWidgets.QLineEdit(str(PDF_PATH))

        row.addWidget(self.le_pdf, 1)

        btn_browse = QtWidgets.QPushButton("...")

        btn_browse.clicked.connect(self.select_pdf)

        row.addWidget(btn_browse)

        btn_parse = QtWidgets.QPushButton("Parsear y Guardar")

        btn_parse.clicked.connect(self.parse_and_save)

        row.addWidget(btn_parse)



        # meta grid

        grid = QtWidgets.QGridLayout()

        l1.addLayout(grid)

        meta_fields = [

            ("Numero", "numero"),

            ("Fecha", "fecha_emision"),

            ("Cliente", "cliente"),

            ("RUC", "ruc_cliente"),

            ("Proveedor", "proveedor"),

            ("Cond.", "condicion_venta"),

            ("Total", "total"),

            ("IVA10", "total_iva10"),

        ]

        self.lbl_meta: Dict[str, QtWidgets.QLabel] = {}

        for i, (lbl, key) in enumerate(meta_fields):

            grid.addWidget(QtWidgets.QLabel(lbl + ":"), i // 3, (i % 3) * 2)

            val_lbl = QtWidgets.QLabel("-")

            self.lbl_meta[key] = val_lbl

            grid.addWidget(val_lbl, i // 3, (i % 3) * 2 + 1)



        # tabla items

        self.table = QtWidgets.QTableWidget()

        self.table.setColumnCount(5)

        self.table.setHorizontalHeaderLabels(["Desc", "KG", "Unitario", "Total", "Bolsas"])

        self.table.horizontalHeader().setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)

        for c in range(1, 5):

            self.table.horizontalHeader().setSectionResizeMode(c, QtWidgets.QHeaderView.ResizeToContents)

        l1.addWidget(self.table, 1)



        btn_save = QtWidgets.QPushButton("Guardar y llenar pestaña Lote")

        btn_save.clicked.connect(self.parse_and_save)

        l1.addWidget(btn_save)



        # --- Tab 2: lote ---

        tab2 = QtWidgets.QWidget()

        tabs.addTab(tab2, "Compras (lote)")

        v2 = QtWidgets.QVBoxLayout(tab2)

        self.lbl_factura = QtWidgets.QLabel("Factura: -")

        v2.addWidget(self.lbl_factura)



        hsplit = QtWidgets.QHBoxLayout()

        v2.addLayout(hsplit, 1)



        # Maestro: lista de facturas guardadas

        self.list_facturas = QtWidgets.QListWidget()

        self.list_facturas.setMinimumWidth(200)

        self.list_facturas.currentItemChanged.connect(self.on_factura_selected)

        hsplit.addWidget(self.list_facturas, 0)



        # Esclavo: todos los items de la factura

        self.table_lote = QtWidgets.QTableWidget()

        self.table_lote.setColumnCount(5)

        self.table_lote.setHorizontalHeaderLabels(["Producto", "Kg", "Kg/bolsa", "Bolsas", "Total linea"])

        self.table_lote.horizontalHeader().setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)

        for c in range(1, 5):

            self.table_lote.horizontalHeader().setSectionResizeMode(c, QtWidgets.QHeaderView.ResizeToContents)

        hsplit.addWidget(self.table_lote, 1)


        # --- Tab 3: Planificador producción ---
        self._build_tab_planificador()
        # --- Tab 3: Planificador producción ---
        self._build_tab_planificador()



        self.statusBar().showMessage("Listo")

        self.refresh_history()



    def select_pdf(self):

        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Selecciona PDF", str(PDF_PATH), "PDF (*.pdf)")

        if path:

            self.le_pdf.setText(path)



    def _fmt(self, v):

        if v is None:

            return "-"

        if isinstance(v, (int, float)):

            return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        return str(v)



    def parse_pdf(self):

        pdf_path = Path(self.le_pdf.text().strip())

        if not pdf_path.exists():

            QtWidgets.QMessageBox.warning(self, "PDF no encontrado", "Selecciona un PDF válido.")

            return

        try:

            res = parse_invoice(pdf_path)

        except Exception as e:

            QtWidgets.QMessageBox.critical(self, "Error de parseo", str(e))

            return

        self.meta = res.get("meta", {}) or {}

        self.items = res.get("items", []) or []

        for k, lbl in self.lbl_meta.items():

            lbl.setText(self._fmt(self.meta.get(k)))



        self.table.setRowCount(len(self.items))

        for r, it in enumerate(self.items):

            kg = it.get("kg")

            bag_w = IFACTURA.peso_bolsa_estandar(it.get("descripcion"), IFACTURA.BOLSA_KG)

            bolsas = bolsas_equivalentes(kg, bag_w)

            vals = [it.get("descripcion"), kg, it.get("precio_unitario"), it.get("total_linea"), bolsas]

            for c, val in enumerate(vals):

                item = QtWidgets.QTableWidgetItem(self._fmt(val))

                if c > 0:

                    item.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)

                self.table.setItem(r, c, item)

        self.statusBar().showMessage("Parseo listo")



    def parse_and_save(self):

        self.parse_pdf()

        if not self.items:

            return

        try:

            IFACTURA.ensure_db()

            pdf_path = Path(self.le_pdf.text().strip()) if self.le_pdf.text().strip() else None
            factura_id = IFACTURA.insert_factura(self.meta, self.items, pdf_path=pdf_path)

        except Exception as e:

            QtWidgets.QMessageBox.critical(self, "Error al guardar", str(e))

            return



        fracc_msg = self._importar_compras_lote()



        # Maestro: numero de factura

        self.lbl_factura.setText(f"Factura: {self.meta.get('numero') or '-'}")



        # Esclavo: todos los items de la factura

        self.table_lote.setRowCount(len(self.items))

        for r, it in enumerate(self.items):

            kg = it.get("kg")

            bag_w = IFACTURA.peso_bolsa_estandar(it.get("descripcion"), IFACTURA.BOLSA_KG)

            bolsas = bolsas_equivalentes(kg, bag_w) or 0

            kg_por_bolsa = (kg / bolsas) if (kg and bolsas) else bag_w

            vals = [

                it.get("descripcion"),

                kg,

                kg_por_bolsa,

                bolsas,

                it.get("total_linea"),

            ]

            for c, val in enumerate(vals):

                item = QtWidgets.QTableWidgetItem(self._fmt(val))

                if c > 0:

                    item.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)

                self.table_lote.setItem(r, c, item)



        msg_parts = [f"Factura guardada (id={factura_id}). Pestana Lote rellenada."]

        if fracc_msg:

            msg_parts.append(fracc_msg)

        QtWidgets.QMessageBox.information(self, "Guardado", "\n".join(msg_parts))

        self.statusBar().showMessage("Guardado, lote rellenado y enviados a Compras/Lote")

        self.refresh_history(select_num=self.meta.get("numero"))



    def _load_fraccionadora(self):

        if self._fracc_repo is not None:

            return self._fracc_repo

        try:

            frac_mod = importlib.import_module("fraccionadora")

            self._fracc_repo = frac_mod.Repo()

            self._normalize_product_key = getattr(frac_mod, "normalize_product_key", self._normalize_product_key)

            self._product_map_cache = None

            return self._fracc_repo

        except Exception as exc:

            # Si no esta presente, no bloqueamos el resto del flujo

            self.statusBar().showMessage(f"No se pudo abrir fraccionadora: {exc}")

            return None



    def _product_map(self) -> Dict[str, tuple[int, str]]:

        repo = self._load_fraccionadora()

        if repo is None:

            return {}

        if self._product_map_cache is not None:

            return self._product_map_cache

        try:

            norm = self._normalize_product_key

            mp = {norm(name): (pid, name) for pid, name in repo.list_products()}

            self._product_map_cache = mp

            return mp

        except Exception:

            return {}



    def _importar_compras_lote(self) -> str | None:

        """

        Envia los items parseados a la pestana Compras/Lote (fraccionadora.db).

        - Usa el mismo nro de factura para todos los items.

        - El nro de lote se deja vacio para completarlo luego.

        """

        repo = self._load_fraccionadora()

        if repo is None:

            return "No se pudo conectar con Compras/Lote (fraccionadora)."



        prod_map = self._product_map()

        norm = self._normalize_product_key

        proveedor = (self.meta.get("proveedor") or "").strip()

        factura_num = (self.meta.get("numero") or "").strip()



        missing = []
        errors = []
        inserted = 0
        detalles = []
        duplicados = []



        for it in self.items:

            desc = (it.get("descripcion") or "").strip()

            key = norm(desc)

            pid_info = prod_map.get(key)

            if not pid_info:

                missing.append(desc or "(sin descripcion)")

                continue



            try:

                kg = float(it.get("kg") or 0)

            except Exception:

                errors.append(f"{desc}: kg invalido")

                continue

            if kg <= 0:

                errors.append(f"{desc}: kg <= 0")

                continue



            try:

                costo = float(it.get("total_linea") or 0)

            except Exception:

                costo = 0.0



            # Derivar bolsas y kg/bolsa para informar

            bag_w = IFACTURA.peso_bolsa_estandar(desc, IFACTURA.BOLSA_KG)
            bolsas = bolsas_equivalentes(kg, bag_w) or 0
            kg_por_bolsa = (kg / bolsas) if (kg and bolsas) else bag_w

            # Evitar duplicar compras para misma factura+producto
            if factura_num:
                try:
                    cur = repo.cn.cursor()
                    cur.execute("SELECT 1 FROM raw_lots WHERE product_id=%s AND factura=%s LIMIT 1;", (pid_info[0], factura_num))
                    if cur.fetchone():
                        duplicados.append(desc)
                        continue
                except Exception as exc:
                    errors.append(f"{desc}: error verificando duplicado ({exc})")
                    continue

            try:
                repo.comprar_lote(pid_info[0], "", kg, proveedor, factura_num, costo)

                inserted += 1

                detalles.append(f"{desc}: kg={kg:.2f}, bolsas={bolsas}, kg/bolsa={kg_por_bolsa:.2f}, total={costo:.0f}")

            except Exception as exc:

                errors.append(f"{desc}: {exc}")



        parts = []

        if inserted:

            parts.append(f"{inserted} item(s) enviados a Compras/Lote (factura {factura_num or '-'})")

            if detalles:

                parts.append("Detalle: " + " | ".join(detalles))

        if missing:
            parts.append("Sin match de producto: " + ", ".join(missing))

        if duplicados:
            parts.append("Omitidos (ya importados para esta factura): " + ", ".join(duplicados))
        if errors:
            parts.append("Errores: " + "; ".join(errors))



        return "\n".join(parts) if parts else None



    # ---------------- Maestro/Detalle ----------------

    def refresh_history(self, select_num: str | None = None):

        """Carga lista de facturas desde la DB (maestro)."""

        self.list_facturas.blockSignals(True)

        self.list_facturas.clear()

        try:

            conn = db.connect("facturas")

            cur = conn.cursor()

            cur.execute("SELECT id, numero, proveedor, total FROM factura ORDER BY id DESC")

            rows = cur.fetchall()

        except Exception:

            rows = []

        finally:

            try:

                conn.close()

            except Exception:

                pass



        to_select = None

        for r in rows:

            proveedor = r["proveedor"] or "-"

            text = f"{r['numero'] or 'SIN_NUM'}  |  {proveedor}  |  {self._fmt(r['total'])}"

            item = QtWidgets.QListWidgetItem(text)

            item.setData(QtCore.Qt.UserRole, r["numero"])

            self.list_facturas.addItem(item)

            if select_num and r["numero"] == select_num:

                to_select = item



        self.list_facturas.blockSignals(False)

        if to_select:

            self.list_facturas.setCurrentItem(to_select)

        elif self.list_facturas.count() > 0:

            self.list_facturas.setCurrentRow(0)

        else:

            self.lbl_factura.setText("Factura: -")

            self.table_lote.setRowCount(0)



    def on_factura_selected(self, current: QtWidgets.QListWidgetItem, _prev):

        if not current:

            return

        numero = current.data(QtCore.Qt.UserRole)

        self.populate_items_for(numero)



    def populate_items_for(self, numero: str | None):

        if not numero:

            return

        try:

            conn = db.connect("facturas")

            cur = conn.cursor()

            cur.execute("SELECT id, total, proveedor FROM factura WHERE numero = %s ORDER BY id DESC LIMIT 1", (numero,))

            row = cur.fetchone()

            if not row:

                return

            factura_id = row["id"]

            total_fact = row["total"]

            proveedor = row["proveedor"]

            cur.execute(

                "SELECT descripcion, kg, precio_unitario, total_linea FROM factura_item WHERE factura_id = %s",

                (factura_id,),

            )

            items = cur.fetchall()

        finally:

            try:

                conn.close()

            except Exception:

                pass



        self.lbl_factura.setText(f"Factura: {numero}  |  Proveedor: {proveedor or '-'}  |  Total: {self._fmt(total_fact)}")

        self.table_lote.setRowCount(len(items))

        for r, it in enumerate(items):

            desc = it["descripcion"]

            kg = it["kg"]

            bag_w = IFACTURA.peso_bolsa_estandar(desc, IFACTURA.BOLSA_KG)

            bolsas = bolsas_equivalentes(kg, bag_w) or 0

            kg_por_bolsa = (kg / bolsas) if (kg and bolsas) else bag_w

            vals = [desc, kg, kg_por_bolsa, bolsas, it["total_linea"]]

            for c, val in enumerate(vals):

                item = QtWidgets.QTableWidgetItem(self._fmt(val))

                if c > 0:

                    item.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)

                self.table_lote.setItem(r, c, item)



    # ---------- Planificador producción ----------
    def _build_tab_planificador(self):
        tab = QtWidgets.QWidget()
        self.tabs.addTab(tab, "Plan produccion")
        v = QtWidgets.QVBoxLayout(tab)

        top = QtWidgets.QHBoxLayout()
        v.addLayout(top)
        top.addWidget(QtWidgets.QLabel("Ventana dias (consumo):"))
        self.le_plan_window = QtWidgets.QLineEdit("30")
        self.le_plan_window.setMaximumWidth(60)
        top.addWidget(self.le_plan_window)
        btn = QtWidgets.QPushButton("Refrescar plan")
        btn.clicked.connect(self.refresh_plan)
        top.addWidget(btn)
        top.addStretch(1)

        self.table_plan = QtWidgets.QTableWidget()
        self.table_plan.setColumnCount(7)
        self.table_plan.setHorizontalHeaderLabels([
            "Producto", "Stock kg", "Cons/dia (kg)", "Dias restantes",
            "Kg en OC", "Dias con OC", "Dias con consumo"
        ])
        self.table_plan.horizontalHeader().setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)
        for c in range(1, 7):
            self.table_plan.horizontalHeader().setSectionResizeMode(c, QtWidgets.QHeaderView.ResizeToContents)
        v.addWidget(self.table_plan, 1)

        self.lbl_plan = QtWidgets.QLabel("Cargando plan...")
        v.addWidget(self.lbl_plan)

        self.refresh_plan()

    def refresh_plan(self):
        try:
            import importlib
            plan_mod = importlib.import_module("planificador_produccion")
            importlib.reload(plan_mod)
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "Planificador", f"No se pudo cargar planificador_produccion.py: {exc}")
            return
        try:
            wnd = int((self.le_plan_window.text() or "30").strip())
            if wnd <= 0:
                wnd = 30
        except Exception:
            wnd = 30
            self.le_plan_window.setText("30")
        try:
            rows = plan_mod.cargar_plan(wnd)
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "Planificador", str(exc))
            return

        self.table_plan.setRowCount(len(rows))
        crit = warn = 0
        for r, row in enumerate(rows):
            dias = row.get("dias_restantes")
            dias_oc = row.get("dias_con_oc")
            tag = "ok"
            try:
                check = float(dias_oc if dias_oc is not None else dias if dias is not None else 1e9)
                if check <= 7:
                    tag = "crit"; crit += 1
                elif check <= 15:
                    tag = "warn"; warn += 1
            except Exception:
                tag = "ok"

            vals = [
                row.get("producto", "-"),
                self._fmt(row.get("stock_kg")),
                self._fmt(row.get("consumo_diario")),
                self._fmt(dias),
                self._fmt(row.get("oc_kg")),
                self._fmt(dias_oc),
                str(row.get("dias_activos", "-")),
            ]
            for c, val in enumerate(vals):
                item = QtWidgets.QTableWidgetItem(val)
                align = QtCore.Qt.AlignLeft if c == 0 else (QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                item.setTextAlignment(align)
                if tag == "crit":
                    item.setForeground(QtCore.Qt.red)
                elif tag == "warn":
                    item.setForeground(QtCore.Qt.darkYellow)
                self.table_plan.setItem(r, c, item)

        self.lbl_plan.setText(
            f"{len(rows)} productos | Criticos (<=7d): {crit} | Aviso (<=15d): {warn} "
            f"| Consumo basado en dias con consumo y kg en OC pendientes."
        )





def main():

    app = QtWidgets.QApplication(sys.argv)

    win = MainTabs()

    win.show()

    sys.exit(app.exec())





if __name__ == "__main__":

    main()





















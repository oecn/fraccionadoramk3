# resumen_compras.py
# -*- coding: utf-8 -*-
import tkinter as tk
from tkinter import ttk, messagebox
import json
import csv
import os
import datetime as _dt
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db

BRANCH_KPIS = ("Aregua", "Luque", "Itaugua")

def bag_kg_por_defecto(product_name: str) -> float:
    n = (product_name or "").strip().lower()
    if n == "arroz":
        return 50.0
    if n == "galleta molida":
        return 25.0
    return 50.0

BASE_DIR = Path(__file__).resolve().parent.parent
PAGOS_FILE = BASE_DIR / "pagos_compras copy.json"
FACT_DB_PATH = BASE_DIR / "importadorfactur" / "facturas.db"


class TabResumenCompras:
    def __init__(self, parent_app, notebook):
        self.app = parent_app
        self.repo = parent_app.repo
        self._cargar_pagos()

        frame = ttk.Frame(notebook)
        notebook.add(frame, text="Resumen de compras")

        # --- Filtros ---
        filtros = ttk.Frame(frame)
        filtros.pack(fill="x", pady=6)

        ttk.Label(filtros, text="Producto:").pack(side="left")
        self.cb_prod = ttk.Combobox(filtros, state="readonly", width=30)
        self.cb_prod.pack(side="left", padx=6)

        ttk.Label(filtros, text="Desde (YYYY-MM-DD):").pack(side="left")
        self.ent_desde = ttk.Entry(filtros, width=12)
        self.ent_desde.pack(side="left", padx=4)

        ttk.Label(filtros, text="Hasta (YYYY-MM-DD):").pack(side="left")
        self.ent_hasta = ttk.Entry(filtros, width=12)
        self.ent_hasta.pack(side="left", padx=4)

        ttk.Button(filtros, text="Refrescar", command=self._refresh).pack(side="left", padx=6)
        ttk.Button(filtros, text="Exportar CSV", command=self._export_csv, style="Export.TButton").pack(side="left", padx=6)
        ttk.Button(filtros, text="Exportar Excel", command=self._export_excel, style="Export.TButton").pack(side="left", padx=6)

        kpis = ttk.Frame(frame)
        kpis.pack(fill="x", padx=8, pady=(2, 8))
        style = ttk.Style()
        style.configure("KPI.TFrame", padding=8, relief="groove", borderwidth=1, background="#f4f6f9")
        style.configure("KPITotal.TFrame", padding=8, relief="groove", borderwidth=1, background="#e8f3ff")
        style.configure("KPIName.TLabel", font=("Segoe UI", 11, "bold"), foreground="#1f2937", background="#f4f6f9")
        style.configure("KPIValue.TLabel", font=("Segoe UI", 12, "bold"), foreground="#0f172a", background="#f4f6f9")
        style.configure("KPITotalName.TLabel", font=("Segoe UI", 11, "bold"), foreground="#0b4f6c", background="#e8f3ff")
        style.configure("KPITotalValue.TLabel", font=("Segoe UI", 12, "bold"), foreground="#0b4f6c", background="#e8f3ff")

        self.lbl_kpis = {}
        for idx, suc in enumerate(BRANCH_KPIS):
            box = ttk.Frame(kpis, style="KPI.TFrame")
            box.grid(row=0, column=idx, sticky="nsew", padx=6, pady=2)
            ttk.Label(box, text=suc, style="KPIName.TLabel", anchor="center", justify="center").grid(row=0, column=0, sticky="ew")
            val_lbl = ttk.Label(box, text="0 Gs", style="KPIValue.TLabel", anchor="center", justify="center")
            val_lbl.grid(row=1, column=0, sticky="ew", pady=(2, 0))
            box.columnconfigure(0, weight=1)
            self.lbl_kpis[suc] = val_lbl

        total_box = ttk.Frame(kpis, style="KPITotal.TFrame")
        total_box.grid(row=0, column=len(BRANCH_KPIS), sticky="nsew", padx=6, pady=2)
        ttk.Label(total_box, text="Total pendiente", style="KPITotalName.TLabel", anchor="center", justify="center").grid(row=0, column=0, sticky="ew")
        self.lbl_total_pend = ttk.Label(total_box, text="0 Gs", style="KPITotalValue.TLabel", anchor="center", justify="center")
        self.lbl_total_pend.grid(row=1, column=0, sticky="ew", pady=(2, 0))
        total_box.columnconfigure(0, weight=1)

        for i in range(len(BRANCH_KPIS) + 1):
            kpis.columnconfigure(i, weight=1, uniform="kpi")


        # --- Panedwindow: Maestro–Detalle ---
        pan = ttk.Panedwindow(frame, orient="horizontal")
        pan.pack(fill="both", expand=True, pady=6)

        # ===== IZQUIERDA: COMPRAS =====
        boxL = ttk.Labelframe(pan, text="Compras registradas (materia prima)")
        pan.add(boxL, weight=1)

        cols = ("id", "fecha", "producto", "kg", "costo_total", "costo_kg", "estado")
        self.tv_comp = ttk.Treeview(boxL, columns=cols, show="headings", height=14)
        for c, t, w, a in [
            ("id", "ID", 50, "center"),
            ("fecha", "Fecha", 130, "w"),
            ("producto", "Producto", 160, "w"),
            ("kg", "Kg totales", 100, "center"),
            ("costo_total", "Costo total (Gs)", 130, "e"),
            ("costo_kg", "Costo/kg (Gs)", 110, "e"),
            ("estado", "Estado", 100, "center"),
        ]:
            self.tv_comp.heading(c, text=t)
            self.tv_comp.column(c, width=w, anchor=a)
        self.tv_comp.pack(fill="both", expand=True, padx=6, pady=6)
        self.tv_comp.bind("<<TreeviewSelect>>", self._on_select_compra)

        # ===== DERECHA: DETALLE =====
        boxR = ttk.Labelframe(pan, text="Detalle de la compra / lote")
        pan.add(boxR, weight=2)

        grid = ttk.Frame(boxR)
        grid.pack(fill="x", padx=8, pady=8)

        self.labels = {}
        def add_row(r, key, label):
            ttk.Label(grid, text=label + ":").grid(row=r, column=0, sticky="w", pady=2)
            lbl = ttk.Label(grid, text="—")
            lbl.grid(row=r, column=1, sticky="w", padx=6)
            self.labels[key] = lbl

        add_row(0, "producto", "Producto")
        add_row(1, "proveedor", "Proveedor")
        add_row(2, "factura", "Factura")
        add_row(3, "lote", "Lote")
        add_row(4, "fecha", "Fecha de compra")
        add_row(5, "bolsas", "Cantidad de bolsas")
        add_row(6, "kg", "Kg totales")
        add_row(7, "costo_total", "Costo total (Gs)")
        add_row(8, "costo_kg", "Costo/kg (Gs)")
        add_row(9, "saldo", "Kg disponibles (saldo)")

        ttk.Label(grid, text="Estado de pago:").grid(row=10, column=0, sticky="w", pady=4)
        self.btn_estado = ttk.Button(grid, text="Pendiente", width=12, command=self._toggle_pagado)
        self.btn_estado.grid(row=10, column=1, sticky="w", padx=6, pady=4)

        ttk.Label(grid, text="Factura PDF:").grid(row=11, column=0, sticky="w", pady=4)
        self.btn_ver_factura = ttk.Button(
            grid, text="Ver factura", width=12, command=self._open_factura_pdf, state=tk.DISABLED
        )
        self.btn_ver_factura.grid(row=11, column=1, sticky="w", padx=6, pady=4)

        # Inicializar combos y datos
        self._fill_products()
        self._refresh()
        self.selected_lot_id = None
        self._factura_pdf_path = None

    # ============================
    #  Estado de pagos compartido con dashboard
    # ============================
    def _ensure_payment_storage(self):
        cur = self.repo.cn.cursor()
        db.run_ddl(self.repo.cn,
            """
            CREATE TABLE IF NOT EXISTS dashboard_payment_flags(
                lot_id INTEGER PRIMARY KEY,
                paid INTEGER NOT NULL DEFAULT 0,
                updated_ts TEXT
            );

            CREATE TABLE IF NOT EXISTS dashboard_payment_details(
                id BIGSERIAL PRIMARY KEY,
                payment_group_id TEXT,
                lot_id INTEGER NOT NULL,
                proveedor TEXT,
                factura TEXT,
                monto_gs REAL NOT NULL DEFAULT 0,
                fecha_pago TEXT,
                medio TEXT,
                referencia TEXT,
                nro_deposito TEXT,
                nro_recibo_dinero TEXT,
                observacion TEXT,
                facturas_grupo_json TEXT,
                total_grupo_gs REAL NOT NULL DEFAULT 0,
                ts_registro TEXT,
                ts_modificacion TEXT
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_dash_payment_detail_group_lot
                ON dashboard_payment_details(payment_group_id, lot_id);
            """
        )
        self.repo.cn.commit()

    def _load_paid_map_from_db(self):
        self._ensure_payment_storage()
        cur = self.repo.cn.cursor()
        cur.execute("SELECT lot_id, paid FROM dashboard_payment_flags;")
        return {str(int(lot_id)): bool(paid) for lot_id, paid in cur.fetchall()}

    def _save_paid_map_to_db(self):
        self._ensure_payment_storage()
        cur = self.repo.cn.cursor()
        for lot_id, paid in self.pagos.items():
            try:
                lot_id_int = int(str(lot_id).strip())
            except Exception:
                continue
            cur.execute(
                """
                INSERT INTO dashboard_payment_flags(lot_id, paid, updated_ts)
                VALUES(%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT(lot_id) DO UPDATE SET
                    paid=excluded.paid,
                    updated_ts=CURRENT_TIMESTAMP;
                """,
                (lot_id_int, 1 if bool(paid) else 0),
            )
        self.repo.cn.commit()

    def _cargar_pagos(self):
        pagos_db = {}
        try:
            pagos_db = self._load_paid_map_from_db()
        except Exception:
            pagos_db = {}

        PAGOS_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not PAGOS_FILE.exists():
            self.pagos = pagos_db
            return
        try:
            with PAGOS_FILE.open("r", encoding="utf-8") as f:
                raw = json.load(f)
                self.pagos = raw if isinstance(raw, dict) else {}
        except Exception:
            self.pagos = {}
        self.pagos.update(pagos_db)

    def _guardar_pagos(self):
        try:
            self._save_paid_map_to_db()
        except Exception as e:
            print("Error guardando pagos en DB:", e)
        try:
            PAGOS_FILE.parent.mkdir(parents=True, exist_ok=True)
            with PAGOS_FILE.open("w", encoding="utf-8") as f:
                json.dump(self.pagos, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print("Error guardando pagos:", e)

    # ============================
    #  Cargar compras principales
    # ============================
    def _fill_products(self):
        prods = [n for _, n in self.repo.list_products()]
        self.cb_prod["values"] = ["Todos"] + prods
        self.cb_prod.set("Todos")

    def _refresh(self):
        self._cargar_pagos()
        for i in self.tv_comp.get_children():
            self.tv_comp.delete(i)

        pid = None
        prod_sel = (self.cb_prod.get() or "Todos").strip()
        if prod_sel != "Todos":
            pid = self.repo.get_product_id_by_name(prod_sel)

        d1 = (self.ent_desde.get() or "").strip()
        d2 = (self.ent_hasta.get() or "").strip()

        cur = self.repo.cn.cursor()
        sql = """
            SELECT rl.id, rl.ts, p.name, rl.kg_inicial,
                rl.costo_total_gs, rl.costo_kg_gs, rl.proveedor
            FROM raw_lots rl
            JOIN products p ON p.id = rl.product_id
        """
        where, params = [], []
        if pid:
            where.append("rl.product_id=%s")
            params.append(pid)
        if d1:
            where.append("rl.ts >= %s")
            params.append(d1)
        if d2:
            where.append("rl.ts <= %s")
            params.append(d2)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY rl.ts DESC;"
        cur.execute(sql, params)

        # Definir colores
        self.tv_comp.tag_configure("pagado", foreground="green")
        self.tv_comp.tag_configure("pendiente", foreground="red")

        # Llenar filas
        total_pend_gs = 0.0
        branch_totals = {s: 0.0 for s in BRANCH_KPIS}
        for rid, fecha, prod, kg, costo_total, costo_kg, proveedor in cur.fetchall():
            # Si la compra no está en el JSON, agregarla como pendiente
            if str(rid) not in self.pagos:
                self.pagos[str(rid)] = False

            pagado = self.pagos[str(rid)]
            estado = "Pagado" if pagado else "Pendiente"
            tag_color = "pagado" if pagado else "pendiente"
            if not pagado:
                total_pend_gs += costo_total
                suc = self._sucursal_from_proveedor(proveedor)
                if suc:
                    branch_totals[suc] += costo_total

            self.tv_comp.insert(
                "",
                "end",
                iid=rid,
                values=(
                    rid,
                    fecha,
                    prod,
                    f"{kg:.3f}",
                    f"{costo_total:,.0f}".replace(",", "."),
                    f"{costo_kg:,.0f}".replace(",", "."),
                    estado,
                ),
                tags=(tag_color,),
            )

        # Guardar los estados actualizados (si había nuevos)
        self._guardar_pagos()
        self.lbl_total_pend.config(
            text=f"{total_pend_gs:,.0f}".replace(",", ".") + " Gs"
        )
        for suc, total in branch_totals.items():
            if suc in self.lbl_kpis:
                self.lbl_kpis[suc].config(
                    text=f"{total:,.0f}".replace(",", ".") + " Gs"
                )

    def _sucursal_from_proveedor(self, proveedor: str):
        p = (proveedor or "").strip().lower()
        if not p:
            return None
        if "areg" in p:
            return "Aregua"
        if "luque" in p:
            return "Luque"
        if "ita" in p:
            return "Itaugua"
        return None

    # ============================
    #   Al seleccionar una compra
    # ============================
    def _on_select_compra(self, _event=None):
        sel = self.tv_comp.selection()
        if not sel:
            return
        rid = int(sel[0])
        self.selected_lot_id = rid
        self._load_detalle_compra(rid)

    def _load_detalle_compra(self, lot_id):
        cur = self.repo.cn.cursor()
        sql = """
            SELECT p.name, rl.proveedor, rl.factura, rl.lote, rl.ts,
                   rl.kg_inicial, rl.costo_total_gs, rl.costo_kg_gs, rl.kg_saldo
            FROM raw_lots rl
            JOIN products p ON p.id = rl.product_id
            WHERE rl.id = %s;
        """
        cur.execute(sql, (lot_id,))
        row = cur.fetchone()
        if not row:
            return

        pname, prov, fact, lote, fecha, kg_tot, costo_total, costo_kg, saldo = row
        self.labels["producto"].config(text=pname or "—")
        self.labels["proveedor"].config(text=prov or "—")
        self.labels["factura"].config(text=fact or "—")
        self.labels["lote"].config(text=lote or "—")
        self.labels["fecha"].config(text=str(fecha or "—"))
        bag_kg = bag_kg_por_defecto(pname)
        self.labels["bolsas"].config(text=f"{(kg_tot/bag_kg):.1f}")
        self.labels["kg"].config(text=f"{kg_tot:.3f}")
        self.labels["costo_total"].config(text=f"{costo_total:,.0f}".replace(",", "."))
        self.labels["costo_kg"].config(text=f"{costo_kg:,.0f}".replace(",", "."))
        self.labels["saldo"].config(text=f"{saldo:.3f}")

        pagado = self.pagos.get(str(lot_id), False)
        if pagado:
            self._set_estado_btn("Pagado", "green")
        else:
            self._set_estado_btn("Pendiente", "red")

        self._factura_pdf_path = self._lookup_factura_pdf(fact)
        if self._factura_pdf_path:
            self.btn_ver_factura.config(state=tk.NORMAL)
        else:
            self.btn_ver_factura.config(state=tk.DISABLED)

    def _lookup_factura_pdf(self, factura_num):
        numero = (factura_num or "").strip()
        if not numero:
            return None
        if not FACT_DB_PATH.exists():
            return None
        try:
            conn = db.connect("fraccionadora")
            cur = conn.cursor()
            cur.execute(
                "SELECT pdf_path FROM factura WHERE numero = %s ORDER BY id DESC LIMIT 1;",
                (numero,),
            )
            row = cur.fetchone()
            if not row or not row[0]:
                return None
            return row[0]
        except Exception:
            return None
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _open_factura_pdf(self):
        path = self._factura_pdf_path
        if not path:
            messagebox.showinfo("Factura", "No hay PDF asociado a esta factura.")
            return
        if not os.path.exists(path):
            messagebox.showerror("Factura", f"No se encuentra el PDF:\n{path}")
            return
        try:
            os.startfile(path)
        except Exception as exc:
            messagebox.showerror("Factura", f"No se pudo abrir el PDF: {exc}")

    # ============================
    #   Alternar estado de pago
    # ============================
    def _toggle_pagado(self):
        if not self.selected_lot_id:
            return
        rid = str(self.selected_lot_id)
        actual = self.pagos.get(rid, False)
        self.pagos[rid] = not actual
        self._guardar_pagos()
        self._load_detalle_compra(self.selected_lot_id)
        self._refresh()

    def _set_estado_btn(self, texto, color):
        self.btn_estado.config(text=texto)
        self.btn_estado.configure(style=f"{color}.TButton")
        style = ttk.Style()
        style.configure("green.TButton", foreground="white", background="green")
        style.map("green.TButton", background=[("active", "#2ecc71")])
        style.configure("red.TButton", foreground="white", background="red")
        style.map("red.TButton", background=[("active", "#e74c3c")])

    # ============================
    #        Exportaciones
    # ============================
    def _build_export_rows(self):
        pid = None
        prod_sel = (self.cb_prod.get() or "Todos").strip()
        if prod_sel != "Todos":
            pid = self.repo.get_product_id_by_name(prod_sel)

        d1 = (self.ent_desde.get() or "").strip()
        d2 = (self.ent_hasta.get() or "").strip()

        cur = self.repo.cn.cursor()
        sql = """
            SELECT rl.id, rl.ts, p.name, rl.proveedor, rl.factura, rl.lote,
                   rl.kg_inicial, rl.kg_saldo, rl.costo_total_gs, rl.costo_kg_gs, rl.cerrado
            FROM raw_lots rl
            JOIN products p ON p.id = rl.product_id
        """
        where, params = [], []
        if pid:
            where.append("rl.product_id=%s")
            params.append(pid)
        if d1:
            where.append("rl.ts >= %s")
            params.append(d1)
        if d2:
            where.append("rl.ts <= %s")
            params.append(d2)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY rl.ts DESC;"
        cur.execute(sql, params)

        rows = []
        for rid, ts, prod, prov, fact, lote, kg_ini, kg_saldo, ctot, ckg, cerrado in cur.fetchall():
            pagado = self.pagos.get(str(rid), False)
            rows.append((
                rid, ts, prod, prov or "", fact or "", lote or "",
                kg_ini, kg_saldo, ctot, ckg, "SI" if cerrado else "NO",
                "SI" if pagado else "NO",
            ))
        headers = [
            "id", "fecha", "producto", "proveedor", "factura", "lote",
            "kg_inicial", "kg_saldo", "costo_total_gs", "costo_kg_gs", "cerrado", "pagado",
        ]
        return headers, rows

    def _write_csv(self, headers, rows, fname):
        with open(fname, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(headers)
            for r in rows:
                w.writerow(r)
        return os.path.abspath(fname)

    def _write_xlsx(self, headers, rows, fname):
        try:
            import openpyxl
        except Exception as exc:
            return False, str(exc)
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(list(headers))
        for r in rows:
            ws.append(list(r))
        wb.save(fname)
        return True, os.path.abspath(fname)

    def _export_csv(self):
        headers, rows = self._build_export_rows()
        stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S") if hasattr(_dt, "datetime") else "export"
        fname = f"compras_{stamp}.csv"
        path = self._write_csv(headers, rows, fname)
        messagebox.showinfo("Exportado", f"Archivo guardado: {path}")

    def _export_excel(self):
        headers, rows = self._build_export_rows()
        stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S") if hasattr(_dt, "datetime") else "export"
        fname = f"compras_{stamp}.xlsx"
        ok, info = self._write_xlsx(headers, rows, fname)
        if ok:
            messagebox.showinfo("Exportado", f"Archivo guardado: {info}")
            return
        fname_csv = f"compras_{stamp}.csv"
        path = self._write_csv(headers, rows, fname_csv)
        messagebox.showinfo("Exportado", f"No se pudo crear Excel ({info}). CSV guardado: {path}")

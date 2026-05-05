# historial_ventas.py
# -*- coding: utf-8 -*-
import importlib.util
import datetime
import csv
import os
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db
import tkinter as tk
from tkinter import ttk, messagebox

_SHEET_CACHE = {"mod": None, "err": None}
BASE_DIR = Path(__file__).resolve().parent.parent
COBROS_FILE = BASE_DIR / "cobros_ventas.json"


def _load_sheet_module():
    if _SHEET_CACHE["mod"] or _SHEET_CACHE["err"]:
        return _SHEET_CACHE["mod"], _SHEET_CACHE["err"]

    mod_path = Path(__file__).resolve().parent.parent / "sheets" / "sheet.py"
    try:
        spec = importlib.util.spec_from_file_location("sheet_gs", mod_path)
        if not spec or not spec.loader:
            raise ImportError("No se pudo crear spec para sheet.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _SHEET_CACHE["mod"] = module
        return module, None
    except Exception as exc:
        _SHEET_CACHE["err"] = exc
        return None, exc

class TabHistorialVentas:
    def __init__(self, parent_app, notebook):
        """
        parent_app: instancia principal de App (de fraccionadora.py)
        notebook: el ttk.Notebook donde se agregará la pestaña
        """
        self.app = parent_app
        self.repo = parent_app.repo
        self._cargar_cobranzas()

        frame = ttk.Frame(notebook)
        notebook.add(frame, text="Historial de ventas")
        self._frame = frame

        # --- Filtros superiores ---
        filtros = ttk.Frame(frame)
        filtros.pack(fill="x", pady=6)

        ttk.Label(filtros, text="Cliente o N° factura:").pack(side="left")
        self.ent_filtro = ttk.Entry(filtros, width=30)
        self.ent_filtro.pack(side="left", padx=6)

        ttk.Label(filtros, text="Desde (YYYY-MM-DD):").pack(side="left")
        self.ent_desde = ttk.Entry(filtros, width=12)
        self.ent_desde.pack(side="left", padx=4)

        ttk.Label(filtros, text="Hasta (YYYY-MM-DD):").pack(side="left")
        self.ent_hasta = ttk.Entry(filtros, width=12)
        self.ent_hasta.pack(side="left", padx=4)

        ttk.Button(filtros, text="Refrescar", command=self._refresh).pack(side="left", padx=6)
        ttk.Button(
            filtros,
            text="Enviar >",
            command=self._preview_send_sheet,
        ).pack(side="left", padx=6)
        ttk.Button(
            filtros,
            text="Editar cliente / factura",
            command=self._edit_header,
        ).pack(side="left", padx=6)
        self.btn_edit_item = ttk.Button(
            filtros,
            text="Editar ítem (cant/precio)",
            command=self._edit_invoice_item,
            state=tk.DISABLED,
        )
        self.btn_edit_item.pack(side="left", padx=6)
        ttk.Button(
            filtros,
            text="Exportar CSV",
            command=self._export_csv,
            style="Export.TButton",
        ).pack(side="left", padx=6)
        ttk.Button(
            filtros,
            text="Exportar Excel",
            command=self._export_excel,
            style="Export.TButton",
        ).pack(side="left", padx=6)

        kpi_row = ttk.Frame(frame)
        kpi_row.pack(fill="x", pady=(0, 6))
        self.lbl_pendiente_cobro = ttk.Label(
            kpi_row,
            text="Pendiente de cobro (con retención): 0",
            font=("Segoe UI", 10, "bold"),
        )
        self.lbl_pendiente_cobro.pack(side="left", padx=6)

        # --- PanedWindow: Maestro (facturas) y Detalle (ítems) ---
        pan = ttk.Panedwindow(frame, orient="horizontal")
        pan.pack(fill="both", expand=True, pady=6)

        # ===== IZQUIERDA: FACTURAS =====
        boxL = ttk.Labelframe(pan, text="Facturas emitidas")
        pan.add(boxL, weight=1)

        cols_inv = ("id", "fecha", "nro", "cliente", "grav5", "iva5", "grav10", "iva10", "total", "total_ret")
        self.tv_inv = ttk.Treeview(boxL, columns=cols_inv, show="headings", height=14)
        for c, t, w, a in [
            ("id", "ID", 60, "center"),
            ("fecha", "Fecha", 130, "w"),
            ("nro", "N° Factura", 120, "w"),
            ("cliente", "Cliente", 180, "w"),
            ("grav5", "Grav. 5%", 90, "e"),
            ("iva5", "IVA 5%", 80, "e"),
            ("grav10", "Grav. 10%", 90, "e"),
            ("iva10", "IVA 10%", 80, "e"),
            ("total", "TOTAL (Gs)", 110, "e"),
            ("total_ret", "Total con retencion (Gs)", 140, "e"),
        ]:
            self.tv_inv.heading(c, text=t)
            self.tv_inv.column(c, width=w, anchor=a)
        self.tv_inv.pack(fill="both", expand=True, padx=6, pady=6)
        self.tv_inv.bind("<<TreeviewSelect>>", self._on_invoice_select)
        self.tv_inv.tag_configure("age_cobrado", background="#b9e6b9", foreground="#111111")
        self.tv_inv.tag_configure("age_0", background="#d7ecff", foreground="#111111")
        self.tv_inv.tag_configure("age_1", background="#ffe8b3", foreground="#111111")
        self.tv_inv.tag_configure("age_2", background="#ffc285", foreground="#111111")
        self.tv_inv.tag_configure("age_3", background="#ffb3b3", foreground="#111111")
        self.tv_inv.tag_configure("age_none", background="#e5e7eb", foreground="#111111")

        # ===== DERECHA: DETALLE =====
        boxR = ttk.Labelframe(pan, text="Detalle de la factura seleccionada")
        pan.add(boxR, weight=2)

        hdr_det = ttk.Frame(boxR)
        hdr_det.pack(fill="x", padx=6, pady=(6, 0))
        self.btn_cobro = ttk.Button(
            hdr_det,
            text="Pendiente",
            width=12,
            command=self._toggle_cobrado,
            state=tk.DISABLED,
        )
        self.btn_cobro.pack(side="right", padx=(0, 8))
        self.lbl_cheque = tk.Label(
            hdr_det,
            text="SIN COBRAR: -",
            anchor="e",
            padx=8,
            pady=6,
            font=("Segoe UI", 10, "bold"),
            bg="#e5e7eb",
            fg="#111111",
        )
        self.lbl_cheque.pack(side="right")

        cols_det = ("producto", "gramaje", "cantidad", "precio", "iva", "total")
        self.tv_det = ttk.Treeview(boxR, columns=cols_det, show="headings", height=14)
        for c, t, w, a in [
            ("producto", "Producto", 180, "w"),
            ("gramaje", "g", 70, "center"),
            ("cantidad", "Cant.", 70, "center"),
            ("precio", "Precio (Gs)", 100, "e"),
            ("iva", "IVA %", 60, "center"),
            ("total", "Total (Gs)", 100, "e"),
        ]:
            self.tv_det.heading(c, text=t)
            self.tv_det.column(c, width=w, anchor=a)
        self.tv_det.pack(fill="both", expand=True, padx=6, pady=6)

        self._inv_row_meta = {}
        self._det_row_meta = {}
        self._frame.bind("<Control-p>", self._on_ctrl_p)
        self.tv_inv.bind("<Control-p>", self._on_ctrl_p)
        self.tv_det.bind("<Control-p>", self._on_ctrl_p)

        # --- Inicializar ---
        self._refresh()

    # =======================
    #     Cargar Facturas
    # =======================
    def _refresh(self):
        for i in self.tv_inv.get_children():
            self.tv_inv.delete(i)
        for i in self.tv_det.get_children():
            self.tv_det.delete(i)
        self._inv_row_meta = {}
        self._det_row_meta = {}
        self._set_cheque_badge(None, cobrado=False)
        self._set_cobro_btn(False, enabled=False)
        if hasattr(self, "btn_edit_item"):
            self.btn_edit_item.config(state=tk.DISABLED)

        ftxt = (self.ent_filtro.get() or "").strip()
        d1 = (self.ent_desde.get() or "").strip()
        d2 = (self.ent_hasta.get() or "").strip()

        try:
            rows = []
            rows.extend(self._fetch_invoice_rows(ftxt, d1, d2))
            rows.extend(self._fetch_bag_sale_rows(ftxt, d1, d2))
        except Exception as exc:
            messagebox.showerror("Error", f"No se pudo cargar el historial de ventas:\n{exc}")
            return
        rows.sort(key=lambda r: r.get("ts") or "", reverse=True)

        for row in rows:
            iid = row["iid"]
            self._inv_row_meta[iid] = row
            self.tv_inv.insert(
                "",
                "end",
                iid=iid,
                tags=(self._age_tag(row),),
                values=(
                    row["display_id"],
                    row.get("ts") or "",
                    row.get("nro") or "-",
                    row.get("cliente") or "-",
                    self._fmt_money(row.get("grav5")),
                    self._fmt_money(row.get("iva5")),
                    self._fmt_money(row.get("grav10")),
                    self._fmt_money(row.get("iva10")),
                    self._fmt_money(row.get("total")),
                    self._fmt_money(row.get("total_ret")),
                ),
            )
        self._update_pendiente_kpi()

    # =======================
    #  Al seleccionar factura
    # =======================
    def _on_invoice_select(self, _event=None):
        sel = self.tv_inv.selection()
        if not sel:
            self._set_cheque_badge(None, cobrado=False)
            self._set_cobro_btn(False, enabled=False)
            if hasattr(self, "btn_edit_item"):
                self.btn_edit_item.config(state=tk.DISABLED)
            return
        iid = sel[0]
        meta = self._inv_row_meta.get(iid)
        if not meta:
            self._set_cheque_badge(None, cobrado=False)
            self._set_cobro_btn(False, enabled=False)
            if hasattr(self, "btn_edit_item"):
                self.btn_edit_item.config(state=tk.DISABLED)
            return
        cobrado = self._is_cobrado(meta)
        self._set_cobro_btn(cobrado, enabled=True)
        self._set_cheque_badge(self._dias_desde_fecha(meta.get("ts")), cobrado=cobrado)
        if hasattr(self, "btn_edit_item"):
            self.btn_edit_item.config(state=(tk.NORMAL if meta.get("tipo") == "std" else tk.DISABLED))
        if meta.get("tipo") == "bag":
            self._load_bag_sale_items(meta.get("sale_id"))
        else:
            self._load_invoice_items(meta.get("invoice_id"))

    # =======================
    #  Editar encabezado
    # =======================
    def _edit_header(self):
        sel = self.tv_inv.selection()
        if not sel:
            messagebox.showinfo("Info", "Seleccione primero una factura.")
            return
        meta = self._inv_row_meta.get(sel[0])
        if not meta:
            messagebox.showerror("Error", "No se pudo leer la factura seleccionada.")
            return

        nro_actual = meta.get("nro") or ""
        cli_actual = meta.get("cliente") or ""

        dlg = tk.Toplevel(self.app)
        dlg.title("Editar factura / cliente")
        dlg.transient(self.app)
        dlg.resizable(False, False)

        ttk.Label(dlg, text="Nro de factura:").grid(row=0, column=0, sticky="e", padx=8, pady=(10, 4))
        ent_nro = ttk.Entry(dlg, width=30)
        ent_nro.grid(row=0, column=1, sticky="w", padx=6, pady=(10, 4))
        ent_nro.insert(0, nro_actual)

        ttk.Label(dlg, text="Cliente:").grid(row=1, column=0, sticky="e", padx=8, pady=4)
        ent_cli = ttk.Entry(dlg, width=30)
        ent_cli.grid(row=1, column=1, sticky="w", padx=6, pady=4)
        ent_cli.insert(0, cli_actual)

        btns = ttk.Frame(dlg)
        btns.grid(row=2, column=0, columnspan=2, pady=10)

        def _guardar():
            nuevo_nro = (ent_nro.get() or "").strip()
            nuevo_cli = (ent_cli.get() or "").strip()
            try:
                if meta.get("tipo") == "bag":
                    self.repo.actualizar_venta_bolsa_header(int(meta.get("sale_id")), nuevo_nro, nuevo_cli)
                else:
                    self.repo.actualizar_factura_header(int(meta.get("invoice_id")), nuevo_nro, nuevo_cli)
                messagebox.showinfo("OK", "Encabezado actualizado.")
                dlg.destroy()
                target_iid = meta.get("iid")
                self._refresh()
                if target_iid in self._inv_row_meta:
                    self.tv_inv.selection_set(target_iid)
                    self.tv_inv.see(target_iid)
                    self._on_invoice_select()
                if hasattr(self.app, "_refresh_resumenes"):
                    self.app._refresh_resumenes()
            except Exception as exc:
                messagebox.showerror("Error", str(exc))

        ttk.Button(btns, text="Guardar cambios", command=_guardar).pack(side="left", padx=6)
        ttk.Button(btns, text="Cancelar", command=dlg.destroy).pack(side="left", padx=6)

    # =======================
    #  Cargar detalle factura
    # =======================
    def _load_invoice_items(self, invoice_id):
        for i in self.tv_det.get_children():
            self.tv_det.delete(i)
        self._det_row_meta = {}

        with db.connection("fraccionadora") as cn:
            cur = cn.cursor()
            sql = """
                SELECT sii.id, p.name, sii.gramaje, sii.cantidad, sii.price_gs, sii.iva, sii.line_total
                FROM sales_invoice_items sii
                JOIN products p ON p.id = sii.product_id
                WHERE sii.invoice_id = %s
                ORDER BY p.name, sii.gramaje;
            """
            cur.execute(sql, (invoice_id,))
            rows = cur.fetchall()
        for item_id, pname, gram, cant, precio, iva, total in rows:
            iid = self.tv_det.insert("", "end",
                values=(pname, gram, cant,
                        f"{precio:,.0f}".replace(",", "."),
                        iva,
                        f"{total:,.0f}".replace(",", ".")))
            self._det_row_meta[iid] = {
                "item_id": int(item_id),
                "invoice_id": int(invoice_id),
                "producto": str(pname or ""),
                "gramaje": int(gram or 0),
                "cantidad": int(cant or 0),
                "precio": float(precio or 0),
                "iva": int(iva or 10),
                "total": float(total or 0),
            }

    def _load_bag_sale_items(self, sale_id):
        for i in self.tv_det.get_children():
            self.tv_det.delete(i)
        self._det_row_meta = {}
        if not sale_id:
            return
        row = self.repo.get_bag_sale(int(sale_id))
        if not row:
            return
        (_sid, _ts, pname, bolsas, kg_bolsa, kg_total,
         price_bolsa, total, _cliente, _factura, notas) = row
        self.tv_det.insert(
            "",
            "end",
            values=(
                pname,
                f"{float(kg_bolsa):.3f} kg",
                int(bolsas),
                f"{float(price_bolsa):,.0f}".replace(",", "."),
                "-",
                f"{float(total):,.0f}".replace(",", "."),
            ),
        )
        if notas:
            self.tv_det.insert("", "end",
                values=(f"Notas: {notas}", "", "", "", "", ""))

    def _edit_invoice_item(self):
        sel_inv = self.tv_inv.selection()
        if not sel_inv:
            messagebox.showinfo("Info", "Seleccione primero una factura.")
            return
        meta_inv = self._inv_row_meta.get(sel_inv[0])
        if not meta_inv or meta_inv.get("tipo") != "std":
            messagebox.showinfo("Info", "La edición de ítems aplica solo a facturas estándar.")
            return

        sel_det = self.tv_det.selection()
        if not sel_det:
            messagebox.showinfo("Info", "Seleccione un ítem en el detalle de la factura.")
            return
        meta_det = self._det_row_meta.get(sel_det[0])
        if not meta_det:
            messagebox.showerror("Error", "No se pudo leer el ítem seleccionado.")
            return

        dlg = tk.Toplevel(self.app)
        dlg.title("Editar ítem de factura")
        dlg.transient(self.app)
        dlg.resizable(False, False)

        ttk.Label(dlg, text="Producto:").grid(row=0, column=0, sticky="e", padx=8, pady=(10, 4))
        ttk.Label(dlg, text=f"{meta_det['producto']} ({meta_det['gramaje']} g)").grid(
            row=0, column=1, sticky="w", padx=6, pady=(10, 4)
        )

        ttk.Label(dlg, text="Cantidad:").grid(row=1, column=0, sticky="e", padx=8, pady=4)
        ent_qty = ttk.Entry(dlg, width=20)
        ent_qty.grid(row=1, column=1, sticky="w", padx=6, pady=4)
        ent_qty.insert(0, str(meta_det["cantidad"]))

        ttk.Label(dlg, text="Precio unitario (Gs):").grid(row=2, column=0, sticky="e", padx=8, pady=4)
        ent_price = ttk.Entry(dlg, width=20)
        ent_price.grid(row=2, column=1, sticky="w", padx=6, pady=4)
        ent_price.insert(0, f"{meta_det['precio']:,.0f}".replace(",", "."))

        ttk.Label(dlg, text="Motivo (obligatorio):").grid(row=3, column=0, sticky="ne", padx=8, pady=4)
        txt_mot = tk.Text(dlg, width=42, height=4)
        txt_mot.grid(row=3, column=1, sticky="w", padx=6, pady=4)

        btns = ttk.Frame(dlg)
        btns.grid(row=4, column=0, columnspan=2, pady=10)

        def _guardar():
            qty_txt = (ent_qty.get() or "").strip()
            price_txt = (ent_price.get() or "").strip()
            motivo = (txt_mot.get("1.0", "end") or "").strip()
            try:
                qty = int(qty_txt)
            except Exception:
                messagebox.showerror("Error", "Cantidad inválida.")
                return
            try:
                price = float(price_txt.replace(".", "").replace(",", "."))
            except Exception:
                messagebox.showerror("Error", "Precio inválido.")
                return
            if not motivo:
                messagebox.showerror("Error", "Debe escribir el motivo del cambio.")
                return

            try:
                self.repo.actualizar_factura_item(meta_det["item_id"], qty, price, motivo)
                messagebox.showinfo("OK", "Ítem actualizado correctamente.")
                dlg.destroy()
                target_iid = meta_inv.get("iid")
                self._refresh()
                if target_iid in self._inv_row_meta:
                    self.tv_inv.selection_set(target_iid)
                    self.tv_inv.see(target_iid)
                    self._on_invoice_select()
                if hasattr(self.app, "_refresh_ventas_grid"):
                    self.app._refresh_ventas_grid()
                if hasattr(self.app, "_refresh_resumenes"):
                    self.app._refresh_resumenes()
            except Exception as exc:
                messagebox.showerror("Error", str(exc))

        ttk.Button(btns, text="Guardar cambios", command=_guardar).pack(side="left", padx=6)
        ttk.Button(btns, text="Cancelar", command=dlg.destroy).pack(side="left", padx=6)

    # =======================
    #   Helpers
    # =======================
    def _ensure_collection_storage(self, cn):
        db.run_ddl(cn,
            """
            CREATE TABLE IF NOT EXISTS dashboard_collection_flags(
                status_key TEXT PRIMARY KEY,
                invoice_id INTEGER NOT NULL,
                invoice_ts TEXT,
                invoice_no TEXT,
                collected INTEGER NOT NULL DEFAULT 0,
                updated_ts TEXT
            );

            CREATE TABLE IF NOT EXISTS dashboard_collection_details(
                id BIGSERIAL PRIMARY KEY,
                invoice_id INTEGER NOT NULL,
                invoice_ts TEXT,
                invoice_no TEXT,
                cliente TEXT,
                monto_total_gs REAL NOT NULL DEFAULT 0,
                monto_total_ret_gs REAL NOT NULL DEFAULT 0,
                fecha_cobro TEXT,
                medio TEXT,
                nro_cheque TEXT,
                nro_deposito TEXT,
                referencia TEXT,
                observacion TEXT,
                ts_registro TEXT,
                ts_modificacion TEXT
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_dash_collection_detail_invoice
                ON dashboard_collection_details(invoice_id, invoice_ts, invoice_no);
            """
        )

    def _collection_keys(self, invoice_id, ts, nro):
        inv_id = int(invoice_id or 0)
        ts_txt = str(ts or "").strip()
        nro_txt = str(nro or "").strip()
        return [f"std:{inv_id}:{ts_txt}:{nro_txt}", f"std:{inv_id}"]

    def _load_cobranzas_from_db(self):
        with db.connection("fraccionadora") as cn:
            self._ensure_collection_storage(cn)
            cur = cn.cursor()
            cur.execute(
                "SELECT status_key, invoice_id, invoice_ts, invoice_no, collected FROM dashboard_collection_flags;"
            )
            out = {}
            for status_key, invoice_id, invoice_ts, invoice_no, collected in cur.fetchall():
                status_val = bool(collected)
                if status_key:
                    out[str(status_key)] = status_val
                for key in self._collection_keys(invoice_id, invoice_ts, invoice_no):
                    out[key] = status_val
            return out

    def _save_std_cobranza_to_db(self, meta, cobrado):
        invoice_id = int(meta.get("invoice_id") or 0)
        invoice_ts = str(meta.get("ts") or "").strip()
        invoice_no = str(meta.get("nro") or "").strip()
        if invoice_id <= 0:
            return
        with db.connection("fraccionadora") as cn:
            self._ensure_collection_storage(cn)
            cur = cn.cursor()
            for key in self._collection_keys(invoice_id, invoice_ts, invoice_no):
                cur.execute(
                    """
                    INSERT INTO dashboard_collection_flags(status_key, invoice_id, invoice_ts, invoice_no, collected, updated_ts)
                    VALUES(%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT(status_key) DO UPDATE SET
                        invoice_id=excluded.invoice_id,
                        invoice_ts=excluded.invoice_ts,
                        invoice_no=excluded.invoice_no,
                        collected=excluded.collected,
                        updated_ts=CURRENT_TIMESTAMP;
                    """,
                    (key, invoice_id, invoice_ts, invoice_no, 1 if cobrado else 0),
                )

    def _cargar_cobranzas(self):
        cobranzas_db = {}
        try:
            cobranzas_db = self._load_cobranzas_from_db()
        except Exception:
            cobranzas_db = {}
        COBROS_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not COBROS_FILE.exists():
            self.cobranzas = cobranzas_db
            return
        try:
            with COBROS_FILE.open("r", encoding="utf-8") as f:
                raw = json.load(f)
                self.cobranzas = raw if isinstance(raw, dict) else {}
        except Exception:
            self.cobranzas = {}
        self.cobranzas.update(cobranzas_db)
        # Limpieza de marcador antiguo (si existe)
        if "__default_cobrado_before__" in self.cobranzas:
            self.cobranzas.pop("__default_cobrado_before__", None)
            self._guardar_cobranzas()

    def _guardar_cobranzas(self):
        try:
            COBROS_FILE.parent.mkdir(parents=True, exist_ok=True)
            with COBROS_FILE.open("w", encoding="utf-8") as f:
                json.dump(self.cobranzas, f, indent=2, ensure_ascii=False)
        except Exception as exc:
            print("Error guardando cobranzas:", exc)

    def _cobro_key(self, meta, legacy=False):
        if not meta:
            return None
        tipo = (meta.get("tipo") or "").strip()
        rid = meta.get("sale_id") if tipo == "bag" else meta.get("invoice_id")
        if rid is None:
            return None
        if legacy:
            return f"{tipo}:{int(rid)}"
        ts = str(meta.get("ts") or "").strip()
        nro = str(meta.get("nro") or "").strip()
        return f"{tipo}:{int(rid)}:{ts}:{nro}"

    def _is_cobrado(self, meta):
        key_new = self._cobro_key(meta, legacy=False)
        key_old = self._cobro_key(meta, legacy=True)
        if not key_new and not key_old:
            return False
        if key_new in self.cobranzas:
            return bool(self.cobranzas.get(key_new, False))
        if key_old in self.cobranzas:
            return bool(self.cobranzas.get(key_old, False))
        # Regla: por defecto toda venta es pendiente hasta que se marque cobrada.
        return False

    def _set_cobro_btn(self, cobrado, enabled=True):
        if not hasattr(self, "btn_cobro"):
            return
        self.btn_cobro.config(
            text=("Cobrado" if cobrado else "Pendiente"),
            state=(tk.NORMAL if enabled else tk.DISABLED),
        )

    def _toggle_cobrado(self):
        sel = self.tv_inv.selection()
        if not sel:
            messagebox.showinfo("Info", "Seleccione primero una factura.")
            return
        meta = self._inv_row_meta.get(sel[0])
        if not meta:
            messagebox.showerror("Error", "No se pudo leer la factura seleccionada.")
            return
        key = self._cobro_key(meta, legacy=False)
        if not key:
            messagebox.showerror("Error", "No se pudo identificar el registro.")
            return
        actual = self._is_cobrado(meta)
        nuevo = not actual
        self.cobranzas[key] = nuevo
        # Compatibilidad: si existia una clave antigua por solo ID, la mantenemos sincronizada.
        key_old = self._cobro_key(meta, legacy=True)
        if key_old:
            self.cobranzas[key_old] = nuevo
        if meta.get("tipo") == "std":
            try:
                self._save_std_cobranza_to_db(meta, nuevo)
            except Exception as exc:
                messagebox.showerror("Error", f"No se pudo sincronizar el cobro con dashboard:\n{exc}")
                return
        self._guardar_cobranzas()
        self.tv_inv.item(sel[0], tags=(self._age_tag(meta),))
        self._set_cobro_btn(nuevo, enabled=True)
        self._set_cheque_badge(self._dias_desde_fecha(meta.get("ts")), cobrado=nuevo)
        self._update_pendiente_kpi()

    def _on_ctrl_p(self, _event=None):
        sel = self.tv_inv.selection()
        if not sel:
            return "break"
        meta = self._inv_row_meta.get(sel[0])
        if not meta:
            return "break"
        if self._is_cobrado(meta):
            return "break"
        self._toggle_cobrado()
        return "break"

    def _dias_desde_fecha(self, fecha_str):
        if not fecha_str:
            return None
        txt = str(fecha_str).strip()
        formatos = ("%Y-%m-%d", "%d/%m/%Y")
        for fmt in formatos:
            try:
                dt = datetime.datetime.strptime(txt[:10], fmt)
                return (datetime.datetime.now().date() - dt.date()).days
            except Exception:
                continue
        try:
            dt = datetime.datetime.fromisoformat(txt.replace("Z", "").replace("T", " "))
            return (datetime.datetime.now().date() - dt.date()).days
        except Exception:
            return None

    def _age_tag(self, meta):
        if self._is_cobrado(meta):
            return "age_cobrado"
        dias = self._dias_desde_fecha(meta.get("ts") if meta else None)
        if dias is None or dias < 0:
            return "age_none"
        if dias <= 0:
            return "age_0"
        if dias == 1:
            return "age_1"
        if dias == 2:
            return "age_2"
        return "age_3"

    def _update_pendiente_kpi(self):
        pendiente_total = 0.0
        for row in self._inv_row_meta.values():
            if self._is_cobrado(row):
                continue
            try:
                pendiente_total += float(row.get("total_ret") or 0.0)
            except Exception:
                pass
        if hasattr(self, "lbl_pendiente_cobro"):
            self.lbl_pendiente_cobro.config(
                text=f"Pendiente de cobro (con retención): {self._fmt_money(pendiente_total)}"
            )

    def _set_cheque_badge(self, dias, cobrado=False):
        if not hasattr(self, "lbl_cheque"):
            return
        if cobrado:
            try:
                self.lbl_cheque.config(text="COBRADO", bg="#b9e6b9", fg="#0b4f2c")
            except Exception:
                pass
            return
        colors = {
            1: ("#d7ecff", "#0a3d62"),  # dia 0
            2: ("#ffe8b3", "#7a4a00"),  # dia 1
            3: ("#ffc285", "#8a3b00"),  # dia 2
            4: ("#ffb3b3", "#8b0000"),  # 3+ dias
        }
        if dias is None or dias < 0:
            bg, fg, txt = "#e5e7eb", "#111111", "SIN COBRAR: -"
        else:
            nivel = min(4, dias + 1)
            bg, fg = colors.get(nivel, ("#e5e7eb", "#111111"))
            fuego = " 🔥" if nivel == 4 else ""
            txt = f"SIN COBRAR: {dias} dias{fuego}"
        try:
            self.lbl_cheque.config(text=txt, bg=bg, fg=fg)
        except Exception:
            pass

    def _fmt_money(self, value):
        if value is None:
            return "-"
        try:
            return f"{float(value):,.0f}".replace(",", ".")
        except Exception:
            return "-"

    def _calc_total_retencion(self, total, iva5, iva10):
        try:
            total = float(total or 0)
        except Exception:
            total = 0.0
        try:
            iva5 = float(iva5 or 0)
        except Exception:
            iva5 = 0.0
        try:
            iva10 = float(iva10 or 0)
        except Exception:
            iva10 = 0.0
        return total - 0.3 * (iva5 + iva10)

    def _build_where_clause(self, alias, filtro_txt, desde, hasta):
        where = []
        params = []
        if filtro_txt:
            like = f"%{filtro_txt}%"
            where.append(f"(COALESCE({alias}.invoice_no, '') ILIKE %s OR COALESCE({alias}.customer, '') ILIKE %s)")
            params.extend([like, like])
        if desde:
            where.append(f"{alias}.ts >= %s")
            params.append(desde)
        if hasta:
            where.append(f"{alias}.ts <= %s")
            params.append(hasta)
        clause = ""
        if where:
            clause = " WHERE " + " AND ".join(where)
        return clause, params

    def _fetch_invoice_rows(self, filtro_txt, desde, hasta):
        clause, params = self._build_where_clause("si", filtro_txt, desde, hasta)
        sql = f"""
            SELECT si.id, si.ts, si.invoice_no, si.customer,
                   si.gravada5_gs, si.iva5_gs, si.gravada10_gs, si.iva10_gs, si.total_gs
            FROM sales_invoices si
            {clause}
            ORDER BY si.ts DESC;
        """
        with db.connection("fraccionadora") as cn:
            cur = cn.cursor()
            cur.execute(sql, params)
            fetched = cur.fetchall()
        rows = []
        for fid, fecha, nro, cli, g5, i5, g10, i10, tot in fetched:
            rows.append({
                "tipo": "std",
                "iid": f"std-{fid}",
                "display_id": fid,
                "ts": fecha,
                "nro": nro,
                "cliente": cli,
                "grav5": g5,
                "iva5": i5,
                "grav10": g10,
                "iva10": i10,
                "total": tot,
                "total_ret": self._calc_total_retencion(tot, i5, i10),
                "invoice_id": fid,
            })
        return rows

    def _fetch_bag_sale_rows(self, filtro_txt, desde, hasta):
        clause, params = self._build_where_clause("bs", filtro_txt, desde, hasta)
        sql = f"""
            SELECT bs.id, bs.ts, bs.invoice_no, bs.customer, bs.total_gs
            FROM bag_sales bs
            {clause}
            ORDER BY bs.ts DESC;
        """
        with db.connection("fraccionadora") as cn:
            cur = cn.cursor()
            cur.execute(sql, params)
            fetched = cur.fetchall()
        rows = []
        for sid, fecha, nro, cli, total in fetched:
            rows.append({
                "tipo": "bag",
                "iid": f"bag-{sid}",
                "display_id": f"Bag-{sid}",
                "ts": fecha,
                "nro": nro,
                "cliente": cli,
                "grav5": None,
                "iva5": None,
                "grav10": None,
                "iva10": None,
                "total": total,
                "total_ret": self._calc_total_retencion(total, None, None),
                "sale_id": sid,
            })
        return rows

    # =======================
    #   Enviar a Google Sheet
    # =======================
    def _preview_send_sheet(self):
        sel = self.tv_inv.selection()
        if not sel:
            messagebox.showinfo("Info", "Seleccione primero una factura.")
            return

        meta = self._inv_row_meta.get(sel[0])
        if not meta:
            messagebox.showerror("Error", "No se pudo leer la factura seleccionada.")
            return
        if meta.get("tipo") != "std":
            messagebox.showinfo("Info", "Por ahora solo se envian facturas estandar.")
            return

        try:
            payload = self._build_sheet_payload(meta)
        except Exception as exc:
            messagebox.showerror("Error", f"No se pudo preparar la fila:\n{exc}")
            return

        vista = "\n".join([
            f"Mes: {payload['mes']}",
            f"Cliente: {payload['cliente']}",
            f"Nro factura: {payload['factura']}",
            f"Fecha: {payload['fecha']}",
            f"Remision: {payload['remision']}",
            f"Estado: {payload['estado']}",
            f"Cobranza: {payload['cobranza']}",
            f"Recibo: {payload['recibo']}",
            f"Total venta: {self._fmt_money(payload['total'])}",
            f"IVA total: {self._fmt_money(payload['iva_total'])}",
        ])

        if not messagebox.askyesno("Enviar a Google Sheets", f"Se enviara la siguiente fila:\n\n{vista}\n\nContinuar?"):
            return

        sheet_mod, err = _load_sheet_module()
        if not sheet_mod:
            messagebox.showerror("Error", f"No se pudo importar sheets/sheet.py:\n{err}")
            return

        try:
            sheet_mod.append_factura(payload)
            messagebox.showinfo("OK", "Factura enviada a Google Sheets.")
        except Exception as exc:
            messagebox.showerror("Error", f"No se pudo enviar a Google Sheets:\n{exc}")

    def preview_send_sheet_for_invoice(self, invoice_id):
        try:
            fid = int(invoice_id)
        except Exception:
            messagebox.showerror("Error", "ID de factura invalido.")
            return

        target_iid = f"std-{fid}"
        self._refresh()
        if target_iid not in self._inv_row_meta:
            messagebox.showerror("Error", "No se encontro la factura recien emitida en el historial.")
            return

        self.tv_inv.selection_set(target_iid)
        self.tv_inv.focus(target_iid)
        self.tv_inv.see(target_iid)
        self._on_invoice_select()
        self._preview_send_sheet()

    def _build_sheet_payload(self, meta):
        def _num(val):
            try:
                return float(val or 0)
            except Exception:
                return 0.0

        def _text(val):
            if val is None:
                return ""
            if isinstance(val, datetime.datetime):
                return val.isoformat(sep=" ", timespec="seconds")
            if isinstance(val, datetime.date):
                return val.isoformat()
            return str(val).strip()

        meses = ["ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO","JULIO","AGOSTO","SEPTIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE"]
        fecha_iso = _text(meta.get("ts"))
        fecha_fmt = fecha_iso
        mes_txt = ""
        try:
            dt = datetime.datetime.fromisoformat(fecha_iso.replace("Z","").replace("T"," "))
            fecha_fmt = dt.strftime("%d/%m/%Y")
            mes_txt = meses[dt.month - 1]
        except Exception:
            # fallback basico si no parsea
            parts = fecha_iso.split("-")
            if len(parts) >= 2 and parts[1].isdigit():
                try:
                    idx = int(parts[1])
                    if 1 <= idx <= 12:
                        mes_txt = meses[idx - 1]
                        if len(parts) >= 3 and parts[2].split():
                            dia = parts[2].split()[0]
                            if dia.isdigit():
                                fecha_fmt = f"{int(dia):02d}/{int(parts[1]):02d}/{parts[0]}"
                except Exception:
                    mes_txt = ""

        allowed_clients = {"LUQUE","AREGUA","ITAUGUA"}
        raw_cli = _text(meta.get("cliente")).upper()
        cliente = raw_cli if raw_cli in allowed_clients else "LUQUE"

        iva_total = _num(meta.get("iva5")) + _num(meta.get("iva10"))
        extra2 = _num(meta.get("total")) - 0.3 * iva_total
        return {
            "mes": mes_txt,
            "cliente": cliente,
            "factura": _text(meta.get("nro")),
            "fecha": fecha_fmt,
            "remision": "Listo",
            "estado": "Entregado",
            "cobranza": "Sin OP",
            "recibo": "",
            "extra1": "",
            "extra2": "",
            "total": _num(meta.get("total")),
            "iva_total": iva_total,
            "extra2": extra2,
        }

    # =======================
    #   Exportaciones
    # =======================
    def _collect_export_rows(self):
        ftxt = (self.ent_filtro.get() or "").strip()
        d1 = (self.ent_desde.get() or "").strip()
        d2 = (self.ent_hasta.get() or "").strip()

        rows = []
        rows.extend(self._fetch_invoice_rows(ftxt, d1, d2))
        rows.extend(self._fetch_bag_sale_rows(ftxt, d1, d2))
        rows.sort(key=lambda r: r.get("ts") or "", reverse=True)
        return rows

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

    def _build_export_table(self):
        rows = self._collect_export_rows()
        headers = [
            "tipo",
            "id",
            "fecha",
            "nro_factura",
            "cliente",
            "grav5",
            "iva5",
            "grav10",
            "iva10",
            "total",
        ]
        out = []
        for row in rows:
            out.append((
                row.get("tipo"),
                row.get("display_id"),
                row.get("ts") or "",
                row.get("nro") or "",
                row.get("cliente") or "",
                row.get("grav5") if row.get("grav5") is not None else "",
                row.get("iva5") if row.get("iva5") is not None else "",
                row.get("grav10") if row.get("grav10") is not None else "",
                row.get("iva10") if row.get("iva10") is not None else "",
                row.get("total") if row.get("total") is not None else "",
            ))
        return headers, out

    def _export_csv(self):
        headers, rows = self._build_export_table()
        stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        fname = f"ventas_{stamp}.csv"
        path = self._write_csv(headers, rows, fname)
        messagebox.showinfo("Exportado", f"Archivo guardado: {path}")

    def _export_excel(self):
        headers, rows = self._build_export_table()
        stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        fname = f"ventas_{stamp}.xlsx"
        ok, info = self._write_xlsx(headers, rows, fname)
        if ok:
            messagebox.showinfo("Exportado", f"Archivo guardado: {info}")
            return
        # Fallback a CSV si no hay openpyxl
        fname_csv = f"ventas_{stamp}.csv"
        path = self._write_csv(headers, rows, fname_csv)
        messagebox.showinfo("Exportado", f"No se pudo crear Excel ({info}). CSV guardado: {path}")

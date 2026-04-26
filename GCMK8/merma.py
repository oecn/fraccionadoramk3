# merma.py
# -*- coding: utf-8 -*-
import datetime as _dt
import tkinter as tk
from tkinter import ttk, messagebox


class TabMerma:
    """
    Pestana para registrar mermas (material no utilizable) por lote.
    """

    def __init__(self, parent, nb, repo):
        self.parent = parent
        self.repo = repo
        self._selected_lot_id = None
        self._lot_iid_to_id: dict[str, int] = {}

        frame = ttk.Frame(nb)
        nb.add(frame, text="Mermas")
        self.frame = frame

        paned = ttk.Panedwindow(frame, orient="horizontal")
        paned.pack(fill="both", expand=True, padx=8, pady=8)

        # ----- panel izquierdo: lotes -----
        left = ttk.Labelframe(paned, text="Lotes cargados")
        paned.add(left, weight=1)

        filtros = ttk.Frame(left)
        filtros.pack(fill="x", padx=6, pady=(4, 2))
        ttk.Label(filtros, text="Producto:").pack(side="left")
        self.cb_filter_prod = ttk.Combobox(filtros, width=28, state="readonly")
        self.cb_filter_prod.pack(side="left", padx=(4, 8))
        self.cb_filter_prod.bind("<<ComboboxSelected>>", lambda *_: self.refresh_lotes())

        self.var_only_open = tk.BooleanVar(value=True)
        ttk.Checkbutton(filtros, text="Solo abiertos", variable=self.var_only_open,
                        command=self.refresh_lotes).pack(side="left")
        ttk.Button(filtros, text="Actualizar", command=self.refresh_lotes).pack(side="right")

        columns = ("producto", "lote", "kg_ini", "kg_saldo", "merma")
        self.tv_lotes = ttk.Treeview(left, columns=columns, show="headings", height=18, selectmode="browse")
        self.tv_lotes.heading("producto", text="Producto")
        self.tv_lotes.heading("lote", text="Lote")
        self.tv_lotes.heading("kg_ini", text="Kg iniciales")
        self.tv_lotes.heading("kg_saldo", text="Saldo (kg)")
        self.tv_lotes.heading("merma", text="Merma total (kg)")
        self.tv_lotes.column("producto", width=180, anchor="w")
        self.tv_lotes.column("lote", width=120, anchor="center")
        self.tv_lotes.column("kg_ini", width=100, anchor="e")
        self.tv_lotes.column("kg_saldo", width=100, anchor="e")
        self.tv_lotes.column("merma", width=130, anchor="e")
        self.tv_lotes.pack(side="left", fill="both", expand=True, padx=(6, 0), pady=(0, 6))
        scroll_lotes = ttk.Scrollbar(left, orient="vertical", command=self.tv_lotes.yview)
        scroll_lotes.pack(side="left", fill="y", pady=(0, 6))
        self.tv_lotes.configure(yscrollcommand=scroll_lotes.set)
        self.tv_lotes.bind("<<TreeviewSelect>>", self._on_select_lot)

        # ----- panel derecho: detalle -----
        right = ttk.Labelframe(paned, text="Detalle y registro de merma")
        paned.add(right, weight=2)

        self.lbl_lote_info = ttk.Label(right, text="Seleccione un lote para ver el detalle.")
        self.lbl_lote_info.pack(anchor="w", padx=8, pady=(6, 4))

        form = ttk.Frame(right)
        form.pack(fill="x", padx=8, pady=4)

        ttk.Label(form, text="Fecha (AAAA-MM-DD):").grid(row=0, column=0, sticky="w")
        self.ent_fecha = ttk.Entry(form, width=14)
        self.ent_fecha.grid(row=0, column=1, padx=(4, 12))
        self.ent_fecha.insert(0, _dt.date.today().strftime("%Y-%m-%d"))

        ttk.Label(form, text="Kg merma:").grid(row=0, column=2, sticky="w")
        self.ent_kg = ttk.Entry(form, width=10)
        self.ent_kg.grid(row=0, column=3, padx=(4, 12))

        ttk.Label(form, text="Motivo / nota:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        self.ent_motivo = ttk.Entry(form, width=60)
        self.ent_motivo.grid(row=1, column=1, columnspan=3, sticky="we", padx=(4, 12), pady=(6, 0))

        self.btn_registrar = ttk.Button(form, text="Registrar merma", command=self._registrar_merma, state=tk.DISABLED)
        self.btn_registrar.grid(row=0, column=4, rowspan=2, padx=(6, 0), pady=2, sticky="ns")

        self.lbl_total_merma = ttk.Label(right, text="Total merma del lote: 0.000 kg")
        self.lbl_total_merma.pack(anchor="w", padx=8, pady=(4, 2))

        box_mermas = ttk.Frame(right)
        box_mermas.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        cols = ("fecha", "kg", "motivo")
        self.tv_mermas = ttk.Treeview(box_mermas, columns=cols, show="headings", height=12)
        self.tv_mermas.heading("fecha", text="Fecha")
        self.tv_mermas.heading("kg", text="Kg")
        self.tv_mermas.heading("motivo", text="Motivo / nota")
        self.tv_mermas.column("fecha", width=150, anchor="center")
        self.tv_mermas.column("kg", width=80, anchor="e")
        self.tv_mermas.column("motivo", width=320, anchor="w")
        self.tv_mermas.pack(side="left", fill="both", expand=True)
        scroll_merma = ttk.Scrollbar(box_mermas, orient="vertical", command=self.tv_mermas.yview)
        scroll_merma.pack(side="right", fill="y")
        self.tv_mermas.configure(yscrollcommand=scroll_merma.set)

        self.refresh_products()
        self.refresh_lotes(keep_selected=False)

    # ----- Helpers -----
    def refresh_products(self):
        prods = ["Todos"] + [name for _, name in self.repo.list_products()]
        self.cb_filter_prod["values"] = prods
        if not self.cb_filter_prod.get():
            self.cb_filter_prod.set("Todos")

    def refresh_lotes(self, keep_selected: bool = True):
        current = self._selected_lot_id if keep_selected else None

        sel = (self.cb_filter_prod.get() or "Todos").strip()
        product_id = None
        if sel and sel != "Todos":
            product_id = self.repo.get_product_id_by_name(sel)

        solo_abiertos = bool(self.var_only_open.get())
        rows = self.repo.listar_lotes_con_merma(product_id, solo_abiertos)

        for iid in self.tv_lotes.get_children():
            self.tv_lotes.delete(iid)
        self._lot_iid_to_id.clear()

        for lot_id, prod, lote, kg_ini, kg_saldo, merma, cerrado, _ts in rows:
            iid = self.tv_lotes.insert(
                "", "end",
                values=(prod, lote or "-", self._fmt_kg(kg_ini), self._fmt_kg(kg_saldo), self._fmt_kg(merma))
            )
            self._lot_iid_to_id[iid] = lot_id
            if current and lot_id == current:
                self.tv_lotes.selection_set(iid)
                self.tv_lotes.focus(iid)

        if not self.tv_lotes.selection():
            self._selected_lot_id = None
            self._clear_lot_detail()

    def refresh_data(self):
        self.refresh_lotes()

    def _fmt_kg(self, value):
        try:
            return f"{float(value):.3f}"
        except Exception:
            return "-"

    def _clear_lot_detail(self):
        self.lbl_lote_info.config(text="Seleccione un lote para ver el detalle.")
        self.lbl_total_merma.config(text="Total merma del lote: 0.000 kg")
        self.btn_registrar.config(state=tk.DISABLED)
        for iid in self.tv_mermas.get_children():
            self.tv_mermas.delete(iid)

    def _on_select_lot(self, *_):
        sel = self.tv_lotes.selection()
        if not sel:
            self._selected_lot_id = None
            self._clear_lot_detail()
            return
        iid = sel[0]
        lot_id = self._lot_iid_to_id.get(iid)
        if not lot_id:
            return
        self._selected_lot_id = lot_id
        self.btn_registrar.config(state=tk.NORMAL)
        self._load_lot_detail(lot_id)

    def _load_lot_detail(self, lot_id: int):
        lot = self.repo.lot_detail(lot_id)
        if not lot:
            self._clear_lot_detail()
            return
        (_lid, lote_txt, product_id, product_name, kg_ini, kg_saldo,
         costo_total, costo_kg, proveedor, factura, ts, cerrado) = lot

        extra = []
        if proveedor:
            extra.append(f"Proveedor: {proveedor}")
        if factura:
            extra.append(f"Factura: {factura}")
        estado = "CERRADO" if cerrado else "abierto"
        info = (f"Lote {lote_txt or '-'} - {product_name} "
                f"(kg iniciales {self._fmt_kg(kg_ini)}, saldo {self._fmt_kg(kg_saldo)}, estado {estado})")
        if extra:
            info += " - " + " | ".join(extra)
        self.lbl_lote_info.config(text=info)

        total_merma = self.repo.total_merma_por_lote(lot_id)
        self.lbl_total_merma.config(text=f"Total merma del lote: {self._fmt_kg(total_merma)} kg")
        self._load_mermas(lot_id)

    def _load_mermas(self, lot_id: int):
        for iid in self.tv_mermas.get_children():
            self.tv_mermas.delete(iid)
        rows = self.repo.listar_mermas_de_lote(lot_id)
        for _mid, ts, kg, motivo in rows:
            fecha_txt = str(ts) if ts else ""
            self.tv_mermas.insert("", "end", values=(fecha_txt, self._fmt_kg(kg), motivo or ""))

    def _registrar_merma(self):
        lot_id = self._selected_lot_id
        if not lot_id:
            messagebox.showwarning("Mermas", "Seleccione un lote primero.")
            return
        kg_txt = (self.ent_kg.get() or "").strip().replace(",", ".")
        try:
            kg = float(kg_txt)
        except ValueError:
            messagebox.showerror("Mermas", "Ingrese un valor numerico para los kg de merma.")
            return
        if kg <= 0:
            messagebox.showerror("Mermas", "Los kg de merma deben ser mayores a cero.")
            return

        fecha = (self.ent_fecha.get() or "").strip()
        if fecha and not self._validar_fecha(fecha):
            messagebox.showerror("Mermas", "Formato de fecha invalido. Use AAAA-MM-DD u horario ISO.")
            return

        motivo = (self.ent_motivo.get() or "").strip()
        try:
            self.repo.registrar_merma_lote(lot_id, kg, fecha or None, motivo)
        except Exception as exc:
            messagebox.showerror("Mermas", f"No se pudo registrar la merma: {exc}")
            return

        self.ent_kg.delete(0, tk.END)
        self.ent_motivo.delete(0, tk.END)
        self.ent_kg.focus_set()
        self.refresh_lotes()
        self._load_lot_detail(lot_id)
        messagebox.showinfo("Mermas", "Merma registrada correctamente.")

    def _validar_fecha(self, fecha_txt: str) -> bool:
        try:
            if len(fecha_txt) == 10:
                _dt.datetime.strptime(fecha_txt, "%Y-%m-%d")
            else:
                _dt.datetime.fromisoformat(fecha_txt)
            return True
        except Exception:
            return False

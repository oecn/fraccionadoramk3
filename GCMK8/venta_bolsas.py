# -*- coding: utf-8 -*-
import datetime as _dt
import tkinter as tk
from tkinter import ttk, messagebox


class TabVentaBolsas:
    """
    Pestana pensada para registrar ventas de bolsas de materia prima (sin fraccionar).

    - Descuenta kilos de raw_stock mediante Repo.registrar_venta_bolsas
    - Guarda un pequeno historial para referencia y conciliacion
    """

    def __init__(self, parent_app, notebook, bag_options=None):
        self.app = parent_app
        self.repo = parent_app.repo
        self.bag_options = bag_options or []

        frame = ttk.Frame(notebook)
        notebook.add(frame, text="Venta bolsas")
        self.frame = frame

        self._build_form(frame)
        self._build_historial(frame)
        self.refresh_products()
        self._refresh_historial()

    # ------------------------------------------------------------------
    # UI builders
    # ------------------------------------------------------------------
    def _build_form(self, parent):
        box = ttk.LabelFrame(parent, text="Registrar venta de bolsas")
        box.pack(fill="x", padx=8, pady=8)

        row = 0
        ttk.Label(box, text="Producto:").grid(row=row, column=0, sticky="w", padx=4, pady=4)
        self.cb_prod = ttk.Combobox(box, state="readonly", width=30, values=[])
        self.cb_prod.grid(row=row, column=1, sticky="w", padx=4, pady=4)

        ttk.Label(box, text="Fecha (AAAA-MM-DD):").grid(row=row, column=2, sticky="w", padx=4, pady=4)
        self.ent_fecha = ttk.Entry(box, width=12)
        self.ent_fecha.grid(row=row, column=3, sticky="w", padx=4, pady=4)
        self.ent_fecha.insert(0, _dt.date.today().strftime("%Y-%m-%d"))

        row += 1
        ttk.Label(box, text="Kg por bolsa:").grid(row=row, column=0, sticky="w", padx=4, pady=4)
        bolsa_vals = [str(v) for v in self.bag_options] + ["Otro"]
        self.cb_bolsa = ttk.Combobox(box, state="readonly", width=10, values=bolsa_vals)
        self.cb_bolsa.grid(row=row, column=1, sticky="w", padx=4, pady=4)
        self.cb_bolsa.bind("<<ComboboxSelected>>", self._toggle_otro_bolsa)
        self.ent_bolsa_otro = ttk.Entry(box, width=10, state="disabled")
        self.ent_bolsa_otro.grid(row=row, column=2, sticky="w", padx=4, pady=4)

        row += 1
        ttk.Label(box, text="Bolsas a vender:").grid(row=row, column=0, sticky="w", padx=4, pady=4)
        self.ent_bolsas = ttk.Entry(box, width=10)
        self.ent_bolsas.grid(row=row, column=1, sticky="w", padx=4, pady=4)
        self.ent_bolsas.bind("<KeyRelease>", self._update_total_label)

        ttk.Label(box, text="Precio por bolsa (Gs):").grid(row=row, column=2, sticky="w", padx=4, pady=4)
        self.ent_precio = ttk.Entry(box, width=12)
        self.ent_precio.grid(row=row, column=3, sticky="w", padx=4, pady=4)
        self.ent_precio.bind("<KeyRelease>", self._update_total_label)

        row += 1
        ttk.Label(box, text="Cliente:").grid(row=row, column=0, sticky="w", padx=4, pady=4)
        self.ent_cliente = ttk.Entry(box, width=30)
        self.ent_cliente.grid(row=row, column=1, sticky="w", padx=4, pady=4)

        ttk.Label(box, text="Nro factura:").grid(row=row, column=2, sticky="w", padx=4, pady=4)
        self.ent_factura = ttk.Entry(box, width=15)
        self.ent_factura.grid(row=row, column=3, sticky="w", padx=4, pady=4)

        row += 1
        ttk.Label(box, text="Notas:").grid(row=row, column=0, sticky="nw", padx=4, pady=4)
        self.txt_notas = tk.Text(box, width=55, height=3)
        self.txt_notas.grid(row=row, column=1, columnspan=3, sticky="we", padx=4, pady=4)

        row += 1
        self.lbl_total = ttk.Label(box, text="Total estimado: 0 Gs", font=("TkDefaultFont", 10, "bold"))
        self.lbl_total.grid(row=row, column=0, columnspan=2, sticky="w", padx=4, pady=4)

        ttk.Button(box, text="Registrar venta", command=self._registrar_venta).grid(
            row=row, column=3, sticky="e", padx=4, pady=4
        )

        row += 1
        self.lbl_status = ttk.Label(box, text="", foreground="#2f4f4f")
        self.lbl_status.grid(row=row, column=0, columnspan=4, sticky="w", padx=4, pady=(0, 4))

        for col in range(4):
            box.grid_columnconfigure(col, weight=1 if col == 1 else 0)

    def _build_historial(self, parent):
        box = ttk.LabelFrame(parent, text="Historial de ventas de bolsas")
        box.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        filtros = ttk.Frame(box)
        filtros.pack(fill="x", pady=4)
        ttk.Label(filtros, text="Desde:").pack(side="left")
        self.ent_filtro_desde = ttk.Entry(filtros, width=12)
        self.ent_filtro_desde.pack(side="left", padx=4)
        ttk.Label(filtros, text="Hasta:").pack(side="left")
        self.ent_filtro_hasta = ttk.Entry(filtros, width=12)
        self.ent_filtro_hasta.pack(side="left", padx=4)
        ttk.Label(filtros, text="Buscar (producto/cliente/factura):").pack(side="left", padx=(12, 4))
        self.ent_filtro_texto = ttk.Entry(filtros, width=25)
        self.ent_filtro_texto.pack(side="left", padx=4)
        ttk.Button(filtros, text="Refrescar", command=self._refresh_historial).pack(side="left", padx=6)

        cols = ("fecha", "producto", "bolsas", "kg_bolsa", "kg_total",
                "precio_bolsa", "total", "cliente", "factura", "nota")
        self.tv_hist = ttk.Treeview(box, columns=cols, show="headings", height=12)
        headings = [
            ("fecha", "Fecha", 140, "w"),
            ("producto", "Producto", 180, "w"),
            ("bolsas", "Bolsas", 70, "center"),
            ("kg_bolsa", "Kg/bolsa", 80, "center"),
            ("kg_total", "Kg total", 90, "center"),
            ("precio_bolsa", "Precio bolsa", 110, "e"),
            ("total", "Total (Gs)", 110, "e"),
            ("cliente", "Cliente", 140, "w"),
            ("factura", "Factura", 100, "w"),
            ("nota", "Notas", 160, "w"),
        ]
        for col, txt, width, anchor in headings:
            self.tv_hist.heading(col, text=txt)
            self.tv_hist.column(col, width=width, anchor=anchor)

        vsb = ttk.Scrollbar(box, orient="vertical", command=self.tv_hist.yview)
        self.tv_hist.configure(yscrollcommand=vsb.set)
        self.tv_hist.pack(side="left", fill="both", expand=True, padx=(6, 0), pady=4)
        vsb.pack(side="right", fill="y", padx=(0, 6), pady=4)

        self.lbl_hist_resumen = ttk.Label(box, text="0 movimientos")
        self.lbl_hist_resumen.pack(anchor="w", padx=8, pady=(0, 6))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def refresh_products(self):
        names = [name for _, name in self.repo.list_products()]
        self.cb_prod["values"] = names
        if names and not self.cb_prod.get():
            self.cb_prod.set(names[0])

    def _toggle_otro_bolsa(self, *_):
        if self.cb_bolsa.get() == "Otro":
            self.ent_bolsa_otro.configure(state="normal")
        else:
            self.ent_bolsa_otro.configure(state="disabled")
            self.ent_bolsa_otro.delete(0, tk.END)

    def _get_bag_kg(self):
        sel = (self.cb_bolsa.get() or "").strip()
        if not sel:
            return None
        if sel == "Otro":
            dato = (self.ent_bolsa_otro.get() or "").strip()
        else:
            dato = sel
        if not dato:
            return None
        try:
            return float(dato.replace(",", "."))
        except Exception:
            return None

    def _fmt_gs(self, val):
        try:
            return f"{float(val):,.0f}".replace(",", ".")
        except Exception:
            return "0"

    def _parse_float(self, txt):
        if not txt:
            return None
        return float(txt.replace(".", "").replace(",", "."))

    def _update_total_label(self, *_):
        try:
            bolsas = int((self.ent_bolsas.get() or "0"))
            precio = self._parse_float(self.ent_precio.get() or "0") or 0.0
            total = bolsas * precio
        except Exception:
            total = 0.0
        self.lbl_total.config(text=f"Total estimado: {self._fmt_gs(total)} Gs")

    def _set_status(self, msg):
        self.lbl_status.config(text=msg or "")

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------
    def _registrar_venta(self):
        prod = (self.cb_prod.get() or "").strip()
        if not prod:
            messagebox.showwarning("Venta de bolsas", "Seleccione un producto.")
            return
        pid = self.repo.get_product_id_by_name(prod)
        if pid is None:
            messagebox.showerror("Venta de bolsas", "Producto invalido.")
            return

        bag_kg = self._get_bag_kg()
        if bag_kg is None or bag_kg <= 0:
            messagebox.showwarning("Venta de bolsas", "Indique los kg por bolsa.")
            return

        try:
            bolsas = int((self.ent_bolsas.get() or "0"))
            if bolsas <= 0:
                raise ValueError
        except Exception:
            messagebox.showwarning("Venta de bolsas", "Cantidad de bolsas invalida.")
            return

        try:
            price = self._parse_float(self.ent_precio.get() or "")
            if price is None or price < 0:
                raise ValueError
        except Exception:
            messagebox.showwarning("Venta de bolsas", "Precio por bolsa invalido.")
            return

        fecha_txt = (self.ent_fecha.get() or "").strip()
        if fecha_txt:
            try:
                _dt.datetime.strptime(fecha_txt, "%Y-%m-%d")
            except ValueError:
                messagebox.showwarning("Venta de bolsas", "Fecha invalida. Use AAAA-MM-DD.")
                return
        else:
            fecha_txt = _dt.date.today().strftime("%Y-%m-%d")

        customer = (self.ent_cliente.get() or "").strip()
        invoice = (self.ent_factura.get() or "").strip()
        notas = self.txt_notas.get("1.0", "end").strip()

        try:
            self.repo.registrar_venta_bolsas(
                product_id=pid,
                bolsas=bolsas,
                kg_por_bolsa=bag_kg,
                price_por_bolsa=price,
                customer=customer,
                invoice_no=invoice,
                fecha=fecha_txt,
                notas=notas,
            )
        except Exception as e:
            messagebox.showerror("Venta de bolsas", str(e))
            return

        self._set_status(f"Venta registrada: {bolsas} bolsas de {prod}.")
        messagebox.showinfo("Venta de bolsas", "Venta registrada correctamente.")
        self._clear_form()
        self._refresh_historial()
        self._trigger_refreshes()

    def _clear_form(self):
        self.ent_bolsas.delete(0, tk.END)
        self.ent_precio.delete(0, tk.END)
        self.ent_cliente.delete(0, tk.END)
        self.ent_factura.delete(0, tk.END)
        self.txt_notas.delete("1.0", "end")
        if self.cb_bolsa.get() == "Otro":
            self.ent_bolsa_otro.delete(0, tk.END)
        self._update_total_label()

    def _trigger_refreshes(self):
        for attr in ("_refresh_raw", "_refresh_raw2", "_refresh_inventarios", "_refresh_resumenes"):
            fn = getattr(self.app, attr, None)
            if callable(fn):
                try:
                    fn()
                except Exception:
                    pass

    def _refresh_historial(self):
        self._refresh_historial_inner()

    def _refresh_historial_inner(self):
        for iid in self.tv_hist.get_children():
            self.tv_hist.delete(iid)

        desde = (self.ent_filtro_desde.get() or "").strip() or None
        hasta = (self.ent_filtro_hasta.get() or "").strip() or None
        texto = (self.ent_filtro_texto.get() or "").strip() or None

        rows = self.repo.listar_ventas_bolsas(desde=desde, hasta=hasta, texto=texto, limit=300)
        total_gs = 0.0
        for (sid, ts, prod, bolsas, kg_bolsa, kg_total,
             price_bolsa, total, cliente, factura, nota) in rows:
            total_gs += float(total or 0)
            self.tv_hist.insert(
                "",
                "end",
                iid=sid,
                values=(
                    ts or "",
                    prod,
                    bolsas,
                    f"{float(kg_bolsa):.3f}",
                    f"{float(kg_total):.3f}",
                    self._fmt_gs(price_bolsa),
                    self._fmt_gs(total),
                    cliente or "-",
                    factura or "-",
                    (nota or "")[:60],
                ),
            )

        self.lbl_hist_resumen.config(text=f"{len(rows)} movimientos - {self._fmt_gs(total_gs)} Gs")
        stripe = getattr(self.app, "_apply_treeview_striping", None)
        if callable(stripe):
            stripe(self.tv_hist)

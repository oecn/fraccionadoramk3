# tab_flujo_dinero.py
# -*- coding: utf-8 -*-
import json, os
import tkinter as tk
from tkinter import ttk, messagebox
import datetime as _dt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt


def _normalizar_seccion_json(seccion):
    """
    Normaliza una secciÃ³n (ventas/compras/gastos) para que siempre sea una lista de
    movimientos con fecha, monto y concepto (este Ãºltimo opcional).
    Soporta el formato anterior (dict fecha -> monto) y el nuevo formato (lista de dicts).
    """
    movs = []
    if isinstance(seccion, dict):
        for fecha, monto in seccion.items():
            movs.append({"fecha": fecha, "monto": monto, "concepto": ""})
    elif isinstance(seccion, list):
        for item in seccion:
            if not isinstance(item, dict):
                continue
            fecha = item.get("fecha") or item.get("date")
            monto = item.get("monto", item.get("valor"))
            if fecha is None or monto is None:
                continue
            concepto = item.get("concepto") or item.get("detalle") or item.get("descripcion") or ""
            movs.append({"fecha": fecha, "monto": monto, "concepto": concepto})
    return movs


def _normalizar_saldo_inicial(seccion):
    """
    Normaliza saldo inicial para usarlo como base del acumulado/banco.
    Soporta dict {fecha, monto} o lista de dicts.
    """
    movs = []
    if isinstance(seccion, dict):
        fecha = seccion.get("fecha") or seccion.get("date")
        monto = seccion.get("monto", seccion.get("valor"))
        if fecha is not None and monto is not None:
            movs.append({"fecha": fecha, "monto": monto})
    elif isinstance(seccion, list):
        for item in seccion:
            if not isinstance(item, dict):
                continue
            fecha = item.get("fecha") or item.get("date")
            monto = item.get("monto", item.get("valor"))
            if fecha is None or monto is None:
                continue
            movs.append({"fecha": fecha, "monto": monto})
    return movs


def cargar_datos_json(path="datos_iniciales.json"):
    """Carga un archivo JSON con ventas/compras/gastos iniciales (si existe)."""
    if not os.path.exists(path):
        print(f"[FlujoDinero] Archivo {path} no encontrado. Se omitirÃ¡ carga inicial.")
        return {"ventas": [], "compras": [], "gastos": [], "saldo_inicial": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        data = {
            "ventas":  _normalizar_seccion_json(raw.get("ventas", [])),
            "compras": _normalizar_seccion_json(raw.get("compras", [])),
            "gastos":  _normalizar_seccion_json(raw.get("gastos", [])),
            "saldo_inicial": _normalizar_saldo_inicial(raw.get("saldo_inicial", [])),
        }
        print(f"[FlujoDinero] Datos iniciales cargados desde {path}.")
        return data
    except Exception as e:
        print(f"[FlujoDinero] Error al leer {path}: {e}")
        return {"ventas": [], "compras": [], "gastos": [], "saldo_inicial": []}


class TabFlujoDinero:
    def __init__(self, parent, nb, repo):
        self.parent = parent
        self.repo = repo
        # Cargar datos iniciales (JSON)
        self.data_json = cargar_datos_json(os.path.join(os.path.dirname(__file__), "datos_iniciales.json"))

        frame = ttk.Frame(nb)
        nb.add(frame, text="Flujo de Dinero")
        self.frame = frame

        # AÃ±o actual por defecto
        self.year_var = tk.StringVar(value=str(_dt.date.today().year))

        # --- Filtros superiores ---
        top = ttk.Frame(frame)
        top.pack(fill="x", padx=6, pady=6)
        ttk.Label(top, text="AÃ±o:").pack(side="left")
        self.cb_year = ttk.Combobox(top, width=8, textvariable=self.year_var, state="readonly")
        self.cb_year.pack(side="left", padx=6)
        self.cb_year["values"] = self._get_years_available()
        ttk.Label(top, text="Trimestre:").pack(side="left")
        self.cb_trim = ttk.Combobox(
            top,
            width=16,
            state="readonly",
            values=["Todos", "T1 (Ene-Mar)", "T2 (Abr-Jun)", "T3 (Jul-Sep)", "T4 (Oct-Dic)"],
        )
        self.cb_trim.pack(side="left", padx=6)
        self.cb_trim.set("Todos")
        ttk.Label(top, text="Vista ventas:").pack(side="left")
        self.cb_venta_view = ttk.Combobox(
            top,
            width=24,
            state="readonly",
            values=["Total", "Con retencion IVA 30%"],
        )
        self.cb_venta_view.pack(side="left", padx=6)
        self.cb_venta_view.set("Total")
        ttk.Button(top, text="Refrescar", command=self._refresh_data).pack(side="left", padx=6)

        # --- Filtros de rango de fechas ---
        frm_filtros = ttk.Frame(frame)
        frm_filtros.pack(fill="x", padx=6, pady=(0,6))
        ttk.Label(frm_filtros, text="Desde (AAAA-MM-DD):").pack(side="left")
        self.ent_desde = ttk.Entry(frm_filtros, width=12)
        self.ent_desde.pack(side="left", padx=4)
        ttk.Label(frm_filtros, text="Hasta:").pack(side="left")
        self.ent_hasta = ttk.Entry(frm_filtros, width=12)
        self.ent_hasta.pack(side="left", padx=4)

        # === Fechas por defecto ===
        today = _dt.date.today()
        start_of_year = _dt.date(today.year, 1, 1)
        self.ent_desde.insert(0, start_of_year.strftime("%Y-%m-%d"))
        self.ent_hasta.insert(0, today.strftime("%Y-%m-%d"))

        ttk.Button(frm_filtros, text="Filtrar rango", command=self._refresh_data).pack(side="left", padx=6)

        kpis = ttk.Frame(frame)
        kpis.pack(fill="x", padx=6, pady=(0, 6))

        def _kpi(parent, title):
            box = ttk.Labelframe(parent, text=title, padding=8)
            val = ttk.Label(box, text="0", font=("TkDefaultFont", 11, "bold"))
            val.pack(anchor="center")
            return box, val

        box_v, self.kpi_ventas = _kpi(kpis, "Total Ventas")
        self.kpi_ventas_box = box_v
        box_vb, self.kpi_ventas_bruto = _kpi(kpis, "Ventas con retencion")
        box_nc, self.kpi_nc = _kpi(kpis, "Notas de crÃ©dito")
        self.kpi_nc_box = box_nc
        box_c, self.kpi_cg = _kpi(kpis, "Compras + Gastos")
        box_f, self.kpi_flujo = _kpi(kpis, "Flujo Neto")
        box_b, self.kpi_banco = _kpi(kpis, "Estimado en Banco")
        box_v.pack(side="left", padx=(0, 8))
        box_vb.pack(side="left", padx=(0, 8))
        box_nc.pack(side="left", padx=(0, 8))
        box_c.pack(side="left", padx=(0, 8))
        box_f.pack(side="left", padx=(0, 8))
        box_b.pack(side="left")
        self.lbl_saldo_inicial = ttk.Label(kpis, text="Saldo inicial: 0 Gs")
        self.lbl_saldo_inicial.pack(side="left", padx=(10, 0))

        # --- Tabla resumen ---
        cols = ("mes","compras","ventas","nota_credito","gastos","flujo","margen")
        self.tv = ttk.Treeview(frame, columns=cols, show="headings", height=12)
        for c, t, w in [
            ("mes","Mes",100),
            ("compras","Compras (Gs)",130),
            ("ventas","Ventas (Gs)",170),
            ("nota_credito","Nota de crÃ©dito (Gs)",145),
            ("gastos","Gastos (Gs)",130),
            ("flujo","Flujo Neto (Gs)",175),
            ("margen","Margen (%)",100)
        ]:
            self.tv.heading(c, text=t)
            anchor = "w" if c == "mes" else "center"
            if c in ("compras", "nota_credito", "gastos", "margen"):
                anchor = "e"  # nÃºmeros a la derecha
            if c in ("ventas", "flujo"):
                anchor = "e"  # nÃºmero a la derecha, flecha va en overlay
            self.tv.column(c, width=w, anchor=anchor)
        # striping suave
        self.tv.tag_configure("row_even", background="#f9f9f9")
        self.tv.tag_configure("row_odd", background="#ffffff")
        self.tv.pack(fill="x", padx=8, pady=6)
        self._cell_overlays = {}
        self._row_tags = {}

        # --- GrÃ¡fico ---
        fig, ax = plt.subplots(figsize=(8.5,3.8))
        self.fig = fig
        self.ax = ax
        self.canvas = FigureCanvasTkAgg(fig, master=frame)
        canvas_widget = self.canvas.get_tk_widget()
        canvas_widget.configure(height=430)
        canvas_widget.pack(fill="x", expand=False, padx=6, pady=6)

        # --- Totales ---
        self.lbl_tot = ttk.Label(frame, text="Totales: â€”")
        self.lbl_tot.pack(anchor="w", padx=8, pady=(4,8))

        # Primera carga
        self._refresh_data()

    # --- Helpers ---
    def _fmt_gs(self, val):
        try:
            return f"{float(val):,.0f}".replace(",", ".")
        except:
            return "-"

    def _trend_info(self, actual: float, anterior: float|None):
        """Devuelve (texto, color_hex) con flecha y % vs. mes anterior."""
        if anterior is None:
            return ("-", None)
        if abs(anterior) < 1e-9:
            if abs(actual) < 1e-9:
                return ("\u2192 (0.0%)", "#6b7280")  # gris
            return ("\u25b2 (N/D)", "#0a7d28")      # verde
        pct = ((actual - anterior) / abs(anterior)) * 100.0
        if pct > 0.05:
            return (f"\u25b2 ({pct:+.1f}%)", "#0a7d28")  # verde
        if pct < -0.05:
            return (f"\u25bc ({pct:+.1f}%)", "#b91c1c")  # rojo
        return ("\u2192 (0.0%)", "#6b7280")  # gris

    def _add_quarter_shading(self, ax):
        shade = True
        start = 1
        while start <= 12:
            if shade:
                ax.axvspan(
                    start - 0.5,
                    min(start + 2, 12) + 0.5,
                    color="#94a3b8",
                    alpha=0.12,
                    zorder=0,
                )
            shade = not shade
            start += 3

    def _clear_overlays(self):
        """Elimina labels superpuestos en ventas/flujo para evitar fugas."""
        if not hasattr(self, "_cell_overlays"):
            return
        for lbls in self._cell_overlays.values():
            for lbl in lbls.values():
                try:
                    lbl.destroy()
                except Exception:
                    pass
        self._cell_overlays = {}
        self._row_tags = {}

    def _row_background(self, iid):
        """Devuelve color de fondo segÃºn zebra o selecciÃ³n."""
        try:
            selected = iid in self.tv.selection()
        except Exception:
            selected = False
        style = ttk.Style()
        if selected:
            sel_bg = style.lookup("Treeview", "selectbackground") or "#cce6ff"
            return sel_bg
        tag = self._row_tags.get(iid, "")
        if tag == "row_even":
            return "#f9f9f9"
        if tag == "row_odd":
            return "#ffffff"
        return style.lookup("Treeview", "background") or "#ffffff"

    def _relayout_overlays(self):
        """Reposiciona labels superpuestos si cambia el tamaAï¿½o."""
        if not hasattr(self, "_cell_overlays"):
            return
        y_offset = 0
        try:
            first_iid = self.tv.get_children()[0]
            first_bbox = self.tv.bbox(first_iid, column="mes")
            if first_bbox and len(first_bbox) == 4 and first_bbox[1] <= 2:
                y_offset = max(int(first_bbox[3]), 18)
        except Exception:
            y_offset = 0
        for iid, lbls in self._cell_overlays.items():
            for col, lbl in lbls.items():
                try:
                    bbox = self.tv.bbox(iid, column=col)
                except Exception:
                    bbox = None
                if bbox and len(bbox) == 4:
                    x, y, w, h = bbox
                    y += y_offset
                    left_w = max(int(w * 0.42), 64)
                    if col == "flujo_val":
                        lbl.place(x=x + left_w, y=y, width=max(w - left_w - 6, 10), height=h)
                    else:
                        # reservar bloque fijo y visible para flecha + porcentaje
                        lbl.place(x=x + 4, y=y, width=left_w, height=h)
                    lbl.configure(background=self._row_background(iid))
                    lbl.lift()
                else:
                    lbl.place_forget()

    def _parse_date(self, value: str):
        if not value:
            return None
        value = value.strip()
        if not value:
            return None
        try:
            # Soporta tanto YYYY-MM-DD como ISO con hora
            return _dt.datetime.fromisoformat(value[:10]).date()
        except Exception:
            try:
                return _dt.datetime.strptime(value[:10], "%Y-%m-%d").date()
            except Exception:
                return None

    def _get_years_available(self):
        """Busca los aÃ±os que existen en las tablas compras y ventas."""
        cur = self.repo.cn.cursor()
        cur.execute("SELECT DISTINCT TO_CHAR((ts)::timestamp, 'YYYY') FROM sales_invoices ORDER BY 1;")
        yrs1 = [r[0] for r in cur.fetchall() if r[0]]
        cur.execute("SELECT DISTINCT TO_CHAR((ts)::timestamp, 'YYYY') FROM raw_lots ORDER BY 1;")
        yrs2 = [r[0] for r in cur.fetchall() if r[0]]
        cur.execute("SELECT DISTINCT TO_CHAR((ts)::timestamp, 'YYYY') FROM bag_sales ORDER BY 1;")
        yrs3 = [r[0] for r in cur.fetchall() if r[0]]
        all_yrs = sorted(set(yrs1 + yrs2 + yrs3))
        if not all_yrs:
            all_yrs = [str(_dt.date.today().year)]
        return all_yrs

    # --- Actualizar datos y grÃ¡fico ---
    def _refresh_data(self):
        year = self.year_var.get()
        if not year:
            messagebox.showwarning("AtenciÃ³n", "Seleccione un aÃ±o.")
            return

        venta_view = (self.cb_venta_view.get() or "Total").strip() if hasattr(self, "cb_venta_view") else "Total"
        retencion_mode = venta_view.startswith("Con")

        # Leer filtros de fecha
        d1 = (self.ent_desde.get() or "").strip() or None
        d2 = (self.ent_hasta.get() or "").strip() or None
        d1_date = self._parse_date(d1)
        d2_date = self._parse_date(d2)

        cur = self.repo.cn.cursor()

        # --- Ventas ---
        cond_v = ""
        params_v = [year]
        if d1:
            cond_v += " AND date(ts) >= date(%s)"
            params_v.append(d1)
        if d2:
            cond_v += " AND date(ts) <= date(%s)"
            params_v.append(d2)
        cur.execute(f"""
            SELECT TO_CHAR((ts)::timestamp, 'MM'), SUM(total_gs)
            FROM sales_invoices
            WHERE TO_CHAR((ts)::timestamp, 'YYYY')=%s {cond_v}
            GROUP BY 1;
        """, params_v)
        ventas_gross = {r[0]: r[1] for r in cur.fetchall()}

        cur.execute(f"""
            SELECT TO_CHAR((ts)::timestamp, 'MM'),
                   SUM(total_gs - 0.30 * (COALESCE(iva5_gs, 0) + COALESCE(iva10_gs, 0)))
            FROM sales_invoices
            WHERE TO_CHAR((ts)::timestamp, 'YYYY')=%s {cond_v}
            GROUP BY 1;
        """, params_v)
        ventas_retencion = {r[0]: r[1] for r in cur.fetchall()}

        if retencion_mode:
            ventas = dict(ventas_retencion)
        else:
            ventas = dict(ventas_gross)


        # --- Ventas de bolsas ---
        cond_b = ""
        params_b = [year]
        if d1:
            cond_b += " AND date(ts) >= date(%s)"
            params_b.append(d1)
        if d2:
            cond_b += " AND date(ts) <= date(%s)"
            params_b.append(d2)
        cur.execute(f"""
            SELECT TO_CHAR((ts)::timestamp, 'MM'), SUM(total_gs)
            FROM bag_sales
            WHERE TO_CHAR((ts)::timestamp, 'YYYY')=%s {cond_b}
            GROUP BY 1;
        """, params_b)
        ventas_bolsas = {r[0]: r[1] for r in cur.fetchall()}
        for mes, monto in ventas_bolsas.items():
            add = float(monto or 0)
            ventas[mes] = ventas.get(mes, 0.0) + add
            ventas_gross[mes] = ventas_gross.get(mes, 0.0) + add
            ventas_retencion[mes] = ventas_retencion.get(mes, 0.0) + add

        # --- Compras ---
        cond_c = ""
        params_c = [year]
        if d1:
            cond_c += " AND date(ts) >= date(%s)"
            params_c.append(d1)
        if d2:
            cond_c += " AND date(ts) <= date(%s)"
            params_c.append(d2)
        cur.execute(f"""
            SELECT TO_CHAR((ts)::timestamp, 'MM'), SUM(costo_total_gs)
            FROM raw_lots
            WHERE TO_CHAR((ts)::timestamp, 'YYYY')=%s {cond_c}
            GROUP BY 1;
        """, params_c)
        compras = {r[0]: r[1] for r in cur.fetchall()}

        # --- Gastos ---
        cond_g = ""
        params_g = [year]
        if d1:
            cond_g += " AND date(ts) >= date(%s)"
            params_g.append(d1)
        if d2:
            cond_g += " AND date(ts) <= date(%s)"
            params_g.append(d2)
        try:
            cur.execute(f"""
                SELECT TO_CHAR((ts)::timestamp, 'MM'), SUM(monto_gs)
                FROM expenses
                WHERE TO_CHAR((ts)::timestamp, 'YYYY')=%s {cond_g}
                GROUP BY 1;
            """, params_g)
            gastos = {r[0]: r[1] for r in cur.fetchall()}
        except Exception:
            gastos = {}

        # --- Notas de crÃ©dito ---
        cond_nc = ""
        params_nc = [year]
        if d1:
            cond_nc += " AND date(ts) >= date(%s)"
            params_nc.append(d1)
        if d2:
            cond_nc += " AND date(ts) <= date(%s)"
            params_nc.append(d2)
        try:
            cur.execute(f"""
                SELECT TO_CHAR((ts)::timestamp, 'MM'), SUM(total_gs)
                FROM credit_notes
                WHERE TO_CHAR((ts)::timestamp, 'YYYY')=%s {cond_nc}
                GROUP BY 1;
            """, params_nc)
            notas_credito_gross = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute(f"""
                SELECT TO_CHAR((ts)::timestamp, 'MM'),
                       SUM(total_gs - 0.30 * (COALESCE(iva5_gs, 0) + COALESCE(iva10_gs, 0)))
                FROM credit_notes
                WHERE TO_CHAR((ts)::timestamp, 'YYYY')=%s {cond_nc}
                GROUP BY 1;
            """, params_nc)
            notas_credito_retencion = {r[0]: r[1] for r in cur.fetchall()}
        except Exception:
            notas_credito_gross = {}
            notas_credito_retencion = {}

        notas_credito = dict(notas_credito_retencion if retencion_mode else notas_credito_gross)
        ventas = dict(ventas_retencion if retencion_mode else ventas_gross)

        # --- Saldo inicial configurado por fuera de la DB ---
        saldo_inicial = 0.0
        if hasattr(self, "data_json") and self.data_json:
            def fecha_en_rango(fecha_txt: str) -> bool:
                f = self._parse_date(fecha_txt)
                if not f:
                    return True
                if d1_date and f < d1_date:
                    return False
                if d2_date and f > d2_date:
                    return False
                return True
            for mov in self.data_json.get("saldo_inicial", []):
                fecha = str(mov.get("fecha") or mov.get("date") or "")
                if not fecha or str(year) not in fecha[:4]:
                    continue
                monto = mov.get("monto", mov.get("valor"))
                if monto is None:
                    continue
                if not fecha_en_rango(fecha):
                    continue
                try:
                    saldo_inicial += float(monto)
                except Exception:
                    continue

        # --- Preparar datos ---
        meses = ["01","02","03","04","05","06","07","08","09","10","11","12"]
        nombres_meses = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
                         "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
        nombres_por_mes = dict(zip(meses, nombres_meses))
        sel_trim = (self.cb_trim.get() or "Todos").strip() if hasattr(self, "cb_trim") else "Todos"
        trim_map = {
            "T1 (Ene-Mar)": ["01","02","03"],
            "T2 (Abr-Jun)": ["04","05","06"],
            "T3 (Jul-Sep)": ["07","08","09"],
            "T4 (Oct-Dic)": ["10","11","12"],
        }
        quarter_mode = False
        if sel_trim in trim_map:
            meses = [m for m in meses if m in trim_map[sel_trim]]
            nombres_meses = [nombres_por_mes[m] for m in meses]
            quarter_mode = True

        for i in self.tv.get_children():
            self.tv.delete(i)
        self._clear_overlays()
        self._cell_overlays = {}
        self._row_tags = {}

        ventas_tot = compras_tot = gastos_tot = notas_credito_tot = flujo_tot = ventas_gross_tot = ventas_retencion_tot = 0.0
        ventas_list, compras_list, notas_credito_list, gastos_list, flujo_list = [], [], [], [], []
        flujo_retencion_list = []

        trend_ventas_por_mes = {}
        trend_flujo_por_mes = {}
        prev_venta_mes = None
        prev_flujo_mes = None
        for m in meses:
            v_mes = float(ventas.get(m, 0.0))
            c_mes = float(compras.get(m, 0.0))
            g_mes = float(gastos.get(m, 0.0))
            nc_mes = float(notas_credito.get(m, 0.0))
            f_mes = v_mes - nc_mes - (c_mes + g_mes)
            trend_ventas_por_mes[m] = self._trend_info(v_mes, prev_venta_mes)
            trend_flujo_por_mes[m] = self._trend_info(f_mes, prev_flujo_mes)
            prev_venta_mes = v_mes
            prev_flujo_mes = f_mes

        for m, nombre in zip(meses, nombres_meses):
            v = float(ventas.get(m, 0.0))
            v_ret = float(ventas_retencion.get(m, 0.0))
            c = float(compras.get(m, 0.0))
            nc = float(notas_credito.get(m, 0.0))
            g = float(gastos.get(m, 0.0))
            f = v - nc - (c + g)
            f_ret = v_ret - nc - (c + g)
            margen = (f / v * 100.0) if v > 0 else 0.0
            trend_v_txt, trend_v_col = trend_ventas_por_mes.get(m, ("-", None))
            trend_f_txt, trend_f_col = trend_flujo_por_mes.get(m, ("-", None))

            ventas_tot += v
            ventas_gross_tot += float(ventas_gross.get(m, 0.0))
            ventas_retencion_tot += float(ventas_retencion.get(m, 0.0))
            compras_tot += c
            notas_credito_tot += nc
            gastos_tot += g
            flujo_tot += f

            ventas_display = self._fmt_gs(v)
            flujo_display = self._fmt_gs(f)
            flujo_val_label = None
            if quarter_mode and f > 0:
                flujo_display = ""
                flujo_val_label = ttk.Label(
                    self.tv,
                    text=f"\u25b2 {self._fmt_gs(f)}",
                    foreground="#0a7d28",
                    background="white",
                    anchor="e",
                )

            row_index = len(self.tv.get_children())
            row_tag = "row_even" if row_index % 2 == 0 else "row_odd"

            iid = self.tv.insert("", "end", values=(nombre, self._fmt_gs(c),
                ventas_display, self._fmt_gs(nc), self._fmt_gs(g), flujo_display, f"{margen:.1f}%"),
                tags=(row_tag,))
            self._row_tags[iid] = row_tag

            # Overlays coloreados solo para ventas y flujo (solo flecha+%)
            if trend_v_txt != "-" or trend_v_col:
                lbl_v = ttk.Label(self.tv, text=trend_v_txt,
                                  foreground=trend_v_col or "black",
                                  background=self._row_background(iid), anchor="w")
                self._cell_overlays.setdefault(iid, {})["ventas"] = lbl_v
            if trend_f_txt != "-" or trend_f_col:
                lbl_f = ttk.Label(self.tv, text=trend_f_txt,
                                  foreground=trend_f_col or "black",
                                  background=self._row_background(iid), anchor="w")
                self._cell_overlays.setdefault(iid, {})["flujo"] = lbl_f
            if flujo_val_label is not None:
                self._cell_overlays.setdefault(iid, {})["flujo_val"] = flujo_val_label

            ventas_list.append(v)
            compras_list.append(c)
            notas_credito_list.append(nc)
            gastos_list.append(g)
            flujo_list.append(f)
            flujo_retencion_list.append(f_ret)

        # Posicionar labels coloreados
        self.tv.update_idletasks()
        self._relayout_overlays()
        self.frame.after_idle(self._relayout_overlays)
        self.frame.after(50, self._relayout_overlays)
        self.tv.bind("<Configure>", lambda _e: self._relayout_overlays())
        self.tv.bind("<<TreeviewSelect>>", lambda _e: self._relayout_overlays())

        margen_total = (flujo_tot / ventas_tot * 100.0) if ventas_tot > 0 else 0.0
        venta_lbl = "Ventas (retencion IVA 30%)" if retencion_mode else "Ventas"
        self.lbl_tot.config(
            text=(
                f"Totales {year} â€” Compras: {self._fmt_gs(compras_tot)}  |  "
                f"Nota de crÃ©dito: {self._fmt_gs(notas_credito_tot)}  |  "
                f"Gastos: {self._fmt_gs(gastos_tot)}  |  "
                f"{venta_lbl}: {self._fmt_gs(ventas_tot)}  |  "
                f"Flujo Neto: {self._fmt_gs(flujo_tot)}  |  Margen: {margen_total:.1f}%"
            )
        )

        # --- GrÃ¡fico ---
        self.ax.clear()
        self.ax.set_facecolor("white")
        self.fig.patch.set_facecolor("white")
        if hasattr(self, "ax2") and self.ax2:
            try:
                self.ax2.remove()
            except Exception:
                try:
                    self.fig.delaxes(self.ax2)
                except Exception:
                    pass
            self.ax2 = None

        if len(meses) == 12:
            self._add_quarter_shading(self.ax)
        x = range(1, len(meses) + 1)
        width = 0.16
        self.ax.bar([i - 1.8 * width for i in x], ventas_list, width=width, color="steelblue", label="Ventas")
        self.ax.bar([i - 0.6 * width for i in x], compras_list, width=width, color="indianred", label="Compras")
        self.ax.bar([i + 0.6 * width for i in x], gastos_list, width=width, color="goldenrod", label="Gastos")
        self.ax.bar([i + 1.8 * width for i in x], notas_credito_list, width=width, color="mediumpurple", label="Nota de crÃ©dito")

        self.ax.plot(x, flujo_list, color="gray", marker="o", linewidth=1.8, label="Flujo neto")

        acumulado, suma = [], saldo_inicial
        for f in flujo_list:
            suma += f
            acumulado.append(suma)

        acumulado_ret, suma_ret = [], saldo_inicial
        for f in flujo_retencion_list:
            suma_ret += f
            acumulado_ret.append(suma_ret)
        self.ax2 = self.ax.twinx()
        self.ax2.plot(x, acumulado, color="dimgray", linestyle="--", linewidth=1.5, label="Flujo acumulado")
        self.ax2.tick_params(axis='y', labelsize=8)
        self.ax2.set_ylabel("Acumulado (Gs)", fontsize=9, color="dimgray")

        titulo = f"Flujo de Dinero {year}"
        if retencion_mode:
            titulo += " (retencion IVA 30%)"
        if d1 or d2:
            rango = f" ({d1 or 'inicio'} a {d2 or 'fin'})"
            titulo += rango
        self.ax.set_title(titulo, fontsize=13, fontweight="bold", pad=18)

        self.ax.set_xticks(x)
        self.ax.set_xticklabels(nombres_meses, rotation=45, ha="right")
        self.ax.set_ylabel("Monto (Gs)", fontsize=10)
        self.ax.tick_params(axis='x', labelsize=9)
        self.ax.tick_params(axis='y', labelsize=9)
        self.ax.grid(axis="y", linestyle="--", alpha=0.3)
        self.ax.set_axisbelow(True)
        for spine in self.ax.spines.values():
            spine.set_alpha(0.4)

        max_val = max(ventas_list + compras_list + notas_credito_list + gastos_list + [1])
        y_pad = max_val * 0.12
        for i, (v, c, nc, g) in enumerate(zip(ventas_list, compras_list, notas_credito_list, gastos_list), start=1):
            if abs(v) > 1e-6:
                self.ax.text(i - 1.8 * width, v + max_val * 0.015, f"{v/1_000_000:.1f}M", ha="center", fontsize=8, color="steelblue")
            if abs(c) > 1e-6:
                self.ax.text(i - 0.6 * width, c + max_val * 0.015, f"{c/1_000_000:.1f}M", ha="center", fontsize=8, color="indianred")
            if abs(g) > 1e-6:
                self.ax.text(i + 0.6 * width, g + max_val * 0.015, f"{g/1_000_000:.1f}M", ha="center", fontsize=8, color="darkgoldenrod")
            if abs(nc) > 1e-6:
                self.ax.text(i + 1.8 * width, nc + max_val * 0.015, f"{nc/1_000_000:.1f}M", ha="center", fontsize=8, color="mediumpurple")

        self.ax.set_ylim(bottom=min(0, min(flujo_list + [0]) - y_pad), top=max_val + y_pad)
        if self.ax2:
            max_acum = max(acumulado + [0])
            min_acum = min(acumulado + [0])
            span = max(max_acum - min_acum, 1)
            self.ax2.set_ylim(min_acum - span * 0.12, max_acum + span * 0.15)

        # KPIs superiores
        if hasattr(self, "kpi_ventas"):
            self.kpi_ventas.config(text=f"{self._fmt_gs(ventas_tot)} Gs")
        if hasattr(self, "kpi_ventas_bruto"):
            self.kpi_ventas_bruto.config(text=f"{self._fmt_gs(ventas_retencion_tot)} Gs")
        if hasattr(self, "kpi_nc"):
            self.kpi_nc.config(text=f"{self._fmt_gs(notas_credito_tot)} Gs")
        if hasattr(self, "kpi_ventas_box"):
            if retencion_mode:
                self.kpi_ventas_box.config(text="Total Ventas (retencion IVA 30%)")
            else:
                self.kpi_ventas_box.config(text="Total Ventas")
        if hasattr(self, "kpi_nc_box"):
            if retencion_mode:
                self.kpi_nc_box.config(text="Notas de crÃ©dito (retencion IVA 30%)")
            else:
                self.kpi_nc_box.config(text="Notas de crÃ©dito")
        if hasattr(self, "kpi_cg"):
            self.kpi_cg.config(text=f"{self._fmt_gs(compras_tot + gastos_tot)} Gs")
        if hasattr(self, "kpi_flujo"):
            flujo_prefix = ""
            flujo_color = "black"
            if quarter_mode and flujo_tot > 0:
                flujo_prefix = "\u25b2 "
                flujo_color = "#0a7d28"
            self.kpi_flujo.config(text=f"{flujo_prefix}{self._fmt_gs(flujo_tot)} Gs", foreground=flujo_color)
        if hasattr(self, "kpi_banco"):
            banco_est = acumulado_ret[-1] if acumulado_ret else 0.0
            self.kpi_banco.config(text=f"{self._fmt_gs(banco_est)} Gs")
        if hasattr(self, "lbl_saldo_inicial"):
            self.lbl_saldo_inicial.config(text=f"Saldo inicial: {self._fmt_gs(saldo_inicial)} Gs")

        lines_labels = [ax.get_legend_handles_labels() for ax in [self.ax, self.ax2]]
        lines, labels = [sum(lol, []) for lol in zip(*lines_labels)]
        self.ax.legend(lines, labels, fontsize=8, loc="upper left",
                       bbox_to_anchor=(0.01, 1.22), ncol=4, frameon=False, columnspacing=1.2)

        self.fig.subplots_adjust(top=0.82, bottom=0.30, left=0.07, right=0.96)

        def on_click(event):
            if event.inaxes != self.ax:
                return
            ix = int(round(event.xdata))
            if 1 <= ix <= len(nombres_meses):
                mes = nombres_meses[ix - 1]
                messagebox.showinfo(
                    "Detalle mensual",
                    f"{mes} {year}\n\n"
                    f"Ventas: {self._fmt_gs(ventas_list[ix-1])}\n"
                    f"Compras: {self._fmt_gs(compras_list[ix-1])}\n"
                    f"Nota de crÃ©dito: {self._fmt_gs(notas_credito_list[ix-1])}\n"
                    f"Gastos: {self._fmt_gs(gastos_list[ix-1])}\n"
                    f"Flujo Neto: {self._fmt_gs(flujo_list[ix-1])}"
                )

        self.fig.canvas.mpl_connect('button_press_event', on_click)
        self.canvas.draw()

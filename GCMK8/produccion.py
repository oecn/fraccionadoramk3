# produccion.py
# -*- coding: utf-8 -*-

import datetime as _dt
import tkinter as tk
from tkinter import ttk, messagebox
import unicodedata
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt


def _normalize_name(name: str) -> str:
    s = (name or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.replace("\ufffd", "o").replace("3", "o")
    alias = {
        "az£car": "azucar",
        "az§car": "azucar",
        "az?car": "azucar",
        "aza?car": "azucar",
        "azocar": "azucar",
        "azaocar": "azucar",
    }
    return alias.get(s, s)


def _maquina_for_producto(name: str) -> str:
    key = _normalize_name(name)
    if key in ("arroz", "azucar"):
        return "Maquina 1"
    return "Maquina 2"


def _unidades_por_paquete(gramaje: int) -> int:
    return 20 if gramaje <= 250 else 10


class TabProduccion:
    def __init__(self, parent, nb, repo):
        self.parent = parent
        self.repo = repo

        frame = ttk.Frame(nb)
        nb.add(frame, text="Produccion")
        self.frame = frame

        top = ttk.Frame(frame)
        top.pack(fill="x", padx=6, pady=6)
        ttk.Label(top, text="Mes:").pack(side="left")
        self.cb_mes = ttk.Combobox(top, width=12, state="readonly")
        self.cb_mes.pack(side="left", padx=6)
        ttk.Label(top, text="Ano:").pack(side="left")
        self.cb_ano = ttk.Combobox(top, width=6, state="readonly")
        self.cb_ano.pack(side="left", padx=6)
        ttk.Label(top, text="Rango:").pack(side="left", padx=(8, 0))
        self.cb_rango = ttk.Combobox(top, width=10, state="readonly",
                                     values=["6 meses", "12 meses", "24 meses"])
        self.cb_rango.pack(side="left", padx=6)
        self.cb_rango.set("12 meses")
        ttk.Button(top, text="Refrescar", command=self._refresh).pack(side="left")

        today = _dt.date.today()
        self._meses = [
            ("01", "Enero"),
            ("02", "Febrero"),
            ("03", "Marzo"),
            ("04", "Abril"),
            ("05", "Mayo"),
            ("06", "Junio"),
            ("07", "Julio"),
            ("08", "Agosto"),
            ("09", "Septiembre"),
            ("10", "Octubre"),
            ("11", "Noviembre"),
            ("12", "Diciembre"),
        ]
        self.cb_mes["values"] = [f"{m} - {n}" for m, n in self._meses]
        self.cb_mes.set(f"{today.strftime('%m')} - {self._meses[today.month - 1][1]}")
        years = self._get_years_available()
        self.cb_ano["values"] = [str(y) for y in years]
        self.cb_ano.set(str(today.year))

        cols = ("producto", "maquina", "unidades")
        self.tv = ttk.Treeview(frame, columns=cols, show="headings", height=14)
        self.tv.heading("producto", text="Producto")
        self.tv.heading("maquina", text="Maquina")
        self.tv.heading("unidades", text="Unidades")
        self.tv.column("producto", width=240, anchor="w")
        self.tv.column("maquina", width=120, anchor="center")
        self.tv.column("unidades", width=140, anchor="e")
        self.tv.tag_configure(
            "total_m1",
            background="#dbeafe",
            foreground="#1e3a8a",
            font=("TkDefaultFont", 10, "bold"),
        )
        self.tv.tag_configure(
            "total_m2",
            background="#fee2e2",
            foreground="#991b1b",
            font=("TkDefaultFont", 10, "bold"),
        )
        self.tv.pack(fill="both", expand=True, padx=6, pady=6)

        chart_box = ttk.Labelframe(frame, text="Produccion mensual")
        chart_box.pack(fill="x", padx=6, pady=(0, 6))
        fig, ax = plt.subplots(figsize=(8.5, 2.8))
        self.fig = fig
        self.ax = ax
        self.canvas = FigureCanvasTkAgg(fig, master=chart_box)
        canvas_widget = self.canvas.get_tk_widget()
        canvas_widget.configure(height=260)
        canvas_widget.pack(fill="x", expand=False, padx=6, pady=6)
        self._chart_data = {"months": [], "total": [], "m1": [], "m2": []}
        self._tooltip = self.ax.annotate(
            "",
            xy=(0, 0),
            xytext=(10, 10),
            textcoords="offset points",
            bbox=dict(boxstyle="round,pad=0.3", fc="#fefce8", ec="#a16207", alpha=0.9),
            arrowprops=dict(arrowstyle="->", color="#a16207"),
        )
        self._tooltip.set_visible(False)
        self.canvas.mpl_connect("motion_notify_event", self._on_hover)

        totals = ttk.Frame(frame)
        totals.pack(fill="x", padx=6, pady=(0, 6))

        def _kpi(parent, title):
            box = ttk.Labelframe(parent, text=title, padding=8)
            val = ttk.Label(box, text="0", font=("TkDefaultFont", 12, "bold"))
            val.pack(anchor="center")
            return box, val

        box_mes, self.lbl_total_mes = _kpi(totals, "Total mes")
        self.lbl_total_mes.config(foreground="#6b8e23")
        box_m1, self.lbl_total_m1 = _kpi(totals, "Total Maquina 1")
        box_m2, self.lbl_total_m2 = _kpi(totals, "Total Maquina 2")
        box_mes.pack(side="left", padx=(0, 8))
        box_m1.pack(side="left", padx=(0, 8))
        box_m2.pack(side="left")

        self._refresh()

    def _fmt_int(self, val):
        try:
            return f"{int(val):,}".replace(",", ".")
        except Exception:
            return "0"

    def _get_years_available(self):
        cur = self.repo.cn.cursor()
        cur.execute("SELECT MIN(TO_CHAR((ts)::timestamp, 'YYYY')), MAX(TO_CHAR((ts)::timestamp, 'YYYY')) FROM fractionations;")
        row = cur.fetchone() or (None, None)
        try:
            min_y = int(row[0]) if row[0] else None
            max_y = int(row[1]) if row[1] else None
        except Exception:
            min_y = max_y = None
        today_y = _dt.date.today().year
        if min_y is None or max_y is None:
            return [today_y]
        years = list(range(min_y, max_y + 1))
        if today_y not in years:
            years.append(today_y)
            years.sort()
        return years

    def _load_rows(self, year: int, month: int):
        start = _dt.date(year, month, 1)
        if month == 12:
            end = _dt.date(year + 1, 1, 1)
        else:
            end = _dt.date(year, month + 1, 1)

        cur = self.repo.cn.cursor()
        cur.execute(
            """
            SELECT p.name, f.gramaje, f.paquetes
            FROM fractionations f
            JOIN products p ON p.id = f.product_id
            WHERE f.ts::date >= %s AND f.ts::date < %s
            ORDER BY p.name;
            """,
            (start.isoformat(), end.isoformat()),
        )
        return cur.fetchall()

    def _load_monthly_totals(self):
        cur = self.repo.cn.cursor()
        cur.execute(
            """
            SELECT TO_CHAR((f.ts)::timestamp, 'YYYY-MM') AS ym, p.name, f.gramaje, f.paquetes
            FROM fractionations f
            JOIN products p ON p.id = f.product_id
            ORDER BY ym;
            """
        )
        rows = cur.fetchall()
        if not rows:
            return [], [], [], []

        totals = {}
        min_ym = rows[0][0]
        max_ym = rows[-1][0]
        for ym, name, gram, paq in rows:
            try:
                unidades = int(paq) * _unidades_por_paquete(int(gram))
            except Exception:
                unidades = 0
            maquina = _maquina_for_producto(name)
            if ym not in totals:
                totals[ym] = {"total": 0, "m1": 0, "m2": 0}
            totals[ym]["total"] += unidades
            if maquina == "Maquina 1":
                totals[ym]["m1"] += unidades
            else:
                totals[ym]["m2"] += unidades

        months = self._month_range(min_ym, max_ym)
        total_vals = [totals.get(m, {}).get("total", 0) for m in months]
        m1_vals = [totals.get(m, {}).get("m1", 0) for m in months]
        m2_vals = [totals.get(m, {}).get("m2", 0) for m in months]
        return months, total_vals, m1_vals, m2_vals

    def _moving_avg(self, values, window=3):
        if window <= 1:
            return values[:]
        out = []
        for i in range(len(values)):
            start = max(0, i - window + 1)
            chunk = values[start:i + 1]
            out.append(sum(chunk) / len(chunk) if chunk else 0)
        return out

    def _apply_range(self, months, total_vals, m1_vals, m2_vals):
        sel = (self.cb_rango.get() or "").strip()
        try:
            n = int(sel.split()[0])
        except Exception:
            n = 12
        if n <= 0:
            return months, total_vals, m1_vals, m2_vals
        return months[-n:], total_vals[-n:], m1_vals[-n:], m2_vals[-n:]

    def _month_range(self, ym_start: str, ym_end: str):
        try:
            y1, m1 = [int(x) for x in ym_start.split("-")]
            y2, m2 = [int(x) for x in ym_end.split("-")]
        except Exception:
            return []
        months = []
        y, m = y1, m1
        while (y < y2) or (y == y2 and m <= m2):
            months.append(f"{y:04d}-{m:02d}")
            m += 1
            if m > 12:
                m = 1
                y += 1
        return months

    def _refresh_chart(self):
        months, total_vals, m1_vals, m2_vals = self._load_monthly_totals()
        months, total_vals, m1_vals, m2_vals = self._apply_range(
            months, total_vals, m1_vals, m2_vals
        )
        self._chart_data = {
            "months": months,
            "total": total_vals,
            "m1": m1_vals,
            "m2": m2_vals,
        }
        self.ax.clear()
        if months:
            x = list(range(len(months)))
            self._add_quarter_shading(months)
            self.ax.plot(x, total_vals, label="Total", color="#2f855a", linewidth=2)
            self.ax.plot(
                x,
                self._moving_avg(total_vals, window=3),
                label="Total (prom. 3m)",
                color="#2f855a",
                linewidth=2,
                linestyle="--",
                alpha=0.7,
            )
            self.ax.plot(x, m1_vals, label="Maquina 1", color="#1d4ed8", linewidth=2)
            self.ax.plot(x, m2_vals, label="Maquina 2", color="#b91c1c", linewidth=2)
            self.ax.set_xticks(x)
            self.ax.set_xticklabels(months, rotation=45, ha="right", fontsize=8)
            self.ax.grid(True, axis="y", alpha=0.25)
            self.ax.legend(loc="upper left", fontsize=8)
        self.ax.set_ylabel("Unidades")
        self.fig.tight_layout()
        self.canvas.draw()

    def _add_quarter_shading(self, months):
        if not months:
            return
        try:
            first_month = int(months[0].split("-")[1])
        except Exception:
            first_month = 1
        start_idx = 0
        offset = (first_month - 1) % 3
        if offset:
            start_idx = 3 - offset
        shade = True
        idx = start_idx
        while idx < len(months):
            if shade:
                self.ax.axvspan(
                    idx - 0.5,
                    min(idx + 3, len(months)) - 0.5,
                    color="#94a3b8",
                    alpha=0.12,
                    zorder=0,
                )
            shade = not shade
            idx += 3

    def _on_hover(self, event):
        if event.inaxes != self.ax:
            if self._tooltip.get_visible():
                self._tooltip.set_visible(False)
                self.canvas.draw_idle()
            return
        months = self._chart_data.get("months", [])
        if not months or event.xdata is None or event.ydata is None:
            return
        idx = int(round(event.xdata))
        if idx < 0 or idx >= len(months):
            if self._tooltip.get_visible():
                self._tooltip.set_visible(False)
                self.canvas.draw_idle()
            return
        total_vals = self._chart_data.get("total", [])
        m1_vals = self._chart_data.get("m1", [])
        m2_vals = self._chart_data.get("m2", [])
        if not total_vals:
            return
        self._tooltip.xy = (idx, total_vals[idx])
        text = (
            f"{months[idx]}\n"
            f"Total: {self._fmt_int(total_vals[idx])}\n"
            f"M1: {self._fmt_int(m1_vals[idx])}\n"
            f"M2: {self._fmt_int(m2_vals[idx])}"
        )
        self._tooltip.set_text(text)
        self._tooltip.set_visible(True)
        self.canvas.draw_idle()

    def _enforce_total_row_styles(self):
        # El striping global agrega evenrow/oddrow al final y puede tapar estilos.
        # En filas TOTAL removemos esos tags para preservar contraste y negrita.
        for iid in self.tv.get_children():
            tags = list(self.tv.item(iid, "tags") or ())
            if "total_m1" in tags or "total_m2" in tags:
                tags = [t for t in tags if t not in ("evenrow", "oddrow")]
                self.tv.item(iid, tags=tuple(tags))

    def _refresh(self):
        try:
            year = int((self.cb_ano.get() or "").strip())
            mes_txt = (self.cb_mes.get() or "").strip()
            month = int(mes_txt.split("-")[0].strip())
            if month < 1 or month > 12:
                raise ValueError
        except Exception:
            messagebox.showerror("Produccion", "Mes o ano invalido.")
            return

        for i in self.tv.get_children():
            self.tv.delete(i)

        totals_by_prod = {}
        maquina_by_prod = {}
        total_mes = 0
        total_m1 = 0
        total_m2 = 0

        for name, gram, paq in self._load_rows(year, month):
            try:
                unidades = int(paq) * _unidades_por_paquete(int(gram))
            except Exception:
                unidades = 0
            totals_by_prod[name] = totals_by_prod.get(name, 0) + unidades
            maquina = _maquina_for_producto(name)
            maquina_by_prod[name] = maquina

        rows = []
        for name, unidades in totals_by_prod.items():
            rows.append((name, unidades, maquina_by_prod.get(name, "Maquina 2")))

        rows_m1 = [r for r in rows if r[2] == "Maquina 1"]
        rows_m2 = [r for r in rows if r[2] != "Maquina 1"]
        rows_m1.sort(key=lambda r: (-r[1], r[0].lower()))
        rows_m2.sort(key=lambda r: (-r[1], r[0].lower()))

        for name, unidades, maquina in rows_m1:
            total_mes += unidades
            total_m1 += unidades
            self.tv.insert("", "end", values=(name, maquina, self._fmt_int(unidades)))
        if rows_m1:
            self.tv.insert(
                "",
                "end",
                values=("TOTAL MAQUINA 1", "Maquina 1", self._fmt_int(total_m1)),
                tags=("total_m1",),
            )

        for name, unidades, maquina in rows_m2:
            total_mes += unidades
            total_m2 += unidades
            self.tv.insert("", "end", values=(name, maquina, self._fmt_int(unidades)))
        if rows_m2:
            self.tv.insert(
                "",
                "end",
                values=("TOTAL MAQUINA 2", "Maquina 2", self._fmt_int(total_m2)),
                tags=("total_m2",),
            )

        if hasattr(self.parent, "_apply_treeview_striping"):
            self.parent._apply_treeview_striping(self.tv)
            self._enforce_total_row_styles()

        self.lbl_total_mes.config(text=self._fmt_int(total_mes))
        self.lbl_total_m1.config(text=self._fmt_int(total_m1))
        self.lbl_total_m2.config(text=self._fmt_int(total_m2))
        self._refresh_chart()

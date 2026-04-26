# -*- coding: utf-8 -*-
from __future__ import annotations

import tkinter as tk
from pathlib import Path
from tkinter import ttk

from rrhh_repo import RRHHRepo


class RRHHApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("RRHH")
        self.geometry("1180x620")
        self.repo = RRHHRepo()

        top = ttk.Frame(self)
        top.pack(fill="x", padx=10, pady=(10, 4))
        self.lbl_info = ttk.Label(
            top,
            text="Funcionarios seed: ADOLFINO (4563502) y LUIS (2319454). Importación de adelanto/salario: módulo Gastos.",
        )
        self.lbl_info.pack(side="left")

        body = ttk.Panedwindow(self, orient="vertical")
        body.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        box_emp = ttk.LabelFrame(body, text="Funcionarios")
        body.add(box_emp, weight=1)
        emp_cols = ("id", "nombre", "ci", "activo", "observacion")
        self.tv_emp = ttk.Treeview(box_emp, columns=emp_cols, show="headings", height=8)
        for col, text, width, anchor in [
            ("id", "ID", 60, "center"),
            ("nombre", "Nombre", 220, "w"),
            ("ci", "CI", 120, "center"),
            ("activo", "Activo", 80, "center"),
            ("observacion", "Observación", 320, "w"),
        ]:
            self.tv_emp.heading(col, text=text)
            self.tv_emp.column(col, width=width, anchor=anchor)
        self.tv_emp.pack(fill="both", expand=True, padx=8, pady=8)

        box_mov = ttk.LabelFrame(body, text="Movimientos RRHH")
        body.add(box_mov, weight=2)
        mov_cols = ("id", "fecha", "funcionario", "ci", "concepto", "monto", "confirmado", "cuenta", "archivo")
        self.tv_mov = ttk.Treeview(box_mov, columns=mov_cols, show="headings", height=14)
        for col, text, width, anchor in [
            ("id", "ID", 60, "center"),
            ("fecha", "Fecha", 100, "center"),
            ("funcionario", "Funcionario", 180, "w"),
            ("ci", "CI", 110, "center"),
            ("concepto", "Concepto", 140, "center"),
            ("monto", "Monto (Gs)", 120, "e"),
            ("confirmado", "Confirmado", 90, "center"),
            ("cuenta", "Cuenta destino", 150, "center"),
            ("archivo", "Archivo origen", 240, "w"),
        ]:
            self.tv_mov.heading(col, text=text)
            self.tv_mov.column(col, width=width, anchor=anchor)
        self.tv_mov.pack(fill="both", expand=True, padx=8, pady=8)

        self.lbl_total = ttk.Label(self, text="Total movimientos mostrados: -")
        self.lbl_total.pack(anchor="w", padx=12, pady=(0, 8))

        self._refresh()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _fmt_gs(self, value) -> str:
        try:
            return f"{float(value):,.0f}".replace(",", ".")
        except Exception:
            return "-"

    def _refresh(self):
        for item in self.tv_emp.get_children():
            self.tv_emp.delete(item)
        for emp_id, nombre, ci, activo, observacion in self.repo.list_employees():
            self.tv_emp.insert("", "end", values=(emp_id, nombre, ci, "Sí" if int(activo or 0) else "No", observacion))

        for item in self.tv_mov.get_children():
            self.tv_mov.delete(item)
        total = 0.0
        for mov_id, fecha, nombre, ci, concepto, monto, confirmado, cuenta, source_file in self.repo.list_movements():
            total += float(monto or 0)
            self.tv_mov.insert(
                "",
                "end",
                values=(
                    mov_id,
                    fecha,
                    nombre,
                    ci,
                    concepto,
                    self._fmt_gs(monto),
                    "Sí" if int(confirmado or 0) else "No",
                    cuenta,
                    Path(source_file).name if source_file else "",
                ),
            )
        self.lbl_total.config(text=f"Total movimientos mostrados: {self._fmt_gs(total)} Gs")

    def _on_close(self):
        self.repo.close()
        self.destroy()


def main():
    app = RRHHApp()
    app.mainloop()


if __name__ == "__main__":
    main()

# tab_gastos.py
# -*- coding: utf-8 -*-
import csv
import datetime as _dt
import json
import locale
import os
from pathlib import Path
import re
import subprocess
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db
import tkinter as tk
from tkinter import messagebox, ttk

from rrhh.rrhh_repo import RRHHRepo

PAYMENT_METHODS = ["Efectivo", "Cheque", "Transferencia", "Homebanking"]


def _expense_type_from_rrhh_concept(concepto: str) -> str:
    txt = (concepto or "").strip().lower()
    if "adelanto" in txt:
        return "Adelanto de salario"
    return "Pago a personal"


class TabGastos:
    def __init__(self, parent, nb, repo):
        self.parent = parent
        self.repo = repo

        frame = ttk.Frame(nb)
        nb.add(frame, text="Gastos y Egresos")
        self.frame = frame

        # Crear tabla en DB si no existe / migrar columnas faltantes.
        self._init_table()

        # --- Campos de carga ---
        box_form = ttk.LabelFrame(frame, text="Registrar nuevo gasto")
        box_form.pack(fill="x", padx=6, pady=6)

        ttk.Label(box_form, text="Fecha (AAAA-MM-DD):").grid(row=0, column=0, padx=6, pady=4, sticky="w")
        self.ent_fecha = ttk.Entry(box_form, width=12)
        self.ent_fecha.grid(row=0, column=1, padx=6, pady=4)
        self.ent_fecha.insert(0, _dt.date.today().strftime("%Y-%m-%d"))

        ttk.Label(box_form, text="Tipo:").grid(row=0, column=2, padx=6, pady=4, sticky="w")
        self.cb_tipo = ttk.Combobox(
            box_form,
            state="readonly",
            width=20,
            values=["Caja chica", "CAJA CHICA", "IPS", "Pago a personal", "Pago a profesionales", "Pagos varios"],
        )
        self.cb_tipo.grid(row=0, column=3, padx=6, pady=4)

        ttk.Label(box_form, text="Monto (Gs):").grid(row=0, column=4, padx=6, pady=4, sticky="w")
        self.ent_monto = ttk.Entry(box_form, width=15)
        self.ent_monto.grid(row=0, column=5, padx=6, pady=4)

        ttk.Label(box_form, text="Nro. factura:").grid(row=1, column=0, padx=6, pady=4, sticky="w")
        self.ent_factura = ttk.Entry(box_form, width=24)
        self.ent_factura.grid(row=1, column=1, padx=6, pady=4, sticky="w")

        ttk.Label(box_form, text="Forma de pago:").grid(row=1, column=2, padx=6, pady=4, sticky="w")
        self.cb_forma_pago = ttk.Combobox(
            box_form,
            state="readonly",
            width=18,
            values=PAYMENT_METHODS,
        )
        self.cb_forma_pago.set("Efectivo")
        self.cb_forma_pago.grid(row=1, column=3, padx=6, pady=4, sticky="w")

        self.lbl_ref_pago = ttk.Label(box_form, text="Referencia:")
        self.lbl_ref_pago.grid(row=1, column=4, padx=6, pady=4, sticky="w")
        self.ent_ref_pago = ttk.Entry(box_form, width=22)
        self.ent_ref_pago.grid(row=1, column=5, padx=6, pady=4, sticky="w")

        ttk.Label(box_form, text="Descripcion:").grid(row=2, column=0, padx=6, pady=4, sticky="w")
        self.ent_desc = ttk.Entry(box_form, width=80)
        self.ent_desc.grid(row=2, column=1, columnspan=5, padx=6, pady=4, sticky="we")

        ttk.Button(box_form, text="Registrar gasto", command=self._registrar_gasto).grid(
            row=0, column=6, rowspan=3, padx=10
        )
        ttk.Button(box_form, text="Importar extracto IPS", command=self._importar_extracto_ips).grid(
            row=0, column=7, rowspan=3, padx=(0, 10)
        )
        ttk.Button(box_form, text="Importar adelanto/salario", command=self._importar_rrhh_salarios).grid(
            row=0, column=8, rowspan=3, padx=(0, 10)
        )
        self.cb_tipo.bind("<<ComboboxSelected>>", self._on_tipo_change)
        self.cb_forma_pago.bind("<<ComboboxSelected>>", self._on_forma_pago_change)
        self._on_forma_pago_change()

        # --- Historial de gastos ---
        box_hist = ttk.LabelFrame(frame, text="Historial de gastos")
        box_hist.pack(fill="both", expand=True, padx=6, pady=6)

        filtros = ttk.Frame(box_hist)
        filtros.pack(fill="x", pady=4)
        ttk.Label(filtros, text="Desde (AAAA-MM-DD):").pack(side="left")
        self.ent_desde = ttk.Entry(filtros, width=12)
        self.ent_desde.pack(side="left", padx=4)
        ttk.Label(filtros, text="Hasta:").pack(side="left")
        self.ent_hasta = ttk.Entry(filtros, width=12)
        self.ent_hasta.pack(side="left", padx=4)
        ttk.Button(filtros, text="Filtrar", command=self._refresh_gastos).pack(side="left", padx=6)
        ttk.Button(filtros, text="Modificar seleccionado", command=self._modificar_gasto).pack(side="left", padx=6)
        ttk.Button(filtros, text="Exportar CSV", command=self._export_csv, style="Export.TButton").pack(side="left", padx=6)
        ttk.Button(filtros, text="Exportar Excel", command=self._export_excel, style="Export.TButton").pack(side="left", padx=6)

        cols = ("id", "fecha", "tipo", "factura", "forma_pago", "ref_pago", "desc", "monto")
        self.tv = ttk.Treeview(box_hist, columns=cols, show="headings", height=14)
        self.tv.heading("id", text="ID")
        self.tv.heading("fecha", text="Fecha")
        self.tv.heading("tipo", text="Tipo")
        self.tv.heading("factura", text="Factura")
        self.tv.heading("forma_pago", text="Pago")
        self.tv.heading("ref_pago", text="Referencia")
        self.tv.heading("desc", text="Descripcion")
        self.tv.heading("monto", text="Monto (Gs)")
        self.tv.column("id", width=0, anchor="center", stretch=False)
        self.tv.column("fecha", width=100, anchor="center")
        self.tv.column("tipo", width=160, anchor="center")
        self.tv.column("factura", width=120, anchor="center")
        self.tv.column("forma_pago", width=110, anchor="center")
        self.tv.column("ref_pago", width=150, anchor="center")
        self.tv.column("desc", width=270, anchor="w")
        self.tv.column("monto", width=120, anchor="center")
        self.tv.pack(fill="both", expand=True, padx=6, pady=6)

        self.lbl_total = ttk.Label(frame, text="Total mostrado: -")
        self.lbl_total.pack(anchor="w", padx=8, pady=(0, 8))

        self._refresh_gastos()

    def _init_table(self):
        db.run_ddl(
            self.repo.cn,
            """
            CREATE TABLE IF NOT EXISTS expenses(
                id BIGSERIAL PRIMARY KEY,
                ts DATETIME NOT NULL,
                tipo TEXT NOT NULL,
                descripcion TEXT,
                monto_gs REAL NOT NULL,
                nro_factura TEXT,
                forma_pago TEXT,
                referencia_pago TEXT
            );
            """
        )
        cur = self.repo.cn.cursor()
        cols = db.table_columns(self.repo.cn, "expenses")
        if "nro_factura" not in cols:
            cur.execute("ALTER TABLE expenses ADD COLUMN nro_factura TEXT;")
        if "forma_pago" not in cols:
            cur.execute("ALTER TABLE expenses ADD COLUMN forma_pago TEXT;")
        if "referencia_pago" not in cols:
            cur.execute("ALTER TABLE expenses ADD COLUMN referencia_pago TEXT;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dashboard_used_checks(
                id BIGSERIAL PRIMARY KEY,
                chequera_id TEXT NOT NULL,
                cheque_no TEXT NOT NULL,
                serie TEXT NOT NULL DEFAULT '',
                referencia TEXT,
                payment_group_id TEXT,
                used_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        self.repo.cn.commit()

    def _on_forma_pago_change(self, _evt=None):
        forma = (self.cb_forma_pago.get() or "").strip().lower()
        if forma == "cheque":
            self.lbl_ref_pago.config(text="Serie y cheque:")
            self.ent_ref_pago.configure(state="normal")
        elif forma == "transferencia":
            self.lbl_ref_pago.config(text="Nro. transferencia:")
            self.ent_ref_pago.configure(state="normal")
        else:
            self.lbl_ref_pago.config(text="Referencia:")
            self.ent_ref_pago.delete(0, tk.END)
            self.ent_ref_pago.configure(state="disabled")

    def _on_tipo_change(self, _evt=None):
        tipo = (self.cb_tipo.get() or "").strip().upper()
        if tipo == "IPS":
            self.cb_forma_pago.set("Homebanking")
        elif (self.cb_forma_pago.get() or "").strip() == "Homebanking":
            self.cb_forma_pago.set("Efectivo")
        self._on_forma_pago_change()

    def _expense_group_id(self, expense_id: int) -> str:
        return f"expense:{int(expense_id)}"

    def _parse_check_reference(self, value: str):
        txt = str(value or "").strip()
        if not txt:
            return None
        m_full = re.search(r"cheque\s*:\s*(\d+)\s*\|\s*serie\s*:\s*([A-Za-z0-9]+)", txt, flags=re.I)
        if m_full:
            cheque_no = re.sub(r"\D+", "", m_full.group(1))
            serie = re.sub(r"[^A-Za-z0-9]+", "", m_full.group(2)).upper()
            if serie and cheque_no:
                return serie, cheque_no, f"Cheque: {cheque_no} | Serie: {serie}"
        m = re.match(r"^\s*([A-Za-z0-9]+)\s*[-/ :]\s*(\d+)\s*$", txt)
        if m:
            serie = re.sub(r"[^A-Za-z0-9]+", "", m.group(1)).upper()
            cheque_no = re.sub(r"\D+", "", m.group(2))
            if serie and cheque_no:
                return serie, cheque_no, f"Cheque: {cheque_no} | Serie: {serie}"
        return None

    def _find_registered_check(self, serie: str, cheque_no: str):
        cur = self.repo.cn.cursor()
        row = cur.execute(
            """
            SELECT c.chequera_id, c.bank_id, b.banco_nombre, b.nro_cuenta,
                   c.formato_chequera, c.tipo_cheque, c.serie, c.nro_inicio, c.nro_fin
            FROM bank_checkbooks c
            JOIN banks b ON b.bank_id = c.bank_id
            WHERE UPPER(TRIM(COALESCE(c.serie, ''))) = %s
              AND %s BETWEEN COALESCE(c.nro_inicio, 0) AND COALESCE(c.nro_fin, 0)
            ORDER BY c.nro_inicio, c.nro_fin
            LIMIT 1;
            """,
            (str(serie or "").strip().upper(), int(cheque_no)),
        ).fetchone()
        if not row:
            return None
        return {
            "chequera_id": str(row[0] or "").strip(),
            "bank_id": str(row[1] or "").strip(),
            "bank_name": str(row[2] or "").strip(),
            "account_no": str(row[3] or "").strip(),
            "form_type": str(row[4] or "").strip(),
            "check_type": str(row[5] or "").strip(),
            "serie": str(row[6] or "").strip().upper(),
            "cheque_no": str(cheque_no).strip(),
        }

    def _is_check_used(self, chequera_id: str, cheque_no: str, serie: str, exclude_group_id: str = "") -> bool:
        cur = self.repo.cn.cursor()
        sql = """
            SELECT id
            FROM dashboard_used_checks
            WHERE chequera_id = %s
              AND cheque_no = %s
              AND UPPER(TRIM(COALESCE(serie, ''))) = %s
        """
        params = [str(chequera_id or "").strip(), str(cheque_no or "").strip(), str(serie or "").strip().upper()]
        if exclude_group_id:
            sql += " AND COALESCE(payment_group_id, '') <> %s"
            params.append(str(exclude_group_id))
        sql += " LIMIT 1;"
        row = cur.execute(sql, params).fetchone()
        return row is not None

    def _validate_registered_check(self, ref_value: str, exclude_group_id: str = ""):
        parsed = self._parse_check_reference(ref_value)
        if not parsed:
            return None, "Debe ingresar el cheque como SERIE-NUMERO. Ejemplo: CS-742127."
        serie, cheque_no, normalized_ref = parsed
        check = self._find_registered_check(serie, cheque_no)
        if not check:
            return None, f"El cheque {serie}-{cheque_no} no está cargado en chequeras."
        if self._is_check_used(check["chequera_id"], cheque_no, serie, exclude_group_id=exclude_group_id):
            return None, f"El cheque {serie}-{cheque_no} ya está marcado como usado."
        check["reference_value"] = normalized_ref
        return check, None

    def _mark_check_used(self, check_data: dict, payment_group_id: str, referencia: str):
        cur = self.repo.cn.cursor()
        cur.execute(
            """
            INSERT INTO dashboard_used_checks(chequera_id, cheque_no, serie, referencia, payment_group_id, used_ts)
            VALUES(%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """,
            (
                str(check_data.get("chequera_id") or "").strip(),
                str(check_data.get("cheque_no") or "").strip(),
                str(check_data.get("serie") or "").strip().upper(),
                str(referencia or "").strip(),
                str(payment_group_id or "").strip(),
            ),
        )
        self.repo.cn.commit()

    def _clear_check_usage(self, payment_group_id: str):
        cur = self.repo.cn.cursor()
        cur.execute("DELETE FROM dashboard_used_checks WHERE COALESCE(payment_group_id, '') = %s;", (str(payment_group_id),))
        self.repo.cn.commit()

    def _registrar_gasto(self):
        fecha = (self.ent_fecha.get() or "").strip()
        tipo = (self.cb_tipo.get() or "").strip()
        desc = (self.ent_desc.get() or "").strip()
        nro_factura = (self.ent_factura.get() or "").strip()
        forma_pago = (self.cb_forma_pago.get() or "").strip() or "Efectivo"
        referencia_pago = (self.ent_ref_pago.get() or "").strip()
        monto_txt = (self.ent_monto.get() or "").replace(".", "").replace(",", ".")

        if not fecha or not tipo or not monto_txt:
            messagebox.showwarning("Atencion", "Debe completar fecha, tipo y monto.")
            return

        try:
            monto = float(monto_txt)
            if monto <= 0:
                raise ValueError
        except Exception:
            messagebox.showerror("Error", "Monto invalido.")
            return

        try:
            _dt.datetime.strptime(fecha, "%Y-%m-%d")
        except Exception:
            messagebox.showerror("Error", "Formato de fecha invalido (use AAAA-MM-DD).")
            return

        if forma_pago.lower() in ("cheque", "transferencia") and not referencia_pago:
            tipo_ref = "cheque" if forma_pago.lower() == "cheque" else "transferencia"
            messagebox.showwarning("Atencion", f"Debe completar el numero de {tipo_ref}.")
            return
        if forma_pago.lower() == "efectivo":
            referencia_pago = ""

        cheque_data = None
        if forma_pago.lower() == "cheque":
            cheque_data, err = self._validate_registered_check(referencia_pago)
            if err:
                messagebox.showwarning("Atencion", err)
                return
            referencia_pago = str((cheque_data or {}).get("reference_value") or referencia_pago)

        expense_id = self._insert_expense(
            fecha=fecha,
            tipo=tipo,
            desc=desc,
            monto=monto,
            nro_factura=nro_factura,
            forma_pago=forma_pago,
            referencia_pago=referencia_pago,
        )
        if forma_pago.lower() == "cheque" and cheque_data:
            self._mark_check_used(cheque_data, self._expense_group_id(expense_id), referencia_pago)

        self.ent_monto.delete(0, tk.END)
        self.ent_desc.delete(0, tk.END)
        self.ent_factura.delete(0, tk.END)
        self.cb_forma_pago.set("Efectivo")
        self._on_forma_pago_change()
        self._refresh_gastos()
        messagebox.showinfo("OK", f"Gasto registrado ({tipo} - {monto:,.0f} Gs).")

    def _insert_expense(self, fecha, tipo, desc, monto, nro_factura="", forma_pago="Efectivo", referencia_pago=""):
        cur = self.repo.cn.cursor()
        cur.execute(
            """
            INSERT INTO expenses(ts,tipo,descripcion,monto_gs,nro_factura,forma_pago,referencia_pago)
            VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id;
            """,
            (fecha, tipo, desc, monto, nro_factura, forma_pago, referencia_pago),
        )
        self.repo.cn.commit()
        return int(cur.fetchone()[0] or 0)

    def _expense_exists_by_ref(self, tipo, nro_factura):
        if not nro_factura:
            return False
        cur = self.repo.cn.cursor()
        cur.execute(
            """
            SELECT id
            FROM expenses
            WHERE tipo = %s AND COALESCE(nro_factura, '') = %s
            LIMIT 1;
            """,
            (tipo, nro_factura),
        )
        return cur.fetchone() is not None

    def _importar_extracto_ips(self):
        script_path = Path(__file__).with_name("ips_preview_qt.py")

        try:
            proc = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=str(script_path.parent),
                capture_output=True,
                text=True,
                encoding=locale.getpreferredencoding(False) or "cp1252",
                errors="replace",
            )
        except Exception as exc:
            messagebox.showerror("IPS", f"No se pudo abrir la previsualización PySide6:\n{exc}")
            return

        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "Error desconocido").strip()
            messagebox.showerror("IPS", f"La previsualización IPS falló.\n\n{detail}")
            return

        payload_txt = (proc.stdout or "").strip()
        if not payload_txt:
            return

        try:
            payload = json.loads(payload_txt)
        except Exception as exc:
            messagebox.showerror("IPS", f"La previsualización devolvió una respuesta inválida:\n{exc}")
            return

        nro_ref = (payload.get("nro_factura") or "").strip()
        if self._expense_exists_by_ref("IPS", nro_ref):
            messagebox.showinfo("IPS", f"El extracto IPS {nro_ref} ya está registrado como gasto.")
            return

        desc = (payload.get("desc") or "").strip()
        monto = float(payload.get("monto") or 0)
        fecha = (payload.get("fecha") or "").strip()
        tipo = (payload.get("tipo") or "IPS").strip() or "IPS"

        self._insert_expense(
            fecha=fecha,
            tipo=tipo,
            desc=desc,
            monto=monto,
            nro_factura=nro_ref,
            forma_pago=(payload.get("forma_pago") or "Homebanking").strip() or "Homebanking",
            referencia_pago=(payload.get("referencia_pago") or "").strip(),
        )
        self._refresh_gastos()

        period_label = payload.get("periodo_display") or payload.get("periodo") or "sin período"
        messagebox.showinfo(
            "IPS",
            f"Gasto IPS insertado.\n\nPeríodo: {period_label}\nMonto: {self._fmt_gs(monto)} Gs\nReferencia: {nro_ref or '-'}",
        )

    def _importar_rrhh_salarios(self):
        script_path = Path(__file__).resolve().parent / "rrhh" / "rrhh_salary_preview_qt.py"
        try:
            proc = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=str(script_path.parent),
                capture_output=True,
                text=True,
                encoding=locale.getpreferredencoding(False) or "cp1252",
                errors="replace",
            )
        except Exception as exc:
            messagebox.showerror("RRHH", f"No se pudo abrir la previsualización PySide6:\n{exc}")
            return

        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "Error desconocido").strip()
            messagebox.showerror("RRHH", f"La previsualización RRHH falló.\n\n{detail}")
            return

        payload_txt = (proc.stdout or "").strip()
        if not payload_txt:
            return

        try:
            payload = json.loads(payload_txt)
        except Exception as exc:
            messagebox.showerror("RRHH", f"La previsualización devolvió una respuesta inválida:\n{exc}")
            return

        fecha = (payload.get("fecha") or "").strip()
        source_file = (payload.get("file_path") or "").strip()
        source_name = Path(source_file).name if source_file else "sin_archivo"
        rows = payload.get("rows") or []
        rrhh_repo = RRHHRepo()
        gastos_insertados = 0
        rrhh_insertados = 0
        omitidos = 0

        try:
            for idx, row in enumerate(rows, start=1):
                employee_id = row.get("employee_id")
                funcionario = (row.get("funcionario") or "").strip()
                documento = (row.get("documento") or "").strip()
                concepto = (row.get("concepto") or "").strip() or "Pago a personal"
                monto = float(row.get("monto") or 0)
                cuenta_destino = (row.get("cuenta_destino") or "").strip()
                confirmado = bool(row.get("confirmado"))

                if not employee_id or monto <= 0:
                    omitidos += 1
                    continue

                if not rrhh_repo.movement_exists(employee_id, fecha, concepto, monto, source_file):
                    rrhh_repo.insert_movement(
                        employee_id=employee_id,
                        fecha=fecha,
                        concepto=concepto,
                        monto_gs=monto,
                        confirmado=confirmado,
                        cuenta_destino=cuenta_destino,
                        documento_ci=documento,
                        source_file=source_file,
                    )
                    rrhh_insertados += 1

                expense_ref = f"{source_name}:{documento}:{idx}"
                expense_type = _expense_type_from_rrhh_concept(concepto)
                if self._expense_exists_by_ref(expense_type, expense_ref):
                    omitidos += 1
                    continue

                desc = f"{concepto} - {funcionario or documento}"
                self._insert_expense(
                    fecha=fecha,
                    tipo=expense_type,
                    desc=desc,
                    monto=monto,
                    nro_factura=expense_ref,
                    forma_pago="Homebanking",
                    referencia_pago=cuenta_destino,
                )
                gastos_insertados += 1
        finally:
            rrhh_repo.close()

        self._refresh_gastos()
        messagebox.showinfo(
            "RRHH / Gastos",
            "Importación finalizada.\n\n"
            f"Movimientos RRHH insertados: {rrhh_insertados}\n"
            f"Gastos insertados: {gastos_insertados}\n"
            f"Omitidos: {omitidos}\n"
            f"Fecha: {fecha}\n"
            f"Archivo: {source_name}",
        )

    def _fmt_gs(self, x):
        try:
            return f"{float(x):,.0f}".replace(",", ".")
        except Exception:
            return "-"

    def _refresh_gastos(self):
        for i in self.tv.get_children():
            self.tv.delete(i)

        d1 = (self.ent_desde.get() or "").strip() or None
        d2 = (self.ent_hasta.get() or "").strip() or None

        sql = """
            SELECT
                id,
                ts,
                tipo,
                COALESCE(nro_factura, ''),
                COALESCE(forma_pago, 'Efectivo'),
                COALESCE(referencia_pago, ''),
                descripcion,
                monto_gs
            FROM expenses
        """
        params = []
        conds = []
        if d1:
            conds.append("date(ts) >= date(%s)")
            params.append(d1)
        if d2:
            conds.append("date(ts) <= date(%s)")
            params.append(d2)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY ts DESC;"

        cur = self.repo.cn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()

        total = 0.0
        for gid, ts, tipo, nro_factura, forma_pago, ref_pago, desc, monto in rows:
            total += float(monto or 0)
            self.tv.insert(
                "",
                "end",
                values=(gid, ts, tipo, nro_factura, forma_pago, ref_pago, desc, self._fmt_gs(monto)),
            )
        self.lbl_total.config(text=f"Total mostrado: {self._fmt_gs(total)} Gs")

    def _modificar_gasto(self):
        item = self.tv.focus()
        if not item:
            messagebox.showinfo("Info", "Seleccione un gasto del historial.")
            return

        try:
            gid, fecha, tipo, nro_factura, forma_pago, ref_pago, desc, monto = self.tv.item(item, "values")
            gid = int(gid)
        except Exception as exc:
            messagebox.showerror("Error", f"Fila invalida: {exc}")
            return

        dlg = tk.Toplevel(self.parent)
        dlg.title(f"Modificar gasto #{gid}")
        dlg.transient(self.parent)
        dlg.grab_set()
        dlg.resizable(False, False)

        ttk.Label(dlg, text="Fecha (AAAA-MM-DD):").grid(row=0, column=0, padx=6, pady=6, sticky="e")
        ent_fecha = ttk.Entry(dlg, width=12)
        ent_fecha.insert(0, str(fecha))
        ent_fecha.grid(row=0, column=1, padx=6, pady=6, sticky="w")

        ttk.Label(dlg, text="Tipo:").grid(row=1, column=0, padx=6, pady=6, sticky="e")
        cb_tipo = ttk.Combobox(
            dlg,
            state="readonly",
            width=20,
            values=["Caja chica", "CAJA CHICA", "IPS", "Pago a personal", "Pago a profesionales", "Pagos varios"],
        )
        cb_tipo.set(tipo)
        cb_tipo.grid(row=1, column=1, padx=6, pady=6, sticky="w")

        ttk.Label(dlg, text="Nro. factura:").grid(row=2, column=0, padx=6, pady=6, sticky="e")
        ent_factura = ttk.Entry(dlg, width=24)
        ent_factura.insert(0, str(nro_factura or ""))
        ent_factura.grid(row=2, column=1, padx=6, pady=6, sticky="w")

        ttk.Label(dlg, text="Forma de pago:").grid(row=3, column=0, padx=6, pady=6, sticky="e")
        cb_forma_pago = ttk.Combobox(
            dlg,
            state="readonly",
            width=18,
            values=PAYMENT_METHODS,
        )
        cb_forma_pago.set(str(forma_pago or "Efectivo"))
        cb_forma_pago.grid(row=3, column=1, padx=6, pady=6, sticky="w")

        lbl_ref = ttk.Label(dlg, text="Referencia:")
        lbl_ref.grid(row=4, column=0, padx=6, pady=6, sticky="e")
        ent_ref = ttk.Entry(dlg, width=24)
        ent_ref.insert(0, str(ref_pago or ""))
        ent_ref.grid(row=4, column=1, padx=6, pady=6, sticky="w")

        ttk.Label(dlg, text="Monto (Gs):").grid(row=5, column=0, padx=6, pady=6, sticky="e")
        ent_monto = ttk.Entry(dlg, width=15)
        ent_monto.insert(0, str(monto).replace(".", ""))
        ent_monto.grid(row=5, column=1, padx=6, pady=6, sticky="w")

        ttk.Label(dlg, text="Descripcion:").grid(row=6, column=0, padx=6, pady=6, sticky="e")
        ent_desc = ttk.Entry(dlg, width=50)
        ent_desc.insert(0, str(desc or ""))
        ent_desc.grid(row=6, column=1, padx=6, pady=6, sticky="w")

        btns = ttk.Frame(dlg)
        btns.grid(row=7, column=0, columnspan=2, pady=8)

        def _sync_ref_label(*_):
            forma = (cb_forma_pago.get() or "").strip().lower()
            if forma == "cheque":
                lbl_ref.config(text="Serie y cheque:")
                ent_ref.configure(state="normal")
            elif forma == "transferencia":
                lbl_ref.config(text="Nro. transferencia:")
                ent_ref.configure(state="normal")
            else:
                lbl_ref.config(text="Referencia:")
                ent_ref.delete(0, tk.END)
                ent_ref.configure(state="disabled")

        def _sync_tipo(*_):
            tipo_sel = (cb_tipo.get() or "").strip().upper()
            if tipo_sel == "IPS":
                cb_forma_pago.set("Homebanking")
            elif (cb_forma_pago.get() or "").strip() == "Homebanking":
                cb_forma_pago.set("Efectivo")
            _sync_ref_label()

        cb_tipo.bind("<<ComboboxSelected>>", _sync_tipo)
        cb_forma_pago.bind("<<ComboboxSelected>>", _sync_ref_label)
        _sync_ref_label()

        def _ok():
            fecha_n = (ent_fecha.get() or "").strip()
            tipo_n = (cb_tipo.get() or "").strip()
            factura_n = (ent_factura.get() or "").strip()
            forma_n = (cb_forma_pago.get() or "").strip() or "Efectivo"
            ref_n = (ent_ref.get() or "").strip()
            desc_n = (ent_desc.get() or "").strip()
            monto_txt = (ent_monto.get() or "").replace(".", "").replace(",", ".")

            if not fecha_n or not tipo_n or not monto_txt:
                messagebox.showwarning("Atencion", "Debe completar fecha, tipo y monto.")
                return

            try:
                monto_n = float(monto_txt)
                if monto_n <= 0:
                    raise ValueError
            except Exception:
                messagebox.showerror("Error", "Monto invalido.")
                return

            try:
                _dt.datetime.strptime(fecha_n, "%Y-%m-%d")
            except Exception:
                messagebox.showerror("Error", "Formato de fecha invalido (use AAAA-MM-DD).")
                return

            if forma_n.lower() in ("cheque", "transferencia") and not ref_n:
                tipo_ref = "cheque" if forma_n.lower() == "cheque" else "transferencia"
                messagebox.showwarning("Atencion", f"Debe completar el numero de {tipo_ref}.")
                return
            if forma_n.lower() == "efectivo":
                ref_n = ""

            cheque_data = None
            if forma_n.lower() == "cheque":
                cheque_data, err = self._validate_registered_check(ref_n, exclude_group_id=self._expense_group_id(gid))
                if err:
                    messagebox.showwarning("Atencion", err)
                    return
                ref_n = str((cheque_data or {}).get("reference_value") or ref_n)

            cur = self.repo.cn.cursor()
            cur.execute(
                """
                UPDATE expenses
                SET ts=%s, tipo=%s, descripcion=%s, monto_gs=%s, nro_factura=%s, forma_pago=%s, referencia_pago=%s
                WHERE id=%s;
                """,
                (fecha_n, tipo_n, desc_n, monto_n, factura_n, forma_n, ref_n, gid),
            )
            self.repo.cn.commit()
            self._clear_check_usage(self._expense_group_id(gid))
            if forma_n.lower() == "cheque" and cheque_data:
                self._mark_check_used(cheque_data, self._expense_group_id(gid), ref_n)
            self._refresh_gastos()
            messagebox.showinfo("OK", "Gasto actualizado.")
            dlg.destroy()

        ttk.Button(btns, text="Guardar cambios", command=_ok).pack(side="left", padx=6)
        ttk.Button(btns, text="Cancelar", command=dlg.destroy).pack(side="left", padx=6)

    # =======================
    #      Exportaciones
    # =======================
    def _build_export_rows(self):
        d1 = (self.ent_desde.get() or "").strip() or None
        d2 = (self.ent_hasta.get() or "").strip() or None

        sql = """
            SELECT
                ts,
                tipo,
                descripcion,
                monto_gs,
                COALESCE(nro_factura, ''),
                COALESCE(forma_pago, 'Efectivo'),
                COALESCE(referencia_pago, '')
            FROM expenses
        """
        params = []
        conds = []
        if d1:
            conds.append("date(ts) >= date(%s)")
            params.append(d1)
        if d2:
            conds.append("date(ts) <= date(%s)")
            params.append(d2)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY ts DESC;"

        cur = self.repo.cn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        headers = ["fecha", "tipo", "descripcion", "monto_gs", "nro_factura", "forma_pago", "referencia_pago"]
        return headers, rows

    def _write_csv(self, headers, rows, fname):
        with open(fname, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(headers)
            for row in rows:
                w.writerow(row)
        return os.path.abspath(fname)

    def _write_xlsx(self, headers, rows, fname):
        try:
            import openpyxl
        except Exception as exc:
            return False, str(exc)
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(list(headers))
        for row in rows:
            ws.append(list(row))
        wb.save(fname)
        return True, os.path.abspath(fname)

    def _export_csv(self):
        headers, rows = self._build_export_rows()
        stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        fname = f"gastos_{stamp}.csv"
        path = self._write_csv(headers, rows, fname)
        messagebox.showinfo("Exportado", f"Archivo guardado: {path}")

    def _export_excel(self):
        headers, rows = self._build_export_rows()
        stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        fname = f"gastos_{stamp}.xlsx"
        ok, info = self._write_xlsx(headers, rows, fname)
        if ok:
            messagebox.showinfo("Exportado", f"Archivo guardado: {info}")
            return
        fname_csv = f"gastos_{stamp}.csv"
        path = self._write_csv(headers, rows, fname_csv)
        messagebox.showinfo("Exportado", f"No se pudo crear Excel ({info}). CSV guardado: {path}")

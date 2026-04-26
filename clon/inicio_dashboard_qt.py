# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import re
import sys
import subprocess
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import db
from PySide6 import QtCore, QtGui, QtWidgets


ORDERS_DB = ROOT_DIR / "PDFMK10" / "db" / "pedidos.db"
INVOICES_DB = ROOT_DIR / "importadorfactur" / "facturas.db"
PAYMENTS_JSON = ROOT_DIR / "pagos_compras copy.json"
PAYMENTS_DETAIL_JSON = ROOT_DIR / "pagos_compras_detalle.json"
FRACC_DB = ROOT_DIR / "GCMK8" / "fraccionadora.db"
COBROS_VENTAS_JSON = ROOT_DIR / "cobros_ventas.json"
COBROS_VENTAS_DETAIL_JSON = ROOT_DIR / "cobros_ventas_detalle.json"
FRACC_SCRIPT = ROOT_DIR / "GCMK8" / "fraccionadora.py"
INVOICE_IMPORTER_SCRIPT = ROOT_DIR / "importadorfactur" / "facturas_tabs.py"
OC_IMPORTER_SCRIPT = ROOT_DIR / "PDFMK10" / "app_tk.py"
SALES_REPORT_SCRIPT = ROOT_DIR / "clon" / "reportes_ventas_qt.py"
MONTHLY_REPORT_SCRIPT = ROOT_DIR / "clon" / "reporte_mensual_qt.py"

DELIVERY_SLA_DAYS = 2
CREDIT_TERM_DAYS = 15


@dataclass
class OrderRow:
    oc_id: int
    numero: str
    sucursal: str
    fecha_pedido: str
    fecha_compromiso: str
    dias_atraso: int | None
    estado: str
    prioridad: str
    pct_listo_entrega: float
    monto_total: float


@dataclass
class PaymentRow:
    factura_id: int
    proveedor: str
    numero_doc: str
    fecha_emision: str
    vencimiento: str
    dias_para_vencer: int | None
    monto: float
    estado: str
    sucursal: str


@dataclass
class CollectionRow:
    invoice_id: int
    ts: str
    invoice_no: str
    customer: str
    gravada5_gs: float
    iva5_gs: float
    gravada10_gs: float
    iva10_gs: float
    total_gs: float
    total_con_retencion: float
    dias_sin_cobrar: int | None
    dias_para_cobro: int | None


class KpiCard(QtWidgets.QFrame):
    def __init__(self, title: str, parent: QtWidgets.QWidget | None = None):
        super().__init__(parent)
        self.setObjectName("kpiCard")
        lay = QtWidgets.QVBoxLayout(self)
        lay.setContentsMargins(10, 8, 10, 8)
        lay.setSpacing(2)
        self.lbl_title = QtWidgets.QLabel(title)
        self.lbl_value = QtWidgets.QLabel("0")
        self.lbl_sub = QtWidgets.QLabel("")
        self.lbl_title.setObjectName("kpiTitle")
        self.lbl_value.setObjectName("kpiValue")
        self.lbl_sub.setObjectName("kpiSub")
        lay.addWidget(self.lbl_title)
        lay.addWidget(self.lbl_value)
        lay.addWidget(self.lbl_sub)

    def set_data(self, value: str, sub: str = "") -> None:
        self.lbl_value.setText(value)
        self.lbl_sub.setText(sub)


class DashboardRepo:
    def __init__(
        self,
        orders_db: Path = ORDERS_DB,
        invoices_db: Path = INVOICES_DB,
        fracc_db: Path = FRACC_DB,
    ):
        self.orders_db = orders_db
        self.invoices_db = invoices_db
        self.fracc_db = fracc_db
        self._ensure_dashboard_state_storage()
        self._ensure_local_check_usage_storage()

    def _connect_fracc(self, row_factory=None):
        return db.connect("fraccionadora")

    def _connect_fracc_local_sqlite(self, row_factory=None):
        return db.connect("fraccionadora")

    @staticmethod
    def _norm_text(value: str) -> str:
        text = str(value or "").strip().lower()
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
        text = re.sub(r"[^a-z0-9]+", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    @classmethod
    def _extract_gramaje(cls, desc: str) -> int | None:
        raw = str(desc or "")
        s = cls._norm_text(raw)
        if not s:
            return None
        m = re.search(r"(\d+)\s*(kg|kilo|kilos)\b", s)
        if m:
            return int(m.group(1)) * 1000
        m = re.search(r"(\d+)\s*(g|gr|gramo|gramos)\b", s)
        if m:
            return int(m.group(1))
        m = re.search(r"\*\s*(\d{2,4})\b", raw.lower())
        if m:
            return int(m.group(1))
        return None

    @classmethod
    def _match_product_name(cls, desc: str, products: list[tuple[int, str]]) -> tuple[int, str] | None:
        norm_desc = f" {cls._norm_text(desc)} "
        if not norm_desc.strip():
            return None
        aliases = {
            "arroz": (" arroz ",),
            "azucar": (" azucar ", " azucar el cacique "),
            "galleta molida": (" galleta molida ", " gall molida ", " gall molida el cacique "),
            "pororo": (" pororo ", " pororo el cacique "),
            "poroto rojo": (" poroto rojo ", " poroto rojo el cacique "),
            "locro": (" locro ",),
            "locrillo": (" locrillo ",),
            "lenteja": (" lenteja ",),
        }
        for pid, name in products:
            norm_name = cls._norm_text(name)
            token = f" {norm_name} "
            if token.strip() and token in norm_desc:
                return int(pid), str(name)
            for alias in aliases.get(norm_name, ()):
                if alias in norm_desc:
                    return int(pid), str(name)
        return None

    def _build_fracc_stock_cache(self) -> tuple[list[tuple[int, str]], dict[tuple[int, int], int]]:
        if not self.fracc_db.exists():
            return [], {}
        cn = self._connect_fracc()
        cur = cn.cursor()
        try:
            products = [(int(pid), str(name or "")) for pid, name in cur.execute("SELECT id, name FROM products;").fetchall()]
            stock_rows = cur.execute("SELECT product_id, gramaje, paquetes FROM package_stock;").fetchall()
        finally:
            cn.close()
        stock_map: dict[tuple[int, int], int] = {}
        for pid, gram, paquetes in stock_rows:
            stock_map[(int(pid), int(gram))] = int(paquetes or 0)
        return products, stock_map

    def _order_ready_percentage(
        self,
        oc_id: int,
        products: list[tuple[int, str]],
        stock_map: dict[tuple[int, int], int],
    ) -> float:
        if not self.orders_db.exists():
            return 0.0
        cn = db.connect("pedidos")
        cur = cn.cursor()
        try:
            rows = cur.execute(
                """
                SELECT descripcion, cantidad
                FROM orden_item
                WHERE oc_id = %s
                  AND COALESCE(enviado, 0) = 0
                  AND descripcion IS NOT NULL;
                """,
                (int(oc_id),),
            ).fetchall()
        finally:
            cn.close()

        total = len(rows)
        if total <= 0:
            return 100.0

        ready = 0
        for row in rows:
            desc = str(row["descripcion"] or "")
            qty = self._safe_int(row["cantidad"])
            gram = self._extract_gramaje(desc)
            match = self._match_product_name(desc, products)
            if gram is None or match is None:
                continue
            pid, _name = match
            disp = int(stock_map.get((int(pid), int(gram)), 0))
            if disp >= qty:
                ready += 1
        return (ready / total) * 100.0

    def order_missing_items(self, oc_id: int) -> list[dict]:
        products, stock_map = self._build_fracc_stock_cache()
        if not self.orders_db.exists():
            return []
        cn = db.connect("pedidos")
        cur = cn.cursor()
        try:
            rows = cur.execute(
                """
                SELECT linea, descripcion, cantidad
                FROM orden_item
                WHERE oc_id = %s
                  AND COALESCE(enviado, 0) = 0
                  AND descripcion IS NOT NULL
                ORDER BY linea, id;
                """,
                (int(oc_id),),
            ).fetchall()
        finally:
            cn.close()

        out: list[dict] = []
        for row in rows:
            desc = str(row["descripcion"] or "")
            qty = self._safe_int(row["cantidad"])
            gram = self._extract_gramaje(desc)
            match = self._match_product_name(desc, products)
            if gram is None or match is None:
                out.append(
                    {
                        "linea": self._safe_int(row["linea"]),
                        "descripcion": desc,
                        "necesario": qty,
                        "disponible": None,
                        "faltante": None,
                        "estado": "Sin mapa",
                        "producto": "",
                    }
                )
                continue

            pid, pname = match
            disp = int(stock_map.get((int(pid), int(gram)), 0))
            if disp >= qty:
                continue
            out.append(
                {
                    "linea": self._safe_int(row["linea"]),
                    "descripcion": desc,
                    "necesario": qty,
                    "disponible": disp,
                    "faltante": max(0, qty - disp),
                    "estado": "Faltante",
                    "producto": pname,
                }
            )
        return out

    def _ensure_dashboard_state_storage(self) -> None:
        if not self.fracc_db.exists():
            return
        cn = self._connect_fracc()
        db.run_ddl(cn,
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
        cn.commit()
        cn.close()
        self._migrate_dashboard_state_from_json()
        self._sync_payment_flags_from_details()

    def _ensure_local_check_usage_storage(self) -> None:
        if not self.fracc_db.exists():
            return
        cn = self._connect_fracc_local_sqlite()
        db.run_ddl(cn,
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

            CREATE UNIQUE INDEX IF NOT EXISTS idx_dash_used_checks_unique
                ON dashboard_used_checks(chequera_id, cheque_no, serie);
            """
        )
        cn.commit()
        cn.close()

    def _migrate_dashboard_state_from_json(self) -> None:
        if PAYMENTS_JSON.exists():
            try:
                existing_flags = self.load_paid_map()
                raw = json.loads(PAYMENTS_JSON.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    for k, v in raw.items():
                        try:
                            lot_id = int(str(k).strip())
                            if lot_id not in existing_flags:
                                self.save_payment_flag(lot_id, bool(v), sync_legacy=False)
                        except Exception:
                            continue
            except Exception:
                pass
        if PAYMENTS_DETAIL_JSON.exists():
            try:
                raw = json.loads(PAYMENTS_DETAIL_JSON.read_text(encoding="utf-8"))
                if isinstance(raw, list):
                    for item in raw:
                        if isinstance(item, dict):
                            self.upsert_payment_detail(item)
            except Exception:
                pass
        if COBROS_VENTAS_JSON.exists():
            try:
                raw = json.loads(COBROS_VENTAS_JSON.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    for key, value in raw.items():
                        if not isinstance(key, str):
                            continue
                        invoice_id = 0
                        invoice_ts = ""
                        invoice_no = ""
                        parts = key.split(":")
                        if len(parts) >= 2:
                            try:
                                invoice_id = int(parts[1])
                            except Exception:
                                invoice_id = 0
                        if len(parts) >= 4:
                            invoice_ts = parts[2]
                            invoice_no = parts[3]
                        self.save_collection_flag(invoice_id, invoice_ts, invoice_no, bool(value), raw_key=key)
            except Exception:
                pass
        if COBROS_VENTAS_DETAIL_JSON.exists():
            try:
                raw = json.loads(COBROS_VENTAS_DETAIL_JSON.read_text(encoding="utf-8"))
                if isinstance(raw, list):
                    for item in raw:
                        if isinstance(item, dict):
                            self.upsert_collection_detail(item)
            except Exception:
                pass

    def _sync_payment_flags_from_details(self) -> None:
        if not self.fracc_db.exists():
            return
        try:
            cn = self._connect_fracc()
            cur = cn.cursor()
            rows = cur.execute(
                """
                SELECT DISTINCT lot_id
                FROM dashboard_payment_details
                WHERE COALESCE(lot_id, 0) > 0;
                """
            ).fetchall()
            cn.close()
        except Exception:
            return
        for (lot_id,) in rows:
            try:
                self.save_payment_flag(int(lot_id), True)
            except Exception:
                continue

    def _write_legacy_payment_flag(self, lot_id: int, paid: bool) -> None:
        try:
            PAYMENTS_JSON.parent.mkdir(parents=True, exist_ok=True)
            raw = {}
            if PAYMENTS_JSON.exists():
                try:
                    loaded = json.loads(PAYMENTS_JSON.read_text(encoding="utf-8"))
                    if isinstance(loaded, dict):
                        raw = loaded
                except Exception:
                    raw = {}
            raw[str(int(lot_id))] = bool(paid)
            PAYMENTS_JSON.write_text(json.dumps(raw, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    @staticmethod
    def _collection_keys(invoice_id: int, ts: str, nro: str) -> list[str]:
        inv_id = int(invoice_id or 0)
        ts_txt = str(ts or "").strip()
        nro_txt = str(nro or "").strip()
        return [f"std:{inv_id}:{ts_txt}:{nro_txt}", f"std:{inv_id}"]

    def load_paid_map(self) -> dict[int, bool]:
        if not self.fracc_db.exists():
            return {}
        cn = self._connect_fracc()
        cur = cn.cursor()
        rows = cur.execute("SELECT lot_id, paid FROM dashboard_payment_flags;").fetchall()
        cn.close()
        out: dict[int, bool] = {}
        for lot_id, paid in rows:
            out[int(lot_id)] = bool(paid)
        return out

    def load_collections_map(self) -> dict[str, bool]:
        if not self.fracc_db.exists():
            return {}
        cn = self._connect_fracc()
        cur = cn.cursor()
        rows = cur.execute(
            "SELECT status_key, invoice_id, invoice_ts, invoice_no, collected FROM dashboard_collection_flags;"
        ).fetchall()
        cn.close()
        out: dict[str, bool] = {}
        for status_key, invoice_id, invoice_ts, invoice_no, collected in rows:
            status_val = bool(collected)
            if status_key:
                out[str(status_key)] = status_val
            for key in self._collection_keys(int(invoice_id or 0), str(invoice_ts or ""), str(invoice_no or "")):
                out[key] = status_val
        return out

    def save_payment_flag(self, lot_id: int, paid: bool, sync_legacy: bool = True) -> None:
        cn = self._connect_fracc()
        cur = cn.cursor()
        cur.execute(
            """
            INSERT INTO dashboard_payment_flags(lot_id, paid, updated_ts)
            VALUES(%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT(lot_id) DO UPDATE SET
                paid=excluded.paid,
                updated_ts=CURRENT_TIMESTAMP;
            """,
            (int(lot_id), 1 if paid else 0),
        )
        cn.commit()
        cn.close()
        if sync_legacy:
            self._write_legacy_payment_flag(lot_id, paid)

    def upsert_payment_detail(self, payload: dict) -> None:
        payment_group_id = str(payload.get("payment_group_id") or "").strip()
        lot_id = int(payload.get("lot_id") or 0)
        if not payment_group_id or lot_id <= 0:
            return
        facturas_grupo = payload.get("facturas_grupo")
        facturas_grupo_json = json.dumps(facturas_grupo, ensure_ascii=False) if facturas_grupo is not None else ""
        cn = self._connect_fracc()
        cur = cn.cursor()
        cur.execute(
            """
            INSERT INTO dashboard_payment_details(
                payment_group_id, lot_id, proveedor, factura, monto_gs, fecha_pago, medio,
                referencia, nro_deposito, nro_recibo_dinero, observacion, facturas_grupo_json,
                total_grupo_gs, ts_registro, ts_modificacion
            )
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(payment_group_id, lot_id) DO UPDATE SET
                proveedor=excluded.proveedor,
                factura=excluded.factura,
                monto_gs=excluded.monto_gs,
                fecha_pago=excluded.fecha_pago,
                medio=excluded.medio,
                referencia=excluded.referencia,
                nro_deposito=excluded.nro_deposito,
                nro_recibo_dinero=excluded.nro_recibo_dinero,
                observacion=excluded.observacion,
                facturas_grupo_json=excluded.facturas_grupo_json,
                total_grupo_gs=excluded.total_grupo_gs,
                ts_registro=excluded.ts_registro,
                ts_modificacion=excluded.ts_modificacion;
            """,
            (
                payment_group_id,
                lot_id,
                str(payload.get("proveedor") or ""),
                str(payload.get("factura") or ""),
                float(payload.get("monto_gs") or 0.0),
                str(payload.get("fecha_pago") or ""),
                str(payload.get("medio") or ""),
                str(payload.get("referencia") or ""),
                str(payload.get("nro_deposito") or ""),
                str(payload.get("nro_recibo_dinero") or ""),
                str(payload.get("observacion") or ""),
                facturas_grupo_json,
                float(payload.get("total_grupo_gs") or 0.0),
                str(payload.get("ts_registro") or ""),
                str(payload.get("ts_modificacion") or ""),
            ),
        )
        cn.commit()
        cn.close()

    def load_payment_details(self) -> list[dict]:
        if not self.fracc_db.exists():
            return []
        cn = self._connect_fracc()
        cur = cn.cursor()
        rows = cur.execute(
            """
            SELECT payment_group_id, lot_id, proveedor, factura, monto_gs, fecha_pago, medio,
                   referencia, nro_deposito, nro_recibo_dinero, observacion, facturas_grupo_json,
                   total_grupo_gs, ts_registro, ts_modificacion
            FROM dashboard_payment_details
            ORDER BY fecha_pago DESC, ts_registro DESC, proveedor DESC, factura DESC;
            """
        ).fetchall()
        cn.close()
        out: list[dict] = []
        for row in rows:
            item = {key: row[key] for key in row.keys()}
            raw_group = str(item.get("facturas_grupo_json") or "")
            if raw_group:
                try:
                    item["facturas_grupo"] = json.loads(raw_group)
                except Exception:
                    item["facturas_grupo"] = []
            else:
                item["facturas_grupo"] = []
            out.append(item)
        return out

    def save_payment_details(self, rows: list[dict]) -> None:
        for row in rows:
            if isinstance(row, dict):
                self.upsert_payment_detail(row)

    def load_available_payment_checks(self) -> list[dict]:
        if not self.fracc_db.exists():
            return []
        cn = self._connect_fracc_local_sqlite()
        cur = cn.cursor()
        try:
            if not db.table_exists(cn, "bank_checkbooks"):
                cn.close()
                return []
            cols = db.table_columns(cn, "bank_checkbooks")
            if "bank_id" in cols and db.table_exists(cn, "banks"):
                if "formato_chequera" in cols and "tipo_cheque" in cols:
                    rows = cur.execute(
                        """
                        SELECT c.chequera_id, c.bank_id, b.banco_nombre, b.nro_cuenta,
                               c.formato_chequera, c.tipo_cheque, c.serie, c.nro_inicio, c.nro_fin
                        FROM bank_checkbooks c
                        JOIN banks b ON b.bank_id = c.bank_id
                        ORDER BY b.banco_nombre, b.nro_cuenta, c.nro_inicio, c.nro_fin;
                        """
                    ).fetchall()
                else:
                    rows = cur.execute(
                        """
                        SELECT c.chequera_id, c.bank_id, b.banco_nombre, b.nro_cuenta,
                               c.formulario_tipo, c.nro_inicio, c.nro_fin
                        FROM bank_checkbooks c
                        JOIN banks b ON b.bank_id = c.bank_id
                        ORDER BY b.banco_nombre, b.nro_cuenta, c.nro_inicio, c.nro_fin;
                        """
                    ).fetchall()
            else:
                rows = cur.execute(
                    """
                    SELECT chequera_id, '' AS bank_id, banco_nombre, nro_cuenta,
                           formulario_tipo, nro_inicio, nro_fin
                    FROM bank_checkbooks
                    ORDER BY banco_nombre, nro_cuenta, nro_inicio, nro_fin;
                    """
                ).fetchall()
            used_rows = cur.execute(
                """
                SELECT DISTINCT referencia
                FROM dashboard_payment_details
                WHERE LOWER(COALESCE(medio, '')) = 'cheque'
                  AND TRIM(COALESCE(referencia, '')) <> '';
                """
            ).fetchall()
        except Exception:
            cn.close()
            return []
        cn.close()

        used_refs = set()
        for row in used_rows:
            ref_txt = str(row["referencia"] or "").strip()
            if not ref_txt:
                continue
            used_refs.add(ref_txt)
            ref_parts = [part.strip() for part in ref_txt.split("|") if part.strip()]
            if ref_parts:
                used_refs.add(ref_parts[0])
            m_ref = re.search(r"cheque\s*:\s*([0-9]+)", ref_txt, re.I)
            if m_ref:
                used_refs.add(m_ref.group(1).strip())
        out: list[dict] = []
        for row in rows:
            bank_name = str(row["banco_nombre"] or "").strip()
            account = str(row["nro_cuenta"] or "").strip()
            row_keys = set(row.keys())
            form_type = str(
                (row["formato_chequera"] if "formato_chequera" in row_keys else row["formulario_tipo"])
                or ""
            ).strip()
            cheque_type = str((row["tipo_cheque"] if "tipo_cheque" in row_keys else "Vista") or "Vista").strip()
            serie = str((row["serie"] if "serie" in row_keys else "") or "").strip()
            chequera_id = str(row["chequera_id"] or "").strip()
            start = int(row["nro_inicio"] or 0)
            end = int(row["nro_fin"] or 0)
            if end < start:
                continue
            for cheque_no in range(start, end + 1):
                cheque_txt = str(cheque_no)
                ref_key = f"Cheque: {cheque_txt} | Serie: {serie or '-'}"
                if cheque_txt in used_refs or ref_key in used_refs:
                    continue
                out.append(
                    {
                        "cheque_no": cheque_txt,
                        "chequera_id": chequera_id,
                        "bank_id": str(row["bank_id"] or "").strip(),
                        "bank_name": bank_name,
                        "account_no": account,
                        "form_type": form_type,
                        "check_type": cheque_type,
                        "serie": serie,
                        "reference_value": ref_key,
                        "group_label": f"{bank_name} / {form_type} / {cheque_type} / Serie {serie or '-'}",
                        "label": f"{cheque_txt} | {bank_name} | Cta {account} | {form_type} | {cheque_type} | Serie {serie or '-'}",
                    }
                )
        return out

    def find_loaded_payment_check(self, serie: str, cheque_no: str) -> dict | None:
        if not self.fracc_db.exists():
            return None
        serie_txt = str(serie or "").strip().upper()
        cheque_txt = re.sub(r"\D+", "", str(cheque_no or "").strip())
        if not serie_txt or not cheque_txt.isdigit():
            return None
        cheque_n = int(cheque_txt)
        cn = self._connect_fracc_local_sqlite()
        cur = cn.cursor()
        try:
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
                (serie_txt, cheque_n),
            ).fetchone()
            if not row:
                return None
            chequera_id, bank_id, banco_nombre, nro_cuenta, formato_chequera, tipo_cheque, serie_db, _nro_inicio, _nro_fin = row
            used = cur.execute(
                """
                SELECT id
                FROM dashboard_used_checks
                WHERE chequera_id = %s
                  AND cheque_no = %s
                  AND UPPER(TRIM(COALESCE(serie, ''))) = %s
                LIMIT 1;
                """,
                (str(chequera_id or "").strip(), cheque_txt, serie_txt),
            ).fetchone()
            return {
                "cheque_no": cheque_txt,
                "chequera_id": str(chequera_id or "").strip(),
                "bank_id": str(bank_id or "").strip(),
                "bank_name": str(banco_nombre or "").strip(),
                "account_no": str(nro_cuenta or "").strip(),
                "form_type": str(formato_chequera or "").strip(),
                "check_type": str(tipo_cheque or "").strip(),
                "serie": str(serie_db or "").strip(),
                "reference_value": f"Cheque: {cheque_txt} | Serie: {serie_txt}",
                "used": used is not None,
            }
        finally:
            cn.close()

    def mark_payment_check_used(self, check_data: dict, payment_group_id: str, referencia: str) -> None:
        chequera_id = str(check_data.get("chequera_id") or "").strip()
        cheque_no = str(check_data.get("cheque_no") or "").strip()
        serie = str(check_data.get("serie") or "").strip().upper()
        if not chequera_id or not cheque_no:
            return
        cn = self._connect_fracc_local_sqlite()
        cur = cn.cursor()
        cur.execute(
            """
            INSERT INTO dashboard_used_checks(chequera_id, cheque_no, serie, referencia, payment_group_id, used_ts)
            VALUES(%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT(chequera_id, cheque_no, serie) DO UPDATE SET
                referencia=excluded.referencia,
                payment_group_id=excluded.payment_group_id,
                used_ts=CURRENT_TIMESTAMP;
            """,
            (chequera_id, cheque_no, serie, str(referencia or ""), str(payment_group_id or "")),
        )
        cn.commit()
        cn.close()

    def save_collection_flag(self, invoice_id: int, ts: str, nro: str, collected: bool, raw_key: str | None = None) -> None:
        cn = self._connect_fracc()
        cur = cn.cursor()
        keys = [str(raw_key).strip()] if raw_key else self._collection_keys(invoice_id, ts, nro)
        for key in keys:
            if not key:
                continue
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
                (key, int(invoice_id or 0), str(ts or "").strip(), str(nro or "").strip(), 1 if collected else 0),
            )
        cn.commit()
        cn.close()

    def upsert_collection_detail(self, payload: dict) -> None:
        invoice_id = int(payload.get("invoice_id") or 0)
        invoice_ts = str(payload.get("invoice_ts") or "").strip()
        invoice_no = str(payload.get("invoice_no") or "").strip()
        if invoice_id <= 0:
            return
        cn = self._connect_fracc()
        cur = cn.cursor()
        cur.execute(
            """
            INSERT INTO dashboard_collection_details(
                invoice_id, invoice_ts, invoice_no, cliente, monto_total_gs, monto_total_ret_gs,
                fecha_cobro, medio, nro_cheque, nro_deposito, referencia, observacion,
                ts_registro, ts_modificacion
            )
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(invoice_id, invoice_ts, invoice_no) DO UPDATE SET
                cliente=excluded.cliente,
                monto_total_gs=excluded.monto_total_gs,
                monto_total_ret_gs=excluded.monto_total_ret_gs,
                fecha_cobro=excluded.fecha_cobro,
                medio=excluded.medio,
                nro_cheque=excluded.nro_cheque,
                nro_deposito=excluded.nro_deposito,
                referencia=excluded.referencia,
                observacion=excluded.observacion,
                ts_registro=excluded.ts_registro,
                ts_modificacion=excluded.ts_modificacion;
            """,
            (
                invoice_id,
                invoice_ts,
                invoice_no,
                str(payload.get("cliente") or ""),
                float(payload.get("monto_total_gs") or 0.0),
                float(payload.get("monto_total_ret_gs") or 0.0),
                str(payload.get("fecha_cobro") or ""),
                str(payload.get("medio") or ""),
                str(payload.get("nro_cheque") or ""),
                str(payload.get("nro_deposito") or ""),
                str(payload.get("referencia") or ""),
                str(payload.get("observacion") or ""),
                str(payload.get("ts_registro") or ""),
                str(payload.get("ts_modificacion") or ""),
            ),
        )
        cn.commit()
        cn.close()

    def load_collection_details(self) -> list[dict]:
        if not self.fracc_db.exists():
            return []
        cn = self._connect_fracc()
        cur = cn.cursor()
        rows = cur.execute(
            """
            SELECT invoice_id, invoice_ts, invoice_no, cliente, monto_total_gs, monto_total_ret_gs,
                   fecha_cobro, medio, nro_cheque, nro_deposito, referencia, observacion,
                   ts_registro, ts_modificacion
            FROM dashboard_collection_details
            ORDER BY ts_registro DESC;
            """
        ).fetchall()
        cn.close()
        return [{key: row[key] for key in row.keys()} for row in rows]

    def save_collection_details(self, rows: list[dict]) -> None:
        for row in rows:
            if isinstance(row, dict):
                self.upsert_collection_detail(row)

    @staticmethod
    def _parse_iso(d: str) -> date | None:
        try:
            return datetime.strptime((d or "").strip(), "%Y-%m-%d").date()
        except Exception:
            return None

    @staticmethod
    def _fmt_gs(n: float) -> str:
        return f"{n:,.0f}".replace(",", ".")

    @staticmethod
    def _safe_int(v) -> int:
        try:
            return int(v)
        except Exception:
            return 0

    @staticmethod
    def _sucursal_from_proveedor(proveedor: str) -> str:
        p = (proveedor or "").strip().lower()
        if not p:
            return ""
        if "areg" in p:
            return "Aregua"
        if "luque" in p:
            return "Luque"
        if "ita" in p:
            return "Itaugua"
        return ""

    def list_sucursales(self) -> list[str]:
        out = set()
        if self.orders_db.exists():
            cn = db.connect("pedidos")
            cur = cn.cursor()
            rows = cur.execute(
                "SELECT DISTINCT COALESCE(sucursal,'') FROM orden_compra WHERE TRIM(COALESCE(sucursal,''))<>''"
            ).fetchall()
            for (s,) in rows:
                out.add(str(s))
            cn.close()
        if self.invoices_db.exists():
            cn = db.connect("facturas")
            cur = cn.cursor()
            rows = cur.execute(
                "SELECT DISTINCT COALESCE(sucursal,'') FROM factura WHERE TRIM(COALESCE(sucursal,''))<>''"
            ).fetchall()
            for (s,) in rows:
                out.add(str(s))
            cn.close()
        if self.fracc_db.exists():
            cn = db.connect("fraccionadora")
            cur = cn.cursor()
            rows = cur.execute(
                "SELECT DISTINCT COALESCE(proveedor,'') FROM raw_lots WHERE TRIM(COALESCE(proveedor,''))<>''"
            ).fetchall()
            for (prov,) in rows:
                suc = self._sucursal_from_proveedor(str(prov or ""))
                if suc:
                    out.add(suc)
            cn.close()
        return sorted(out)

    def _load_paid_map(self) -> dict[int, bool]:
        return self.load_paid_map()

    def _load_collections_map(self) -> dict[str, bool]:
        return self.load_collections_map()

    @staticmethod
    def _is_collected(cobros: dict[str, bool], invoice_id: int, ts: str, nro: str) -> bool:
        key_new = f"std:{int(invoice_id)}:{str(ts or '').strip()}:{str(nro or '').strip()}"
        key_old = f"std:{int(invoice_id)}"
        if key_new in cobros:
            return bool(cobros.get(key_new, False))
        if key_old in cobros:
            return bool(cobros.get(key_old, False))
        return False

    @staticmethod
    def _total_con_retencion(total: float, iva5: float, iva10: float) -> float:
        return float(total or 0.0) - 0.3 * (float(iva5 or 0.0) + float(iva10 or 0.0))

    def pending_collection_total(
        self,
        search: str = "",
        from_date: str = "",
        to_date: str = "",
    ) -> tuple[float, int]:
        if not self.fracc_db.exists():
            return 0.0, 0
        cobros = self._load_collections_map()
        cn = db.connect("fraccionadora")
        cur = cn.cursor()
        sql = """
            SELECT
                si.id,
                COALESCE(CAST(si.ts AS TEXT),'') AS ts,
                COALESCE(si.invoice_no,'') AS invoice_no,
                COALESCE(si.customer,'') AS customer,
                COALESCE(si.total_gs,0) AS total_gs,
                COALESCE(si.iva5_gs,0) AS iva5_gs,
                COALESCE(si.iva10_gs,0) AS iva10_gs
            FROM sales_invoices si
            WHERE (%s = '' OR (COALESCE(si.invoice_no,'') LIKE %s OR COALESCE(si.customer,'') LIKE %s))
              AND (%s = '' OR si.ts::date >= CAST(NULLIF(%s, '') AS date))
              AND (%s = '' OR si.ts::date <= CAST(NULLIF(%s, '') AS date))
            ORDER BY si.ts DESC, si.id DESC;
        """
        like_search = f"%{search.strip()}%" if search.strip() else ""
        params = (
            like_search,
            like_search,
            like_search,
            from_date.strip(),
            from_date.strip(),
            to_date.strip(),
            to_date.strip(),
        )
        rows = cur.execute(sql, params).fetchall()
        cn.close()

        total = 0.0
        count = 0
        for r in rows:
            inv_id = self._safe_int(r["id"])
            if inv_id <= 0:
                continue
            if self._is_collected(cobros, inv_id, str(r["ts"] or ""), str(r["invoice_no"] or "")):
                continue
            total += self._total_con_retencion(
                float(r["total_gs"] or 0.0),
                float(r["iva5_gs"] or 0.0),
                float(r["iva10_gs"] or 0.0),
            )
            count += 1
        return total, count

    def pending_collections(
        self,
        search: str = "",
        from_date: str = "",
        to_date: str = "",
        limit: int = 200,
    ) -> list[CollectionRow]:
        if not self.fracc_db.exists():
            return []
        cobros = self._load_collections_map()
        cn = db.connect("fraccionadora")
        cur = cn.cursor()
        sql = """
            SELECT
                si.id,
                COALESCE(CAST(si.ts AS TEXT),'') AS ts,
                COALESCE(si.invoice_no,'') AS invoice_no,
                COALESCE(si.customer,'') AS customer,
                COALESCE(si.gravada5_gs,0) AS gravada5_gs,
                COALESCE(si.iva5_gs,0) AS iva5_gs,
                COALESCE(si.gravada10_gs,0) AS gravada10_gs,
                COALESCE(si.iva10_gs,0) AS iva10_gs,
                COALESCE(si.total_gs,0) AS total_gs
            FROM sales_invoices si
            WHERE (%s = '' OR (COALESCE(si.invoice_no,'') LIKE %s OR COALESCE(si.customer,'') LIKE %s))
              AND (%s = '' OR si.ts::date >= CAST(NULLIF(%s, '') AS date))
              AND (%s = '' OR si.ts::date <= CAST(NULLIF(%s, '') AS date))
            ORDER BY si.ts DESC, si.id DESC;
        """
        like_search = f"%{search.strip()}%" if search.strip() else ""
        params = (
            like_search,
            like_search,
            like_search,
            from_date.strip(),
            from_date.strip(),
            to_date.strip(),
            to_date.strip(),
        )
        rows = cur.execute(sql, params).fetchall()
        cn.close()

        out: list[CollectionRow] = []
        today = date.today()
        for r in rows:
            inv_id = self._safe_int(r["id"])
            ts = str(r["ts"] or "")
            nro = str(r["invoice_no"] or "")
            if inv_id <= 0 or self._is_collected(cobros, inv_id, ts, nro):
                continue

            f_emi = self._parse_iso(ts[:10]) if ts else None
            dias_sin_cobrar = None
            dias_para_cobro = None
            if f_emi is not None:
                dias_sin_cobrar = (today - f_emi).days
                dias_para_cobro = CREDIT_TERM_DAYS - dias_sin_cobrar

            total = float(r["total_gs"] or 0.0)
            iva5 = float(r["iva5_gs"] or 0.0)
            iva10 = float(r["iva10_gs"] or 0.0)
            out.append(
                CollectionRow(
                    invoice_id=inv_id,
                    ts=ts,
                    invoice_no=nro,
                    customer=str(r["customer"] or ""),
                    gravada5_gs=float(r["gravada5_gs"] or 0.0),
                    iva5_gs=iva5,
                    gravada10_gs=float(r["gravada10_gs"] or 0.0),
                    iva10_gs=iva10,
                    total_gs=total,
                    total_con_retencion=self._total_con_retencion(total, iva5, iva10),
                    dias_sin_cobrar=dias_sin_cobrar,
                    dias_para_cobro=dias_para_cobro,
                )
            )
            if len(out) >= max(1, int(limit)):
                break
        return out

    def pending_orders(
        self,
        sucursal: str = "",
        search: str = "",
        from_date: str = "",
        to_date: str = "",
    ) -> list[OrderRow]:
        if not self.orders_db.exists():
            return []
        products, stock_map = self._build_fracc_stock_cache()
        cn = db.connect("pedidos")
        cur = cn.cursor()
        sql = """
            SELECT
                oc.id,
                COALESCE(oc.nro_oc, '') AS nro_oc,
                COALESCE(oc.sucursal, '') AS sucursal,
                COALESCE(oc.fecha_pedido, '') AS fecha_pedido,
                COALESCE(oc.completada, 0) AS completada,
                COALESCE(oc.monto_total, 0) AS monto_total,
                COALESCE(SUM(CASE WHEN COALESCE(oi.enviado,0)=0 THEN oi.cantidad ELSE 0 END), 0) AS cant_pend,
                COALESCE(SUM(CASE WHEN COALESCE(oi.enviado,0)=1 THEN oi.cantidad ELSE 0 END), 0) AS cant_env
            FROM orden_compra oc
            LEFT JOIN orden_item oi ON oi.oc_id = oc.id
            WHERE COALESCE(oc.completada,0) = 0
            AND (%s = '' OR UPPER(COALESCE(oc.sucursal,'')) = UPPER(%s))
            AND (%s = '' OR COALESCE(oc.nro_oc,'') LIKE %s)
            AND (%s = '' OR oc.fecha_pedido::date >= CAST(NULLIF(%s, '') AS date))
            AND (%s = '' OR oc.fecha_pedido::date <= CAST(NULLIF(%s, '') AS date))
            GROUP BY oc.id, oc.nro_oc, oc.sucursal, oc.fecha_pedido, oc.completada, oc.monto_total
            HAVING COALESCE(SUM(CASE WHEN COALESCE(oi.enviado,0)=0 THEN oi.cantidad ELSE 0 END), 0) > 0
            ORDER BY oc.fecha_pedido ASC NULLS LAST, oc.id DESC;
        """
        like_search = f"%{search.strip()}%" if search.strip() else ""
        params = (
            sucursal.strip(),
            sucursal.strip(),
            like_search,
            like_search,
            from_date.strip(),
            from_date.strip(),
            to_date.strip(),
            to_date.strip(),
        )
        rows = cur.execute(sql, params).fetchall()
        cn.close()

        today = date.today()
        out: list[OrderRow] = []
        for r in rows:
            fp = self._parse_iso(str(r["fecha_pedido"] or ""))
            compromiso = fp + timedelta(days=DELIVERY_SLA_DAYS) if fp else None
            dias = (today - compromiso).days if compromiso else None
            if dias is None:
                prioridad = "Media"
            elif dias > 0:
                prioridad = "Alta"
            elif dias >= -1:
                prioridad = "Media"
            else:
                prioridad = "Baja"
            estado = "Parcial" if float(r["cant_env"] or 0) > 0 else "Pendiente"
            pct_listo = self._order_ready_percentage(self._safe_int(r["id"]), products, stock_map)
            out.append(
                OrderRow(
                    oc_id=self._safe_int(r["id"]),
                    numero=str(r["nro_oc"] or ""),
                    sucursal=str(r["sucursal"] or ""),
                    fecha_pedido=str(r["fecha_pedido"] or ""),
                    fecha_compromiso=compromiso.isoformat() if compromiso else "",
                    dias_atraso=dias,
                    estado=estado,
                    prioridad=prioridad,
                    pct_listo_entrega=float(pct_listo),
                    monto_total=float(r["monto_total"] or 0),
                )
            )
        return out

    def pending_payments(
        self,
        sucursal: str = "",
        search: str = "",
        from_date: str = "",
        to_date: str = "",
    ) -> list[PaymentRow]:
        # Misma fuente que "Resumen de compras": raw_lots + pagos_compras copy.json
        if not self.fracc_db.exists():
            return []
        paid = self._load_paid_map()
        cn = db.connect("fraccionadora")
        cur = cn.cursor()
        sql = """
            SELECT
                rl.id,
                COALESCE(CAST(rl.ts AS TEXT),'') AS ts,
                COALESCE(rl.factura,'') AS factura,
                COALESCE(rl.proveedor,'') AS proveedor,
                COALESCE(rl.costo_total_gs,0) AS costo_total_gs
            FROM raw_lots rl
            WHERE (%s = '' OR rl.ts::date >= CAST(NULLIF(%s, '') AS date))
              AND (%s = '' OR rl.ts::date <= CAST(NULLIF(%s, '') AS date))
              AND (
                    %s = ''
                    OR COALESCE(rl.factura,'') LIKE %s
                    OR COALESCE(rl.proveedor,'') LIKE %s
                  )
            ORDER BY rl.ts DESC, rl.id DESC;
        """
        like_search = f"%{search.strip()}%" if search.strip() else ""
        params = (
            from_date.strip(),
            from_date.strip(),
            to_date.strip(),
            to_date.strip(),
            like_search,
            like_search,
            like_search,
        )
        rows = cur.execute(sql, params).fetchall()
        cn.close()

        today = date.today()
        out: list[PaymentRow] = []
        for r in rows:
            lot_id = self._safe_int(r["id"])
            suc = self._sucursal_from_proveedor(str(r["proveedor"] or ""))
            if sucursal.strip() and suc.upper() != sucursal.strip().upper():
                continue
            # Igual que Resumen de compras: clave por lot_id; si no existe, se considera pendiente.
            is_paid = bool(paid.get(lot_id, paid.get(str(lot_id), False)))
            if is_paid:
                continue
            ts_txt = str(r["ts"] or "").strip()
            fe = self._parse_iso(ts_txt[:10] if ts_txt else "")
            due = fe + timedelta(days=CREDIT_TERM_DAYS) if fe else None
            dias = (due - today).days if due else None
            estado = "Vencido" if (dias is not None and dias < 0) else "Pendiente"
            out.append(
                PaymentRow(
                    factura_id=lot_id,
                    proveedor=str(r["proveedor"] or ""),
                    numero_doc=str(r["factura"] or ""),
                    fecha_emision=(ts_txt[:10] if ts_txt else ""),
                    vencimiento=due.isoformat() if due else "",
                    dias_para_vencer=dias,
                    monto=float(r["costo_total_gs"] or 0),
                    estado=estado,
                    sucursal=suc,
                )
            )
        return out

    def trend_data(self) -> tuple[int, int]:
        if not self.orders_db.exists():
            return 0, 0
        cn = db.connect("pedidos")
        cur = cn.cursor()
        d7 = (date.today() - timedelta(days=7)).isoformat()
        d30 = (date.today() - timedelta(days=30)).isoformat()
        c7 = cur.execute(
            """
            SELECT COUNT(*)
            FROM orden_compra
            WHERE COALESCE(completada,0)=0
              AND fecha_pedido::date >= CAST(%s AS date)
            """,
            (d7,),
        ).fetchone()[0]
        c30 = cur.execute(
            """
            SELECT COUNT(*)
            FROM orden_compra
            WHERE COALESCE(completada,0)=0
              AND fecha_pedido::date >= CAST(%s AS date)
            """,
            (d30,),
        ).fetchone()[0]
        cn.close()
        return int(c7 or 0), int(c30 or 0)


class InicioDashboardWindow(QtWidgets.QMainWindow):
    def __init__(self, dark_mode: bool = False):
        super().__init__()
        self.repo = DashboardRepo()
        self._dark_mode = bool(dark_mode)
        self._order_rows: list[OrderRow] = []
        self._payment_rows: list[PaymentRow] = []
        self._payment_display_rows: list[PaymentRow] = []
        self._payments_grouped_by_supplier = False
        self._payments_supplier_desc = False
        self._collection_rows: list[CollectionRow] = []
        self.setWindowTitle("Inicio operativo")
        self.resize(1240, 780)
        self._build_ui()
        self._load_sucursales()
        self._apply_theme()
        self.refresh_data()
        self.showMaximized()

    def _build_ui(self) -> None:
        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        root = QtWidgets.QVBoxLayout(central)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        header = QtWidgets.QFrame()
        header_l = QtWidgets.QHBoxLayout(header)
        header_l.setContentsMargins(10, 8, 10, 8)
        header_l.setSpacing(8)
        self.lbl_title = QtWidgets.QLabel("Inicio Operativo")
        self.lbl_title.setObjectName("dashTitle")
        header_l.addWidget(self.lbl_title)
        header_l.addSpacing(10)
        header_l.addWidget(QtWidgets.QLabel("Sucursal:"))
        self.cb_sucursal = QtWidgets.QComboBox()
        self.cb_sucursal.setMinimumWidth(140)
        header_l.addWidget(self.cb_sucursal)
        header_l.addWidget(QtWidgets.QLabel("Desde:"))
        self.dt_desde = QtWidgets.QDateEdit()
        self.dt_desde.setCalendarPopup(True)
        self.dt_desde.setDate(QtCore.QDate.currentDate().addDays(-30))
        header_l.addWidget(self.dt_desde)
        header_l.addWidget(QtWidgets.QLabel("Hasta:"))
        self.dt_hasta = QtWidgets.QDateEdit()
        self.dt_hasta.setCalendarPopup(True)
        self.dt_hasta.setDate(QtCore.QDate.currentDate())
        header_l.addWidget(self.dt_hasta)
        self.txt_buscar = QtWidgets.QLineEdit()
        self.txt_buscar.setPlaceholderText("Buscar por Nro. pedido/factura")
        self.txt_buscar.setMinimumWidth(220)
        header_l.addWidget(self.txt_buscar)
        self.btn_refrescar = QtWidgets.QPushButton("Refrescar")
        header_l.addWidget(self.btn_refrescar)
        self.btn_hamburger = QtWidgets.QToolButton()
        self.btn_hamburger.setObjectName("hamburgerBtn")
        self.btn_hamburger.setText("☰")
        self.btn_hamburger.setPopupMode(QtWidgets.QToolButton.InstantPopup)
        self.btn_hamburger.setToolButtonStyle(QtCore.Qt.ToolButtonTextOnly)
        menu = QtWidgets.QMenu(self.btn_hamburger)
        self.act_open_fraccionadora = menu.addAction("Lanzador de fraccionadora")
        self.act_open_facturas = menu.addAction("Importador de facturas")
        self.act_open_oc = menu.addAction("Importador de OC")
        self.act_open_ventas = menu.addAction("Reporte de ventas")
        self.act_open_reporte_mensual = menu.addAction("Reporte mensual")
        self.btn_hamburger.setMenu(menu)
        header_l.addWidget(self.btn_hamburger)
        root.addWidget(header)

        kpi_row = QtWidgets.QHBoxLayout()
        kpi_row.setSpacing(8)
        self.kpi_ped_pend = KpiCard("Pedidos pendientes")
        self.kpi_ped_venc = KpiCard("Pedidos vencidos")
        self.kpi_pag_pend = KpiCard("Pagos pendientes")
        self.kpi_pag_venc = KpiCard("Cobro pendiente")
        for w in [self.kpi_ped_pend, self.kpi_ped_venc, self.kpi_pag_pend, self.kpi_pag_venc]:
            kpi_row.addWidget(w, 1)
        root.addLayout(kpi_row)

        split = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        left = QtWidgets.QFrame()
        ll = QtWidgets.QVBoxLayout(left)
        ll.setContentsMargins(8, 8, 8, 8)
        ll.setSpacing(6)
        ll.addWidget(QtWidgets.QLabel("Pedidos pendientes de entrega (Importador OC)"))
        self.tbl_orders = QtWidgets.QTableWidget(0, 9)
        self.tbl_orders.setHorizontalHeaderLabels(
            ["Pedido", "Sucursal", "Fecha pedido", "Compromiso", "Días atraso", "Estado", "Prioridad", "% listo para entrega", "Monto"]
        )
        self.tbl_orders.horizontalHeader().setStretchLastSection(True)
        self.tbl_orders.verticalHeader().setVisible(False)
        self.tbl_orders.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.tbl_orders.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.tbl_orders.setAlternatingRowColors(True)
        ll.addWidget(self.tbl_orders, 1)
        left_btns = QtWidgets.QHBoxLayout()
        self.btn_detalle_pedido = QtWidgets.QPushButton("Ver detalle")
        self.btn_reprogramar = QtWidgets.QPushButton("Reprogramar")
        left_btns.addWidget(self.btn_detalle_pedido)
        left_btns.addWidget(self.btn_reprogramar)
        left_btns.addStretch(1)
        ll.addLayout(left_btns)

        right = QtWidgets.QFrame()
        rl = QtWidgets.QVBoxLayout(right)
        rl.setContentsMargins(8, 8, 8, 8)
        rl.setSpacing(6)
        rl.addWidget(QtWidgets.QLabel("Pagos pendientes de mercadería"))
        self.tbl_payments = QtWidgets.QTableWidget(0, 7)
        self.tbl_payments.setHorizontalHeaderLabels(
            ["Proveedor", "Factura", "Emisión", "Vencimiento", "Días", "Estado", "Monto"]
        )
        self.tbl_payments.horizontalHeader().setStretchLastSection(True)
        self.tbl_payments.verticalHeader().setVisible(False)
        self.tbl_payments.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.tbl_payments.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        self.tbl_payments.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.tbl_payments.setAlternatingRowColors(True)
        self.tbl_payments.horizontalHeader().sectionClicked.connect(self._on_payments_header_clicked)
        rl.addWidget(self.tbl_payments, 1)
        right_btns = QtWidgets.QHBoxLayout()
        self.btn_registrar_pago = QtWidgets.QPushButton("Registrar pago")
        self.btn_detalle_factura = QtWidgets.QPushButton("Ver factura")
        self.btn_ver_pagos = QtWidgets.QPushButton("Ver pagos realizados")
        right_btns.addWidget(self.btn_registrar_pago)
        right_btns.addWidget(self.btn_detalle_factura)
        right_btns.addWidget(self.btn_ver_pagos)
        right_btns.addStretch(1)
        rl.addLayout(right_btns)

        split.addWidget(left)
        split.addWidget(right)
        split.setSizes([700, 520])
        root.addWidget(split, 1)

        bottom = QtWidgets.QHBoxLayout()
        bottom.setSpacing(8)
        collections = QtWidgets.QFrame()
        cl = QtWidgets.QVBoxLayout(collections)
        cl.setContentsMargins(8, 8, 8, 8)
        cl.setSpacing(6)
        cl.addWidget(QtWidgets.QLabel("Facturas emitidas"))
        self.tbl_collections = QtWidgets.QTableWidget(0, 11)
        self.tbl_collections.setHorizontalHeaderLabels(
            [
                "ID",
                "Fecha",
                "N Factura",
                "Cliente",
                "Dias sin cobrar",
                "Grav. 5%",
                "IVA 5%",
                "Grav. 10%",
                "IVA 10%",
                "TOTAL (Gs)",
                "Total con retencion (Gs)",
            ]
        )
        self.tbl_collections.horizontalHeader().setStretchLastSection(True)
        self.tbl_collections.verticalHeader().setVisible(False)
        self.tbl_collections.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.tbl_collections.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.tbl_collections.setAlternatingRowColors(True)
        cl.addWidget(self.tbl_collections, 1)
        collection_btns = QtWidgets.QHBoxLayout()
        self.btn_registrar_cobro = QtWidgets.QPushButton("Registrar cobro")
        self.btn_ver_cobradas = QtWidgets.QPushButton("Ver facturas cobradas")
        collection_btns.addWidget(self.btn_registrar_cobro)
        collection_btns.addWidget(self.btn_ver_cobradas)
        collection_btns.addStretch(1)
        cl.addLayout(collection_btns)
        bottom.addWidget(collections, 2)

        trend = QtWidgets.QFrame()
        tl = QtWidgets.QVBoxLayout(trend)
        tl.setContentsMargins(8, 8, 8, 8)
        tl.setSpacing(6)
        tl.addWidget(QtWidgets.QLabel("Tendencia"))
        self.lbl_trend_7 = QtWidgets.QLabel("Pendientes últimos 7 días: 0")
        self.lbl_trend_30 = QtWidgets.QLabel("Pendientes últimos 30 días: 0")
        self.lbl_note = QtWidgets.QLabel(
            f"* Compromiso estimado: fecha pedido + {DELIVERY_SLA_DAYS} días / crédito + {CREDIT_TERM_DAYS} días"
        )
        self.lbl_note.setWordWrap(True)
        tl.addWidget(self.lbl_trend_7)
        tl.addWidget(self.lbl_trend_30)
        tl.addStretch(1)
        tl.addWidget(self.lbl_note)
        bottom.addWidget(trend, 1)

        root.addLayout(bottom)

        self.lbl_status = QtWidgets.QLabel("Listo")
        root.addWidget(self.lbl_status)

        self.btn_refrescar.clicked.connect(self.refresh_data)
        self.dt_desde.dateChanged.connect(self.refresh_data)
        self.dt_hasta.dateChanged.connect(self.refresh_data)
        self.cb_sucursal.currentIndexChanged.connect(self.refresh_data)
        self.txt_buscar.returnPressed.connect(self.refresh_data)
        self.act_open_fraccionadora.triggered.connect(self._open_fraccionadora)
        self.act_open_facturas.triggered.connect(self._open_importador_facturas)
        self.act_open_oc.triggered.connect(self._open_importador_oc)
        self.act_open_ventas.triggered.connect(self._open_reporte_ventas)
        self.act_open_reporte_mensual.triggered.connect(self._open_reporte_mensual)
        self.btn_detalle_pedido.clicked.connect(self._ver_faltantes_pedido)
        self.btn_reprogramar.clicked.connect(self._not_implemented)
        self.btn_registrar_pago.clicked.connect(self._registrar_pago_dialog)
        self.btn_detalle_factura.clicked.connect(self._open_selected_factura_pdf)
        self.btn_ver_pagos.clicked.connect(self._ver_pagos_realizados_dialog)
        self.btn_registrar_cobro.clicked.connect(self._registrar_cobro_dialog)
        self.btn_ver_cobradas.clicked.connect(self._ver_cobradas_dialog)

    def _load_sucursales(self) -> None:
        self.cb_sucursal.clear()
        self.cb_sucursal.addItem("Todas")
        for s in self.repo.list_sucursales():
            self.cb_sucursal.addItem(s)

    def _qdate_to_iso(self, qd: QtCore.QDate) -> str:
        return qd.toString("yyyy-MM-dd")

    def _set_item(
        self,
        table: QtWidgets.QTableWidget,
        row: int,
        col: int,
        text: str,
        align: QtCore.Qt.AlignmentFlag = QtCore.Qt.AlignLeft,
        fg: QtGui.QColor | None = None,
        bg: QtGui.QColor | None = None,
    ) -> None:
        it = QtWidgets.QTableWidgetItem(text)
        it.setTextAlignment(align | QtCore.Qt.AlignVCenter)
        if fg is not None:
            it.setForeground(QtGui.QBrush(fg))
        if bg is not None:
            it.setBackground(QtGui.QBrush(bg))
        table.setItem(row, col, it)

    def _color_by_urgency(self, value: int | None) -> QtGui.QColor | None:
        if value is None:
            return None
        if value > 0:
            return QtGui.QColor("#dbeafe")
        if value >= -1:
            return QtGui.QColor("#eff6ff")
        return QtGui.QColor("#f8fbff")

    def _color_by_due(self, value: int | None) -> QtGui.QColor | None:
        if value is None:
            return None
        if value < 0:
            return QtGui.QColor("#dbeafe")
        if value <= 2:
            return QtGui.QColor("#eff6ff")
        return QtGui.QColor("#f8fbff")

    @staticmethod
    def _color_by_ready_pct(value: float | None) -> QtGui.QColor | None:
        if value is None:
            return None
        pct = float(value)
        if pct >= 100.0:
            return QtGui.QColor("#dcfce7")
        if pct <= 0.0:
            return QtGui.QColor("#fee2e2")
        return QtGui.QColor("#fef3c7")

    def _color_by_collection_due(self, value: int | None) -> QtGui.QColor | None:
        if value is None:
            return None
        if value < 0:
            return QtGui.QColor("#fecaca")
        if value <= 3:
            return QtGui.QColor("#fde68a")
        if value <= 7:
            return QtGui.QColor("#fef3c7")
        return QtGui.QColor("#bbf7d0")

    def refresh_data(self) -> None:
        suc = self.cb_sucursal.currentText().strip()
        if suc == "Todas":
            suc = ""
        srch = self.txt_buscar.text().strip()
        d1 = self._qdate_to_iso(self.dt_desde.date())
        d2 = self._qdate_to_iso(self.dt_hasta.date())

        orders = self.repo.pending_orders(sucursal=suc, search=srch, from_date=d1, to_date=d2)
        payments = self.repo.pending_payments(sucursal=suc, search=srch, from_date="", to_date="")
        cobro_pend, cobro_count = self.repo.pending_collection_total(search=srch, from_date=d1, to_date=d2)
        collections = self.repo.pending_collections(search=srch, from_date=d1, to_date=d2)

        self._fill_orders(orders)
        self._fill_payments(payments)
        self._fill_collections(collections)
        self._fill_kpis(orders, payments, cobro_pend, cobro_count)

        t7, t30 = self.repo.trend_data()
        self.lbl_trend_7.setText(f"Pendientes últimos 7 días: {t7}")
        self.lbl_trend_30.setText(f"Pendientes últimos 30 días: {t30}")
        self.lbl_status.setText(
            f"Actualizado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  |  Fuente OC: {self.repo.orders_db}"
        )

    def _fill_orders(self, rows: list[OrderRow]) -> None:
        self._order_rows = list(rows)
        self.tbl_orders.setRowCount(len(rows))
        for i, r in enumerate(rows):
            bg = self._color_by_urgency(r.dias_atraso)
            ready_bg = self._color_by_ready_pct(r.pct_listo_entrega)
            self._set_item(self.tbl_orders, i, 0, r.numero or f"ID {r.oc_id}")
            self._set_item(self.tbl_orders, i, 1, r.sucursal or "-", bg=bg)
            self._set_item(self.tbl_orders, i, 2, r.fecha_pedido or "-", align=QtCore.Qt.AlignCenter, bg=bg)
            self._set_item(self.tbl_orders, i, 3, r.fecha_compromiso or "-", align=QtCore.Qt.AlignCenter, bg=bg)
            d_txt = "-" if r.dias_atraso is None else str(r.dias_atraso)
            self._set_item(self.tbl_orders, i, 4, d_txt, align=QtCore.Qt.AlignCenter, bg=bg)
            self._set_item(self.tbl_orders, i, 5, r.estado, align=QtCore.Qt.AlignCenter, bg=bg)
            self._set_item(self.tbl_orders, i, 6, r.prioridad, align=QtCore.Qt.AlignCenter, bg=bg)
            self._set_item(
                self.tbl_orders,
                i,
                7,
                f"{float(r.pct_listo_entrega or 0.0):.1f}%",
                align=QtCore.Qt.AlignCenter,
                bg=ready_bg,
            )
            self._set_item(
                self.tbl_orders,
                i,
                8,
                DashboardRepo._fmt_gs(r.monto_total),
                align=QtCore.Qt.AlignRight,
                bg=bg,
            )
        self.tbl_orders.resizeColumnsToContents()

    def _fill_payments(self, rows: list[PaymentRow]) -> None:
        self._payment_rows = list(rows)
        display_rows = list(rows)
        if self._payments_grouped_by_supplier:
            display_rows.sort(
                key=lambda r: (
                    (str(r.proveedor or "").strip().lower(), str(r.vencimiento or ""), str(r.numero_doc or ""))
                ),
                reverse=self._payments_supplier_desc,
            )
        self._payment_display_rows = display_rows
        self.tbl_payments.setRowCount(len(rows))
        for i, r in enumerate(display_rows):
            bg = self._color_by_due(r.dias_para_vencer)
            self._set_item(self.tbl_payments, i, 0, r.proveedor or "-", bg=bg)
            self._set_item(self.tbl_payments, i, 1, r.numero_doc or f"ID {r.factura_id}", bg=bg)
            self._set_item(self.tbl_payments, i, 2, r.fecha_emision or "-", align=QtCore.Qt.AlignCenter, bg=bg)
            self._set_item(self.tbl_payments, i, 3, r.vencimiento or "-", align=QtCore.Qt.AlignCenter, bg=bg)
            d_txt = "-" if r.dias_para_vencer is None else str(r.dias_para_vencer)
            self._set_item(self.tbl_payments, i, 4, d_txt, align=QtCore.Qt.AlignCenter, bg=bg)
            self._set_item(self.tbl_payments, i, 5, r.estado, align=QtCore.Qt.AlignCenter, bg=bg)
            self._set_item(
                self.tbl_payments,
                i,
                6,
                DashboardRepo._fmt_gs(r.monto),
                align=QtCore.Qt.AlignRight,
                bg=bg,
            )
        self.tbl_payments.resizeColumnsToContents()

    def _selected_payment_rows(self) -> list[PaymentRow]:
        indexes = self.tbl_payments.selectionModel().selectedRows() if self.tbl_payments.selectionModel() else []
        if not indexes:
            row = self.tbl_payments.currentRow()
            if 0 <= row < len(self._payment_display_rows):
                return [self._payment_display_rows[row]]
            return []
        out: list[PaymentRow] = []
        seen: set[int] = set()
        for idx in sorted(indexes, key=lambda x: x.row()):
            row = idx.row()
            if row in seen or row < 0 or row >= len(self._payment_display_rows):
                continue
            seen.add(row)
            out.append(self._payment_display_rows[row])
        return out

    def _on_payments_header_clicked(self, section: int) -> None:
        if section != 0:
            return
        if self._payments_grouped_by_supplier:
            self._payments_supplier_desc = not self._payments_supplier_desc
        else:
            self._payments_grouped_by_supplier = True
            self._payments_supplier_desc = False
        self._fill_payments(self._payment_rows)

    def _fill_collections(self, rows: list[CollectionRow]) -> None:
        self._collection_rows = rows
        self.tbl_collections.setRowCount(len(rows))
        for i, r in enumerate(rows):
            bg = self._color_by_collection_due(r.dias_para_cobro)
            self._set_item(self.tbl_collections, i, 0, str(r.invoice_id), align=QtCore.Qt.AlignCenter, bg=bg)
            self._set_item(self.tbl_collections, i, 1, r.ts or "-", align=QtCore.Qt.AlignCenter, bg=bg)
            self._set_item(self.tbl_collections, i, 2, r.invoice_no or "-", bg=bg)
            self._set_item(self.tbl_collections, i, 3, r.customer or "-", bg=bg)
            ds_txt = "-" if r.dias_sin_cobrar is None else str(r.dias_sin_cobrar)
            self._set_item(self.tbl_collections, i, 4, ds_txt, align=QtCore.Qt.AlignCenter, bg=bg)
            self._set_item(
                self.tbl_collections,
                i,
                5,
                DashboardRepo._fmt_gs(r.gravada5_gs),
                align=QtCore.Qt.AlignRight,
                bg=bg,
            )
            self._set_item(
                self.tbl_collections,
                i,
                6,
                DashboardRepo._fmt_gs(r.iva5_gs),
                align=QtCore.Qt.AlignRight,
                bg=bg,
            )
            self._set_item(
                self.tbl_collections,
                i,
                7,
                DashboardRepo._fmt_gs(r.gravada10_gs),
                align=QtCore.Qt.AlignRight,
                bg=bg,
            )
            self._set_item(
                self.tbl_collections,
                i,
                8,
                DashboardRepo._fmt_gs(r.iva10_gs),
                align=QtCore.Qt.AlignRight,
                bg=bg,
            )
            self._set_item(
                self.tbl_collections,
                i,
                9,
                DashboardRepo._fmt_gs(r.total_gs),
                align=QtCore.Qt.AlignRight,
                bg=bg,
            )
            self._set_item(
                self.tbl_collections,
                i,
                10,
                DashboardRepo._fmt_gs(r.total_con_retencion),
                align=QtCore.Qt.AlignRight,
                bg=bg,
            )
        self.tbl_collections.resizeColumnsToContents()

    def _save_collection_flag(self, invoice_id: int, ts: str, nro: str, collected: bool) -> None:
        self.repo.save_collection_flag(invoice_id, ts, nro, collected)

    def _append_collection_detail(self, payload: dict) -> None:
        self.repo.upsert_collection_detail(payload)

    def _load_collection_details(self) -> list[dict]:
        return self.repo.load_collection_details()

    def _save_collection_details(self, rows: list[dict]) -> None:
        self.repo.save_collection_details(rows)

    def _ver_cobradas_dialog(self) -> None:
        rows = self._load_collection_details()
        if not rows:
            QtWidgets.QMessageBox.information(self, "Facturas cobradas", "No hay facturas cobradas registradas.")
            return

        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("Facturas cobradas")
        dlg.resize(980, 520)
        lay = QtWidgets.QVBoxLayout(dlg)
        lay.setContentsMargins(10, 10, 10, 10)
        lay.setSpacing(8)

        split = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        lay.addWidget(split, 1)

        left = QtWidgets.QFrame()
        ll = QtWidgets.QVBoxLayout(left)
        ll.setContentsMargins(6, 6, 6, 6)
        ll.setSpacing(6)
        ll.addWidget(QtWidgets.QLabel("Facturas cobradas"))
        tbl = QtWidgets.QTableWidget(0, 6)
        tbl.setHorizontalHeaderLabels(
            ["Fecha cobro", "N Factura", "Cliente", "Medio", "Total c/ret.", "Nro cheque"]
        )
        tbl.horizontalHeader().setStretchLastSection(True)
        tbl.verticalHeader().setVisible(False)
        tbl.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        tbl.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        tbl.setAlternatingRowColors(True)
        ll.addWidget(tbl, 1)
        split.addWidget(left)

        right = QtWidgets.QFrame()
        rl = QtWidgets.QVBoxLayout(right)
        rl.setContentsMargins(6, 6, 6, 6)
        rl.setSpacing(6)
        rl.addWidget(QtWidgets.QLabel("Detalle del cobro seleccionado"))
        detail = QtWidgets.QTableWidget(0, 2)
        detail.setHorizontalHeaderLabels(["Campo", "Valor"])
        detail.horizontalHeader().setStretchLastSection(True)
        detail.verticalHeader().setVisible(False)
        detail.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        detail.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        rl.addWidget(detail, 1)
        split.addWidget(right)

        split.setSizes([560, 420])

        def _fill_table() -> None:
            tbl.setRowCount(0)
            for i, r in enumerate(rows):
                tbl.insertRow(i)
                self._set_item(tbl, i, 0, str(r.get("fecha_cobro") or "-"), align=QtCore.Qt.AlignCenter)
                self._set_item(tbl, i, 1, str(r.get("invoice_no") or "-"))
                self._set_item(tbl, i, 2, str(r.get("cliente") or "-"))
                self._set_item(tbl, i, 3, str(r.get("medio") or "-").capitalize(), align=QtCore.Qt.AlignCenter)
                self._set_item(
                    tbl,
                    i,
                    4,
                    DashboardRepo._fmt_gs(float(r.get("monto_total_ret_gs") or 0.0)),
                    align=QtCore.Qt.AlignRight,
                )
                self._set_item(tbl, i, 5, str(r.get("nro_cheque") or "-"), align=QtCore.Qt.AlignCenter)
            tbl.resizeColumnsToContents()

        def _fill_detail(idx: int) -> None:
            detail.setRowCount(0)
            if idx < 0 or idx >= len(rows):
                return
            r = rows[idx]
            campos = [
                ("Factura ID", str(r.get("invoice_id") or "-")),
                ("N Factura", str(r.get("invoice_no") or "-")),
                ("Cliente", str(r.get("cliente") or "-")),
                ("Fecha emisión", str(r.get("invoice_ts") or "-")),
                ("Fecha cobro", str(r.get("fecha_cobro") or "-")),
                ("Medio", str(r.get("medio") or "-").capitalize()),
                ("Nro cheque", str(r.get("nro_cheque") or "-")),
                ("Nro depósito", str(r.get("nro_deposito") or "-")),
                ("Referencia", str(r.get("referencia") or "-")),
                ("Observación", str(r.get("observacion") or "-")),
                ("Monto total (Gs)", DashboardRepo._fmt_gs(float(r.get("monto_total_gs") or 0.0))),
                ("Total con retención (Gs)", DashboardRepo._fmt_gs(float(r.get("monto_total_ret_gs") or 0.0))),
                ("Registrado en", str(r.get("ts_registro") or "-")),
            ]
            detail.setRowCount(len(campos))
            for j, (k, v) in enumerate(campos):
                self._set_item(detail, j, 0, k)
                self._set_item(detail, j, 1, v)
            detail.resizeColumnsToContents()

        def _on_row_change(_current, _previous):
            _fill_detail(tbl.currentRow())

        def _edit_selected() -> None:
            idx = tbl.currentRow()
            if idx < 0 or idx >= len(rows):
                QtWidgets.QMessageBox.information(dlg, "Editar cobro", "Seleccione una factura cobrada.")
                return
            src = dict(rows[idx])

            edit = QtWidgets.QDialog(dlg)
            edit.setWindowTitle("Editar datos de cobro")
            edit.resize(520, 340)
            el = QtWidgets.QVBoxLayout(edit)
            el.setContentsMargins(12, 10, 12, 10)
            el.setSpacing(8)

            resumen = QtWidgets.QLabel(
                f"Factura: {src.get('invoice_no') or '-'}   |   Cliente: {src.get('cliente') or '-'}"
            )
            resumen.setWordWrap(True)
            el.addWidget(resumen)

            form = QtWidgets.QFormLayout()
            form.setLabelAlignment(QtCore.Qt.AlignRight)
            el.addLayout(form)

            dt_cobro = QtWidgets.QDateEdit()
            dt_cobro.setCalendarPopup(True)
            ftxt = str(src.get("fecha_cobro") or "").strip()
            qd = QtCore.QDate.fromString(ftxt, "yyyy-MM-dd")
            dt_cobro.setDate(qd if qd.isValid() else QtCore.QDate.currentDate())
            form.addRow("Fecha de cobro:", dt_cobro)

            cb_medio = QtWidgets.QComboBox()
            cb_medio.addItems(["Cheque", "Efectivo"])
            medio = str(src.get("medio") or "cheque").strip().lower()
            cb_medio.setCurrentText("Cheque" if medio == "cheque" else "Efectivo")
            form.addRow("Medio:", cb_medio)

            lbl_cheque = QtWidgets.QLabel("Nro cheque:")
            ent_cheque = QtWidgets.QLineEdit(str(src.get("nro_cheque") or ""))
            form.addRow(lbl_cheque, ent_cheque)

            lbl_deposito = QtWidgets.QLabel("Nro depósito:")
            ent_deposito = QtWidgets.QLineEdit(str(src.get("nro_deposito") or ""))
            form.addRow(lbl_deposito, ent_deposito)

            lbl_ref = QtWidgets.QLabel("Referencia:")
            ent_ref = QtWidgets.QLineEdit(str(src.get("referencia") or ""))
            form.addRow(lbl_ref, ent_ref)

            ent_obs = QtWidgets.QPlainTextEdit(str(src.get("observacion") or ""))
            ent_obs.setFixedHeight(80)
            form.addRow("Observación:", ent_obs)

            def _on_medio_change(txt: str):
                is_cheque = (txt or "").strip().lower() == "cheque"
                lbl_cheque.setVisible(is_cheque)
                ent_cheque.setVisible(is_cheque)
                lbl_deposito.setVisible(is_cheque)
                ent_deposito.setVisible(is_cheque)
                if is_cheque:
                    lbl_ref.setText("Referencia:")
                    ent_ref.setPlaceholderText("Opcional")
                else:
                    lbl_ref.setText("Nro recibo/caja:")
                    ent_ref.setPlaceholderText("Opcional")

            cb_medio.currentTextChanged.connect(_on_medio_change)
            _on_medio_change(cb_medio.currentText())

            eb = QtWidgets.QHBoxLayout()
            eb.addStretch(1)
            btn_cancel_edit = QtWidgets.QPushButton("Cancelar")
            btn_save_edit = QtWidgets.QPushButton("Guardar cambios")
            eb.addWidget(btn_cancel_edit)
            eb.addWidget(btn_save_edit)
            el.addLayout(eb)

            btn_cancel_edit.clicked.connect(edit.reject)

            def _confirm_edit():
                medio_edit = cb_medio.currentText().strip().lower()
                nro_cheque = ent_cheque.text().strip()
                nro_deposito = ent_deposito.text().strip()
                if medio_edit == "cheque":
                    if not nro_cheque:
                        QtWidgets.QMessageBox.warning(edit, "Validación", "Debe ingresar el número de cheque.")
                        return
                    if not nro_deposito:
                        QtWidgets.QMessageBox.warning(edit, "Validación", "Debe ingresar el número de depósito.")
                        return
                src["fecha_cobro"] = dt_cobro.date().toString("yyyy-MM-dd")
                src["medio"] = medio_edit
                src["nro_cheque"] = nro_cheque
                src["nro_deposito"] = nro_deposito
                src["referencia"] = ent_ref.text().strip()
                src["observacion"] = ent_obs.toPlainText().strip()
                src["ts_modificacion"] = datetime.now().isoformat(timespec="seconds")
                rows[idx] = src
                try:
                    self._save_collection_details(rows)
                except Exception as exc:
                    QtWidgets.QMessageBox.critical(edit, "Editar cobro", f"No se pudo guardar: {exc}")
                    return
                edit.accept()

            btn_save_edit.clicked.connect(_confirm_edit)
            if edit.exec() != QtWidgets.QDialog.Accepted:
                return

            _fill_table()
            if rows:
                new_idx = min(idx, len(rows) - 1)
                tbl.selectRow(new_idx)
                _fill_detail(new_idx)

        tbl.selectionModel().currentRowChanged.connect(_on_row_change)
        _fill_table()
        if rows:
            tbl.selectRow(0)
            _fill_detail(0)

        btns = QtWidgets.QHBoxLayout()
        btn_edit = QtWidgets.QPushButton("Editar datos de cobro")
        btn_edit.clicked.connect(_edit_selected)
        btns.addWidget(btn_edit)
        btns.addStretch(1)
        btn_cerrar = QtWidgets.QPushButton("Cerrar")
        btn_cerrar.clicked.connect(dlg.accept)
        btns.addWidget(btn_cerrar)
        lay.addLayout(btns)

        dlg.exec()

    def _registrar_cobro_dialog(self) -> None:
        row = self.tbl_collections.currentRow()
        if row < 0 or row >= len(self._collection_rows):
            QtWidgets.QMessageBox.information(self, "Registrar cobro", "Seleccione primero una factura emitida.")
            return
        meta = self._collection_rows[row]
        try:
            available_checks = self.repo.load_available_payment_checks()
        except Exception:
            available_checks = []

        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("Registrar cobro")
        dlg.resize(560, 360)
        lay = QtWidgets.QVBoxLayout(dlg)
        lay.setContentsMargins(12, 10, 12, 10)
        lay.setSpacing(8)

        resumen = QtWidgets.QLabel(
            f"Factura ID: {meta.invoice_id}   |   Cliente: {meta.customer or '-'}   |   "
            f"Nro: {meta.invoice_no or '-'}   |   Total c/ret.: {DashboardRepo._fmt_gs(meta.total_con_retencion)} Gs"
        )
        resumen.setWordWrap(True)
        lay.addWidget(resumen)

        form = QtWidgets.QFormLayout()
        form.setLabelAlignment(QtCore.Qt.AlignRight)
        lay.addLayout(form)

        dt_cobro = QtWidgets.QDateEdit()
        dt_cobro.setCalendarPopup(True)
        dt_cobro.setDate(QtCore.QDate.currentDate())
        form.addRow("Fecha de cobro:", dt_cobro)

        cb_medio = QtWidgets.QComboBox()
        cb_medio.addItems(["Cheque", "Efectivo"])
        cb_medio.setCurrentIndex(0)
        form.addRow("Medio:", cb_medio)

        lbl_cheque = QtWidgets.QLabel("Nro cheque:")
        ent_cheque = QtWidgets.QLineEdit()
        ent_cheque.setPlaceholderText("Ingrese número de cheque")
        form.addRow(lbl_cheque, ent_cheque)

        lbl_deposito = QtWidgets.QLabel("Nro depósito:")
        ent_deposito = QtWidgets.QLineEdit()
        ent_deposito.setPlaceholderText("Ingrese número de depósito")
        form.addRow(lbl_deposito, ent_deposito)

        lbl_ref = QtWidgets.QLabel("Referencia:")
        ent_ref = QtWidgets.QLineEdit()
        ent_ref.setPlaceholderText("Opcional")
        form.addRow(lbl_ref, ent_ref)
        lbl_cheque_sistema = QtWidgets.QLabel("Cheque cargado:")
        cb_cheque = QtWidgets.QComboBox()
        cb_cheque.setEditable(False)
        cb_cheque.addItem("Seleccione un cheque cargado", "")
        cheque_model = cb_cheque.model()
        last_group = None
        for item in available_checks:
            group_label = str(item.get("group_label") or "").strip()
            if group_label and group_label != last_group:
                cb_cheque.addItem(f"--- {group_label} ---", "")
                header_idx = cb_cheque.count() - 1
                header_item = cheque_model.item(header_idx)
                if header_item is not None:
                    header_item.setEnabled(False)
                last_group = group_label
            cb_cheque.addItem(str(item.get("label") or ""), dict(item))
        form.addRow(lbl_cheque_sistema, cb_cheque)

        ent_obs = QtWidgets.QPlainTextEdit()
        ent_obs.setPlaceholderText("Observación opcional")
        ent_obs.setFixedHeight(80)
        form.addRow("Observación:", ent_obs)

        def _on_medio_change(txt: str):
            is_cheque = (txt or "").strip().lower() == "cheque"
            lbl_cheque.setVisible(is_cheque)
            ent_cheque.setVisible(is_cheque)
            lbl_deposito.setVisible(is_cheque)
            ent_deposito.setVisible(is_cheque)
            if is_cheque:
                lbl_ref.setText("Referencia:")
                ent_ref.setPlaceholderText("Opcional")
            else:
                lbl_ref.setText("Nro recibo/caja:")
                ent_ref.setPlaceholderText("Opcional")

        cb_medio.currentTextChanged.connect(_on_medio_change)
        _on_medio_change(cb_medio.currentText())

        def _on_cheque_selected(_idx: int):
            data = cb_cheque.currentData()
            if not isinstance(data, dict):
                return
            cheque_no = str(data.get("cheque_no") or "").strip()
            if cheque_no:
                ent_cheque.setText(cheque_no)
            reference_value = str(data.get("reference_value") or "").strip()
            if reference_value and not ent_ref.text().strip():
                ent_ref.setText(reference_value)

        cb_cheque.currentIndexChanged.connect(_on_cheque_selected)

        btns = QtWidgets.QHBoxLayout()
        btns.addStretch(1)
        btn_cancel = QtWidgets.QPushButton("Cancelar")
        btn_ok = QtWidgets.QPushButton("Confirmar cobro")
        btns.addWidget(btn_cancel)
        btns.addWidget(btn_ok)
        lay.addLayout(btns)

        btn_cancel.clicked.connect(dlg.reject)

        def _confirm():
            medio = cb_medio.currentText().strip().lower()
            nro_cheque = ent_cheque.text().strip()
            nro_deposito = ent_deposito.text().strip()
            if medio == "cheque":
                if not nro_cheque:
                    QtWidgets.QMessageBox.warning(dlg, "Validación", "Debe ingresar el número de cheque.")
                    return
                if not nro_deposito:
                    QtWidgets.QMessageBox.warning(dlg, "Validación", "Debe ingresar el número de depósito.")
                    return
            payload = {
                "ts_registro": datetime.now().isoformat(timespec="seconds"),
                "invoice_id": int(meta.invoice_id),
                "invoice_ts": str(meta.ts or "").strip(),
                "invoice_no": meta.invoice_no or "",
                "cliente": meta.customer or "",
                "monto_total_gs": float(meta.total_gs or 0.0),
                "monto_total_ret_gs": float(meta.total_con_retencion or 0.0),
                "fecha_cobro": dt_cobro.date().toString("yyyy-MM-dd"),
                "medio": medio,
                "nro_cheque": nro_cheque,
                "nro_deposito": nro_deposito,
                "referencia": ent_ref.text().strip(),
                "observacion": ent_obs.toPlainText().strip(),
            }
            try:
                self._save_collection_flag(meta.invoice_id, meta.ts, meta.invoice_no, True)
                self._append_collection_detail(payload)
            except Exception as exc:
                QtWidgets.QMessageBox.critical(dlg, "Registrar cobro", f"No se pudo guardar el cobro: {exc}")
                return
            dlg.accept()
            self.refresh_data()
            self.lbl_status.setText(
                f"Cobro registrado factura {meta.invoice_no or meta.invoice_id} ({cb_medio.currentText()}) - "
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

        btn_ok.clicked.connect(_confirm)
        dlg.exec()

    def _lookup_factura_pdf_path(self, factura_num: str) -> str | None:
        numero = (factura_num or "").strip()
        if not numero:
            return None
        if not self.repo.invoices_db.exists():
            return None
        try:
            cn = db.connect("facturas")
            cur = cn.cursor()
            cur.execute(
                "SELECT pdf_path FROM factura WHERE numero = %s ORDER BY id DESC LIMIT 1;",
                (numero,),
            )
            row = cur.fetchone()
            if not row or not row[0]:
                return None
            return str(row[0])
        except Exception:
            return None
        finally:
            try:
                cn.close()
            except Exception:
                pass

    def _open_selected_factura_pdf(self) -> None:
        selected = self._selected_payment_rows()
        if not selected:
            QtWidgets.QMessageBox.information(self, "Factura", "Seleccione primero una fila de pagos pendientes.")
            return
        meta = selected[0]
        factura_num = (meta.numero_doc or "").strip()
        if not factura_num:
            QtWidgets.QMessageBox.information(
                self,
                "Factura",
                "El registro seleccionado no tiene número de factura asociado.",
            )
            return
        pdf_path = self._lookup_factura_pdf_path(factura_num)
        if not pdf_path:
            QtWidgets.QMessageBox.information(
                self,
                "Factura",
                f"No hay PDF asociado para la factura {factura_num}.",
            )
            return
        p = Path(pdf_path)
        if not p.exists():
            QtWidgets.QMessageBox.critical(self, "Factura", f"No se encuentra el PDF:\n{pdf_path}")
            return
        ok = QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(p)))
        if not ok:
            QtWidgets.QMessageBox.critical(self, "Factura", f"No se pudo abrir el PDF:\n{pdf_path}")

    def _selected_order_row(self) -> OrderRow | None:
        row = self.tbl_orders.currentRow()
        if row < 0 or row >= len(self._order_rows):
            return None
        return self._order_rows[row]

    def _ver_faltantes_pedido(self) -> None:
        meta = self._selected_order_row()
        if meta is None:
            QtWidgets.QMessageBox.information(self, "Ver faltantes", "Seleccione primero un pedido pendiente.")
            return

        faltantes = self.repo.order_missing_items(meta.oc_id)

        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("Faltantes del pedido")
        dlg.resize(980, 520)
        lay = QtWidgets.QVBoxLayout(dlg)
        lay.setContentsMargins(12, 10, 12, 10)
        lay.setSpacing(8)

        resumen = QtWidgets.QLabel(
            f"Pedido: {meta.numero or meta.oc_id}   |   Sucursal: {meta.sucursal or '-'}   |   "
            f"% listo: {float(meta.pct_listo_entrega or 0.0):.1f}%   |   "
            f"Monto: {DashboardRepo._fmt_gs(meta.monto_total)} Gs"
        )
        resumen.setWordWrap(True)
        lay.addWidget(resumen)

        if not faltantes:
            ok_lbl = QtWidgets.QLabel("Todos los ítems pendientes del pedido están cubiertos con el stock actual.")
            ok_lbl.setStyleSheet("color: #166534; font-weight: 600;")
            lay.addWidget(ok_lbl)
        else:
            info_lbl = QtWidgets.QLabel(f"Ítems no cubiertos: {len(faltantes)}")
            info_lbl.setStyleSheet("color: #991b1b; font-weight: 600;")
            lay.addWidget(info_lbl)

            tbl = QtWidgets.QTableWidget(0, 6)
            tbl.setHorizontalHeaderLabels(
                ["Línea", "Descripción", "Necesario", "Disponible", "Faltante", "Estado"]
            )
            tbl.horizontalHeader().setStretchLastSection(True)
            tbl.verticalHeader().setVisible(False)
            tbl.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
            tbl.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
            tbl.setAlternatingRowColors(True)
            tbl.setRowCount(len(faltantes))
            for i, item in enumerate(faltantes):
                estado = str(item.get("estado") or "-")
                bg = QtGui.QColor("#fee2e2") if estado == "Faltante" else QtGui.QColor("#fef3c7")
                self._set_item(tbl, i, 0, str(item.get("linea") or "-"), align=QtCore.Qt.AlignCenter, bg=bg)
                self._set_item(tbl, i, 1, str(item.get("descripcion") or "-"), bg=bg)
                self._set_item(tbl, i, 2, str(item.get("necesario") or "-"), align=QtCore.Qt.AlignCenter, bg=bg)
                self._set_item(
                    tbl, i, 3, "-" if item.get("disponible") is None else str(item.get("disponible")),
                    align=QtCore.Qt.AlignCenter, bg=bg
                )
                self._set_item(
                    tbl, i, 4, "-" if item.get("faltante") is None else str(item.get("faltante")),
                    align=QtCore.Qt.AlignCenter, bg=bg
                )
                self._set_item(tbl, i, 5, estado, align=QtCore.Qt.AlignCenter, bg=bg)
            tbl.resizeColumnsToContents()
            lay.addWidget(tbl, 1)

        btns = QtWidgets.QHBoxLayout()
        btns.addStretch(1)
        btn_close = QtWidgets.QPushButton("Cerrar")
        btn_close.clicked.connect(dlg.accept)
        btns.addWidget(btn_close)
        lay.addLayout(btns)

        dlg.exec()

    def _open_fraccionadora(self) -> None:
        self._open_script(
            FRACC_SCRIPT,
            "Fraccionadora",
            "Abriendo fraccionadora.py...",
            "No se encontro el script",
        )

    def _open_importador_facturas(self) -> None:
        self._open_script(
            INVOICE_IMPORTER_SCRIPT,
            "Importador de facturas",
            "Abriendo IFACTURA.PY...",
            "No se encontro el script",
        )

    def _open_importador_oc(self) -> None:
        self._open_script(
            OC_IMPORTER_SCRIPT,
            "Importador de OC",
            "Abriendo app_tk.py...",
            "No se encontro el script",
        )

    def _open_reporte_ventas(self) -> None:
        self._open_script(
            SALES_REPORT_SCRIPT,
            "Reporte de ventas",
            "Abriendo reportes_ventas_qt.py...",
            "No se encontro el script",
        )

    def _open_reporte_mensual(self) -> None:
        self._open_script(
            MONTHLY_REPORT_SCRIPT,
            "Reporte mensual",
            "Abriendo reporte_mensual_qt.py...",
            "No se encontro el script",
        )

    def _open_script(self, script_path: Path, title: str, status_text: str, missing_text: str) -> None:
        if not script_path.exists():
            QtWidgets.QMessageBox.critical(self, title, f"{missing_text}:\n{script_path}")
            return
        try:
            subprocess.Popen([sys.executable, str(script_path)], cwd=str(script_path.parent))
            self.lbl_status.setText(status_text)
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, title, f"No se pudo abrir: {exc}")

    def _save_payment_flag(self, lot_id: int, paid: bool) -> None:
        self.repo.save_payment_flag(lot_id, paid)

    def _append_payment_detail(self, payload: dict) -> None:
        self.repo.upsert_payment_detail(payload)

    def _load_payment_details(self) -> list[dict]:
        return self.repo.load_payment_details()

    def _save_payment_details(self, rows: list[dict]) -> None:
        self.repo.save_payment_details(rows)

    def _registrar_pago_dialog(self) -> None:
        selected = self._selected_payment_rows()
        if not selected:
            QtWidgets.QMessageBox.information(self, "Registrar pago", "Seleccione primero una o mas filas de pagos pendientes.")
            return
        total_pago = sum(float(r.monto or 0.0) for r in selected)
        proveedores = sorted({(r.proveedor or "-").strip() or "-" for r in selected})
        facturas = [r.numero_doc or f"ID {r.factura_id}" for r in selected]

        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("Registrar pago")
        dlg.resize(700, 460)
        lay = QtWidgets.QVBoxLayout(dlg)
        lay.setContentsMargins(12, 10, 12, 10)
        lay.setSpacing(8)

        resumen = QtWidgets.QLabel(
            f"Facturas seleccionadas: {len(selected)}   |   "
            f"Proveedores: {', '.join(proveedores)}   |   "
            f"Total: {DashboardRepo._fmt_gs(total_pago)} Gs"
        )
        resumen.setWordWrap(True)
        lay.addWidget(resumen)

        detalle = QtWidgets.QPlainTextEdit()
        detalle.setReadOnly(True)
        detalle.setFixedHeight(110)
        detalle.setPlainText(
            "\n".join(
                f"{r.proveedor or '-'} | {r.numero_doc or f'ID {r.factura_id}'} | {DashboardRepo._fmt_gs(r.monto)} Gs"
                for r in selected
            )
        )
        lay.addWidget(detalle)

        form = QtWidgets.QFormLayout()
        form.setLabelAlignment(QtCore.Qt.AlignRight)
        lay.addLayout(form)

        dt_pago = QtWidgets.QDateEdit()
        dt_pago.setCalendarPopup(True)
        dt_pago.setDate(QtCore.QDate.currentDate())
        form.addRow("Fecha de pago:", dt_pago)

        cb_medio = QtWidgets.QComboBox()
        cb_medio.addItems(["Efectivo", "Transferencia", "Cheque"])
        form.addRow("Medio:", cb_medio)

        lbl_ref = QtWidgets.QLabel("Nro comprobante:")
        ent_ref = QtWidgets.QLineEdit()
        ent_ref.setPlaceholderText("Ingrese numero")
        form.addRow(lbl_ref, ent_ref)

        lbl_serie = QtWidgets.QLabel("Serie cheque:")
        ent_serie = QtWidgets.QLineEdit()
        ent_serie.setPlaceholderText("Ingrese serie")
        form.addRow(lbl_serie, ent_serie)

        lbl_cheque_sistema = QtWidgets.QLabel("Cheque cargado:")
        lbl_cheque_estado = QtWidgets.QLabel("No")
        form.addRow(lbl_cheque_sistema, lbl_cheque_estado)

        lbl_deposito = QtWidgets.QLabel("Nro deposito:")
        ent_deposito = QtWidgets.QLineEdit()
        ent_deposito.setPlaceholderText("Ingrese numero de deposito")
        form.addRow(lbl_deposito, ent_deposito)

        lbl_recibo = QtWidgets.QLabel("Nro recibo dinero:")
        ent_recibo = QtWidgets.QLineEdit()
        ent_recibo.setPlaceholderText("Ingrese numero de recibo")
        form.addRow(lbl_recibo, ent_recibo)

        ent_obs = QtWidgets.QPlainTextEdit()
        ent_obs.setPlaceholderText("Observacion opcional")
        ent_obs.setFixedHeight(80)
        form.addRow("Observacion:", ent_obs)

        def _on_medio_change(txt: str):
            t = (txt or "").strip().lower()
            show_cheque = t == "cheque"
            if show_cheque:
                lbl_ref.setText("Nro cheque:")
                ent_ref.setEnabled(True)
                ent_ref.setPlaceholderText("Ingrese número de cheque")
            else:
                lbl_ref.setText("Nro comprobante:")
                ent_ref.setEnabled(True)
                if t == "transferencia":
                    ent_ref.setPlaceholderText("Ingrese numero de comprobante")
                else:
                    ent_ref.setPlaceholderText("Opcional (recibo/caja)")
                ent_serie.clear()
                lbl_cheque_estado.setText("No aplica")
                lbl_cheque_estado.setStyleSheet("")
            lbl_cheque_sistema.setVisible(show_cheque)
            lbl_cheque_estado.setVisible(show_cheque)
            lbl_serie.setVisible(show_cheque)
            ent_serie.setVisible(show_cheque)
            show_deposito = t in {"transferencia", "cheque"}
            show_recibo = t == "efectivo"
            lbl_deposito.setVisible(show_deposito)
            ent_deposito.setVisible(show_deposito)
            lbl_recibo.setVisible(show_recibo)
            ent_recibo.setVisible(show_recibo)

        current_check_data: dict[str, object] = {}

        def _refresh_check_status(*_args):
            nonlocal current_check_data
            if cb_medio.currentText().strip().lower() != "cheque":
                current_check_data = {}
                return
            serie_txt = ent_serie.text().strip().upper()
            cheque_txt = re.sub(r"\D+", "", ent_ref.text().strip())
            current_check_data = self.repo.find_loaded_payment_check(serie_txt, cheque_txt) or {}
            if not serie_txt or not cheque_txt:
                lbl_cheque_estado.setText("No")
                lbl_cheque_estado.setStyleSheet("")
                return
            if not current_check_data:
                lbl_cheque_estado.setText("No, no está cargado")
                lbl_cheque_estado.setStyleSheet("color: #b91c1c;")
                return
            if current_check_data.get("used"):
                lbl_cheque_estado.setText("Sí, pero ya está usado")
                lbl_cheque_estado.setStyleSheet("color: #b45309;")
                return
            lbl_cheque_estado.setText("Sí")
            lbl_cheque_estado.setStyleSheet("color: #166534;")

        cb_medio.currentTextChanged.connect(_on_medio_change)
        ent_serie.textChanged.connect(_refresh_check_status)
        ent_ref.textChanged.connect(_refresh_check_status)
        ent_serie.editingFinished.connect(_refresh_check_status)
        ent_ref.editingFinished.connect(_refresh_check_status)
        _on_medio_change(cb_medio.currentText())
        _refresh_check_status()

        btns = QtWidgets.QHBoxLayout()
        btns.addStretch(1)
        btn_cancel = QtWidgets.QPushButton("Cancelar")
        btn_ok = QtWidgets.QPushButton("Confirmar pago")
        btns.addWidget(btn_cancel)
        btns.addWidget(btn_ok)
        lay.addLayout(btns)

        btn_cancel.clicked.connect(dlg.reject)

        def _confirm():
            nonlocal current_check_data
            medio = cb_medio.currentText().strip()
            ref = re.sub(r"\D+", "", ent_ref.text().strip()) if medio == "Cheque" else ent_ref.text().strip()
            if medio == "Cheque":
                serie_txt = ent_serie.text().strip().upper()
                if not serie_txt or not ref:
                    QtWidgets.QMessageBox.warning(dlg, "Validacion", "Debe ingresar la serie y el número de cheque.")
                    return
                current_check_data = self.repo.find_loaded_payment_check(serie_txt, ref) or {}
                if not current_check_data:
                    QtWidgets.QMessageBox.warning(dlg, "Validacion", "Ese cheque no está cargado en el sistema.")
                    return
                if current_check_data.get("used"):
                    QtWidgets.QMessageBox.warning(dlg, "Validacion", "Ese cheque ya está marcado como usado.")
                    return
                ref = str((current_check_data or {}).get("reference_value") or "").strip()
            if medio == "Transferencia" and not ref:
                QtWidgets.QMessageBox.warning(dlg, "Validacion", "Debe ingresar el numero correspondiente.")
                return
            ts_registro = datetime.now().isoformat(timespec="seconds")
            payment_group_id = f"paygrp:{ts_registro}:{medio.lower()}:{ref or 'sin-ref'}"
            try:
                for meta in selected:
                    payload = {
                        "ts_registro": ts_registro,
                        "payment_group_id": payment_group_id,
                        "lot_id": int(meta.factura_id),
                        "proveedor": meta.proveedor or "",
                        "factura": meta.numero_doc or "",
                        "monto_gs": float(meta.monto or 0.0),
                        "fecha_pago": dt_pago.date().toString("yyyy-MM-dd"),
                        "medio": medio.lower(),
                        "referencia": ref,
                        "nro_deposito": ent_deposito.text().strip(),
                        "nro_recibo_dinero": ent_recibo.text().strip(),
                        "observacion": ent_obs.toPlainText().strip(),
                        "facturas_grupo": list(facturas),
                        "total_grupo_gs": float(total_pago),
                    }
                    self._save_payment_flag(meta.factura_id, True)
                    self._append_payment_detail(payload)
                if medio == "Cheque" and current_check_data:
                    self.repo.mark_payment_check_used(current_check_data, payment_group_id, ref)
            except Exception as exc:
                QtWidgets.QMessageBox.critical(dlg, "Registrar pago", f"No se pudo guardar el pago: {exc}")
                return
            dlg.accept()
            self.refresh_data()
            self.lbl_status.setText(
                f"Pago registrado para {len(selected)} factura(s) ({medio}) - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

        btn_ok.clicked.connect(_confirm)
        dlg.exec()

    def _ver_pagos_realizados_dialog(self) -> None:
        rows = self._load_payment_details()
        if not rows:
            QtWidgets.QMessageBox.information(self, "Pagos realizados", "No hay pagos registrados.")
            return

        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("Pagos realizados")
        dlg.resize(1180, 560)
        lay = QtWidgets.QVBoxLayout(dlg)
        lay.setContentsMargins(10, 10, 10, 10)
        lay.setSpacing(8)

        tbl = QtWidgets.QTableWidget(0, 10)
        tbl.setHorizontalHeaderLabels(
            [
                "Fecha pago",
                "Proveedor",
                "Factura",
                "Monto",
                "Medio",
                "Nro cheque/comprobante",
                "Nro deposito",
                "Nro recibo dinero",
                "ID de pago",
                "Registro",
            ]
        )
        tbl.horizontalHeader().setStretchLastSection(True)
        tbl.verticalHeader().setVisible(False)
        tbl.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        tbl.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        tbl.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        tbl.setAlternatingRowColors(True)
        lay.addWidget(tbl, 1)

        tbl.setRowCount(len(rows))
        for i, r in enumerate(rows):
            self._set_item(tbl, i, 0, str(r.get("fecha_pago") or "-"), align=QtCore.Qt.AlignCenter)
            self._set_item(tbl, i, 1, str(r.get("proveedor") or "-"))
            self._set_item(tbl, i, 2, str(r.get("factura") or "-"))
            self._set_item(tbl, i, 3, DashboardRepo._fmt_gs(float(r.get("monto_gs") or 0.0)), align=QtCore.Qt.AlignRight)
            self._set_item(tbl, i, 4, str(r.get("medio") or "-").capitalize(), align=QtCore.Qt.AlignCenter)
            self._set_item(tbl, i, 5, str(r.get("referencia") or "-"), align=QtCore.Qt.AlignCenter)
            self._set_item(tbl, i, 6, str(r.get("nro_deposito") or "-"), align=QtCore.Qt.AlignCenter)
            self._set_item(tbl, i, 7, str(r.get("nro_recibo_dinero") or "-"), align=QtCore.Qt.AlignCenter)
            self._set_item(tbl, i, 8, str(r.get("payment_group_id") or "-"))
            self._set_item(tbl, i, 9, str(r.get("ts_registro") or "-"), align=QtCore.Qt.AlignCenter)
        tbl.resizeColumnsToContents()

        def _selected_receipt_group_ids() -> tuple[list[int], list[str]]:
            selected_rows = sorted({idx.row() for idx in tbl.selectionModel().selectedRows()})
            group_ids: list[str] = []
            seen: set[str] = set()
            for row_idx in selected_rows:
                if row_idx < 0 or row_idx >= len(rows):
                    continue
                group_id = str(rows[row_idx].get("payment_group_id") or "").strip()
                if not group_id or group_id == "-" or group_id in seen:
                    continue
                seen.add(group_id)
                group_ids.append(group_id)
            return selected_rows, group_ids

        def _apply_receipt_to_selection(default_group_id: str = "", default_value: str = "") -> None:
            selected_rows, group_ids = _selected_receipt_group_ids()
            if not group_ids:
                QtWidgets.QMessageBox.information(
                    dlg,
                    "Pagos realizados",
                    "Seleccione una o mas filas con ID de pago valido.",
                )
                return
            if not default_value and selected_rows:
                first_row = selected_rows[0]
                default_value = str(rows[first_row].get("nro_recibo_dinero") or "").strip()
            resumen = (
                f"Filas seleccionadas: {len(selected_rows)}\n"
                f"Pagos a actualizar: {len(group_ids)}"
            )
            if default_group_id:
                resumen = f"ID de pago base:\n{default_group_id}\n\n{resumen}"
            nuevo, ok = QtWidgets.QInputDialog.getText(
                dlg,
                "Nro recibo dinero",
                f"{resumen}\n\nIngrese o actualice el numero de recibo:",
                text=default_value,
            )
            if not ok:
                return
            nuevo = nuevo.strip()
            updated = False
            for rec in rows:
                if str(rec.get("payment_group_id") or "").strip() in group_ids:
                    rec["nro_recibo_dinero"] = nuevo
                    updated = True
            if not updated:
                return
            try:
                self._save_payment_details(rows)
            except Exception as exc:
                QtWidgets.QMessageBox.critical(dlg, "Pagos realizados", f"No se pudo guardar el recibo:\n{exc}")
                return
            for i, r in enumerate(rows):
                self._set_item(tbl, i, 7, str(r.get("nro_recibo_dinero") or "-"), align=QtCore.Qt.AlignCenter)
            self.lbl_status.setText(
                f"Recibo actualizado en {len(group_ids)} pago(s) y {len(selected_rows)} fila(s) - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

        def _edit_receipt(item: QtWidgets.QTableWidgetItem) -> None:
            if not item or item.column() != 8:
                return
            row_idx = item.row()
            if row_idx < 0 or row_idx >= len(rows):
                return
            group_id = str(rows[row_idx].get("payment_group_id") or "").strip()
            if not group_id or group_id == "-":
                QtWidgets.QMessageBox.information(dlg, "Pagos realizados", "Ese registro no tiene ID de pago.")
                return
            current = str(rows[row_idx].get("nro_recibo_dinero") or "").strip()
            tbl.selectRow(row_idx)
            _apply_receipt_to_selection(default_group_id=group_id, default_value=current)

        tbl.itemDoubleClicked.connect(_edit_receipt)

        btns = QtWidgets.QHBoxLayout()
        btn_add_receipt = QtWidgets.QPushButton("Agregar numero de recibo")
        btns.addWidget(btn_add_receipt)
        btns.addStretch(1)
        btn_close = QtWidgets.QPushButton("Cerrar")
        btns.addWidget(btn_close)
        lay.addLayout(btns)
        btn_add_receipt.clicked.connect(_apply_receipt_to_selection)
        btn_close.clicked.connect(dlg.accept)

        dlg.exec()

    def _fill_kpis(
        self,
        orders: list[OrderRow],
        payments: list[PaymentRow],
        cobro_pend: float,
        cobro_count: int,
    ) -> None:
        venc_orders = [o for o in orders if (o.dias_atraso is not None and o.dias_atraso > 0)]
        monto_pend = sum(p.monto for p in payments)
        self.kpi_ped_pend.set_data(str(len(orders)))
        self.kpi_ped_venc.set_data(str(len(venc_orders)))
        self.kpi_pag_pend.set_data(DashboardRepo._fmt_gs(monto_pend), "Gs")
        self.kpi_pag_venc.set_data(
            DashboardRepo._fmt_gs(cobro_pend),
            f"Gs ({cobro_count} facturas, con retención)",
        )

    def _not_implemented(self) -> None:
        QtWidgets.QMessageBox.information(
            self,
            "Acción rápida",
            "Esta acción se deja conectada al dashboard para integrarla con tu flujo actual.",
        )

    def _apply_theme(self) -> None:
        if self._dark_mode:
            self.setStyleSheet(
                """
                QWidget { background: #f6f9ff; color: #0f172a; }
                QFrame { background: #ffffff; border: 1px solid #cfe0ff; border-radius: 10px; }
                QLabel#dashTitle { font-size: 22px; font-weight: 700; color: #1d4ed8; }
                QFrame#kpiCard { background: #ffffff; border: 1px solid #bfdbfe; border-radius: 10px; }
                QLabel#kpiTitle { color: #2563eb; font-size: 12px; }
                QLabel#kpiValue { color: #0f172a; font-size: 24px; font-weight: 700; }
                QLabel#kpiSub { color: #334155; font-size: 11px; }
                QLineEdit, QComboBox, QDateEdit {
                    background: #ffffff; color: #0f172a; border: 1px solid #93c5fd; border-radius: 6px; padding: 4px;
                }
                QTableWidget {
                    background: #ffffff; color: #0f172a; border: 1px solid #bfdbfe; border-radius: 8px;
                    gridline-color: #dbeafe; alternate-background-color: #f8fbff;
                    selection-background-color: #bfdbfe; selection-color: #0f172a;
                }
                QHeaderView::section { background: #eaf2ff; color: #1e3a8a; border: none; padding: 6px; }
                QListWidget {
                    background: #ffffff; color: #0f172a; border: 1px solid #bfdbfe; border-radius: 8px;
                    selection-background-color: #dbeafe; selection-color: #0f172a;
                }
                QPushButton {
                    background: #2563eb; color: #ffffff; border: none; border-radius: 6px; padding: 6px 10px; font-weight: 600;
                }
                QPushButton:hover { background: #1d4ed8; }
                QToolButton {
                    background: #2563eb; color: #ffffff; border: none; border-radius: 6px; padding: 6px 10px; font-weight: 600;
                }
                QToolButton:hover { background: #1d4ed8; }
                QToolButton#hamburgerBtn {
                    min-width: 28px; font-size: 16px; font-weight: 700;
                }
                QToolButton#hamburgerBtn:hover {
                    background: #1e40af; border: 1px solid #93c5fd;
                }
                QMenu {
                    background: #ffffff; color: #0f172a; border: 1px solid #bfdbfe; padding: 4px;
                }
                QMenu::item {
                    padding: 6px 12px; border-radius: 6px;
                }
                QMenu::item:selected {
                    background: #dbeafe; color: #1e3a8a;
                }
                """
            )
        else:
            self.setStyleSheet(
                """
                QWidget { background: #f6f9ff; color: #0f172a; }
                QFrame { background: #ffffff; border: 1px solid #cfe0ff; border-radius: 10px; }
                QLabel#dashTitle { font-size: 22px; font-weight: 700; color: #1d4ed8; }
                QFrame#kpiCard { background: #ffffff; border: 1px solid #bfdbfe; border-radius: 10px; }
                QLabel#kpiTitle { color: #2563eb; font-size: 12px; }
                QLabel#kpiValue { color: #0f172a; font-size: 24px; font-weight: 700; }
                QLabel#kpiSub { color: #334155; font-size: 11px; }
                QLineEdit, QComboBox, QDateEdit {
                    background: #ffffff; color: #0f172a; border: 1px solid #93c5fd; border-radius: 6px; padding: 4px;
                }
                QTableWidget {
                    background: #ffffff; color: #0f172a; border: 1px solid #bfdbfe; border-radius: 8px;
                    gridline-color: #dbeafe; alternate-background-color: #f8fbff;
                    selection-background-color: #bfdbfe; selection-color: #0f172a;
                }
                QHeaderView::section { background: #eaf2ff; color: #1e3a8a; border: none; padding: 6px; }
                QListWidget {
                    background: #ffffff; color: #0f172a; border: 1px solid #bfdbfe; border-radius: 8px;
                    selection-background-color: #dbeafe; selection-color: #0f172a;
                }
                QPushButton {
                    background: #0ea5e9; color: #ffffff; border: none; border-radius: 6px; padding: 6px 10px; font-weight: 600;
                }
                QPushButton:hover { background: #0284c7; }
                QToolButton {
                    background: #0ea5e9; color: #ffffff; border: none; border-radius: 6px; padding: 6px 10px; font-weight: 600;
                }
                QToolButton:hover { background: #0284c7; }
                QToolButton#hamburgerBtn {
                    min-width: 28px; font-size: 16px; font-weight: 700;
                }
                QToolButton#hamburgerBtn:hover {
                    background: #0369a1; border: 1px solid #7dd3fc;
                }
                QMenu {
                    background: #ffffff; color: #0f172a; border: 1px solid #bfdbfe; padding: 4px;
                }
                QMenu::item {
                    padding: 6px 12px; border-radius: 6px;
                }
                QMenu::item:selected {
                    background: #e0f2fe; color: #0c4a6e;
                }
                """
            )


def main() -> None:
    app = QtWidgets.QApplication(sys.argv)
    win = InicioDashboardWindow(dark_mode=False)
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()


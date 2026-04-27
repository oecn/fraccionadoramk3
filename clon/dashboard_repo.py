# -*- coding: utf-8 -*-
"""Acceso a datos y refresco asincrono del dashboard operativo.

Este modulo contiene la parte no visual del dashboard:
- modelos simples de filas que consume la UI;
- consultas y persistencia de estado local del dashboard;
- el worker que ejecuta esas consultas fuera del hilo principal de Qt.

La ventana principal debe encargarse solo de widgets, eventos y renderizado.
"""
from __future__ import annotations

import json
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import db
from PySide6 import QtCore


ORDERS_DB = ROOT_DIR / "PDFMK10" / "db" / "pedidos.db"
INVOICES_DB = ROOT_DIR / "importadorfactur" / "facturas.db"
PAYMENTS_JSON = ROOT_DIR / "pagos_compras copy.json"
PAYMENTS_DETAIL_JSON = ROOT_DIR / "pagos_compras_detalle.json"
FRACC_DB = ROOT_DIR / "GCMK8" / "fraccionadora.db"
COBROS_VENTAS_JSON = ROOT_DIR / "cobros_ventas.json"
COBROS_VENTAS_DETAIL_JSON = ROOT_DIR / "cobros_ventas_detalle.json"

DELIVERY_SLA_DAYS = 2
CREDIT_TERM_DAYS = 15


@dataclass
class OrderRow:
    """Fila normalizada para la tabla de ordenes de compra pendientes."""

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
    """Fila normalizada para la tabla de facturas de compra pendientes de pago."""

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
    """Fila normalizada para la tabla de facturas de venta pendientes de cobro."""

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


class DashboardRepo:
    """Repositorio de datos del dashboard.

    Centraliza consultas a las bases usadas por el inicio operativo y mantiene
    el estado propio del dashboard: pagos marcados, cobros marcados, detalles
    de pago/cobro y cheques ya usados desde esta pantalla.
    """

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

    def _connect_fracc(self):
        """Abre una conexion a la base principal de fraccionadora."""

        return db.connect("fraccionadora")

    @staticmethod
    def _norm_text(value: str) -> str:
        """Normaliza texto para comparaciones tolerantes a acentos y signos."""

        text = str(value or "").strip().lower()
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
        text = re.sub(r"[^a-z0-9]+", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    @classmethod
    def _extract_gramaje(cls, desc: str) -> int | None:
        """Extrae el gramaje de una descripcion de producto, en gramos."""

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
        """Relaciona una descripcion de OC con un producto de fraccionadora."""

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
        """Carga productos y stock empaquetado para calcular disponibilidad."""

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
        """Calcula que porcentaje de items pendientes de una OC tiene stock."""

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
        """Devuelve los items de una OC que no alcanzan stock o no se pueden mapear."""

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
        """Crea las tablas PostgreSQL que guardan el estado propio del dashboard."""

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
        """Crea el registro local de cheques usados desde el dashboard."""

        cn = self._connect_fracc()
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
        """Migra archivos JSON legacy a las tablas actuales si todavia existen."""

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
        """Marca como pagadas las facturas que ya tienen detalle de pago guardado."""

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
        """Mantiene compatibilidad escribiendo el JSON legacy de pagos."""

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
        """Genera claves nuevas y legacy para identificar un cobro."""

        inv_id = int(invoice_id or 0)
        ts_txt = str(ts or "").strip()
        nro_txt = str(nro or "").strip()
        return [f"std:{inv_id}:{ts_txt}:{nro_txt}", f"std:{inv_id}"]

    def load_paid_map(self) -> dict[int, bool]:
        """Carga el mapa lot_id -> pagado usado para filtrar compras pendientes."""

        cn = self._connect_fracc()
        cur = cn.cursor()
        rows = cur.execute("SELECT lot_id, paid FROM dashboard_payment_flags;").fetchall()
        cn.close()
        out: dict[int, bool] = {}
        for lot_id, paid in rows:
            out[int(lot_id)] = bool(paid)
        return out

    def load_collections_map(self) -> dict[str, bool]:
        """Carga el mapa de facturas de venta marcadas como cobradas."""

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
        """Marca o desmarca una factura de compra como pagada."""

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
        """Inserta o actualiza el detalle operativo de un pago de compra."""

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
        """Carga el historial de pagos registrados desde el dashboard."""

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
        """Guarda varios detalles de pago reutilizando el upsert individual."""

        for row in rows:
            if isinstance(row, dict):
                self.upsert_payment_detail(row)

    def load_available_payment_checks(self) -> list[dict]:
        """Lista cheques disponibles en bancos, excluyendo los ya usados aqui."""

        cn = self._connect_fracc()
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
        """Busca si un cheque existe en una chequera y si ya fue usado aqui."""

        serie_txt = str(serie or "").strip().upper()
        cheque_txt = re.sub(r"\D+", "", str(cheque_no or "").strip())
        if not serie_txt or not cheque_txt.isdigit():
            return None
        cheque_n = int(cheque_txt)
        cn = self._connect_fracc()
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
        """Registra un cheque como usado para no ofrecerlo dos veces."""

        chequera_id = str(check_data.get("chequera_id") or "").strip()
        cheque_no = str(check_data.get("cheque_no") or "").strip()
        serie = str(check_data.get("serie") or "").strip().upper()
        if not chequera_id or not cheque_no:
            return
        cn = self._connect_fracc()
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
        """Marca o desmarca una factura de venta como cobrada."""

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
        """Inserta o actualiza el detalle operativo de un cobro de venta."""

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
        """Carga el historial de cobros registrados desde el dashboard."""

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
        """Guarda varios detalles de cobro reutilizando el upsert individual."""

        for row in rows:
            if isinstance(row, dict):
                self.upsert_collection_detail(row)

    @staticmethod
    def _parse_iso(d: str) -> date | None:
        """Convierte yyyy-mm-dd a date; devuelve None si el dato no sirve."""

        try:
            return datetime.strptime((d or "").strip(), "%Y-%m-%d").date()
        except Exception:
            return None

    @staticmethod
    def _fmt_gs(n: float) -> str:
        """Formatea importes en guaranies con separador de miles local."""

        return f"{n:,.0f}".replace(",", ".")

    @staticmethod
    def _safe_int(v) -> int:
        """Convierte a int sin romper el flujo del dashboard."""

        try:
            return int(v)
        except Exception:
            return 0

    @staticmethod
    def _sucursal_from_proveedor(proveedor: str) -> str:
        """Deduce sucursal desde el texto del proveedor de raw_lots."""

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
        """Combina sucursales disponibles desde OC, facturas y compras."""

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
        """Alias interno mantenido para compatibilidad con llamadas existentes."""

        return self.load_paid_map()

    def _load_collections_map(self) -> dict[str, bool]:
        """Alias interno mantenido para compatibilidad con llamadas existentes."""

        return self.load_collections_map()

    @staticmethod
    def _is_collected(cobros: dict[str, bool], invoice_id: int, ts: str, nro: str) -> bool:
        """Valida una factura contra clave nueva y clave legacy de cobro."""

        key_new = f"std:{int(invoice_id)}:{str(ts or '').strip()}:{str(nro or '').strip()}"
        key_old = f"std:{int(invoice_id)}"
        if key_new in cobros:
            return bool(cobros.get(key_new, False))
        if key_old in cobros:
            return bool(cobros.get(key_old, False))
        return False

    @staticmethod
    def _total_con_retencion(total: float, iva5: float, iva10: float) -> float:
        """Aplica la retencion usada por el dashboard sobre IVA 5/10."""

        return float(total or 0.0) - 0.3 * (float(iva5 or 0.0) + float(iva10 or 0.0))

    def pending_collection_total(
        self,
        search: str = "",
        from_date: str = "",
        to_date: str = "",
    ) -> tuple[float, int]:
        """Calcula total y cantidad de facturas de venta aun no cobradas."""

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
        """Lista facturas de venta pendientes para alimentar la tabla de cobros."""

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
        """Lista OC pendientes y calcula prioridad/disponibilidad de entrega."""

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
        """Lista compras pendientes de pago desde raw_lots y flags del dashboard."""

        # Misma fuente que "Resumen de compras": raw_lots + flags de pago en PostgreSQL.
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
        """Cuenta OC pendientes nuevas en los ultimos 7 y 30 dias."""

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


class DashboardRefreshWorker(QtCore.QObject):
    """Worker Qt que refresca los datos sin congelar la ventana.

    Vive en un QThread y comunica el resultado con signals. Por eso este modulo
    importa QtCore, pero no toca widgets ni clases visuales.
    """

    finished = QtCore.Signal(int, object)
    failed = QtCore.Signal(int, str)

    def __init__(self, request_id: int, repo: DashboardRepo, filters: dict):
        """Guarda el pedido de refresco y una copia de los filtros activos."""

        super().__init__()
        self.request_id = int(request_id)
        self.repo = repo
        self.filters = dict(filters)

    @QtCore.Slot()
    def run(self) -> None:
        """Ejecuta todas las consultas del dashboard y emite finished/failed."""

        try:
            # No tocar widgets aca: este objeto vive en un QThread.
            suc = self.filters.get("sucursal", "")
            srch = self.filters.get("search", "")
            d1 = self.filters.get("from_date", "")
            d2 = self.filters.get("to_date", "")
            orders = self.repo.pending_orders(sucursal=suc, search=srch, from_date=d1, to_date=d2)
            payments = self.repo.pending_payments(sucursal=suc, search=srch, from_date="", to_date="")
            cobro_pend, cobro_count = self.repo.pending_collection_total(search=srch, from_date=d1, to_date=d2)
            collections = self.repo.pending_collections(search=srch, from_date=d1, to_date=d2)
            t7, t30 = self.repo.trend_data()
            self.finished.emit(
                self.request_id,
                {
                    "orders": orders,
                    "payments": payments,
                    "cobro_pend": cobro_pend,
                    "cobro_count": cobro_count,
                    "collections": collections,
                    "trend_7": t7,
                    "trend_30": t30,
                },
            )
        except Exception as exc:
            self.failed.emit(self.request_id, str(exc))


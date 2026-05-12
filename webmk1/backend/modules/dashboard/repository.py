from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime, timedelta
from typing import Any

from core.database import connection
from modules.dashboard.schemas import (
    CollectionRow,
    DashboardSummary,
    Kpi,
    OrderRow,
    OrderDeliveryResponse,
    PaymentCheckOption,
    PaymentCheckStatus,
    PaymentDetailRow,
    PaymentReceiptUpdateRequest,
    PaymentReceiptUpdateResponse,
    PaymentRegisterRequest,
    PaymentRegisterResponse,
    PaymentRow,
)


DELIVERY_SLA_DAYS = 2
CREDIT_TERM_DAYS = 15


class DashboardRepository:
    def _ensure_state_tables(self) -> None:
        with connection("fraccionadora") as cn:
            cn.execute(
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
            cn.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_collection_flags(
                    status_key TEXT PRIMARY KEY,
                    invoice_id INTEGER NOT NULL,
                    invoice_ts TEXT,
                    invoice_no TEXT,
                    collected INTEGER NOT NULL DEFAULT 0,
                    updated_ts TEXT
                );
                """
            )

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
            if norm_name and f" {norm_name} " in norm_desc:
                return int(pid), str(name)
            for alias in aliases.get(norm_name, ()):
                if alias in norm_desc:
                    return int(pid), str(name)
        return None

    @staticmethod
    def _parse_iso(value: str) -> date | None:
        try:
            return datetime.strptime((value or "").strip()[:10], "%Y-%m-%d").date()
        except Exception:
            return None

    @staticmethod
    def _safe_int(value: Any) -> int:
        try:
            return int(value)
        except Exception:
            return 0

    @staticmethod
    def _fmt_gs(value: float) -> str:
        return f"{float(value or 0):,.0f}".replace(",", ".")

    @staticmethod
    def _sucursal_from_proveedor(proveedor: str) -> str:
        p = (proveedor or "").strip().lower()
        if "areg" in p:
            return "Aregua"
        if "luque" in p:
            return "Luque"
        if "ita" in p:
            return "Itaugua"
        return ""

    @staticmethod
    def _total_con_retencion(total: float, iva5: float, iva10: float) -> float:
        return float(total or 0.0) - 0.3 * (float(iva5 or 0.0) + float(iva10 or 0.0))

    @staticmethod
    def _is_collected(cobros: dict[str, bool], invoice_id: int, ts: str, nro: str) -> bool:
        key_new = f"std:{int(invoice_id)}:{str(ts or '').strip()}:{str(nro or '').strip()}"
        key_old = f"std:{int(invoice_id)}"
        return bool(cobros.get(key_new, cobros.get(key_old, False)))

    def list_sucursales(self) -> list[str]:
        out: set[str] = set()
        with connection("pedidos") as cn:
            rows = cn.execute(
                "SELECT DISTINCT COALESCE(sucursal,'') AS sucursal FROM orden_compra WHERE TRIM(COALESCE(sucursal,''))<>''"
            ).fetchall()
            out.update(str(r["sucursal"]) for r in rows if r["sucursal"])
        with connection("facturas") as cn:
            rows = cn.execute(
                "SELECT DISTINCT COALESCE(sucursal,'') AS sucursal FROM factura WHERE TRIM(COALESCE(sucursal,''))<>''"
            ).fetchall()
            out.update(str(r["sucursal"]) for r in rows if r["sucursal"])
        with connection("fraccionadora") as cn:
            rows = cn.execute(
                "SELECT DISTINCT COALESCE(proveedor,'') AS proveedor FROM raw_lots WHERE TRIM(COALESCE(proveedor,''))<>''"
            ).fetchall()
            for r in rows:
                suc = self._sucursal_from_proveedor(str(r["proveedor"] or ""))
                if suc:
                    out.add(suc)
        return sorted(out)

    def _load_paid_map(self) -> dict[int, bool]:
        self._ensure_state_tables()
        with connection("fraccionadora") as cn:
            rows = cn.execute("SELECT lot_id, paid FROM dashboard_payment_flags;").fetchall()
        return {int(r["lot_id"]): bool(r["paid"]) for r in rows}

    def load_available_payment_checks(self) -> list[PaymentCheckOption]:
        self._ensure_state_tables()
        with connection("fraccionadora") as cn:
            rows = cn.execute(
                """
                SELECT c.chequera_id, c.bank_id, b.banco_nombre, b.nro_cuenta,
                       c.formato_chequera, c.tipo_cheque, c.serie, c.nro_inicio, c.nro_fin
                FROM bank_checkbooks c
                JOIN banks b ON b.bank_id = c.bank_id
                ORDER BY b.banco_nombre, b.nro_cuenta, c.nro_inicio, c.nro_fin
                """
            ).fetchall()
            used_rows = cn.execute(
                """
                SELECT chequera_id, cheque_no, UPPER(TRIM(COALESCE(serie, ''))) AS serie
                FROM dashboard_used_checks
                """
            ).fetchall()
        used = {
            (str(r["chequera_id"] or "").strip(), str(r["cheque_no"] or "").strip(), str(r["serie"] or "").strip().upper())
            for r in used_rows
        }
        out: list[PaymentCheckOption] = []
        for row in rows:
            chequera_id = str(row["chequera_id"] or "").strip()
            serie = str(row["serie"] or "").strip().upper()
            start = int(row["nro_inicio"] or 0)
            end = int(row["nro_fin"] or 0)
            if end < start:
                continue
            bank_name = str(row["banco_nombre"] or "").strip()
            account = str(row["nro_cuenta"] or "").strip()
            form_type = str(row["formato_chequera"] or "").strip()
            check_type = str(row["tipo_cheque"] or "").strip()
            for cheque_no in range(start, end + 1):
                cheque_txt = str(cheque_no)
                if (chequera_id, cheque_txt, serie) in used:
                    continue
                ref = f"Cheque: {cheque_txt} | Serie: {serie or '-'}"
                out.append(
                    PaymentCheckOption(
                        cheque_no=cheque_txt,
                        chequera_id=chequera_id,
                        bank_id=str(row["bank_id"] or "").strip(),
                        bank_name=bank_name,
                        account_no=account,
                        form_type=form_type,
                        check_type=check_type,
                        serie=serie,
                        reference_value=ref,
                        group_label=f"{bank_name} / {form_type} / {check_type} / Serie {serie or '-'}",
                        label=f"{cheque_txt} | {bank_name} | Cta {account} | {form_type} | {check_type} | Serie {serie or '-'}",
                    )
                )
        return out

    def payment_check_status(self, serie: str, cheque_no: str) -> PaymentCheckStatus:
        self._ensure_state_tables()
        serie_txt = str(serie or "").strip().upper()
        cheque_txt = re.sub(r"\D+", "", str(cheque_no or ""))
        if not serie_txt or not cheque_txt:
            return PaymentCheckStatus(available=False, found=False, used=False, message="Ingrese serie y numero de cheque.")
        with connection("fraccionadora") as cn:
            row = cn.execute(
                """
                SELECT c.chequera_id, c.bank_id, b.banco_nombre, b.nro_cuenta,
                       c.formato_chequera, c.tipo_cheque, c.serie, c.nro_inicio, c.nro_fin
                FROM bank_checkbooks c
                JOIN banks b ON b.bank_id = c.bank_id
                WHERE UPPER(TRIM(COALESCE(c.serie, ''))) = %s
                  AND %s BETWEEN c.nro_inicio AND c.nro_fin
                ORDER BY c.nro_inicio, c.nro_fin
                LIMIT 1
                """,
                (serie_txt, int(cheque_txt)),
            ).fetchone()
            if not row:
                return PaymentCheckStatus(available=False, found=False, used=False, message="Cheque no cargado en chequeras.")
            used = cn.execute(
                """
                SELECT 1 FROM dashboard_used_checks
                WHERE chequera_id = %s AND cheque_no = %s AND UPPER(TRIM(COALESCE(serie, ''))) = %s
                LIMIT 1
                """,
                (str(row["chequera_id"] or "").strip(), cheque_txt, serie_txt),
            ).fetchone()
        check = PaymentCheckOption(
            cheque_no=cheque_txt,
            chequera_id=str(row["chequera_id"] or "").strip(),
            bank_id=str(row["bank_id"] or "").strip(),
            bank_name=str(row["banco_nombre"] or "").strip(),
            account_no=str(row["nro_cuenta"] or "").strip(),
            form_type=str(row["formato_chequera"] or "").strip(),
            check_type=str(row["tipo_cheque"] or "").strip(),
            serie=str(row["serie"] or "").strip().upper(),
            reference_value=f"Cheque: {cheque_txt} | Serie: {serie_txt}",
            group_label=f"{row['banco_nombre']} / {row['formato_chequera']} / {row['tipo_cheque']} / Serie {serie_txt}",
            label=f"{cheque_txt} | {row['banco_nombre']} | Cta {row['nro_cuenta']} | {row['formato_chequera']} | {row['tipo_cheque']} | Serie {serie_txt}",
        )
        if used:
            return PaymentCheckStatus(available=False, found=True, used=True, message="Cheque cargado, pero ya usado.", check=check)
        return PaymentCheckStatus(available=True, found=True, used=False, message="Cheque cargado y disponible.", check=check)

    def register_payment(self, payload: PaymentRegisterRequest) -> PaymentRegisterResponse:
        self._ensure_state_tables()
        lot_ids = [int(x) for x in payload.lot_ids if int(x or 0) > 0]
        if not lot_ids:
            raise ValueError("Seleccione una o mas facturas pendientes.")

        medio = (payload.medio or "Efectivo").strip()
        medio_norm = medio.lower()
        referencia = (payload.referencia or "").strip()
        cheque_data: dict[str, str] | None = None

        with connection("fraccionadora") as cn:
            placeholders = ",".join(["%s"] * len(lot_ids))
            rows = cn.execute(
                f"""
                SELECT id, COALESCE(factura, '') AS factura, COALESCE(proveedor, '') AS proveedor,
                       COALESCE(costo_total_gs, 0) AS monto
                FROM raw_lots
                WHERE id IN ({placeholders})
                """,
                lot_ids,
            ).fetchall()
            if len(rows) != len(set(lot_ids)):
                raise ValueError("Una o mas facturas ya no existen.")
            paid_rows = cn.execute(
                f"SELECT lot_id FROM dashboard_payment_flags WHERE paid = 1 AND lot_id IN ({placeholders})",
                lot_ids,
            ).fetchall()
            if paid_rows:
                raise ValueError("Una o mas facturas ya fueron marcadas como pagadas.")

            if medio_norm == "cheque":
                cheque_no = re.sub(r"\D+", "", payload.cheque_no or referencia)
                serie = (payload.serie or "").strip().upper()
                if not payload.chequera_id or not cheque_no or not serie:
                    raise ValueError("Seleccione un cheque cargado.")
                check = cn.execute(
                    """
                    SELECT c.chequera_id, c.bank_id, b.banco_nombre, b.nro_cuenta,
                           c.formato_chequera, c.tipo_cheque, c.serie, c.nro_inicio, c.nro_fin
                    FROM bank_checkbooks c
                    JOIN banks b ON b.bank_id = c.bank_id
                    WHERE c.chequera_id = %s
                      AND UPPER(TRIM(COALESCE(c.serie, ''))) = %s
                      AND %s BETWEEN c.nro_inicio AND c.nro_fin
                    LIMIT 1
                    """,
                    (payload.chequera_id.strip(), serie, int(cheque_no)),
                ).fetchone()
                if not check:
                    raise ValueError("El cheque seleccionado no esta cargado en chequeras.")
                used = cn.execute(
                    """
                    SELECT 1 FROM dashboard_used_checks
                    WHERE chequera_id = %s AND cheque_no = %s AND UPPER(TRIM(COALESCE(serie, ''))) = %s
                    LIMIT 1
                    """,
                    (payload.chequera_id.strip(), cheque_no, serie),
                ).fetchone()
                if used:
                    raise ValueError("Ese cheque ya esta marcado como usado.")
                referencia = f"Cheque: {cheque_no} | Serie: {serie}"
                cheque_data = {"chequera_id": payload.chequera_id.strip(), "cheque_no": cheque_no, "serie": serie}
            elif medio_norm == "transferencia" and not referencia:
                raise ValueError("Ingrese el numero de comprobante de transferencia.")

            now = datetime.now().isoformat(timespec="seconds")
            total = sum(float(r["monto"] or 0) for r in rows)
            facturas = [str(r["factura"] or f"ID {r['id']}") for r in rows]
            group_id = f"paygrp:{now}:{medio_norm}:{referencia or 'sin-ref'}"
            facturas_json = __import__("json").dumps(facturas, ensure_ascii=False)
            for row in rows:
                lot_id = int(row["id"])
                cn.execute(
                    """
                    INSERT INTO dashboard_payment_flags(lot_id, paid, updated_ts)
                    VALUES(%s, 1, CURRENT_TIMESTAMP)
                    ON CONFLICT(lot_id) DO UPDATE SET paid=1, updated_ts=CURRENT_TIMESTAMP
                    """,
                    (lot_id,),
                )
                cn.execute(
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
                        ts_modificacion=excluded.ts_modificacion
                    """,
                    (
                        group_id,
                        lot_id,
                        str(row["proveedor"] or ""),
                        str(row["factura"] or ""),
                        float(row["monto"] or 0),
                        payload.fecha_pago,
                        medio_norm,
                        referencia,
                        payload.nro_deposito.strip(),
                        payload.nro_recibo_dinero.strip(),
                        payload.observacion.strip(),
                        facturas_json,
                        total,
                        now,
                        now,
                    ),
                )
            if cheque_data:
                cn.execute(
                    """
                    INSERT INTO dashboard_used_checks(chequera_id, cheque_no, serie, referencia, payment_group_id, used_ts)
                    VALUES(%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT(chequera_id, cheque_no, serie) DO UPDATE SET
                        referencia=excluded.referencia,
                        payment_group_id=excluded.payment_group_id,
                        used_ts=CURRENT_TIMESTAMP
                    """,
                    (cheque_data["chequera_id"], cheque_data["cheque_no"], cheque_data["serie"], referencia, group_id),
                )

        return PaymentRegisterResponse(
            payment_group_id=group_id,
            facturas=len(rows),
            total_gs=total,
            medio=medio_norm,
            referencia=referencia,
        )

    def payment_details(self) -> list[PaymentDetailRow]:
        self._ensure_state_tables()
        with connection("fraccionadora") as cn:
            rows = cn.execute(
                """
                SELECT id, COALESCE(payment_group_id, '') AS payment_group_id,
                       COALESCE(lot_id, 0) AS lot_id,
                       COALESCE(proveedor, '') AS proveedor,
                       COALESCE(factura, '') AS factura,
                       COALESCE(monto_gs, 0) AS monto_gs,
                       COALESCE(fecha_pago, '') AS fecha_pago,
                       COALESCE(medio, '') AS medio,
                       COALESCE(referencia, '') AS referencia,
                       COALESCE(nro_recibo_dinero, '') AS nro_recibo_dinero,
                       COALESCE(observacion, '') AS observacion,
                       COALESCE(total_grupo_gs, 0) AS total_grupo_gs,
                       COALESCE(ts_registro, '') AS ts_registro
                FROM dashboard_payment_details
                ORDER BY fecha_pago DESC, ts_registro DESC, id DESC
                LIMIT 500
                """
            ).fetchall()
        return [PaymentDetailRow(**dict(r)) for r in rows]

    def update_payment_receipt(self, payload: PaymentReceiptUpdateRequest) -> PaymentReceiptUpdateResponse:
        detail_ids = [int(x) for x in payload.detail_ids if int(x or 0) > 0]
        recibo = str(payload.nro_recibo_dinero or "").strip()
        if not detail_ids:
            raise ValueError("Seleccione uno o mas pagos.")
        if not recibo:
            raise ValueError("Ingrese numero de recibo.")
        with connection("fraccionadora") as cn:
            placeholders = ",".join(["%s"] * len(detail_ids))
            cn.execute(
                f"""
                UPDATE dashboard_payment_details
                SET nro_recibo_dinero = %s, ts_modificacion = %s
                WHERE id IN ({placeholders})
                """,
                [recibo, datetime.now().isoformat(timespec="seconds"), *detail_ids],
            )
        return PaymentReceiptUpdateResponse(updated=len(detail_ids), nro_recibo_dinero=recibo)

    def _load_collections_map(self) -> dict[str, bool]:
        self._ensure_state_tables()
        with connection("fraccionadora") as cn:
            rows = cn.execute(
                "SELECT status_key, invoice_id, invoice_ts, invoice_no, collected FROM dashboard_collection_flags;"
            ).fetchall()
        out: dict[str, bool] = {}
        for r in rows:
            status = bool(r["collected"])
            if r["status_key"]:
                out[str(r["status_key"])] = status
            inv_id = int(r["invoice_id"] or 0)
            out[f"std:{inv_id}:{str(r['invoice_ts'] or '').strip()}:{str(r['invoice_no'] or '').strip()}"] = status
            out[f"std:{inv_id}"] = status
        return out

    def _build_fracc_stock_cache(self) -> tuple[list[tuple[int, str]], dict[tuple[int, int], int]]:
        with connection("fraccionadora") as cn:
            products_rows = cn.execute("SELECT id, name FROM products;").fetchall()
            stock_rows = cn.execute("SELECT product_id, gramaje, paquetes FROM package_stock;").fetchall()
        products = [(int(r["id"]), str(r["name"] or "")) for r in products_rows]
        stock_map = {
            (int(r["product_id"]), int(r["gramaje"])): int(r["paquetes"] or 0)
            for r in stock_rows
        }
        return products, stock_map

    def _order_ready_percentage(
        self,
        oc_id: int,
        products: list[tuple[int, str]],
        stock_map: dict[tuple[int, int], int],
    ) -> float:
        with connection("pedidos") as cn:
            rows = cn.execute(
                """
                SELECT descripcion, cantidad
                FROM orden_item
                WHERE oc_id = %s
                  AND COALESCE(enviado, 0) = 0
                  AND descripcion IS NOT NULL;
                """,
                (int(oc_id),),
            ).fetchall()
        if not rows:
            return 100.0
        ready = 0
        for row in rows:
            gram = self._extract_gramaje(str(row["descripcion"] or ""))
            match = self._match_product_name(str(row["descripcion"] or ""), products)
            if gram is None or match is None:
                continue
            pid, _name = match
            if int(stock_map.get((pid, int(gram)), 0)) >= self._safe_int(row["cantidad"]):
                ready += 1
        return (ready / len(rows)) * 100.0

    def pending_orders(self, sucursal: str, search: str, from_date: str, to_date: str) -> list[OrderRow]:
        products, stock_map = self._build_fracc_stock_cache()
        with connection("pedidos") as cn:
            rows = cn.execute(
                """
                SELECT oc.id, COALESCE(oc.nro_oc, '') AS nro_oc, COALESCE(oc.sucursal, '') AS sucursal,
                       COALESCE(CAST(oc.fecha_pedido AS TEXT), '') AS fecha_pedido,
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
                GROUP BY oc.id, oc.nro_oc, oc.sucursal, oc.fecha_pedido, oc.monto_total
                HAVING COALESCE(SUM(CASE WHEN COALESCE(oi.enviado,0)=0 THEN oi.cantidad ELSE 0 END), 0) > 0
                ORDER BY oc.fecha_pedido ASC NULLS LAST, oc.id DESC;
                """,
                (
                    sucursal.strip(),
                    sucursal.strip(),
                    f"%{search.strip()}%" if search.strip() else "",
                    f"%{search.strip()}%" if search.strip() else "",
                    from_date.strip(),
                    from_date.strip(),
                    to_date.strip(),
                    to_date.strip(),
                ),
            ).fetchall()
        today = date.today()
        out: list[OrderRow] = []
        for r in rows:
            pedido_date = self._parse_iso(str(r["fecha_pedido"] or ""))
            compromiso = pedido_date + timedelta(days=DELIVERY_SLA_DAYS) if pedido_date else None
            dias = (today - compromiso).days if compromiso else None
            prioridad = "Media" if dias is None else ("Alta" if dias > 0 else "Media" if dias >= -1 else "Baja")
            out.append(
                OrderRow(
                    oc_id=self._safe_int(r["id"]),
                    numero=str(r["nro_oc"] or ""),
                    sucursal=str(r["sucursal"] or ""),
                    fecha_pedido=str(r["fecha_pedido"] or "")[:10],
                    fecha_compromiso=compromiso.isoformat() if compromiso else "",
                    dias_atraso=dias,
                    estado="Parcial" if float(r["cant_env"] or 0) > 0 else "Pendiente",
                    prioridad=prioridad,
                    pct_listo_entrega=self._order_ready_percentage(self._safe_int(r["id"]), products, stock_map),
                    monto_total=float(r["monto_total"] or 0),
                )
            )
        return out

    def mark_order_delivered(self, oc_id: int) -> OrderDeliveryResponse:
        order_id = int(oc_id or 0)
        if order_id <= 0:
            raise ValueError("Pedido invalido.")
        with connection("pedidos") as cn:
            row = cn.execute("SELECT id FROM orden_compra WHERE id = %s", (order_id,)).fetchone()
            if not row:
                raise ValueError("Pedido no encontrado.")
            cn.execute("UPDATE orden_item SET enviado = 1 WHERE oc_id = %s", (order_id,))
            cn.execute("UPDATE orden_compra SET completada = 1 WHERE id = %s", (order_id,))
        return OrderDeliveryResponse(oc_id=order_id, updated=True, message="Pedido marcado como entregado.")

    def pending_payments(self, sucursal: str, search: str, from_date: str, to_date: str) -> list[PaymentRow]:
        paid = self._load_paid_map()
        like_search = f"%{search.strip()}%" if search.strip() else ""
        with connection("fraccionadora") as cn:
            rows = cn.execute(
                """
                SELECT id, COALESCE(CAST(ts AS TEXT),'') AS ts, COALESCE(factura,'') AS factura,
                       COALESCE(proveedor,'') AS proveedor, COALESCE(costo_total_gs,0) AS costo_total_gs
                FROM raw_lots
                WHERE (%s = '' OR ts::date >= CAST(NULLIF(%s, '') AS date))
                  AND (%s = '' OR ts::date <= CAST(NULLIF(%s, '') AS date))
                  AND (%s = '' OR COALESCE(factura,'') LIKE %s OR COALESCE(proveedor,'') LIKE %s)
                ORDER BY ts DESC, id DESC;
                """,
                (from_date.strip(), from_date.strip(), to_date.strip(), to_date.strip(), like_search, like_search, like_search),
            ).fetchall()
        today = date.today()
        out: list[PaymentRow] = []
        for r in rows:
            lot_id = self._safe_int(r["id"])
            suc = self._sucursal_from_proveedor(str(r["proveedor"] or ""))
            if sucursal.strip() and suc.upper() != sucursal.strip().upper():
                continue
            if bool(paid.get(lot_id, False)):
                continue
            fecha = str(r["ts"] or "")[:10]
            fecha_date = self._parse_iso(fecha)
            due = fecha_date + timedelta(days=CREDIT_TERM_DAYS) if fecha_date else None
            dias = (due - today).days if due else None
            out.append(
                PaymentRow(
                    factura_id=lot_id,
                    proveedor=str(r["proveedor"] or ""),
                    numero_doc=str(r["factura"] or ""),
                    fecha_emision=fecha,
                    vencimiento=due.isoformat() if due else "",
                    dias_para_vencer=dias,
                    monto=float(r["costo_total_gs"] or 0),
                    estado="Vencido" if dias is not None and dias < 0 else "Pendiente",
                    sucursal=suc,
                )
            )
        return out

    def pending_collections(self, search: str, from_date: str, to_date: str, limit: int = 200) -> list[CollectionRow]:
        cobros = self._load_collections_map()
        like_search = f"%{search.strip()}%" if search.strip() else ""
        with connection("fraccionadora") as cn:
            rows = cn.execute(
                """
                SELECT id, COALESCE(CAST(ts AS TEXT),'') AS ts, COALESCE(invoice_no,'') AS invoice_no,
                       COALESCE(customer,'') AS customer, COALESCE(gravada5_gs,0) AS gravada5_gs,
                       COALESCE(iva5_gs,0) AS iva5_gs, COALESCE(gravada10_gs,0) AS gravada10_gs,
                       COALESCE(iva10_gs,0) AS iva10_gs, COALESCE(total_gs,0) AS total_gs
                FROM sales_invoices
                WHERE (%s = '' OR COALESCE(invoice_no,'') LIKE %s OR COALESCE(customer,'') LIKE %s)
                  AND (%s = '' OR ts::date >= CAST(NULLIF(%s, '') AS date))
                  AND (%s = '' OR ts::date <= CAST(NULLIF(%s, '') AS date))
                ORDER BY ts DESC, id DESC;
                """,
                (like_search, like_search, like_search, from_date.strip(), from_date.strip(), to_date.strip(), to_date.strip()),
            ).fetchall()
        today = date.today()
        out: list[CollectionRow] = []
        for r in rows:
            inv_id = self._safe_int(r["id"])
            ts = str(r["ts"] or "")
            nro = str(r["invoice_no"] or "")
            if inv_id <= 0 or self._is_collected(cobros, inv_id, ts, nro):
                continue
            fecha = self._parse_iso(ts[:10])
            dias_sin = (today - fecha).days if fecha else None
            dias_para = CREDIT_TERM_DAYS - dias_sin if dias_sin is not None else None
            total = float(r["total_gs"] or 0.0)
            iva5 = float(r["iva5_gs"] or 0.0)
            iva10 = float(r["iva10_gs"] or 0.0)
            out.append(
                CollectionRow(
                    invoice_id=inv_id,
                    ts=ts[:10],
                    invoice_no=nro,
                    customer=str(r["customer"] or ""),
                    gravada5_gs=float(r["gravada5_gs"] or 0.0),
                    iva5_gs=iva5,
                    gravada10_gs=float(r["gravada10_gs"] or 0.0),
                    iva10_gs=iva10,
                    total_gs=total,
                    total_con_retencion=self._total_con_retencion(total, iva5, iva10),
                    dias_sin_cobrar=dias_sin,
                    dias_para_cobro=dias_para,
                )
            )
            if len(out) >= limit:
                break
        return out

    def trend_data(self) -> tuple[int, int]:
        with connection("pedidos") as cn:
            row7 = cn.execute(
                """
                SELECT COUNT(*) AS c FROM orden_compra
                WHERE COALESCE(completada,0)=0 AND fecha_pedido::date >= CAST(%s AS date)
                """,
                ((date.today() - timedelta(days=7)).isoformat(),),
            ).fetchone()
            row30 = cn.execute(
                """
                SELECT COUNT(*) AS c FROM orden_compra
                WHERE COALESCE(completada,0)=0 AND fecha_pedido::date >= CAST(%s AS date)
                """,
                ((date.today() - timedelta(days=30)).isoformat(),),
            ).fetchone()
        return int(row7["c"] if row7 else 0), int(row30["c"] if row30 else 0)

    def summary(self, sucursal: str = "", search: str = "", from_date: str = "", to_date: str = "") -> DashboardSummary:
        orders = self.pending_orders(sucursal, search, from_date, to_date)
        payments = self.pending_payments(sucursal, search, "", "")
        collections = self.pending_collections(search, from_date, to_date)
        trend_7, trend_30 = self.trend_data()
        pedidos_vencidos = sum(1 for r in orders if r.dias_atraso is not None and r.dias_atraso > 0)
        pagos_pendientes = sum(float(r.monto or 0.0) for r in payments)
        cobro_pendiente = sum(float(r.total_con_retencion or 0.0) for r in collections)
        return DashboardSummary(
            kpis=[
                Kpi(title="Pedidos pendientes", value=str(len(orders)), subtitle="OC"),
                Kpi(title="Pedidos vencidos", value=str(pedidos_vencidos), subtitle="con atraso"),
                Kpi(title="Pagos pendientes", value=self._fmt_gs(pagos_pendientes), subtitle="Gs"),
                Kpi(title="Cobro pendiente", value=self._fmt_gs(cobro_pendiente), subtitle=f"{len(collections)} facturas"),
            ],
            orders=orders,
            payments=payments,
            collections=collections,
            trend_7=trend_7,
            trend_30=trend_30,
            updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

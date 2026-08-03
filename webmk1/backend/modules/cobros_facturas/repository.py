from __future__ import annotations

import sys
import tempfile
from datetime import date
from pathlib import Path

from fastapi import UploadFile
from core.database import connection
from modules.cobros_facturas.schemas import (
    CobroFacturaCreate,
    CobroFacturaItemRow,
    CobroFacturaRow,
    CobrosSummary,
    FacturaPendienteRow,
    ReciboFacturaItem,
    ReciboPreview,
)

ROOT_DIR = Path(__file__).resolve().parents[4]
FACTURAS_DIR = ROOT_DIR / "importadorfactur"
if str(FACTURAS_DIR) not in sys.path:
    sys.path.insert(0, str(FACTURAS_DIR))

from recibo_parser import parse_receipt  # noqa: E402


class CobrosFacturasRepository:
    def _ensure_schema(self, cn) -> None:
        cn.execute(
            """
            CREATE TABLE IF NOT EXISTS dashboard_collection_flags(
                status_key TEXT PRIMARY KEY,
                invoice_id INTEGER NOT NULL,
                invoice_ts TEXT,
                invoice_no TEXT,
                collected INTEGER NOT NULL DEFAULT 0,
                updated_ts TEXT
            )
            """
        )
        cn.execute(
            """
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
            )
            """
        )
        cn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_dash_collection_detail_invoice
            ON dashboard_collection_details(invoice_id, invoice_ts, invoice_no)
            """
        )
        cn.execute(
            """
            CREATE TABLE IF NOT EXISTS invoice_collections(
                id BIGSERIAL PRIMARY KEY,
                fecha_cobro DATE NOT NULL DEFAULT CURRENT_DATE,
                cheque_no TEXT NOT NULL,
                boleta_deposito TEXT NOT NULL,
                banco TEXT NOT NULL DEFAULT '',
                observacion TEXT NOT NULL DEFAULT '',
                total_gs NUMERIC NOT NULL DEFAULT 0,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cn.execute(
            """
            CREATE TABLE IF NOT EXISTS invoice_collection_items(
                id BIGSERIAL PRIMARY KEY,
                collection_id BIGINT NOT NULL REFERENCES invoice_collections(id) ON DELETE CASCADE,
                invoice_id BIGINT NOT NULL REFERENCES sales_invoices(id) ON DELETE CASCADE,
                monto_gs NUMERIC NOT NULL,
                UNIQUE(collection_id, invoice_id)
            )
            """
        )
        cn.execute(
            """
            CREATE TABLE IF NOT EXISTS bag_collection_items(
                id BIGSERIAL PRIMARY KEY,
                collection_id BIGINT NOT NULL REFERENCES invoice_collections(id) ON DELETE CASCADE,
                bag_sale_id BIGINT NOT NULL REFERENCES bag_sales(id) ON DELETE CASCADE,
                monto_gs NUMERIC NOT NULL,
                UNIQUE(collection_id, bag_sale_id)
            )
            """
        )
        cn.execute("CREATE INDEX IF NOT EXISTS idx_invoice_collection_items_invoice ON invoice_collection_items(invoice_id)")
        cn.execute("CREATE INDEX IF NOT EXISTS idx_bag_collection_items_sale ON bag_collection_items(bag_sale_id)")
        self._migrate_legacy_collections(cn)

    def summary(self, from_date: str = "", to_date: str = "") -> CobrosSummary:
        with connection("fraccionadora") as cn:
            self._ensure_schema(cn)
            return CobrosSummary(pendientes=self._pendientes(cn, from_date, to_date), cobros=self._cobros(cn))

    def create(self, payload: CobroFacturaCreate) -> CobroFacturaRow:
        fecha = (payload.fecha_cobro or "").strip() or date.today().isoformat()
        invoice_ids = self._normalize_invoice_ids(payload.items)
        with connection("fraccionadora") as cn:
            self._ensure_schema(cn)
            items = self._full_collection_items(cn, invoice_ids)
            total = sum(monto for _, _, monto in items)
            row = cn.execute(
                """
                INSERT INTO invoice_collections(
                    fecha_cobro, cheque_no, boleta_deposito, banco, observacion, total_gs
                )
                VALUES(%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    fecha,
                    payload.cheque_no.strip(),
                    payload.boleta_deposito.strip(),
                    payload.banco.strip(),
                    payload.observacion.strip(),
                    total,
                ),
            ).fetchone()
            collection_id = int(row["id"])
            self._insert_items(cn, collection_id, items)
            cobro = self._cobro(cn, collection_id)
            self._sync_legacy_collection(cn, cobro)
            return cobro

    def parse_recibo_pdf(self, upload: UploadFile) -> ReciboPreview:
        filename = upload.filename or ""
        if not filename.lower().endswith(".pdf"):
            raise ValueError("Seleccione un archivo PDF.")

        tmp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                tmp.write(upload.file.read())
                tmp_path = Path(tmp.name)
            result = parse_receipt(tmp_path)
        finally:
            upload.file.close()
            if tmp_path and tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

        meta = result.get("meta") or {}
        items = [
            ReciboFacturaItem(
                linea=int(item.get("linea") or idx),
                factura=item.get("factura") or "",
                monto=item.get("monto"),
            )
            for idx, item in enumerate(result.get("items") or [], start=1)
        ]
        if not meta.get("numero"):
            raise ValueError("No se pudo detectar el numero de recibo.")
        if not items:
            raise ValueError("No se detectaron facturas en el recibo.")

        return ReciboPreview(
            numero=meta.get("numero") or "",
            ruc_emisor=meta.get("ruc_emisor") or "",
            cliente=meta.get("cliente") or "",
            ruc_cliente=meta.get("ruc_cliente") or "",
            fecha=meta.get("fecha") or "",
            cheque_no=meta.get("cheque_no") or "",
            banco=meta.get("banco") or "",
            total=float(meta.get("total") or 0),
            items=items,
        )

    def update(self, collection_id: int, payload: CobroFacturaCreate) -> CobroFacturaRow:
        fecha = (payload.fecha_cobro or "").strip() or date.today().isoformat()
        invoice_ids = self._normalize_invoice_ids(payload.items)
        with connection("fraccionadora") as cn:
            self._ensure_schema(cn)
            exists = cn.execute("SELECT id FROM invoice_collections WHERE id = %s", (collection_id,)).fetchone()
            if not exists:
                raise ValueError("Cobro no encontrado.")
            old_items = cn.execute(
                "SELECT invoice_id FROM invoice_collection_items WHERE collection_id = %s",
                (collection_id,),
            ).fetchall()
            old_bag_items = cn.execute(
                "SELECT bag_sale_id FROM bag_collection_items WHERE collection_id = %s",
                (collection_id,),
            ).fetchall()
            items = self._full_collection_items(cn, invoice_ids)
            total = sum(monto for _, _, monto in items)
            cn.execute(
                """
                UPDATE invoice_collections
                   SET fecha_cobro = %s,
                       cheque_no = %s,
                       boleta_deposito = %s,
                       banco = %s,
                       observacion = %s,
                       total_gs = %s,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = %s
                """,
                (
                    fecha,
                    payload.cheque_no.strip(),
                    payload.boleta_deposito.strip(),
                    payload.banco.strip(),
                    payload.observacion.strip(),
                    total,
                    collection_id,
                ),
            )
            cn.execute("DELETE FROM invoice_collection_items WHERE collection_id = %s", (collection_id,))
            cn.execute("DELETE FROM bag_collection_items WHERE collection_id = %s", (collection_id,))
            self._insert_items(cn, collection_id, items)
            cobro = self._cobro(cn, collection_id)
            self._sync_legacy_collection(cn, cobro)
            current_ids = {(item.invoice_source, item.invoice_id) for item in cobro.items}
            for old in old_items:
                old_id = int(old["invoice_id"] or 0)
                if old_id and ("sales", old_id) not in current_ids:
                    self._mark_legacy_invoice(cn, old_id, False)
            for old in old_bag_items:
                old_id = int(old["bag_sale_id"] or 0)
                if old_id and ("bag", old_id) not in current_ids:
                    self._mark_legacy_bag(cn, old_id, False)
            return cobro

    def _normalize_invoice_ids(self, raw_items) -> list[int]:
        invoice_ids: list[tuple[str, int]] = []
        for item in raw_items:
            invoice_id = int(item.invoice_id)
            source = str(getattr(item, "invoice_source", "sales") or "sales").strip().lower()
            if source not in {"sales", "bag"}:
                source = "sales"
            key = (source, invoice_id)
            if invoice_id > 0 and key not in invoice_ids:
                invoice_ids.append(key)
        if not invoice_ids:
            raise ValueError("Seleccione al menos una factura.")
        return invoice_ids

    def _full_collection_items(self, cn, invoice_ids: list[tuple[str, int]]) -> list[tuple[str, int, float]]:
        sales_ids = [invoice_id for source, invoice_id in invoice_ids if source == "sales"]
        bag_ids = [invoice_id for source, invoice_id in invoice_ids if source == "bag"]
        amounts: dict[tuple[str, int], float] = {}
        if sales_ids:
            rows = cn.execute(
                """
                SELECT id,
                       COALESCE(total_gs, 0) - 0.30 * (COALESCE(iva5_gs, 0) + COALESCE(iva10_gs, 0)) AS total_ret_gs
                FROM sales_invoices
                WHERE id = ANY(%s)
                """,
                (sales_ids,),
            ).fetchall()
            amounts.update({("sales", int(r["id"])): float(r["total_ret_gs"] or 0) for r in rows})
        if bag_ids:
            rows = cn.execute(
                """
                SELECT id, COALESCE(total_gs, 0) AS total_gs
                FROM bag_sales
                WHERE id = ANY(%s)
                """,
                (bag_ids,),
            ).fetchall()
            amounts.update({("bag", int(r["id"])): float(r["total_gs"] or 0) for r in rows})
        missing = [f"{source}:{invoice_id}" for source, invoice_id in invoice_ids if (source, invoice_id) not in amounts]
        if missing:
            raise ValueError(f"Factura no encontrada: {', '.join(missing)}")
        items = [(source, invoice_id, amounts[(source, invoice_id)]) for source, invoice_id in invoice_ids]
        if any(monto <= 0 for _, _, monto in items):
            raise ValueError("Una o mas facturas no tienen importe cobrable.")
        return items

    def _insert_items(self, cn, collection_id: int, items: list[tuple[str, int, float]]) -> None:
        for source, invoice_id, monto in items:
            if source == "bag":
                cn.execute(
                    """
                    INSERT INTO bag_collection_items(collection_id, bag_sale_id, monto_gs)
                    VALUES(%s, %s, %s)
                    """,
                    (collection_id, invoice_id, monto),
                )
            else:
                cn.execute(
                    """
                    INSERT INTO invoice_collection_items(collection_id, invoice_id, monto_gs)
                    VALUES(%s, %s, %s)
                    """,
                    (collection_id, invoice_id, monto),
                )

    def _pendientes(self, cn, from_date: str = "", to_date: str = "") -> list[FacturaPendienteRow]:
        rows = cn.execute(
            """
            SELECT si.id AS invoice_id,
                   'sales' AS invoice_source,
                   CAST(si.ts AS TEXT) AS ts,
                   COALESCE(si.invoice_no, '') AS invoice_no,
                   COALESCE(si.customer, '') AS customer,
                   COALESCE(si.total_gs, 0) - 0.30 * (COALESCE(si.iva5_gs, 0) + COALESCE(si.iva10_gs, 0)) AS total_gs,
                   0 AS cobrado_gs,
                   COALESCE(si.total_gs, 0) - 0.30 * (COALESCE(si.iva5_gs, 0) + COALESCE(si.iva10_gs, 0)) AS saldo_gs
            FROM sales_invoices si
            WHERE (%s = '' OR si.ts::date >= CAST(NULLIF(%s, '') AS date))
              AND (%s = '' OR si.ts::date <= CAST(NULLIF(%s, '') AS date))
              AND NOT EXISTS (
                SELECT 1 FROM invoice_collection_items ici WHERE ici.invoice_id = si.id
              )
              AND NOT EXISTS (
                SELECT 1
                FROM dashboard_collection_flags dcf
                WHERE dcf.invoice_id = si.id AND COALESCE(dcf.collected, 0) = 1
              )
            ORDER BY si.ts DESC, si.id DESC
            LIMIT 500
            """,
            (from_date.strip(), from_date.strip(), to_date.strip(), to_date.strip()),
        ).fetchall()
        bag_rows = cn.execute(
            """
            SELECT bs.id AS invoice_id,
                   'bag' AS invoice_source,
                   CAST(bs.ts AS TEXT) AS ts,
                   COALESCE(bs.invoice_no, '') AS invoice_no,
                   COALESCE(bs.customer, '') AS customer,
                   COALESCE(bs.total_gs, 0) AS total_gs,
                   0 AS cobrado_gs,
                   COALESCE(bs.total_gs, 0) AS saldo_gs
            FROM bag_sales bs
            WHERE (%s = '' OR bs.ts::date >= CAST(NULLIF(%s, '') AS date))
              AND (%s = '' OR bs.ts::date <= CAST(NULLIF(%s, '') AS date))
              AND NOT EXISTS (
                SELECT 1 FROM bag_collection_items bci WHERE bci.bag_sale_id = bs.id
              )
              AND NOT EXISTS (
                SELECT 1
                FROM dashboard_collection_flags dcf
                WHERE dcf.status_key IN (
                    'bag:' || bs.id::text,
                    'bag:' || bs.id::text || ':' || COALESCE(CAST(bs.ts AS TEXT),'') || ':' || COALESCE(bs.invoice_no,'')
                )
                  AND COALESCE(dcf.collected, 0) = 1
              )
            ORDER BY bs.ts DESC, bs.id DESC
            LIMIT 500
            """,
            (from_date.strip(), from_date.strip(), to_date.strip(), to_date.strip()),
        ).fetchall()
        all_rows = [dict(r) for r in rows] + [dict(r) for r in bag_rows]
        all_rows.sort(key=lambda r: (str(r.get("ts") or ""), int(r.get("invoice_id") or 0)), reverse=True)
        return [FacturaPendienteRow(**r) for r in all_rows[:500]]

    def _cobros(self, cn) -> list[CobroFacturaRow]:
        rows = cn.execute(
            """
            SELECT id, CAST(fecha_cobro AS TEXT) AS fecha_cobro, cheque_no,
                   boleta_deposito, banco, observacion, total_gs,
                   CAST(created_at AS TEXT) AS created_at,
                   CAST(updated_at AS TEXT) AS updated_at
            FROM invoice_collections
            ORDER BY fecha_cobro DESC, id DESC
            LIMIT 500
            """
        ).fetchall()
        return [self._row_to_cobro(cn, row) for row in rows]

    def _cobro(self, cn, collection_id: int) -> CobroFacturaRow:
        row = cn.execute(
            """
            SELECT id, CAST(fecha_cobro AS TEXT) AS fecha_cobro, cheque_no,
                   boleta_deposito, banco, observacion, total_gs,
                   CAST(created_at AS TEXT) AS created_at,
                   CAST(updated_at AS TEXT) AS updated_at
            FROM invoice_collections
            WHERE id = %s
            """,
            (collection_id,),
        ).fetchone()
        if not row:
            raise ValueError("Cobro no encontrado.")
        return self._row_to_cobro(cn, row)

    def _row_to_cobro(self, cn, row) -> CobroFacturaRow:
        items = cn.execute(
            """
            SELECT ici.id, ici.invoice_id, COALESCE(si.invoice_no, '') AS invoice_no,
                   'sales' AS invoice_source,
                   COALESCE(si.customer, '') AS customer,
                   COALESCE(si.total_gs, 0) AS factura_total_gs,
                   ici.monto_gs
            FROM invoice_collection_items ici
            JOIN sales_invoices si ON si.id = ici.invoice_id
            WHERE ici.collection_id = %s
            ORDER BY si.ts DESC, si.id DESC
            """,
            (int(row["id"]),),
        ).fetchall()
        bag_items = cn.execute(
            """
            SELECT bci.id, bci.bag_sale_id AS invoice_id, COALESCE(bs.invoice_no, '') AS invoice_no,
                   'bag' AS invoice_source,
                   COALESCE(bs.customer, '') AS customer,
                   COALESCE(bs.total_gs, 0) AS factura_total_gs,
                   bci.monto_gs
            FROM bag_collection_items bci
            JOIN bag_sales bs ON bs.id = bci.bag_sale_id
            WHERE bci.collection_id = %s
            ORDER BY bs.ts DESC, bs.id DESC
            """,
            (int(row["id"]),),
        ).fetchall()
        data = dict(row)
        data["items"] = [CobroFacturaItemRow(**dict(item)) for item in list(items) + list(bag_items)]
        return CobroFacturaRow(**data)

    def _migrate_legacy_collections(self, cn) -> None:
        legacy_rows = cn.execute(
            """
            SELECT d.invoice_id, COALESCE(d.invoice_ts, '') AS invoice_ts,
                   COALESCE(d.invoice_no, '') AS invoice_no,
                   COALESCE(d.cliente, '') AS cliente,
                   COALESCE(d.monto_total_gs, 0) AS monto_total_gs,
                   COALESCE(d.monto_total_ret_gs, 0) AS monto_total_ret_gs,
                   COALESCE(d.fecha_cobro, '') AS fecha_cobro,
                   COALESCE(d.medio, '') AS medio,
                   COALESCE(d.nro_cheque, '') AS nro_cheque,
                   COALESCE(d.nro_deposito, '') AS nro_deposito,
                   COALESCE(d.referencia, '') AS referencia,
                   COALESCE(d.observacion, '') AS observacion,
                   COALESCE(d.ts_registro, '') AS ts_registro,
                   COALESCE(d.ts_modificacion, '') AS ts_modificacion
            FROM dashboard_collection_details d
            LEFT JOIN invoice_collection_items ici ON ici.invoice_id = d.invoice_id
            WHERE ici.id IS NULL
            ORDER BY d.ts_registro, d.id
            """
        ).fetchall()
        groups: dict[tuple[str, str, str, str, str, str], list] = {}
        for row in legacy_rows:
            key = (
                str(row["fecha_cobro"] or ""),
                str(row["medio"] or ""),
                str(row["nro_cheque"] or ""),
                str(row["nro_deposito"] or ""),
                str(row["referencia"] or ""),
                str(row["observacion"] or ""),
            )
            groups.setdefault(key, []).append(row)

        for (fecha, medio, cheque, deposito, referencia, obs), rows in groups.items():
            total = sum(float(r["monto_total_ret_gs"] or r["monto_total_gs"] or 0) for r in rows)
            created = str(rows[0]["ts_registro"] or "") or None
            inserted = cn.execute(
                """
                INSERT INTO invoice_collections(
                    fecha_cobro, cheque_no, boleta_deposito, banco, observacion, total_gs, created_at, updated_at
                )
                VALUES(%s, %s, %s, %s, %s, %s, COALESCE(%s::timestamp, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP)
                RETURNING id
                """,
                (fecha or date.today().isoformat(), cheque or referencia or "-", deposito or "-", medio or "", obs, total, created),
            ).fetchone()
            collection_id = int(inserted["id"])
            for row in rows:
                cn.execute(
                    """
                    INSERT INTO invoice_collection_items(collection_id, invoice_id, monto_gs)
                    VALUES(%s, %s, %s)
                    ON CONFLICT(collection_id, invoice_id) DO UPDATE SET monto_gs = excluded.monto_gs
                    """,
                    (
                        collection_id,
                        int(row["invoice_id"]),
                        float(row["monto_total_ret_gs"] or row["monto_total_gs"] or 0),
                    ),
                )

    def _sync_legacy_collection(self, cn, cobro: CobroFacturaRow) -> None:
        for item in cobro.items:
            if item.invoice_source == "bag":
                inv = cn.execute(
                    """
                    SELECT id, CAST(ts AS TEXT) AS invoice_ts, COALESCE(invoice_no, '') AS invoice_no,
                           COALESCE(customer, '') AS cliente, COALESCE(total_gs, 0) AS monto_total_gs
                    FROM bag_sales
                    WHERE id = %s
                    """,
                    (item.invoice_id,),
                ).fetchone()
                if inv:
                    self._save_legacy_flags(
                        cn,
                        item.invoice_id,
                        str(inv["invoice_ts"] or ""),
                        str(inv["invoice_no"] or ""),
                        True,
                        source="bag",
                    )
                continue
            inv = cn.execute(
                """
                SELECT id, CAST(ts AS TEXT) AS invoice_ts, COALESCE(invoice_no, '') AS invoice_no,
                       COALESCE(customer, '') AS cliente, COALESCE(total_gs, 0) AS monto_total_gs,
                       COALESCE(total_gs, 0) - 0.30 * (COALESCE(iva5_gs, 0) + COALESCE(iva10_gs, 0)) AS monto_total_ret_gs
                FROM sales_invoices
                WHERE id = %s
                """,
                (item.invoice_id,),
            ).fetchone()
            if not inv:
                continue
            cn.execute(
                """
                INSERT INTO dashboard_collection_details(
                    invoice_id, invoice_ts, invoice_no, cliente, monto_total_gs, monto_total_ret_gs,
                    fecha_cobro, medio, nro_cheque, nro_deposito, referencia, observacion,
                    ts_registro, ts_modificacion
                )
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
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
                    ts_modificacion=CURRENT_TIMESTAMP
                """,
                (
                    item.invoice_id,
                    str(inv["invoice_ts"] or ""),
                    str(inv["invoice_no"] or ""),
                    str(inv["cliente"] or ""),
                    float(inv["monto_total_gs"] or 0),
                    float(item.monto_gs or inv["monto_total_ret_gs"] or 0),
                    cobro.fecha_cobro,
                    cobro.banco or "cheque",
                    cobro.cheque_no,
                    cobro.boleta_deposito,
                    cobro.cheque_no,
                    cobro.observacion,
                ),
            )
            total_cobrado = cn.execute(
                "SELECT COALESCE(SUM(monto_gs), 0) AS total FROM invoice_collection_items WHERE invoice_id = %s",
                (item.invoice_id,),
            ).fetchone()
            saldo = float(inv["monto_total_ret_gs"] or 0) - float(total_cobrado["total"] if total_cobrado else 0)
            self._save_legacy_flags(cn, item.invoice_id, str(inv["invoice_ts"] or ""), str(inv["invoice_no"] or ""), saldo <= 0.5)

    def _mark_legacy_invoice(self, cn, invoice_id: int, collected: bool) -> None:
        inv = cn.execute(
            "SELECT CAST(ts AS TEXT) AS invoice_ts, COALESCE(invoice_no, '') AS invoice_no FROM sales_invoices WHERE id = %s",
            (invoice_id,),
        ).fetchone()
        if not inv:
            return
        self._save_legacy_flags(cn, invoice_id, str(inv["invoice_ts"] or ""), str(inv["invoice_no"] or ""), collected)

    def _mark_legacy_bag(self, cn, invoice_id: int, collected: bool) -> None:
        inv = cn.execute(
            "SELECT CAST(ts AS TEXT) AS invoice_ts, COALESCE(invoice_no, '') AS invoice_no FROM bag_sales WHERE id = %s",
            (invoice_id,),
        ).fetchone()
        if not inv:
            return
        self._save_legacy_flags(
            cn,
            invoice_id,
            str(inv["invoice_ts"] or ""),
            str(inv["invoice_no"] or ""),
            collected,
            source="bag",
        )

    def _save_legacy_flags(
        self,
        cn,
        invoice_id: int,
        invoice_ts: str,
        invoice_no: str,
        collected: bool,
        source: str = "sales",
    ) -> None:
        prefix = "bag" if source == "bag" else "std"
        keys = [f"{prefix}:{int(invoice_id)}:{invoice_ts.strip()}:{invoice_no.strip()}", f"{prefix}:{int(invoice_id)}"]
        for key in keys:
            cn.execute(
                """
                INSERT INTO dashboard_collection_flags(status_key, invoice_id, invoice_ts, invoice_no, collected, updated_ts)
                VALUES(%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT(status_key) DO UPDATE SET
                    invoice_id=excluded.invoice_id,
                    invoice_ts=excluded.invoice_ts,
                    invoice_no=excluded.invoice_no,
                    collected=excluded.collected,
                    updated_ts=CURRENT_TIMESTAMP
                """,
                (key, int(invoice_id), invoice_ts, invoice_no, 1 if collected else 0),
            )

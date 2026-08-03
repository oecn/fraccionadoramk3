from __future__ import annotations

import math
import sys
import tempfile
import unicodedata
import re
from pathlib import Path

from fastapi import UploadFile

from core.database import connection
from modules.compra_materia_prima.schemas import (
    CompraMateriaPrimaCreate,
    FacturaCompraImportRequest,
    FacturaCompraImportResponse,
    FacturaCompraItem,
    FacturaCompraPreview,
    LoteDeleteRequest,
    LoteDeleteResponse,
    CompraMateriaPrimaOptions,
    CompraMateriaPrimaSummary,
    LoteAbiertoRow,
    LotePrecioReviewRequest,
    LotePrecioReviewHistoryRow,
    LotePrecioReviewResponse,
    ProductoItem,
    RawStockRow,
)

ROOT_DIR = Path(__file__).resolve().parents[4]
FACTURAS_DIR = ROOT_DIR / "importadorfactur"
if str(FACTURAS_DIR) not in sys.path:
    sys.path.insert(0, str(FACTURAS_DIR))

from factura_parser import parse_invoice  # noqa: E402


def _norm_text(value: str) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _peso_bolsa_estandar(desc: str | None) -> float:
    s = _norm_text(desc or "")
    if "arroz" in s:
        return 50.0
    if "gallet" in s and "molid" in s:
        return 25.0
    return 50.0


def _bolsas_equivalentes(kg: float | None, bolsa_kg: float) -> int:
    if kg is None or kg <= 0 or bolsa_kg <= 0:
        return 0
    return int(math.ceil(float(kg) / float(bolsa_kg)))


def _tax_split(total: float, iva_rate: int) -> dict[str, float]:
    total = float(total or 0)
    if iva_rate not in (5, 10) or total <= 0:
        return {"gravada5_gs": 0.0, "iva5_gs": 0.0, "gravada10_gs": 0.0, "iva10_gs": 0.0, "exenta_gs": total}
    base = total / (1.0 + iva_rate / 100.0)
    iva = total - base
    if iva_rate == 5:
        return {"gravada5_gs": base, "iva5_gs": iva, "gravada10_gs": 0.0, "iva10_gs": 0.0, "exenta_gs": 0.0}
    return {"gravada5_gs": 0.0, "iva5_gs": 0.0, "gravada10_gs": base, "iva10_gs": iva, "exenta_gs": 0.0}


def _invoice_tax_rate(meta: dict) -> int:
    iva5 = float(meta.get("total_iva5") or 0)
    iva10 = float(meta.get("total_iva10") or 0)
    if iva10 > 0 and iva5 <= 0:
        return 10
    return 5


def _variation_pct(current: float, previous: float | None) -> float | None:
    if previous is None or previous <= 0:
        return None
    return ((float(current or 0) - previous) / previous) * 100.0


def _price_change_detected(current: float, previous: float | None) -> bool:
    if previous is None or previous <= 0:
        return False
    return abs(float(current or 0) - float(previous)) > 0.01


def _price_state(change_detected: bool, reviewed: bool, state: str | None = None) -> str:
    state_text = (state or "").strip()
    if state_text:
        return state_text
    if not change_detected:
        return "Sin cambio"
    return "Revisado y OK" if reviewed else "Pendiente"


class CompraMateriaPrimaRepository:
    def _ensure_raw_lots_tax(self, cn) -> None:
        cn.execute("ALTER TABLE raw_lots ADD COLUMN IF NOT EXISTS gravada5_gs REAL NOT NULL DEFAULT 0")
        cn.execute("ALTER TABLE raw_lots ADD COLUMN IF NOT EXISTS iva5_gs REAL NOT NULL DEFAULT 0")
        cn.execute("ALTER TABLE raw_lots ADD COLUMN IF NOT EXISTS gravada10_gs REAL NOT NULL DEFAULT 0")
        cn.execute("ALTER TABLE raw_lots ADD COLUMN IF NOT EXISTS iva10_gs REAL NOT NULL DEFAULT 0")
        cn.execute("ALTER TABLE raw_lots ADD COLUMN IF NOT EXISTS exenta_gs REAL NOT NULL DEFAULT 0")
        cn.execute("ALTER TABLE raw_lots ADD COLUMN IF NOT EXISTS precio_cambio_detectado INTEGER NOT NULL DEFAULT 0")
        cn.execute("ALTER TABLE raw_lots ADD COLUMN IF NOT EXISTS precio_estado TEXT NOT NULL DEFAULT 'Sin cambio'")
        cn.execute("ALTER TABLE raw_lots ADD COLUMN IF NOT EXISTS precio_revisado INTEGER NOT NULL DEFAULT 0")
        cn.execute("ALTER TABLE raw_lots ADD COLUMN IF NOT EXISTS precio_revisado_por TEXT NOT NULL DEFAULT ''")
        cn.execute("ALTER TABLE raw_lots ADD COLUMN IF NOT EXISTS precio_revisado_at TIMESTAMP NULL")
        cn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_lot_price_review_history(
                id BIGSERIAL PRIMARY KEY,
                lot_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                producto TEXT NOT NULL DEFAULT '',
                estado TEXT NOT NULL,
                revisado_por TEXT NOT NULL DEFAULT '',
                costo_kg_anterior_gs REAL,
                costo_kg_gs REAL NOT NULL DEFAULT 0,
                diferencia_costo_kg_gs REAL NOT NULL DEFAULT 0,
                variacion_costo_pct REAL,
                note TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cn.execute("CREATE INDEX IF NOT EXISTS idx_raw_lot_price_review_history_lot ON raw_lot_price_review_history(lot_id)")
        cn.execute(
            """
            UPDATE raw_lots
               SET gravada5_gs = costo_total_gs / 1.05,
                   iva5_gs = costo_total_gs - (costo_total_gs / 1.05)
             WHERE COALESCE(costo_total_gs, 0) > 0
               AND COALESCE(gravada5_gs, 0) = 0
               AND COALESCE(iva5_gs, 0) = 0
               AND COALESCE(gravada10_gs, 0) = 0
               AND COALESCE(iva10_gs, 0) = 0
               AND COALESCE(exenta_gs, 0) = 0
            """
        )

    def _ensure_delete_audit(self, cn) -> None:
        cn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_lot_deletions(
                id BIGSERIAL PRIMARY KEY,
                lot_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                producto TEXT NOT NULL DEFAULT '',
                lote TEXT NOT NULL DEFAULT '',
                proveedor TEXT NOT NULL DEFAULT '',
                factura TEXT NOT NULL DEFAULT '',
                kg_inicial REAL NOT NULL DEFAULT 0,
                kg_saldo REAL NOT NULL DEFAULT 0,
                costo_total_gs REAL NOT NULL DEFAULT 0,
                costo_kg_gs REAL NOT NULL DEFAULT 0,
                lot_ts TEXT NOT NULL DEFAULT '',
                motivo TEXT NOT NULL,
                deleted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cn.execute("CREATE INDEX IF NOT EXISTS idx_raw_lot_deletions_deleted_at ON raw_lot_deletions(deleted_at)")

    def _table_exists(self, cn, table_name: str) -> bool:
        row = cn.execute("SELECT to_regclass(%s) AS table_name", (table_name,)).fetchone()
        return bool(row and row["table_name"])

    def _product_map(self) -> dict[str, tuple[int, str]]:
        with connection("fraccionadora") as cn:
            rows = cn.execute("SELECT id, name FROM products ORDER BY name").fetchall()
        out: dict[str, tuple[int, str]] = {}
        for row in rows:
            out[_norm_text(row["name"] or "")] = (int(row["id"]), row["name"] or "")
        return out

    def _match_product(self, desc: str, product_map: dict[str, tuple[int, str]]) -> tuple[int, str] | None:
        norm_desc = f" {_norm_text(desc)} "
        aliases = {
            "arroz": (" arroz ",),
            "azucar": (" azucar ",),
            "galleta molida": (" galleta molida ", " gall molida ", " gall molida el cacique "),
            "pororo": (" pororo ",),
            "poroto rojo": (" poroto rojo ",),
            "locro": (" locro ",),
            "locrillo": (" locrillo ",),
            "lenteja": (" lenteja ",),
        }
        for key, value in product_map.items():
            if key and f" {key} " in norm_desc:
                return value
            for alias in aliases.get(key, ()):
                if alias in norm_desc:
                    return value
        return None

    def options(self) -> CompraMateriaPrimaOptions:
        with connection("fraccionadora") as cn:
            rows = cn.execute(
                """
                SELECT p.id, p.name, COALESCE(rs.kg, 0) AS raw_kg,
                       (
                         SELECT rl.costo_kg_gs
                         FROM raw_lots rl
                         WHERE rl.product_id = p.id
                           AND COALESCE(rl.costo_kg_gs, 0) > 0
                         ORDER BY rl.ts DESC, rl.id DESC
                         LIMIT 1
                       ) AS ultimo_costo_kg_gs
                FROM products p
                LEFT JOIN raw_stock rs ON rs.product_id = p.id
                ORDER BY p.name
                """
            ).fetchall()
        return CompraMateriaPrimaOptions(
            productos=[
                ProductoItem(
                    id=int(r["id"]),
                    name=r["name"] or "",
                    raw_kg=float(r["raw_kg"] or 0),
                    ultimo_costo_kg_gs=(
                        float(r["ultimo_costo_kg_gs"]) if r["ultimo_costo_kg_gs"] is not None else None
                    ),
                )
                for r in rows
            ],
            bolsa_kg_presets=[25, 30, 50],
        )

    def _previous_cost_for_product(self, cn, product_id: int, before_ts: str | None = None, before_id: int | None = None) -> float | None:
        where = ["product_id = %s", "COALESCE(costo_kg_gs, 0) > 0"]
        params: list[object] = [int(product_id)]
        if before_ts is not None and before_id is not None:
            where.append("(ts < %s::timestamp OR (ts = %s::timestamp AND id < %s))")
            params.extend([before_ts, before_ts, int(before_id)])
        row = cn.execute(
            f"""
            SELECT costo_kg_gs
            FROM raw_lots
            WHERE {" AND ".join(where)}
            ORDER BY ts DESC, id DESC
            LIMIT 1
            """,
            params,
        ).fetchone()
        if not row or row["costo_kg_gs"] is None:
            return None
        return float(row["costo_kg_gs"])

    def summary(self, product_id: int | None = None) -> CompraMateriaPrimaSummary:
        raw_where = ""
        raw_params: list[int] = []
        lot_where = "WHERE rl.kg_saldo > 1e-9 AND COALESCE(rl.cerrado, 0) = 0"
        lot_params: list[int] = []

        if product_id is not None:
            raw_where = "WHERE p.id = %s"
            raw_params.append(product_id)
            lot_where += " AND rl.product_id = %s"
            lot_params.append(product_id)

        with connection("fraccionadora") as cn:
            self._ensure_raw_lots_tax(cn)
            raw_rows = cn.execute(
                f"""
                SELECT p.id AS product_id, p.name AS producto, COALESCE(rs.kg, 0) AS kg
                FROM products p
                LEFT JOIN raw_stock rs ON rs.product_id = p.id
                {raw_where}
                ORDER BY p.name
                """,
                raw_params,
            ).fetchall()
            lot_rows = cn.execute(
                f"""
                SELECT rl.id, rl.product_id, p.name AS producto,
                       COALESCE(rl.lote, '') AS lote,
                       COALESCE(rl.proveedor, '') AS proveedor,
                       COALESCE(rl.factura, '') AS factura,
                       COALESCE(rl.kg_inicial, 0) AS kg_inicial,
                       COALESCE(rl.kg_saldo, 0) AS kg_saldo,
                       COALESCE(rl.costo_total_gs, 0) AS costo_total_gs,
                       COALESCE(rl.costo_kg_gs, 0) AS costo_kg_gs,
                       COALESCE(rl.gravada5_gs, 0) AS gravada5_gs,
                       COALESCE(rl.iva5_gs, 0) AS iva5_gs,
                       COALESCE(rl.gravada10_gs, 0) AS gravada10_gs,
                       COALESCE(rl.iva10_gs, 0) AS iva10_gs,
                       COALESCE(rl.exenta_gs, 0) AS exenta_gs,
                       COALESCE(rl.precio_cambio_detectado, 0) AS precio_cambio_detectado,
                       COALESCE(rl.precio_estado, '') AS precio_estado,
                       COALESCE(rl.precio_revisado, 0) AS precio_revisado,
                       COALESCE(rl.precio_revisado_por, '') AS precio_revisado_por,
                       CAST(rl.precio_revisado_at AS TEXT) AS precio_revisado_at,
                       CAST(rl.ts AS TEXT) AS ts
                FROM raw_lots rl
                JOIN products p ON p.id = rl.product_id
                {lot_where}
                ORDER BY p.name, rl.ts DESC, rl.id DESC
                """,
                lot_params,
            ).fetchall()
            hist_where = ""
            hist_params: list[int] = []
            if product_id is not None:
                hist_where = "WHERE h.product_id = %s"
                hist_params.append(product_id)
            history_rows = cn.execute(
                f"""
                SELECT h.id, h.lot_id, h.product_id, h.producto, h.estado, h.revisado_por,
                       h.costo_kg_anterior_gs, h.costo_kg_gs, h.diferencia_costo_kg_gs,
                       h.variacion_costo_pct, h.note, CAST(h.created_at AS TEXT) AS created_at
                FROM raw_lot_price_review_history h
                {hist_where}
                ORDER BY h.created_at DESC, h.id DESC
                LIMIT 100
                """,
                hist_params,
            ).fetchall()
            lotes_abiertos: list[LoteAbiertoRow] = []
            for r in lot_rows:
                previous_cost = self._previous_cost_for_product(cn, int(r["product_id"]), str(r["ts"] or ""), int(r["id"]))
                current_cost = float(r["costo_kg_gs"] or 0)
                precio_revisado = bool(int(r["precio_revisado"] or 0))
                change_detected = bool(int(r["precio_cambio_detectado"] or 0))
                diferencia_kg = current_cost - float(previous_cost or 0)
                lotes_abiertos.append(
                    LoteAbiertoRow(
                        id=int(r["id"]),
                        product_id=int(r["product_id"]),
                        producto=r["producto"] or "",
                        lote=r["lote"] or "",
                        proveedor=r["proveedor"] or "",
                        factura=r["factura"] or "",
                        kg_inicial=float(r["kg_inicial"] or 0),
                        kg_saldo=float(r["kg_saldo"] or 0),
                        costo_total_gs=float(r["costo_total_gs"] or 0),
                        costo_kg_gs=current_cost,
                        gravada5_gs=float(r["gravada5_gs"] or 0),
                        iva5_gs=float(r["iva5_gs"] or 0),
                        gravada10_gs=float(r["gravada10_gs"] or 0),
                        iva10_gs=float(r["iva10_gs"] or 0),
                        exenta_gs=float(r["exenta_gs"] or 0),
                        costo_kg_anterior_gs=previous_cost,
                        variacion_costo_pct=_variation_pct(current_cost, previous_cost),
                        precio_cambio_detectado=change_detected and not precio_revisado,
                        precio_estado=_price_state(change_detected, precio_revisado, r["precio_estado"]),
                        precio_revisado=precio_revisado,
                        precio_revisado_por=r["precio_revisado_por"] or "",
                        precio_revisado_at=r["precio_revisado_at"] or "",
                        diferencia_costo_kg_gs=diferencia_kg if previous_cost is not None else 0,
                        diferencia_costo_total_gs=(diferencia_kg * float(r["kg_inicial"] or 0)) if previous_cost is not None else 0,
                        ts=r["ts"] or "",
                    )
                )

        return CompraMateriaPrimaSummary(
            raw_stock=[
                RawStockRow(product_id=int(r["product_id"]), producto=r["producto"] or "", kg=float(r["kg"] or 0))
                for r in raw_rows
            ],
            lotes_abiertos=lotes_abiertos,
            historial_revisiones_precio=[
                LotePrecioReviewHistoryRow(
                    id=int(r["id"]),
                    lot_id=int(r["lot_id"]),
                    product_id=int(r["product_id"]),
                    producto=r["producto"] or "",
                    estado=r["estado"] or "",
                    revisado_por=r["revisado_por"] or "",
                    costo_kg_anterior_gs=(float(r["costo_kg_anterior_gs"]) if r["costo_kg_anterior_gs"] is not None else None),
                    costo_kg_gs=float(r["costo_kg_gs"] or 0),
                    diferencia_costo_kg_gs=float(r["diferencia_costo_kg_gs"] or 0),
                    variacion_costo_pct=(float(r["variacion_costo_pct"]) if r["variacion_costo_pct"] is not None else None),
                    note=r["note"] or "",
                    created_at=r["created_at"] or "",
                )
                for r in history_rows
            ],
        )

    def create(self, payload: CompraMateriaPrimaCreate) -> LoteAbiertoRow:
        kg_total = payload.bolsa_kg * payload.bolsas
        costo_kg = payload.costo_total_gs / kg_total if kg_total > 0 else 0.0
        tax = _tax_split(payload.costo_total_gs, int(payload.iva or 5))
        fecha = (payload.fecha or "").strip()
        lot_ts = f"{fecha} 12:00:00" if fecha else None

        with connection("fraccionadora") as cn:
            self._ensure_raw_lots_tax(cn)
            product = cn.execute("SELECT name FROM products WHERE id = %s", (payload.product_id,)).fetchone()
            if not product:
                raise ValueError("Producto no encontrado.")
            previous_cost = self._previous_cost_for_product(cn, payload.product_id)

            price_change_detected = _price_change_detected(costo_kg, previous_cost)
            cn.execute(
                """
                INSERT INTO raw_stock(product_id, kg)
                VALUES(%s, %s)
                ON CONFLICT(product_id)
                DO UPDATE SET kg = raw_stock.kg + excluded.kg
                """,
                (payload.product_id, kg_total),
            )
            row = cn.execute(
                """
                INSERT INTO raw_lots(
                    product_id, lote, proveedor, factura,
                    kg_inicial, kg_saldo, costo_total_gs, costo_kg_gs,
                    gravada5_gs, iva5_gs, gravada10_gs, iva10_gs, exenta_gs,
                    precio_cambio_detectado, precio_estado, ts
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s::timestamp, CURRENT_TIMESTAMP))
                RETURNING id, CAST(ts AS TEXT) AS ts
                """,
                (
                    payload.product_id,
                    payload.lote.strip(),
                    payload.proveedor.strip(),
                    payload.factura.strip(),
                    kg_total,
                    kg_total,
                    payload.costo_total_gs,
                    costo_kg,
                    tax["gravada5_gs"],
                    tax["iva5_gs"],
                    tax["gravada10_gs"],
                    tax["iva10_gs"],
                    tax["exenta_gs"],
                    1 if price_change_detected else 0,
                    "Pendiente" if price_change_detected else "Sin cambio",
                    lot_ts,
                ),
            ).fetchone()

        return LoteAbiertoRow(
            id=int(row["id"]),
            product_id=payload.product_id,
            producto=product["name"] or "",
            lote=payload.lote.strip(),
            proveedor=payload.proveedor.strip(),
            factura=payload.factura.strip(),
            kg_inicial=kg_total,
            kg_saldo=kg_total,
            costo_total_gs=payload.costo_total_gs,
            costo_kg_gs=costo_kg,
            **tax,
            costo_kg_anterior_gs=previous_cost,
            variacion_costo_pct=_variation_pct(costo_kg, previous_cost),
            precio_cambio_detectado=price_change_detected,
            precio_estado="Pendiente" if price_change_detected else "Sin cambio",
            precio_revisado=False,
            precio_revisado_por="",
            precio_revisado_at="",
            diferencia_costo_kg_gs=(costo_kg - previous_cost) if previous_cost is not None else 0,
            diferencia_costo_total_gs=((costo_kg - previous_cost) * kg_total) if previous_cost is not None else 0,
            ts=row["ts"] or "",
        )

    def parse_factura_pdf(self, upload: UploadFile) -> FacturaCompraPreview:
        filename = upload.filename or ""
        if not filename.lower().endswith(".pdf"):
            raise ValueError("Seleccione un archivo PDF.")

        tmp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                tmp.write(upload.file.read())
                tmp_path = Path(tmp.name)
            result = parse_invoice(tmp_path)
        finally:
            upload.file.close()
            if tmp_path and tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

        meta = result.get("meta") or {}
        iva_rate = _invoice_tax_rate(meta)
        product_map = self._product_map()
        previous_costs: dict[int, float | None] = {}
        with connection("fraccionadora") as cn:
            for product_id, _product_name in product_map.values():
                previous_costs[product_id] = self._previous_cost_for_product(cn, product_id)
        items: list[FacturaCompraItem] = []
        for idx, raw in enumerate(result.get("items") or [], start=1):
            desc = raw.get("descripcion") or ""
            kg = float(raw.get("kg") or 0)
            total_linea = float(raw.get("total_linea") or 0)
            tax = _tax_split(total_linea, iva_rate)
            match = self._match_product(desc, product_map)
            previous_cost = previous_costs.get(match[0]) if match else None
            costo_kg = total_linea / kg if kg > 0 else 0.0
            bolsa_kg = _peso_bolsa_estandar(desc)
            bolsas = _bolsas_equivalentes(kg, bolsa_kg)
            importable = bool(match and kg > 0 and bolsas > 0)
            items.append(
                FacturaCompraItem(
                    linea=idx,
                    descripcion=desc,
                    kg=kg,
                    precio_unitario=raw.get("precio_unitario"),
                    total_linea=total_linea,
                    **tax,
                    product_id=match[0] if match else None,
                    producto=match[1] if match else "",
                    bolsa_kg=bolsa_kg,
                    bolsas=bolsas,
                    costo_kg_anterior_gs=previous_cost,
                    variacion_costo_pct=_variation_pct(costo_kg, previous_cost),
                    importable=importable,
                    message="" if importable else "Sin match de producto o kg invalido.",
                )
            )

        return FacturaCompraPreview(
            numero=meta.get("numero") or "",
            proveedor=meta.get("proveedor") or "",
            fecha_emision=meta.get("fecha_emision") or "",
            total=float(meta.get("total") or 0),
            items=items,
        )

    def import_factura(self, payload: FacturaCompraImportRequest) -> FacturaCompraImportResponse:
        inserted = 0
        skipped = 0
        lotes: list[LoteAbiertoRow] = []
        proveedor = payload.proveedor.strip()
        factura = payload.numero.strip()

        for item in payload.items:
            exists = None
            with connection("fraccionadora") as cn:
                self._ensure_raw_lots_tax(cn)
                product = cn.execute("SELECT name FROM products WHERE id = %s", (item.product_id,)).fetchone()
                if not product:
                    skipped += 1
                    continue
                previous_cost = self._previous_cost_for_product(cn, item.product_id)
                if factura:
                    exists = cn.execute(
                        "SELECT id FROM raw_lots WHERE product_id = %s AND factura = %s LIMIT 1",
                        (item.product_id, factura),
                    ).fetchone()
                if exists:
                    skipped += 1
                    continue

                costo_kg = item.total_linea / item.kg if item.kg > 0 else 0.0
                price_change_detected = _price_change_detected(costo_kg, previous_cost)
                cn.execute(
                    """
                    INSERT INTO raw_stock(product_id, kg)
                    VALUES(%s, %s)
                    ON CONFLICT(product_id)
                    DO UPDATE SET kg = raw_stock.kg + excluded.kg
                    """,
                    (item.product_id, item.kg),
                )
                row = cn.execute(
                    """
                    INSERT INTO raw_lots(
                        product_id, lote, proveedor, factura,
                        kg_inicial, kg_saldo, costo_total_gs, costo_kg_gs,
                        gravada5_gs, iva5_gs, gravada10_gs, iva10_gs, exenta_gs,
                        precio_cambio_detectado, precio_estado
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, CAST(ts AS TEXT) AS ts
                    """,
                    (
                        item.product_id,
                        "",
                        proveedor,
                        factura,
                        item.kg,
                        item.kg,
                        item.total_linea,
                        costo_kg,
                        item.gravada5_gs,
                        item.iva5_gs,
                        item.gravada10_gs,
                        item.iva10_gs,
                        item.exenta_gs,
                        1 if price_change_detected else 0,
                        "Pendiente" if price_change_detected else "Sin cambio",
                    ),
                ).fetchone()

            inserted += 1
            lotes.append(
                LoteAbiertoRow(
                    id=int(row["id"]),
                    product_id=item.product_id,
                    producto=product["name"] or "",
                    lote="",
                    proveedor=proveedor,
                    factura=factura,
                    kg_inicial=item.kg,
                    kg_saldo=item.kg,
                    costo_total_gs=item.total_linea,
                    costo_kg_gs=costo_kg,
                    gravada5_gs=item.gravada5_gs,
                    iva5_gs=item.iva5_gs,
                    gravada10_gs=item.gravada10_gs,
                    iva10_gs=item.iva10_gs,
                    exenta_gs=item.exenta_gs,
                    costo_kg_anterior_gs=previous_cost,
                    variacion_costo_pct=_variation_pct(costo_kg, previous_cost),
                    precio_cambio_detectado=price_change_detected,
                    precio_estado="Pendiente" if price_change_detected else "Sin cambio",
                    precio_revisado=False,
                    precio_revisado_por="",
                    precio_revisado_at="",
                    diferencia_costo_kg_gs=(costo_kg - previous_cost) if previous_cost is not None else 0,
                    diferencia_costo_total_gs=((costo_kg - previous_cost) * item.kg) if previous_cost is not None else 0,
                    ts=row["ts"] or "",
                )
            )

        return FacturaCompraImportResponse(
            inserted=inserted,
            skipped=skipped,
            lotes=lotes,
            message=f"Factura {factura or '-'}: {inserted} lote(s) cargado(s), {skipped} omitido(s).",
        )

    def delete_lote(self, lot_id: int, payload: LoteDeleteRequest) -> LoteDeleteResponse:
        motivo = (payload.motivo or "").strip()
        if len(motivo) < 3:
            raise ValueError("Ingrese el motivo de eliminacion del lote.")

        with connection("fraccionadora") as cn:
            self._ensure_delete_audit(cn)
            lot = cn.execute(
                """
                SELECT rl.id, rl.product_id, p.name AS producto,
                       COALESCE(rl.lote, '') AS lote,
                       COALESCE(rl.proveedor, '') AS proveedor,
                       COALESCE(rl.factura, '') AS factura,
                       COALESCE(rl.kg_inicial, 0) AS kg_inicial,
                       COALESCE(rl.kg_saldo, 0) AS kg_saldo,
                       COALESCE(rl.costo_total_gs, 0) AS costo_total_gs,
                       COALESCE(rl.costo_kg_gs, 0) AS costo_kg_gs,
                       CAST(rl.ts AS TEXT) AS lot_ts
                FROM raw_lots rl
                JOIN products p ON p.id = rl.product_id
                WHERE rl.id = %s
                """,
                (int(lot_id),),
            ).fetchone()
            if not lot:
                raise ValueError("Lote no encontrado.")

            used = cn.execute("SELECT 1 FROM lot_fractionations WHERE lot_id = %s LIMIT 1", (int(lot_id),)).fetchone()
            merma = cn.execute("SELECT 1 FROM lot_mermas WHERE lot_id = %s LIMIT 1", (int(lot_id),)).fetchone()
            if used or merma:
                raise ValueError("No se puede eliminar un lote con fraccionamientos o merma. Cierre o ajuste el lote en Resumenes.")

            kg_inicial = float(lot["kg_inicial"] or 0)
            kg_saldo = float(lot["kg_saldo"] or 0)
            if abs(kg_inicial - kg_saldo) > 1e-6:
                raise ValueError("No se puede eliminar un lote parcialmente consumido.")

            cn.execute(
                """
                INSERT INTO raw_lot_deletions(
                    lot_id, product_id, producto, lote, proveedor, factura,
                    kg_inicial, kg_saldo, costo_total_gs, costo_kg_gs, lot_ts, motivo, deleted_at
                )
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
                """,
                (
                    int(lot["id"]),
                    int(lot["product_id"]),
                    str(lot["producto"] or ""),
                    str(lot["lote"] or ""),
                    str(lot["proveedor"] or ""),
                    str(lot["factura"] or ""),
                    kg_inicial,
                    kg_saldo,
                    float(lot["costo_total_gs"] or 0),
                    float(lot["costo_kg_gs"] or 0),
                    str(lot["lot_ts"] or ""),
                    motivo,
                ),
            )
            cn.execute(
                """
                UPDATE raw_stock
                   SET kg = GREATEST(COALESCE(kg, 0) - %s, 0)
                 WHERE product_id = %s
                """,
                (kg_saldo, int(lot["product_id"])),
            )
            if self._table_exists(cn, "dashboard_payment_flags"):
                cn.execute("DELETE FROM dashboard_payment_flags WHERE lot_id = %s", (int(lot_id),))
            if self._table_exists(cn, "dashboard_payment_details"):
                cn.execute("DELETE FROM dashboard_payment_details WHERE lot_id = %s", (int(lot_id),))
            cn.execute("DELETE FROM raw_lots WHERE id = %s", (int(lot_id),))

        return LoteDeleteResponse(deleted=True, lot_id=int(lot_id), message="Lote eliminado y auditado.")

    def mark_price_reviewed(self, lot_id: int, payload: LotePrecioReviewRequest) -> LotePrecioReviewResponse:
        estado = (payload.estado or "Revisado y OK").strip()
        if estado not in ("Pendiente", "Precio cambiado", "Revisado y OK"):
            raise ValueError("Estado de revision invalido.")
        revisado = estado in ("Precio cambiado", "Revisado y OK")
        revisado_por = (payload.revisado_por or "").strip() or "Sistema"
        with connection("fraccionadora") as cn:
            self._ensure_raw_lots_tax(cn)
            lot = cn.execute(
                """
                SELECT rl.id, rl.product_id, p.name AS producto,
                       COALESCE(rl.costo_kg_gs, 0) AS costo_kg_gs,
                       COALESCE(CAST(rl.ts AS TEXT), '') AS ts
                FROM raw_lots rl
                JOIN products p ON p.id = rl.product_id
                WHERE rl.id = %s
                """,
                (int(lot_id),),
            ).fetchone()
            if not lot:
                raise ValueError("Lote no encontrado.")
            previous_cost = self._previous_cost_for_product(cn, int(lot["product_id"]), str(lot["ts"] or ""), int(lot["id"]))
            current_cost = float(lot["costo_kg_gs"] or 0)
            variation = _variation_pct(current_cost, previous_cost)
            difference = current_cost - float(previous_cost or 0) if previous_cost is not None else 0.0
            row = cn.execute(
                """
                UPDATE raw_lots
                   SET precio_estado = %s,
                       precio_revisado = %s,
                       precio_revisado_por = %s,
                       precio_revisado_at = CURRENT_TIMESTAMP
                 WHERE id = %s
                 RETURNING id, COALESCE(precio_estado, '') AS precio_estado,
                           COALESCE(precio_revisado_por, '') AS precio_revisado_por,
                           CAST(precio_revisado_at AS TEXT) AS precio_revisado_at
                """,
                (estado, 1 if revisado else 0, revisado_por, int(lot_id)),
            ).fetchone()
            cn.execute(
                """
                INSERT INTO raw_lot_price_review_history(
                    lot_id, product_id, producto, estado, revisado_por,
                    costo_kg_anterior_gs, costo_kg_gs, diferencia_costo_kg_gs,
                    variacion_costo_pct, note
                )
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    int(lot["id"]),
                    int(lot["product_id"]),
                    lot["producto"] or "",
                    estado,
                    revisado_por,
                    previous_cost,
                    current_cost,
                    difference,
                    variation,
                    "Revision de cambio de precio de materia prima",
                ),
            )
        return LotePrecioReviewResponse(
            lot_id=int(row["id"]),
            estado=row["precio_estado"] or estado,
            revisado_por=row["precio_revisado_por"] or revisado_por,
            revisado_at=row["precio_revisado_at"] or "",
            message=f"Alerta de precio marcada como {estado}.",
        )

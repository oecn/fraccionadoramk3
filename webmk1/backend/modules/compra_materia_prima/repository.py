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
    CompraMateriaPrimaOptions,
    CompraMateriaPrimaSummary,
    LoteAbiertoRow,
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


class CompraMateriaPrimaRepository:
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
                SELECT p.id, p.name, COALESCE(rs.kg, 0) AS raw_kg
                FROM products p
                LEFT JOIN raw_stock rs ON rs.product_id = p.id
                ORDER BY p.name
                """
            ).fetchall()
        return CompraMateriaPrimaOptions(
            productos=[
                ProductoItem(id=int(r["id"]), name=r["name"] or "", raw_kg=float(r["raw_kg"] or 0))
                for r in rows
            ],
            bolsa_kg_presets=[25, 30, 50],
        )

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
                       CAST(rl.ts AS TEXT) AS ts
                FROM raw_lots rl
                JOIN products p ON p.id = rl.product_id
                {lot_where}
                ORDER BY p.name, rl.ts DESC, rl.id DESC
                """,
                lot_params,
            ).fetchall()

        return CompraMateriaPrimaSummary(
            raw_stock=[
                RawStockRow(product_id=int(r["product_id"]), producto=r["producto"] or "", kg=float(r["kg"] or 0))
                for r in raw_rows
            ],
            lotes_abiertos=[
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
                    costo_kg_gs=float(r["costo_kg_gs"] or 0),
                    ts=r["ts"] or "",
                )
                for r in lot_rows
            ],
        )

    def create(self, payload: CompraMateriaPrimaCreate) -> LoteAbiertoRow:
        kg_total = payload.bolsa_kg * payload.bolsas
        costo_kg = payload.costo_total_gs / kg_total if kg_total > 0 else 0.0

        with connection("fraccionadora") as cn:
            product = cn.execute("SELECT name FROM products WHERE id = %s", (payload.product_id,)).fetchone()
            if not product:
                raise ValueError("Producto no encontrado.")

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
                    kg_inicial, kg_saldo, costo_total_gs, costo_kg_gs
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
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
        product_map = self._product_map()
        items: list[FacturaCompraItem] = []
        for idx, raw in enumerate(result.get("items") or [], start=1):
            desc = raw.get("descripcion") or ""
            kg = float(raw.get("kg") or 0)
            total_linea = float(raw.get("total_linea") or 0)
            match = self._match_product(desc, product_map)
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
                    product_id=match[0] if match else None,
                    producto=match[1] if match else "",
                    bolsa_kg=bolsa_kg,
                    bolsas=bolsas,
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
                product = cn.execute("SELECT name FROM products WHERE id = %s", (item.product_id,)).fetchone()
                if not product:
                    skipped += 1
                    continue
                if factura:
                    exists = cn.execute(
                        "SELECT id FROM raw_lots WHERE product_id = %s AND factura = %s LIMIT 1",
                        (item.product_id, factura),
                    ).fetchone()
                if exists:
                    skipped += 1
                    continue

                costo_kg = item.total_linea / item.kg if item.kg > 0 else 0.0
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
                    INSERT INTO raw_lots(product_id, lote, proveedor, factura, kg_inicial, kg_saldo, costo_total_gs, costo_kg_gs)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, CAST(ts AS TEXT) AS ts
                    """,
                    (item.product_id, "", proveedor, factura, item.kg, item.kg, item.total_linea, costo_kg),
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
                    ts=row["ts"] or "",
                )
            )

        return FacturaCompraImportResponse(
            inserted=inserted,
            skipped=skipped,
            lotes=lotes,
            message=f"Factura {factura or '-'}: {inserted} lote(s) cargado(s), {skipped} omitido(s).",
        )

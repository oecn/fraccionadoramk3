from __future__ import annotations

import re
import sys
import tempfile
import unicodedata
import math
from pathlib import Path

from fastapi import UploadFile

from core.database import connection
from modules.ordenes_compra.schemas import (
    BolsasPendientesRow,
    BolsasPendientesSummary,
    OrdenCompraDeleteResponse,
    OrdenCompraDetail,
    OrdenCompraImportResponse,
    OrdenCompraItem,
    OrdenCompraRow,
    OrdenCompraStatusResponse,
    PendienteAcumuladoRow,
    PendientesAcumuladosSummary,
)


ROOT_DIR = Path(__file__).resolve().parents[4]
PDFMK10_DIR = ROOT_DIR / "PDFMK10"
if str(PDFMK10_DIR) not in sys.path:
    sys.path.insert(0, str(PDFMK10_DIR))

from parser.pdf_parser import parse_pdf  # noqa: E402


def ajustar_cantidad(descripcion: str, cantidad: float | int | None) -> int | float | None:
    if cantidad is None or cantidad == 0:
        return cantidad

    desc = (descripcion or "").lower()

    m = re.search(r"(\d+)\s*(?:g|gr)\b", desc, flags=re.I)
    if m:
        gramos = int(m.group(1))
        return int(round(cantidad / 20.0 if gramos <= 300 else cantidad / 10.0))

    m = re.search(r"(\d+)\s*kg\b", desc, flags=re.I)
    if m:
        gramos = int(m.group(1)) * 1000
        return int(round(cantidad / 20.0 if gramos <= 300 else cantidad / 10.0))

    m = re.search(r"\*\s*(\d{2,4})\s*\(", desc)
    if m:
        valor = int(m.group(1))
        if valor in (200, 250, 300, 400, 500):
            return int(round(cantidad / 20.0 if valor <= 300 else cantidad / 10.0))

    return int(round(cantidad))


def _norm_text(value: str) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_gramaje(desc: str) -> int | None:
    raw = str(desc or "")
    s = _norm_text(raw)
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


def _match_product_name(desc: str, products: list[tuple[int, str]]) -> tuple[int, str] | None:
    norm_desc = f" {_norm_text(desc)} "
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
        norm_name = _norm_text(name)
        if norm_name and f" {norm_name} " in norm_desc:
            return int(pid), str(name)
        for alias in aliases.get(norm_name, ()):
            if alias in norm_desc:
                return int(pid), str(name)
    return None


def _safe_int(value: object) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _categoria_bolsa(desc: str) -> str:
    base = _norm_text(desc)
    if "arroz" in base:
        return "ARROZ"
    if "gallet" in base and "molid" in base:
        return "GALLETAS"
    return "OTROS"


def _peso_bolsa_estandar(desc: str) -> float:
    tipo = _categoria_bolsa(desc)
    if tipo == "ARROZ":
        return 50.0
    if tipo == "GALLETAS":
        return 25.0
    return 50.0


def _producto_por_desc(desc: str) -> str:
    base = f" {_norm_text(desc)} "
    if " arroz " in base:
        return "Arroz"
    if " azucar " in base:
        return "Azucar"
    if " pororo " in base:
        return "Pororo"
    if " poroto rojo " in base:
        return "Poroto Rojo"
    if " locro " in base and " locrillo " not in base:
        return "Locro"
    if " locrillo " in base:
        return "Locrillo"
    if " lenteja " in base:
        return "Lenteja"
    if (" gallet" in base or " gall " in base) and " molid" in base:
        return "Galleta molida"
    return "Otros"


def _unidades_por_paquete(desc: str) -> int:
    gramaje = _extract_gramaje(desc)
    if gramaje is None:
        return 10
    return 20 if gramaje <= 300 else 10


def _gramaje_total_por_paquete(desc: str) -> int | None:
    gramaje = _extract_gramaje(desc)
    if gramaje is None:
        return None
    return gramaje * _unidades_por_paquete(desc)


def _bolsas_necesarias(desc: str, paquetes_requeridos: object, paquetes_disponibles: int | None) -> int | None:
    if paquetes_requeridos is None or paquetes_disponibles is None:
        return None
    faltantes = _safe_int(paquetes_requeridos) - int(paquetes_disponibles)
    if faltantes <= 0:
        return 0
    gramos_pack = _gramaje_total_por_paquete(desc)
    if not gramos_pack:
        return None
    kg_faltantes = (faltantes * gramos_pack) / 1000.0
    peso_bolsa = _peso_bolsa_estandar(desc)
    if peso_bolsa <= 0:
        return None
    return int(math.ceil(kg_faltantes / peso_bolsa))


class OrdenesCompraRepository:
    def _stock_cache(self) -> tuple[list[tuple[int, str]], dict[tuple[int, int], int]]:
        with connection("fraccionadora") as cn:
            product_rows = cn.execute("SELECT id, name FROM products;").fetchall()
            stock_rows = cn.execute("SELECT product_id, gramaje, paquetes FROM package_stock;").fetchall()
        products = [(int(r["id"]), str(r["name"] or "")) for r in product_rows]
        stock = {
            (int(r["product_id"]), int(r["gramaje"])): int(r["paquetes"] or 0)
            for r in stock_rows
        }
        return products, stock

    def _stock_bolsas_por_producto(self) -> dict[str, float]:
        with connection("fraccionadora") as cn:
            rows = cn.execute(
                """
                SELECT p.name, COALESCE(rs.kg, 0) AS kg
                FROM raw_stock rs
                JOIN products p ON p.id = rs.product_id
                """
            ).fetchall()
        out: dict[str, float] = {}
        for row in rows:
            name = row["name"] or ""
            bolsa = _peso_bolsa_estandar(name)
            if bolsa <= 0:
                continue
            producto = _producto_por_desc(name)
            out[producto] = out.get(producto, 0.0) + (float(row["kg"] or 0) / bolsa)
        return out

    def _stock_state(
        self,
        descripcion: str,
        cantidad: object,
        products: list[tuple[int, str]],
        stock: dict[tuple[int, int], int],
    ) -> tuple[int | None, bool, str, int | None]:
        gramaje = _extract_gramaje(descripcion)
        match = _match_product_name(descripcion, products)
        producto_match = match[1] if match else ""
        disponible = stock.get((match[0], gramaje), 0) if match and gramaje is not None else None
        stock_ok = disponible is not None and disponible >= _safe_int(cantidad)
        return disponible, stock_ok, producto_match, gramaje

    def _pct_listo_por_oc(
        self,
        item_rows: list[object],
        products: list[tuple[int, str]],
        stock: dict[tuple[int, int], int],
    ) -> dict[int, float]:
        stats: dict[int, list[int]] = {}
        for row in item_rows:
            oc_id = int(row["oc_id"])
            stats.setdefault(oc_id, [0, 0])
            stats[oc_id][1] += 1
            _disp, stock_ok, _prod, _gram = self._stock_state(
                row["descripcion"] or "",
                row["cantidad"],
                products,
                stock,
            )
            if stock_ok:
                stats[oc_id][0] += 1
        return {oc_id: (ok / total * 100.0) if total else 100.0 for oc_id, (ok, total) in stats.items()}

    def list_imported(self) -> list[OrdenCompraRow]:
        products, stock = self._stock_cache()
        with connection("pedidos") as cn:
            rows = cn.execute(
                """
                SELECT oc.id,
                       COALESCE(oc.nro_oc, '') AS nro_oc,
                       COALESCE(oc.sucursal, '') AS sucursal,
                       COALESCE(CAST(oc.fecha_pedido AS TEXT), '') AS fecha_pedido,
                       COALESCE(oc.monto_total, 0) AS monto_total,
                       COALESCE(oc.completada, 0) AS completada,
                       COALESCE(CAST(oc.created_at AS TEXT), '') AS created_at,
                       COUNT(oi.id) AS items_count
                FROM orden_compra oc
                LEFT JOIN orden_item oi ON oi.oc_id = oc.id
                GROUP BY oc.id, oc.nro_oc, oc.sucursal, oc.fecha_pedido, oc.monto_total, oc.completada, oc.created_at
                ORDER BY COALESCE(oc.completada, 0) ASC,
                         oc.fecha_pedido ASC NULLS LAST,
                         oc.created_at DESC NULLS LAST,
                         oc.id DESC
                LIMIT 100
                """
            ).fetchall()
            oc_ids = [int(r["id"]) for r in rows]
            item_rows = []
            if oc_ids:
                placeholders = ",".join(["%s"] * len(oc_ids))
                item_rows = cn.execute(
                    f"""
                    SELECT oc_id, descripcion, cantidad
                    FROM orden_item
                    WHERE oc_id IN ({placeholders})
                      AND COALESCE(enviado, 0) = 0
                      AND descripcion IS NOT NULL
                    """,
                    oc_ids,
                ).fetchall()

        pct_by_oc = self._pct_listo_por_oc(item_rows, products, stock)

        return [
            OrdenCompraRow(
                oc_id=int(r["id"]),
                nro_oc=r["nro_oc"] or "",
                sucursal=r["sucursal"] or "",
                fecha_pedido=r["fecha_pedido"] or "",
                monto_total=float(r["monto_total"] or 0),
                items_count=int(r["items_count"] or 0),
                completada=bool(r["completada"]),
                created_at=r["created_at"] or "",
                pct_listo_envio=pct_by_oc.get(int(r["id"]), 100.0),
            )
            for r in rows
        ]

    def detail(self, oc_id: int) -> OrdenCompraDetail:
        products, stock = self._stock_cache()
        with connection("pedidos") as cn:
            row = cn.execute(
                """
                SELECT oc.id,
                       COALESCE(oc.nro_oc, '') AS nro_oc,
                       COALESCE(oc.sucursal, '') AS sucursal,
                       COALESCE(CAST(oc.fecha_pedido AS TEXT), '') AS fecha_pedido,
                       COALESCE(oc.monto_total, 0) AS monto_total,
                       COALESCE(oc.completada, 0) AS completada,
                       COALESCE(CAST(oc.created_at AS TEXT), '') AS created_at,
                       COUNT(oi.id) AS items_count
                FROM orden_compra oc
                LEFT JOIN orden_item oi ON oi.oc_id = oc.id
                WHERE oc.id = %s
                GROUP BY oc.id, oc.nro_oc, oc.sucursal, oc.fecha_pedido, oc.monto_total, oc.completada, oc.created_at
                """,
                (oc_id,),
            ).fetchone()
            if not row:
                raise ValueError("OC no encontrada.")

            item_rows = cn.execute(
                """
                SELECT COALESCE(linea, 0) AS linea,
                       COALESCE(descripcion, '') AS descripcion,
                       cantidad,
                       unidad
                FROM orden_item
                WHERE oc_id = %s
                ORDER BY linea, id
                """,
                (oc_id,),
            ).fetchall()

        items: list[OrdenCompraItem] = []
        for r in item_rows:
            descripcion = r["descripcion"] or ""
            cantidad = r["cantidad"]
            gramaje = _extract_gramaje(descripcion)
            match = _match_product_name(descripcion, products)
            producto_match = match[1] if match else ""
            disponible, stock_ok, producto_match, gramaje = self._stock_state(
                descripcion,
                cantidad,
                products,
                stock,
            )
            items.append(
                OrdenCompraItem(
                    linea=int(r["linea"] or 0),
                    descripcion=descripcion,
                    cantidad=cantidad,
                    unidad=r["unidad"],
                    stock_disponible=disponible,
                    stock_ok=stock_ok,
                    producto_match=producto_match,
                    gramaje=gramaje,
                )
            )
        items_ok = sum(1 for item in items if item.stock_ok)
        pct_listo = (items_ok / len(items) * 100.0) if items else 100.0

        return OrdenCompraDetail(
            oc_id=int(row["id"]),
            nro_oc=row["nro_oc"] or "",
            sucursal=row["sucursal"] or "",
            fecha_pedido=row["fecha_pedido"] or "",
            monto_total=float(row["monto_total"] or 0),
            items_count=int(row["items_count"] or 0),
            completada=bool(row["completada"]),
            created_at=row["created_at"] or "",
            pct_listo_envio=pct_listo,
            items=items,
        )

    def pendientes_acumulados(self) -> PendientesAcumuladosSummary:
        products, stock = self._stock_cache()
        with connection("pedidos") as cn:
            rows = cn.execute(
                """
                SELECT TRIM(CAST(oi.descripcion AS TEXT)) AS descripcion,
                       COALESCE(SUM(oi.cantidad), 0) AS necesario
                FROM orden_item oi
                JOIN orden_compra oc ON oc.id = oi.oc_id
                WHERE COALESCE(oc.completada, 0) = 0
                  AND COALESCE(oi.enviado, 0) = 0
                  AND oi.descripcion IS NOT NULL
                GROUP BY TRIM(CAST(oi.descripcion AS TEXT))
                HAVING COALESCE(SUM(oi.cantidad), 0) > 0
                ORDER BY TRIM(CAST(oi.descripcion AS TEXT))
                """
            ).fetchall()
            totals = cn.execute(
                """
                SELECT COUNT(*) AS total_ocs,
                       COALESCE(SUM(COALESCE(monto_total, 0)), 0) AS total_monto
                FROM orden_compra
                WHERE COALESCE(completada, 0) = 0
                """
            ).fetchone()

        out: list[PendienteAcumuladoRow] = []
        for row in rows:
            descripcion = row["descripcion"] or ""
            necesario = row["necesario"] or 0
            gramaje = _extract_gramaje(descripcion)
            match = _match_product_name(descripcion, products)
            producto_match = match[1] if match else ""
            disponible = stock.get((match[0], gramaje), 0) if match and gramaje is not None else None
            stock_ok = disponible is not None and disponible >= _safe_int(necesario)
            out.append(
                PendienteAcumuladoRow(
                    descripcion=descripcion,
                    necesario=necesario,
                    stock_disponible=disponible,
                    stock_ok=stock_ok,
                    producto_match=producto_match,
                    gramaje=gramaje,
                )
            )

        out.sort(key=lambda r: (not r.stock_ok, r.producto_match or r.descripcion, r.gramaje or 999999, r.descripcion))
        total_items = len(out)
        items_ok = sum(1 for row in out if row.stock_ok)
        return PendientesAcumuladosSummary(
            total_ocs=int(totals["total_ocs"] or 0),
            total_monto=float(totals["total_monto"] or 0),
            total_items=total_items,
            items_ok=items_ok,
            items_falta=sum(1 for row in out if not row.stock_ok),
            pct_listo_envio=(items_ok / total_items * 100.0) if total_items > 0 else 100.0,
            rows=out,
        )

    def bolsas_pendientes(self) -> BolsasPendientesSummary:
        pendientes = self.pendientes_acumulados()
        stock_bolsas = self._stock_bolsas_por_producto()
        productos = ["Arroz", "Azucar", "Galleta molida", "Pororo", "Poroto Rojo", "Locro"]
        necesarias = {producto: 0 for producto in productos}

        for row in pendientes.rows:
            bolsas = _bolsas_necesarias(row.descripcion, row.necesario, row.stock_disponible)
            if bolsas is None:
                continue
            producto = row.producto_match or _producto_por_desc(row.descripcion)
            if producto not in necesarias:
                continue
            necesarias[producto] += bolsas

        rows = [
            BolsasPendientesRow(
                producto=producto,
                bolsa_kg=_peso_bolsa_estandar(producto),
                necesarias=int(necesarias.get(producto, 0)),
                disponibles=float(stock_bolsas.get(producto, 0.0)),
                stock_ok=float(stock_bolsas.get(producto, 0.0)) >= int(necesarias.get(producto, 0)),
            )
            for producto in productos
        ]
        return BolsasPendientesSummary(rows=rows)

    def import_pdf(self, upload: UploadFile) -> OrdenCompraImportResponse:
        filename = upload.filename or ""
        if not filename.lower().endswith(".pdf"):
            raise ValueError("Seleccione un archivo PDF.")

        tmp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                tmp.write(upload.file.read())
                tmp_path = Path(tmp.name)

            result = parse_pdf(tmp_path)
        finally:
            upload.file.close()
            if tmp_path and tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

        meta = result.get("meta") or {}
        raw_items = result.get("items") or []
        nro_oc = (meta.get("nro_oc") or "").strip()
        if not nro_oc:
            raise ValueError("No se pudo detectar el numero de OC en el PDF.")
        if not raw_items:
            raise ValueError("No se detectaron items en el PDF.")

        monto_total = meta.get("monto_total")
        with connection("pedidos") as cn:
            row = cn.execute(
                """
                INSERT INTO orden_compra (nro_oc, sucursal, fecha_pedido, raw_text, monto_total)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (nro_oc) DO UPDATE
                SET sucursal = EXCLUDED.sucursal,
                    fecha_pedido = EXCLUDED.fecha_pedido,
                    raw_text = EXCLUDED.raw_text,
                    monto_total = EXCLUDED.monto_total
                RETURNING id
                """,
                (
                    nro_oc,
                    meta.get("sucursal"),
                    meta.get("fecha_pedido"),
                    meta.get("raw_text"),
                    monto_total,
                ),
            ).fetchone()
            oc_id = int(row["id"])

            cn.execute("DELETE FROM orden_item WHERE oc_id = %s", (oc_id,))
            items: list[OrdenCompraItem] = []
            for idx, item in enumerate(raw_items, start=1):
                descripcion = item.get("descripcion") or ""
                cantidad = ajustar_cantidad(descripcion, item.get("cantidad"))
                unidad = item.get("unidad")
                cn.execute(
                    """
                    INSERT INTO orden_item (oc_id, linea, descripcion, cantidad, unidad)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (oc_id, idx, descripcion, cantidad, unidad),
                )
                items.append(
                    OrdenCompraItem(
                        linea=idx,
                        descripcion=descripcion,
                        cantidad=cantidad,
                        unidad=unidad,
                    )
                )

        return OrdenCompraImportResponse(
            oc_id=oc_id,
            nro_oc=nro_oc,
            sucursal=meta.get("sucursal") or "",
            fecha_pedido=meta.get("fecha_pedido") or "",
            monto_total=float(monto_total) if monto_total is not None else None,
            items=items,
            message=f"OC {nro_oc} importada con {len(items)} items.",
        )

    def delete(self, oc_id: int) -> OrdenCompraDeleteResponse:
        with connection("pedidos") as cn:
            row = cn.execute(
                "SELECT nro_oc FROM orden_compra WHERE id = %s",
                (oc_id,),
            ).fetchone()
            if not row:
                raise ValueError("OC no encontrada.")

            cn.execute("DELETE FROM orden_item WHERE oc_id = %s", (oc_id,))
            cn.execute("DELETE FROM orden_compra WHERE id = %s", (oc_id,))

        nro_oc = row["nro_oc"] or f"ID {oc_id}"
        return OrdenCompraDeleteResponse(
            oc_id=oc_id,
            deleted=True,
            message=f"OC {nro_oc} eliminada.",
        )

    def set_status(self, oc_id: int, completada: bool) -> OrdenCompraStatusResponse:
        with connection("pedidos") as cn:
            row = cn.execute("SELECT nro_oc FROM orden_compra WHERE id = %s", (oc_id,)).fetchone()
            if not row:
                raise ValueError("OC no encontrada.")

            flag = 1 if completada else 0
            cn.execute("UPDATE orden_compra SET completada = %s WHERE id = %s", (flag, oc_id))
            cn.execute("UPDATE orden_item SET enviado = %s WHERE oc_id = %s", (flag, oc_id))

        nro_oc = row["nro_oc"] or f"ID {oc_id}"
        estado = "entregada" if completada else "pendiente"
        return OrdenCompraStatusResponse(oc_id=oc_id, completada=completada, message=f"OC {nro_oc} marcada como {estado}.")

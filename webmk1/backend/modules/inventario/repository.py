from __future__ import annotations

import unicodedata

from core.database import connection
from modules.inventario.schemas import InventoryLotRow, InventoryPackageRow, InventoryRawRow, InventorySummary


PRODUCT_ORDER = [
    "arroz",
    "azucar",
    "pororo",
    "poroto rojo",
    "galleta molida",
    "locro",
    "locrillo",
    "lenteja",
]


def _normalize_product_key(name: str) -> str:
    text = (name or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    if "azucar" in text or "azukita" in text:
        return "azucar"
    if "arroz" in text:
        return "arroz"
    if "pororo" in text:
        return "pororo"
    if "poroto" in text:
        return "poroto rojo"
    if "gallet" in text or "molida" in text:
        return "galleta molida"
    if "locrillo" in text:
        return "locrillo"
    if "locro" in text:
        return "locro"
    if "lenteja" in text:
        return "lenteja"
    return text


def _product_order_idx(name: str) -> int:
    key = _normalize_product_key(name)
    try:
        return PRODUCT_ORDER.index(key)
    except ValueError:
        return 10_000


def _gram_order_idx(gramaje: int) -> int:
    order = {200: 0, 250: 1, 400: 2, 500: 3, 800: 4, 1000: 5}
    return order.get(int(gramaje), 999)


def _unidades_por_paquete(gramaje: int) -> int:
    return 20 if gramaje <= 250 else 10


class InventarioRepository:
    def summary(self) -> InventorySummary:
        with connection("fraccionadora") as cn:
            raw_rows = cn.execute(
                """
                SELECT p.id AS product_id, p.name AS producto, COALESCE(rs.kg, 0) AS kg,
                       COUNT(rl.id) AS lotes_abiertos,
                       COALESCE(SUM(rl.kg_saldo * rl.costo_kg_gs), 0) AS valor_stock_gs
                FROM products p
                LEFT JOIN raw_stock rs ON rs.product_id = p.id
                LEFT JOIN raw_lots rl
                  ON rl.product_id = p.id
                 AND rl.kg_saldo > 1e-9
                 AND COALESCE(rl.cerrado, 0) = 0
                GROUP BY p.id, p.name, rs.kg
                ORDER BY p.name
                """
            ).fetchall()
            package_rows = cn.execute(
                """
                SELECT ps.product_id, p.name AS producto, ps.gramaje, ps.paquetes,
                       pp.price_gs, pp.iva
                FROM package_stock ps
                JOIN products p ON p.id = ps.product_id
                LEFT JOIN package_prices pp ON pp.product_id = ps.product_id AND pp.gramaje = ps.gramaje
                WHERE ps.paquetes <> 0
                ORDER BY p.name, ps.gramaje
                """
            ).fetchall()
            lot_rows = cn.execute(
                """
                SELECT rl.id, rl.product_id, p.name AS producto,
                       COALESCE(rl.lote, '') AS lote,
                       COALESCE(rl.proveedor, '') AS proveedor,
                       COALESCE(rl.factura, '') AS factura,
                       COALESCE(rl.kg_saldo, 0) AS kg_saldo,
                       COALESCE(rl.costo_kg_gs, 0) AS costo_kg_gs,
                       CAST(rl.ts AS TEXT) AS ts
                FROM raw_lots rl
                JOIN products p ON p.id = rl.product_id
                WHERE rl.kg_saldo > 1e-9 AND COALESCE(rl.cerrado, 0) = 0
                ORDER BY p.name, rl.ts DESC, rl.id DESC
                """
            ).fetchall()

        raw_stock = [
            InventoryRawRow(
                product_id=int(r["product_id"]),
                producto=r["producto"] or "",
                kg=float(r["kg"] or 0),
                bolsas_50=float(r["kg"] or 0) / 50.0,
                bolsas_25=float(r["kg"] or 0) / 25.0,
                lotes_abiertos=int(r["lotes_abiertos"] or 0),
                costo_promedio_gs=(float(r["valor_stock_gs"] or 0) / float(r["kg"] or 0)) if float(r["kg"] or 0) else 0,
                valor_stock_gs=float(r["valor_stock_gs"] or 0),
            )
            for r in raw_rows
        ]
        package_stock = [
            InventoryPackageRow(
                product_id=int(r["product_id"]),
                producto=r["producto"] or "",
                gramaje=int(r["gramaje"] or 0),
                paquetes=int(r["paquetes"] or 0),
                unidades=int(r["paquetes"] or 0) * _unidades_por_paquete(int(r["gramaje"] or 0)),
                price_gs=float(r["price_gs"]) if r["price_gs"] is not None else None,
                iva=int(r["iva"]) if r["iva"] is not None else None,
                valor_venta_gs=float(r["price_gs"] or 0) * int(r["paquetes"] or 0),
            )
            for r in package_rows
        ]
        package_stock = sorted(
            package_stock,
            key=lambda r: (_product_order_idx(r.producto), r.producto, _gram_order_idx(r.gramaje), r.gramaje),
        )
        lotes = [
            InventoryLotRow(
                id=int(r["id"]),
                product_id=int(r["product_id"]),
                producto=r["producto"] or "",
                lote=r["lote"] or "",
                proveedor=r["proveedor"] or "",
                factura=r["factura"] or "",
                kg_saldo=float(r["kg_saldo"] or 0),
                costo_kg_gs=float(r["costo_kg_gs"] or 0),
                valor_saldo_gs=float(r["kg_saldo"] or 0) * float(r["costo_kg_gs"] or 0),
                ts=r["ts"] or "",
            )
            for r in lot_rows
        ]

        return InventorySummary(
            raw_stock=raw_stock,
            package_stock=package_stock,
            lotes_abiertos=lotes,
            total_raw_kg=sum(r.kg for r in raw_stock),
            total_raw_valor_gs=sum(r.valor_stock_gs for r in raw_stock),
            total_paquetes=sum(r.paquetes for r in package_stock),
            total_unidades=sum(r.unidades for r in package_stock),
            total_venta_gs=sum(r.valor_venta_gs for r in package_stock),
        )

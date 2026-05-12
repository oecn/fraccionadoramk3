from __future__ import annotations

import unicodedata

from core.database import connection
from modules.productos.schemas import PrecioHistoryResponse, PrecioHistoryRow, PrecioRow, PrecioUpdate, ProductoPrecioSummary


PRODUCT_ORDER = ["arroz", "azucar", "pororo", "poroto rojo", "galleta molida", "locro", "locrillo", "lenteja"]


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


def _gramajes(product_name: str) -> list[int]:
    key = _normalize_product_key(product_name)
    if key in {"arroz", "azucar"}:
        return [250, 500, 1000]
    if key == "lenteja":
        return [250, 500]
    return [200, 400, 800]


def _gram_order_idx(gramaje: int) -> int:
    return {200: 0, 250: 1, 400: 2, 500: 3, 800: 4, 1000: 5}.get(int(gramaje), 999)


class ProductosRepository:
    def precios(self) -> ProductoPrecioSummary:
        with connection("fraccionadora") as cn:
            products = cn.execute("SELECT id, name FROM products ORDER BY name").fetchall()
            prices = cn.execute("SELECT product_id, gramaje, price_gs, iva FROM package_prices").fetchall()
            stock = cn.execute("SELECT product_id, gramaje, paquetes FROM package_stock").fetchall()

        price_map = {(int(r["product_id"]), int(r["gramaje"])): r for r in prices}
        stock_map = {(int(r["product_id"]), int(r["gramaje"])): int(r["paquetes"] or 0) for r in stock}
        rows: list[PrecioRow] = []
        for product in products:
            pid = int(product["id"])
            name = product["name"] or ""
            grams = set(_gramajes(name))
            grams.update(g for p, g in price_map if p == pid)
            grams.update(g for p, g in stock_map if p == pid)
            for gramaje in sorted(grams, key=lambda g: (_gram_order_idx(g), g)):
                price = price_map.get((pid, gramaje))
                rows.append(
                    PrecioRow(
                        product_id=pid,
                        producto=name,
                        gramaje=gramaje,
                        price_gs=float(price["price_gs"] or 0) if price else 0.0,
                        iva=int(price["iva"] or 10) if price else 10,
                        paquetes_stock=stock_map.get((pid, gramaje), 0),
                    )
                )
        rows.sort(key=lambda r: (_product_order_idx(r.producto), r.producto, _gram_order_idx(r.gramaje), r.gramaje))
        return ProductoPrecioSummary(rows=rows)

    def update_precio(self, payload: PrecioUpdate) -> PrecioRow:
        if payload.price_gs < 0 or payload.iva not in (5, 10):
            raise ValueError("Precio/IVA invalidos.")
        with connection("fraccionadora") as cn:
            product = cn.execute("SELECT name FROM products WHERE id = %s", (payload.product_id,)).fetchone()
            if not product:
                raise ValueError("Producto no encontrado.")
            old = cn.execute(
                "SELECT price_gs, iva FROM package_prices WHERE product_id = %s AND gramaje = %s",
                (payload.product_id, payload.gramaje),
            ).fetchone()
            changed = not old or float(old["price_gs"] or 0) != payload.price_gs or int(old["iva"] or 0) != payload.iva
            cn.execute(
                """
                INSERT INTO package_prices(product_id, gramaje, price_gs, iva)
                VALUES(%s, %s, %s, %s)
                ON CONFLICT(product_id, gramaje)
                DO UPDATE SET price_gs = excluded.price_gs, iva = excluded.iva
                """,
                (payload.product_id, payload.gramaje, payload.price_gs, payload.iva),
            )
            if changed:
                cn.execute(
                    """
                    INSERT INTO package_price_history(product_id, gramaje, price_gs, iva)
                    VALUES(%s, %s, %s, %s)
                    """,
                    (payload.product_id, payload.gramaje, payload.price_gs, payload.iva),
                )
            stock = cn.execute(
                "SELECT paquetes FROM package_stock WHERE product_id = %s AND gramaje = %s",
                (payload.product_id, payload.gramaje),
            ).fetchone()
        return PrecioRow(
            product_id=payload.product_id,
            producto=product["name"] or "",
            gramaje=payload.gramaje,
            price_gs=payload.price_gs,
            iva=payload.iva,
            paquetes_stock=int((stock or {}).get("paquetes") or 0),
        )

    def history(self, product_id: int, gramaje: int) -> PrecioHistoryResponse:
        with connection("fraccionadora") as cn:
            product = cn.execute("SELECT name FROM products WHERE id = %s", (product_id,)).fetchone()
            if not product:
                raise ValueError("Producto no encontrado.")
            rows = cn.execute(
                """
                SELECT CAST(ts AS TEXT) AS fecha, price_gs, iva
                FROM package_price_history
                WHERE product_id = %s AND gramaje = %s
                ORDER BY ts ASC, id ASC
                """,
                (product_id, gramaje),
            ).fetchall()
            if not rows:
                rows = cn.execute(
                    """
                    SELECT CAST(CURRENT_TIMESTAMP AS TEXT) AS fecha, price_gs, iva
                    FROM package_prices
                    WHERE product_id = %s AND gramaje = %s
                    """,
                    (product_id, gramaje),
                ).fetchall()
        return PrecioHistoryResponse(
            product_id=product_id,
            producto=product["name"] or "",
            gramaje=gramaje,
            rows=[
                PrecioHistoryRow(
                    fecha=r["fecha"] or "",
                    price_gs=float(r["price_gs"] or 0),
                    iva=int(r["iva"] or 10),
                )
                for r in rows
            ],
        )

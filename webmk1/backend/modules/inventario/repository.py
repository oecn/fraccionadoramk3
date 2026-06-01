from __future__ import annotations

import unicodedata

from core.database import connection
from modules.inventario.schemas import (
    InventoryAdjustmentUpdate,
    InventoryLotRow,
    InventoryPackageRow,
    InventoryRawRow,
    InventorySummary,
    RawStockAlertsUpdate,
)


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


def _gramajes(product_name: str) -> list[int]:
    key = _normalize_product_key(product_name)
    if key in {"arroz", "azucar"}:
        return [250, 500, 1000]
    if key == "lenteja":
        return [250, 500]
    if key in {"poroto rojo", "locro", "locrillo"}:
        return [200, 400]
    return [200, 400, 800]


def _unidades_por_paquete(gramaje: int) -> int:
    return 20 if gramaje <= 250 else 10


def _alert_bag_kg(name: str) -> float:
    return 25.0 if _normalize_product_key(name) == "galleta molida" else 50.0


def _bolsas_25(name: str, kg: float) -> float:
    return kg / 25.0 if _normalize_product_key(name) == "galleta molida" else 0.0


class InventarioRepository:
    def _ensure_schema(self, cn) -> None:
        cn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_stock_alerts(
                product_id INTEGER PRIMARY KEY,
                min_kg REAL NOT NULL,
                reposicion_bolsas REAL,
                proveedor_whatsapp TEXT NOT NULL DEFAULT '',
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cn.execute("ALTER TABLE raw_stock_alerts ADD COLUMN IF NOT EXISTS proveedor_whatsapp TEXT NOT NULL DEFAULT ''")
        cn.execute("ALTER TABLE raw_stock_alerts ADD COLUMN IF NOT EXISTS reposicion_bolsas REAL")
        cn.execute(
            """
            CREATE TABLE IF NOT EXISTS inventory_adjustments(
                id BIGSERIAL PRIMARY KEY,
                kind TEXT NOT NULL,
                product_id INTEGER NOT NULL,
                gramaje INTEGER,
                old_qty REAL NOT NULL,
                new_qty REAL NOT NULL,
                motivo TEXT NOT NULL DEFAULT '',
                ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def summary(self) -> InventorySummary:
        with connection("fraccionadora") as cn:
            self._ensure_schema(cn)
            raw_rows = cn.execute(
                """
                SELECT p.id AS product_id, p.name AS producto, COALESCE(rs.kg, 0) AS kg,
                       COUNT(rl.id) AS lotes_abiertos,
                       COALESCE(SUM(rl.kg_saldo * rl.costo_kg_gs), 0) AS valor_stock_gs,
                       rsa.min_kg AS alerta_min_kg,
                       rsa.reposicion_bolsas AS reposicion_bolsas,
                       COALESCE(rsa.proveedor_whatsapp, '') AS proveedor_whatsapp
                FROM products p
                LEFT JOIN raw_stock rs ON rs.product_id = p.id
                LEFT JOIN raw_stock_alerts rsa ON rsa.product_id = p.id
                LEFT JOIN raw_lots rl
                  ON rl.product_id = p.id
                 AND rl.kg_saldo > 1e-9
                 AND COALESCE(rl.cerrado, 0) = 0
                GROUP BY p.id, p.name, rs.kg, rsa.min_kg, rsa.reposicion_bolsas, rsa.proveedor_whatsapp
                ORDER BY p.name
                """
            ).fetchall()
            package_rows = cn.execute(
                """
                SELECT product_id, gramaje, paquetes
                FROM package_stock
                """
            ).fetchall()
            price_rows = cn.execute(
                """
                SELECT product_id, gramaje, price_gs, iva
                FROM package_prices
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
                alerta_min_kg=float(r["alerta_min_kg"]) if r["alerta_min_kg"] is not None else None,
                alerta_min_bolsas=(
                    float(r["alerta_min_kg"]) / _alert_bag_kg(r["producto"] or "")
                    if r["alerta_min_kg"] is not None
                    else None
                ),
                reposicion_bolsas=float(r["reposicion_bolsas"]) if r["reposicion_bolsas"] is not None else None,
                alerta_bolsa_kg=_alert_bag_kg(r["producto"] or ""),
                alerta_estado=self._alert_state(float(r["kg"] or 0), float(r["alerta_min_kg"]) if r["alerta_min_kg"] is not None else None),
                proveedor_whatsapp=r["proveedor_whatsapp"] or "",
                bolsas_50=float(r["kg"] or 0) / 50.0,
                bolsas_25=_bolsas_25(r["producto"] or "", float(r["kg"] or 0)),
                lotes_abiertos=int(r["lotes_abiertos"] or 0),
                costo_promedio_gs=(float(r["valor_stock_gs"] or 0) / float(r["kg"] or 0)) if float(r["kg"] or 0) else 0,
                valor_stock_gs=float(r["valor_stock_gs"] or 0),
            )
            for r in raw_rows
        ]
        stock_map = {(int(r["product_id"]), int(r["gramaje"])): int(r["paquetes"] or 0) for r in package_rows}
        price_map = {(int(r["product_id"]), int(r["gramaje"])): r for r in price_rows}
        package_stock: list[InventoryPackageRow] = []
        for product in raw_rows:
            product_id = int(product["product_id"])
            producto = product["producto"] or ""
            gramajes = set(_gramajes(producto))
            gramajes.update(g for pid, g in stock_map if pid == product_id)
            gramajes.update(g for pid, g in price_map if pid == product_id)
            for gramaje in gramajes:
                paquetes = stock_map.get((product_id, gramaje), 0)
                price = price_map.get((product_id, gramaje))
                price_gs = float(price["price_gs"]) if price and price["price_gs"] is not None else None
                package_stock.append(
                    InventoryPackageRow(
                        product_id=product_id,
                        producto=producto,
                        gramaje=gramaje,
                        paquetes=paquetes,
                        unidades=paquetes * _unidades_por_paquete(gramaje),
                        price_gs=price_gs,
                        iva=int(price["iva"]) if price and price["iva"] is not None else None,
                        valor_venta_gs=float(price_gs or 0) * paquetes,
                    )
                )
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
            raw_alerts_count=sum(1 for r in raw_stock if r.alerta_estado == "bajo"),
            total_raw_kg=sum(r.kg for r in raw_stock),
            total_raw_valor_gs=sum(r.valor_stock_gs for r in raw_stock),
            total_paquetes=sum(r.paquetes for r in package_stock),
            total_unidades=sum(r.unidades for r in package_stock),
            total_venta_gs=sum(r.valor_venta_gs for r in package_stock),
        )

    def update_alerts(self, payload: RawStockAlertsUpdate) -> InventorySummary:
        with connection("fraccionadora") as cn:
            self._ensure_schema(cn)
            for alert in payload.alerts:
                phone = self._clean_phone(alert.proveedor_whatsapp)
                if alert.min_kg is None or float(alert.min_kg) <= 0:
                    cn.execute("DELETE FROM raw_stock_alerts WHERE product_id = %s", (alert.product_id,))
                    continue
                cn.execute(
                    """
                    INSERT INTO raw_stock_alerts(product_id, min_kg, reposicion_bolsas, proveedor_whatsapp, updated_at)
                    VALUES(%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT(product_id) DO UPDATE SET
                        min_kg = excluded.min_kg,
                        reposicion_bolsas = excluded.reposicion_bolsas,
                        proveedor_whatsapp = excluded.proveedor_whatsapp,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (alert.product_id, float(alert.min_kg), float(alert.reposicion_bolsas or 0) or None, phone),
                )
        return self.summary()

    def adjust_inventory(self, payload: InventoryAdjustmentUpdate) -> InventorySummary:
        motivo = (payload.motivo or "").strip()
        with connection("fraccionadora") as cn:
            self._ensure_schema(cn)
            for item in payload.raw_stock:
                new_kg = max(float(item.kg or 0), 0.0)
                current = cn.execute(
                    "SELECT kg FROM raw_stock WHERE product_id = %s",
                    (item.product_id,),
                ).fetchone()
                old_kg = float((current or {}).get("kg") or 0)
                cn.execute(
                    """
                    INSERT INTO raw_stock(product_id, kg)
                    VALUES(%s, %s)
                    ON CONFLICT(product_id) DO UPDATE SET kg = excluded.kg
                    """,
                    (item.product_id, new_kg),
                )
                if abs(old_kg - new_kg) > 1e-9:
                    cn.execute(
                        """
                        INSERT INTO inventory_adjustments(kind, product_id, old_qty, new_qty, motivo)
                        VALUES('raw', %s, %s, %s, %s)
                        """,
                        (item.product_id, old_kg, new_kg, motivo),
                    )

            for item in payload.package_stock:
                new_paquetes = max(int(item.paquetes or 0), 0)
                current = cn.execute(
                    "SELECT paquetes FROM package_stock WHERE product_id = %s AND gramaje = %s",
                    (item.product_id, item.gramaje),
                ).fetchone()
                old_paquetes = int((current or {}).get("paquetes") or 0)
                cn.execute(
                    """
                    INSERT INTO package_stock(product_id, gramaje, paquetes)
                    VALUES(%s, %s, %s)
                    ON CONFLICT(product_id, gramaje) DO UPDATE SET paquetes = excluded.paquetes
                    """,
                    (item.product_id, item.gramaje, new_paquetes),
                )
                if old_paquetes != new_paquetes:
                    cn.execute(
                        """
                        INSERT INTO inventory_adjustments(kind, product_id, gramaje, old_qty, new_qty, motivo)
                        VALUES('package', %s, %s, %s, %s, %s)
                        """,
                        (item.product_id, item.gramaje, old_paquetes, new_paquetes, motivo),
                    )
        return self.summary()

    def _alert_state(self, kg: float, min_kg: float | None) -> str:
        if min_kg is None or min_kg <= 0:
            return "normal"
        if kg <= min_kg:
            return "bajo"
        if kg <= min_kg * 1.2:
            return "cerca"
        return "normal"

    def _clean_phone(self, value: str) -> str:
        digits = "".join(ch for ch in str(value or "") if ch.isdigit())
        if digits.startswith("0"):
            digits = "595" + digits[1:]
        return digits

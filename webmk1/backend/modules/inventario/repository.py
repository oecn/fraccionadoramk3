from __future__ import annotations

import unicodedata

from core.database import connection
from modules.inventario.schemas import (
    FilmStockAlertsUpdate,
    InventoryAdjustmentUpdate,
    InventoryControlAuditCreate,
    InventoryFilmRow,
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
        cn.execute(
            """
            CREATE TABLE IF NOT EXISTS weekly_inventory_counts(
                id BIGSERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL,
                sistema_kg REAL NOT NULL,
                planta_bolsas REAL NOT NULL,
                bolsa_kg REAL NOT NULL,
                planta_kg REAL NOT NULL,
                diferencia_kg REAL NOT NULL,
                motivo TEXT NOT NULL DEFAULT '',
                ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cn.execute(
            """
            CREATE TABLE IF NOT EXISTS weekly_package_counts(
                id BIGSERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL,
                gramaje INTEGER NOT NULL,
                sistema_paquetes INTEGER NOT NULL,
                planta_paquetes INTEGER NOT NULL,
                diferencia_paquetes INTEGER NOT NULL,
                motivo TEXT NOT NULL DEFAULT '',
                ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cn.execute(
            """
            CREATE TABLE IF NOT EXISTS film_stock(
                product_id INTEGER NOT NULL,
                gramaje INTEGER NOT NULL,
                rollos INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(product_id, gramaje)
            )
            """
        )
        cn.execute(
            """
            CREATE TABLE IF NOT EXISTS film_stock_alerts(
                product_id INTEGER NOT NULL,
                gramaje INTEGER NOT NULL,
                min_rollos INTEGER NOT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(product_id, gramaje)
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
                       COALESCE(rsa.proveedor_whatsapp, '') AS proveedor_whatsapp,
                       CASE
                         WHEN wc.ts IS NULL OR wc.bolsa_kg <= 0 THEN wc.planta_bolsas
                         ELSE (
                           wc.planta_kg
                           + COALESCE((SELECT SUM(rl2.kg_inicial) FROM raw_lots rl2 WHERE rl2.product_id = p.id AND rl2.ts > wc.ts), 0)
                           - COALESCE((SELECT SUM(f.kg_consumidos) FROM fractionations f WHERE f.product_id = p.id AND f.ts > wc.ts), 0)
                           - COALESCE((SELECT SUM(bs.kg_total) FROM bag_sales bs WHERE bs.product_id = p.id AND bs.ts > wc.ts), 0)
                           + COALESCE((SELECT SUM(ia.new_qty - ia.old_qty) FROM inventory_adjustments ia WHERE ia.kind = 'raw' AND ia.product_id = p.id AND ia.ts > wc.ts), 0)
                         ) / wc.bolsa_kg
                       END AS conteo_planta_bolsas,
                       CASE
                         WHEN wc.ts IS NULL THEN wc.planta_kg
                         ELSE (
                           wc.planta_kg
                           + COALESCE((SELECT SUM(rl2.kg_inicial) FROM raw_lots rl2 WHERE rl2.product_id = p.id AND rl2.ts > wc.ts), 0)
                           - COALESCE((SELECT SUM(f.kg_consumidos) FROM fractionations f WHERE f.product_id = p.id AND f.ts > wc.ts), 0)
                           - COALESCE((SELECT SUM(bs.kg_total) FROM bag_sales bs WHERE bs.product_id = p.id AND bs.ts > wc.ts), 0)
                           + COALESCE((SELECT SUM(ia.new_qty - ia.old_qty) FROM inventory_adjustments ia WHERE ia.kind = 'raw' AND ia.product_id = p.id AND ia.ts > wc.ts), 0)
                         )
                       END AS conteo_planta_kg,
                       CASE
                         WHEN wc.ts IS NULL THEN wc.diferencia_kg
                         ELSE (
                           wc.planta_kg
                           + COALESCE((SELECT SUM(rl2.kg_inicial) FROM raw_lots rl2 WHERE rl2.product_id = p.id AND rl2.ts > wc.ts), 0)
                           - COALESCE((SELECT SUM(f.kg_consumidos) FROM fractionations f WHERE f.product_id = p.id AND f.ts > wc.ts), 0)
                           - COALESCE((SELECT SUM(bs.kg_total) FROM bag_sales bs WHERE bs.product_id = p.id AND bs.ts > wc.ts), 0)
                           + COALESCE((SELECT SUM(ia.new_qty - ia.old_qty) FROM inventory_adjustments ia WHERE ia.kind = 'raw' AND ia.product_id = p.id AND ia.ts > wc.ts), 0)
                           - COALESCE(rs.kg, 0)
                         )
                       END AS conteo_diferencia_kg,
                       CAST(wc.ts AS TEXT) AS conteo_fecha
                FROM products p
                LEFT JOIN raw_stock rs ON rs.product_id = p.id
                LEFT JOIN raw_stock_alerts rsa ON rsa.product_id = p.id
                LEFT JOIN LATERAL (
                    SELECT planta_bolsas, bolsa_kg, planta_kg, diferencia_kg, ts
                    FROM weekly_inventory_counts wic
                    WHERE wic.product_id = p.id
                    ORDER BY wic.ts DESC, wic.id DESC
                    LIMIT 1
                ) wc ON TRUE
                LEFT JOIN raw_lots rl
                  ON rl.product_id = p.id
                 AND rl.kg_saldo > 1e-9
                 AND COALESCE(rl.cerrado, 0) = 0
                GROUP BY p.id, p.name, rs.kg, rsa.min_kg, rsa.reposicion_bolsas, rsa.proveedor_whatsapp,
                         wc.planta_bolsas, wc.bolsa_kg, wc.planta_kg, wc.diferencia_kg, wc.ts
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
            film_rows = cn.execute(
                """
                SELECT product_id, gramaje, rollos
                FROM film_stock
                """
            ).fetchall()
            film_alert_rows = cn.execute(
                """
                SELECT product_id, gramaje, min_rollos
                FROM film_stock_alerts
                """
            ).fetchall()
            package_count_rows = cn.execute(
                """
                WITH latest AS (
                    SELECT DISTINCT ON (product_id, gramaje)
                           product_id, gramaje, planta_paquetes, diferencia_paquetes, ts
                    FROM weekly_package_counts
                    ORDER BY product_id, gramaje, ts DESC, id DESC
                )
                SELECT l.product_id,
                       l.gramaje,
                       (
                         l.planta_paquetes
                         + COALESCE((SELECT SUM(f.paquetes) FROM fractionations f WHERE f.product_id = l.product_id AND f.gramaje = l.gramaje AND f.ts > l.ts), 0)
                         - COALESCE((
                             SELECT SUM(sii.cantidad)
                             FROM sales_invoice_items sii
                             JOIN sales_invoices si ON si.id = sii.invoice_id
                             WHERE sii.product_id = l.product_id AND sii.gramaje = l.gramaje AND si.ts > l.ts
                           ), 0)
                         + COALESCE((
                             SELECT SUM(ia.new_qty - ia.old_qty)
                             FROM inventory_adjustments ia
                             WHERE ia.kind = 'package' AND ia.product_id = l.product_id AND ia.gramaje = l.gramaje AND ia.ts > l.ts
                           ), 0)
                       ) AS planta_paquetes,
                       (
                         l.planta_paquetes
                         + COALESCE((SELECT SUM(f.paquetes) FROM fractionations f WHERE f.product_id = l.product_id AND f.gramaje = l.gramaje AND f.ts > l.ts), 0)
                         - COALESCE((
                             SELECT SUM(sii.cantidad)
                             FROM sales_invoice_items sii
                             JOIN sales_invoices si ON si.id = sii.invoice_id
                             WHERE sii.product_id = l.product_id AND sii.gramaje = l.gramaje AND si.ts > l.ts
                           ), 0)
                         + COALESCE((
                             SELECT SUM(ia.new_qty - ia.old_qty)
                             FROM inventory_adjustments ia
                             WHERE ia.kind = 'package' AND ia.product_id = l.product_id AND ia.gramaje = l.gramaje AND ia.ts > l.ts
                           ), 0)
                         - COALESCE(ps.paquetes, 0)
                       ) AS diferencia_paquetes,
                       CAST(l.ts AS TEXT) AS ts
                FROM latest l
                LEFT JOIN package_stock ps ON ps.product_id = l.product_id AND ps.gramaje = l.gramaje
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
                conteo_planta_bolsas=float(r["conteo_planta_bolsas"]) if r["conteo_planta_bolsas"] is not None else None,
                conteo_planta_kg=float(r["conteo_planta_kg"]) if r["conteo_planta_kg"] is not None else None,
                conteo_diferencia_kg=float(r["conteo_diferencia_kg"]) if r["conteo_diferencia_kg"] is not None else None,
                conteo_fecha=r["conteo_fecha"] or None,
            )
            for r in raw_rows
        ]
        stock_map = {(int(r["product_id"]), int(r["gramaje"])): int(r["paquetes"] or 0) for r in package_rows}
        price_map = {(int(r["product_id"]), int(r["gramaje"])): r for r in price_rows}
        film_map = {(int(r["product_id"]), int(r["gramaje"])): int(r["rollos"] or 0) for r in film_rows}
        film_alert_map = {(int(r["product_id"]), int(r["gramaje"])): int(r["min_rollos"] or 0) for r in film_alert_rows}
        package_count_map = {
            (int(r["product_id"]), int(r["gramaje"])): r
            for r in package_count_rows
        }
        package_stock: list[InventoryPackageRow] = []
        film_stock: list[InventoryFilmRow] = []
        for product in raw_rows:
            product_id = int(product["product_id"])
            producto = product["producto"] or ""
            gramajes = set(_gramajes(producto))
            gramajes.update(g for pid, g in stock_map if pid == product_id)
            gramajes.update(g for pid, g in price_map if pid == product_id)
            gramajes.update(g for pid, g in film_map if pid == product_id)
            gramajes.update(g for pid, g in film_alert_map if pid == product_id)
            for gramaje in gramajes:
                paquetes = stock_map.get((product_id, gramaje), 0)
                price = price_map.get((product_id, gramaje))
                package_count = package_count_map.get((product_id, gramaje))
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
                        conteo_planta_paquetes=(
                            int(package_count["planta_paquetes"]) if package_count is not None else None
                        ),
                        conteo_diferencia_paquetes=(
                            int(package_count["diferencia_paquetes"]) if package_count is not None else None
                        ),
                        conteo_fecha=(package_count["ts"] if package_count is not None else None),
                    )
                )
                rollos = film_map.get((product_id, gramaje), 0)
                min_rollos = film_alert_map.get((product_id, gramaje))
                film_stock.append(
                    InventoryFilmRow(
                        product_id=product_id,
                        producto=producto,
                        gramaje=gramaje,
                        rollos=rollos,
                        alerta_min_rollos=min_rollos if min_rollos and min_rollos > 0 else None,
                        alerta_estado=self._alert_state(float(rollos), float(min_rollos) if min_rollos else None),
                    )
                )
        package_stock = sorted(
            package_stock,
            key=lambda r: (_product_order_idx(r.producto), r.producto, _gram_order_idx(r.gramaje), r.gramaje),
        )
        film_stock = sorted(
            film_stock,
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
            film_stock=film_stock,
            lotes_abiertos=lotes,
            raw_alerts_count=sum(1 for r in raw_stock if r.alerta_estado == "bajo") + sum(1 for r in film_stock if r.alerta_estado == "bajo"),
            film_alerts_count=sum(1 for r in film_stock if r.alerta_estado == "bajo"),
            total_raw_kg=sum(r.kg for r in raw_stock),
            total_raw_valor_gs=sum(r.valor_stock_gs for r in raw_stock),
            total_paquetes=sum(r.paquetes for r in package_stock),
            total_unidades=sum(r.unidades for r in package_stock),
            total_venta_gs=sum(r.valor_venta_gs for r in package_stock),
            total_film_rollos=sum(r.rollos for r in film_stock),
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

    def update_film_alerts(self, payload: FilmStockAlertsUpdate) -> InventorySummary:
        with connection("fraccionadora") as cn:
            self._ensure_schema(cn)
            for alert in payload.alerts:
                if alert.min_rollos is None or int(alert.min_rollos) <= 0:
                    cn.execute(
                        "DELETE FROM film_stock_alerts WHERE product_id = %s AND gramaje = %s",
                        (alert.product_id, alert.gramaje),
                    )
                    continue
                cn.execute(
                    """
                    INSERT INTO film_stock_alerts(product_id, gramaje, min_rollos, updated_at)
                    VALUES(%s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT(product_id, gramaje) DO UPDATE SET
                        min_rollos = excluded.min_rollos,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (alert.product_id, alert.gramaje, int(alert.min_rollos)),
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

            for item in payload.film_stock:
                new_rollos = max(int(item.rollos or 0), 0)
                current = cn.execute(
                    "SELECT rollos FROM film_stock WHERE product_id = %s AND gramaje = %s",
                    (item.product_id, item.gramaje),
                ).fetchone()
                old_rollos = int((current or {}).get("rollos") or 0)
                cn.execute(
                    """
                    INSERT INTO film_stock(product_id, gramaje, rollos)
                    VALUES(%s, %s, %s)
                    ON CONFLICT(product_id, gramaje) DO UPDATE SET rollos = excluded.rollos
                    """,
                    (item.product_id, item.gramaje, new_rollos),
                )
                if old_rollos != new_rollos:
                    cn.execute(
                        """
                        INSERT INTO inventory_adjustments(kind, product_id, gramaje, old_qty, new_qty, motivo)
                        VALUES('film', %s, %s, %s, %s, %s)
                        """,
                        (item.product_id, item.gramaje, old_rollos, new_rollos, motivo),
                    )
        return self.summary()

    def record_weekly_count(self, payload: InventoryControlAuditCreate) -> InventorySummary:
        motivo = (payload.motivo or "").strip()
        with connection("fraccionadora") as cn:
            self._ensure_schema(cn)
            for item in payload.raw_stock:
                bolsa_kg = max(float(item.bolsa_kg or 0), 0.0)
                planta_bolsas = max(float(item.planta_bolsas or 0), 0.0)
                sistema_kg = max(float(item.sistema_kg or 0), 0.0)
                planta_kg = planta_bolsas * bolsa_kg
                cn.execute(
                    """
                    INSERT INTO weekly_inventory_counts(
                        product_id, sistema_kg, planta_bolsas, bolsa_kg, planta_kg, diferencia_kg, motivo
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (item.product_id, sistema_kg, planta_bolsas, bolsa_kg, planta_kg, planta_kg - sistema_kg, motivo),
                )
            for item in payload.package_stock:
                sistema = max(int(item.sistema_paquetes or 0), 0)
                planta = max(int(item.planta_paquetes or 0), 0)
                cn.execute(
                    """
                    INSERT INTO weekly_package_counts(
                        product_id, gramaje, sistema_paquetes, planta_paquetes, diferencia_paquetes, motivo
                    )
                    VALUES(%s, %s, %s, %s, %s, %s)
                    """,
                    (item.product_id, item.gramaje, sistema, planta, planta - sistema, motivo),
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

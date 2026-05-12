from __future__ import annotations

import unicodedata
from typing import Any

from core.database import connection
from modules.resumenes.schemas import (
    LoteDetalle,
    LoteFraccionamientoRow,
    RecargoPresentacionRow,
    LoteResumenRow,
    ProductoOption,
    ResumenesOptions,
)


def _normalize_product_key(name: str) -> str:
    text = (name or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    if "galleta" in text or "gall" in text or "molida" in text:
        return "galleta molida"
    if "arroz" in text:
        return "arroz"
    return text


def _bag_kg(product_name: str) -> float:
    return 25.0 if _normalize_product_key(product_name) == "galleta molida" else 50.0


def _unidades_por_paquete(gramaje: int) -> int:
    return 20 if gramaje <= 250 else 10


def _kg_por_paquete(gramaje: int) -> float:
    return (_unidades_por_paquete(gramaje) * gramaje) / 1000.0


class ResumenesRepository:
    def options(self) -> ResumenesOptions:
        with connection("fraccionadora") as cn:
            rows = cn.execute("SELECT id, name FROM products ORDER BY name").fetchall()
        return ResumenesOptions(productos=[ProductoOption(id=int(r["id"]), name=r["name"] or "") for r in rows])

    def lotes(self, product_id: int | None = None, solo_abiertos: bool = False) -> list[LoteResumenRow]:
        where: list[str] = []
        params: list[Any] = []
        if product_id is not None:
            where.append("rl.product_id = %s")
            params.append(product_id)
        if solo_abiertos:
            where.append("rl.kg_saldo > 1e-9")
            where.append("COALESCE(rl.cerrado, 0) = 0")
        sql = """
            SELECT rl.id, rl.lote, p.name AS producto, rl.kg_inicial,
                   (rl.kg_inicial - rl.kg_saldo) AS kg_usado, rl.kg_saldo,
                   rl.costo_total_gs, rl.costo_kg_gs, rl.proveedor,
                   rl.factura, rl.ts, COALESCE(rl.cerrado, 0) AS cerrado
            FROM raw_lots rl
            JOIN products p ON p.id = rl.product_id
        """
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY rl.ts DESC, rl.id DESC"
        with connection("fraccionadora") as cn:
            rows = cn.execute(sql, params).fetchall()
        return [
            LoteResumenRow(
                id=int(r["id"]),
                lote=r["lote"] or f"L{r['id']}",
                producto=r["producto"] or "",
                kg_total=float(r["kg_inicial"] or 0),
                kg_usado=float(r["kg_usado"] or 0),
                kg_disponible=float(r["kg_saldo"] or 0),
                costo_total_gs=float(r["costo_total_gs"] or 0),
                costo_kg_gs=float(r["costo_kg_gs"] or 0),
                proveedor=r["proveedor"] or "",
                factura=r["factura"] or "",
                fecha=str(r["ts"] or ""),
                cerrado=bool(r["cerrado"]),
            )
            for r in rows
        ]

    def detalle(self, lot_id: int) -> LoteDetalle:
        with connection("fraccionadora") as cn:
            lot = cn.execute(
                """
                SELECT rl.id, rl.lote, p.id AS product_id, p.name AS producto,
                       rl.kg_inicial, rl.kg_saldo, rl.costo_total_gs,
                       rl.costo_kg_gs, rl.proveedor, rl.factura, rl.ts,
                       COALESCE(rl.cerrado, 0) AS cerrado
                FROM raw_lots rl
                JOIN products p ON p.id = rl.product_id
                WHERE rl.id = %s
                """,
                (lot_id,),
            ).fetchone()
            if not lot:
                raise ValueError("Lote no encontrado.")
            merma = cn.execute(
                "SELECT COALESCE(SUM(kg), 0) AS kg FROM lot_mermas WHERE lot_id = %s",
                (lot_id,),
            ).fetchone()
            history = cn.execute(
                """
                SELECT f.ts, f.gramaje, f.paquetes, lf.kg_consumidos,
                       COALESCE(pp.price_gs, 0) AS price_gs
                FROM lot_fractionations lf
                JOIN fractionations f ON f.id = lf.fractionation_id
                LEFT JOIN package_prices pp
                  ON pp.product_id = f.product_id AND pp.gramaje = f.gramaje
                WHERE lf.lot_id = %s
                ORDER BY f.ts ASC, lf.id ASC
                """,
                (lot_id,),
            ).fetchall()
            prices = cn.execute(
                """
                SELECT gramaje, price_gs
                FROM package_prices
                WHERE product_id = %s
                ORDER BY gramaje
                """,
                (int(lot["product_id"]),),
            ).fetchall()

        producto = lot["producto"] or ""
        bag = _bag_kg(producto)
        kg_total = float(lot["kg_inicial"] or 0)
        kg_disponible = float(lot["kg_saldo"] or 0)
        kg_usado = kg_total - kg_disponible
        costo_kg = float(lot["costo_kg_gs"] or 0)
        merma_kg = float((merma or {}).get("kg") or 0)
        venta_estimada = 0.0
        fraccionamientos: list[LoteFraccionamientoRow] = []
        for row in history:
            kg = float(row["kg_consumidos"] or 0)
            paquetes = int(row["paquetes"] or 0)
            price = float(row["price_gs"] or 0)
            venta_linea = price * paquetes
            costo_linea = costo_kg * kg
            venta_estimada += venta_linea
            fraccionamientos.append(
                LoteFraccionamientoRow(
                    fecha=str(row["ts"] or ""),
                    gramaje=int(row["gramaje"] or 0),
                    paquetes=paquetes,
                    kg_consumidos=kg,
                    bolsas_eq=(kg / bag) if bag else None,
                    costo_kg_gs=costo_kg,
                    costo_total_gs=costo_linea,
                    precio_venta_gs=price,
                    beneficio_gs=venta_linea,
                )
            )

        compra = float(lot["costo_total_gs"] or 0)
        beneficio = venta_estimada - compra
        venta_kg = (venta_estimada / kg_usado) if kg_usado else 0.0
        recargo_kg = venta_kg - costo_kg
        recargos: list[RecargoPresentacionRow] = []
        for price_row in prices:
            gramaje = int(price_row["gramaje"] or 0)
            precio = float(price_row["price_gs"] or 0)
            kg_pack = _kg_por_paquete(gramaje)
            venta_por_kg = (precio / kg_pack) if kg_pack else 0.0
            recargo_por_kg = venta_por_kg - costo_kg
            recargos.append(
                RecargoPresentacionRow(
                    gramaje=gramaje,
                    kg_por_paquete=kg_pack,
                    precio_paquete_gs=precio,
                    venta_kg_gs=venta_por_kg,
                    recargo_kg_gs=recargo_por_kg,
                    recargo_pct=(recargo_por_kg / costo_kg * 100.0) if costo_kg else None,
                )
            )
        consumo_pct = (kg_usado / kg_total * 100.0) if kg_total else 0.0
        merma_pct = (merma_kg / kg_total * 100.0) if kg_total else 0.0
        return LoteDetalle(
            id=int(lot["id"]),
            lote=lot["lote"] or f"L{lot['id']}",
            producto_id=int(lot["product_id"]),
            producto=producto,
            kg_total=kg_total,
            kg_usado=kg_usado,
            kg_disponible=kg_disponible,
            costo_total_gs=compra,
            costo_kg_gs=costo_kg,
            proveedor=lot["proveedor"] or "",
            factura=lot["factura"] or "",
            fecha=str(lot["ts"] or ""),
            cerrado=bool(lot["cerrado"]),
            bolsas_total=(kg_total / bag) if bag else None,
            bolsas_usadas=(kg_usado / bag) if bag else None,
            bolsas_disponibles=(kg_disponible / bag) if bag else None,
            merma_kg=merma_kg,
            consumo_pct=max(0.0, min(100.0, consumo_pct)),
            merma_pct=max(0.0, min(100.0, merma_pct)),
            venta_estimada_gs=venta_estimada,
            venta_kg_gs=venta_kg,
            recargo_kg_gs=recargo_kg,
            recargo_pct=(recargo_kg / costo_kg * 100.0) if costo_kg else None,
            beneficio_estimado_gs=beneficio,
            beneficio_pct=(beneficio / compra * 100.0) if compra else None,
            recargos=recargos,
            fraccionamientos=fraccionamientos,
        )

    def set_cerrado(self, lot_id: int, cerrado: bool) -> None:
        with connection("fraccionadora") as cn:
            row = cn.execute(
                "UPDATE raw_lots SET cerrado = %s WHERE id = %s RETURNING id",
                (1 if cerrado else 0, lot_id),
            ).fetchone()
            if not row:
                raise ValueError("Lote no encontrado.")

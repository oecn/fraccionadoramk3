from __future__ import annotations

import datetime as dt
import unicodedata
from typing import Any

from core.database import connection
from modules.fraccionamiento.schemas import (
    ConsumoPreview,
    FraccionamientoCreate,
    FraccionamientoDeleteResponse,
    FraccionamientoHistoryRow,
    FraccionamientoOptions,
    FraccionamientoSummary,
    FraccionamientoUpdate,
    LoteItem,
    PackageStockRow,
    ProductoItem,
    RawStockRow,
)


def _normalize_product_key(name: str) -> str:
    text = (name or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    if "azucar" in text:
        return "azucar"
    if "arroz" in text:
        return "arroz"
    if "lenteja" in text:
        return "lenteja"
    return text


def _gramajes(product_name: str) -> list[int]:
    key = _normalize_product_key(product_name)
    if key in {"arroz", "azucar"}:
        return [250, 500, 1000]
    if key == "lenteja":
        return [250, 500]
    return [200, 400, 800]


def _sort_gramajes(values: set[int]) -> list[int]:
    order = {200: 0, 250: 1, 400: 2, 500: 3, 800: 4, 1000: 5}
    return sorted(values, key=lambda gramaje: (order.get(gramaje, 999), gramaje))


def _unidades_por_paquete(gramaje: int) -> int:
    return 20 if gramaje <= 250 else 10


def _kg_requeridos(gramaje: int, paquetes: int) -> float:
    return paquetes * (_unidades_por_paquete(gramaje) * gramaje) / 1000.0


def _fractionation_timestamp(fecha: str) -> str:
    parsed = dt.datetime.strptime(fecha, "%Y-%m-%d").date()
    if parsed == dt.date.today():
        return dt.datetime.now().replace(microsecond=0).isoformat(sep=" ")
    return f"{fecha} 00:00:00"


def _bag_kg(product_name: str) -> float:
    key = _normalize_product_key(product_name)
    if key in {"arroz", "azucar"}:
        return 50.0
    if "galleta" in key or "molida" in key:
        return 25.0
    return 50.0


def _bolsas_25(product_name: str, kg: float) -> float:
    key = _normalize_product_key(product_name)
    return kg / 25.0 if "galleta" in key or "molida" in key else 0.0


def _bolsas_eq(kg: float, product_name: str) -> str:
    bag = _bag_kg(product_name)
    bag_txt = f"{bag:.0f}" if bag.is_integer() else str(bag)
    return f"{kg / bag:.3f} bolsas x {bag_txt} kg"


class FraccionamientoRepository:
    def options(self) -> FraccionamientoOptions:
        with connection("fraccionadora") as cn:
            products = cn.execute(
                """
                SELECT p.id, p.name, COALESCE(rs.kg, 0) AS raw_kg
                FROM products p
                LEFT JOIN raw_stock rs ON rs.product_id = p.id
                ORDER BY p.name
                """
            ).fetchall()
            stock_grams = cn.execute(
                """
                SELECT product_id, gramaje
                FROM package_stock
                ORDER BY product_id, gramaje
                """
            ).fetchall()
            grams_by_product: dict[int, set[int]] = {}
            for row in stock_grams:
                grams_by_product.setdefault(int(row["product_id"]), set()).add(int(row["gramaje"]))
            lots = self._list_lotes(cn)
        return FraccionamientoOptions(
            productos=[
                ProductoItem(
                    id=int(r["id"]),
                    name=r["name"] or "",
                    gramajes=_sort_gramajes(
                        set(_gramajes(r["name"] or "")) | grams_by_product.get(int(r["id"]), set())
                    ),
                    raw_kg=float(r["raw_kg"] or 0),
                )
                for r in products
            ],
            lotes=lots,
            hoy=dt.date.today().isoformat(),
        )

    def summary(
        self,
        product_id: int | None = None,
        desde: str | None = None,
        hasta: str | None = None,
    ) -> FraccionamientoSummary:
        raw_where = ""
        raw_params: list[Any] = []
        if product_id is not None:
            raw_where = "WHERE p.id = %s"
            raw_params.append(product_id)

        pkg_where = ""
        pkg_params: list[Any] = []
        if product_id is not None:
            pkg_where = "WHERE p.id = %s"
            pkg_params.append(product_id)

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
            pkg_rows = cn.execute(
                f"""
                SELECT p.id AS product_id, p.name AS producto, ps.gramaje, ps.paquetes
                FROM package_stock ps
                JOIN products p ON p.id = ps.product_id
                {pkg_where}
                ORDER BY p.name, ps.gramaje
                """,
                pkg_params,
            ).fetchall()
            history = self._history(cn, product_id, desde, hasta, limit=300)
            month_start = dt.date.today().replace(day=1).isoformat()
            month_rows = self._history(cn, product_id, month_start, None, limit=None)

        raw_stock = [
            RawStockRow(
                product_id=int(r["product_id"]),
                producto=r["producto"] or "",
                kg=float(r["kg"] or 0),
                bolsas_50=float(r["kg"] or 0) / 50.0,
                bolsas_25=_bolsas_25(r["producto"] or "", float(r["kg"] or 0)),
            )
            for r in raw_rows
        ]
        package_stock = [
            PackageStockRow(
                product_id=int(r["product_id"]),
                producto=r["producto"] or "",
                gramaje=int(r["gramaje"] or 0),
                paquetes=int(r["paquetes"] or 0),
            )
            for r in pkg_rows
        ]
        return FraccionamientoSummary(
            raw_stock=raw_stock,
            package_stock=package_stock,
            history=history,
            total_raw_kg=sum(r.kg for r in raw_stock),
            total_paquetes=sum(r.paquetes for r in package_stock),
            total_kg_mes=sum(r.kg_consumidos for r in month_rows),
            total_paquetes_mes=sum(r.paquetes for r in month_rows),
        )

    def preview(self, product_id: int, gramaje: int, paquetes: int) -> ConsumoPreview:
        if paquetes <= 0:
            return ConsumoPreview(kg_consumidos=0, unidades_por_paquete=_unidades_por_paquete(gramaje), bolsas_eq="-")
        product = self._product_name(product_id)
        kg = _kg_requeridos(gramaje, paquetes)
        return ConsumoPreview(
            kg_consumidos=kg,
            unidades_por_paquete=_unidades_por_paquete(gramaje),
            bolsas_eq=_bolsas_eq(kg, product),
        )

    def create(self, payload: FraccionamientoCreate) -> FraccionamientoHistoryRow:
        fecha = (payload.fecha or "").strip() or dt.date.today().isoformat()
        try:
            fecha_sql = _fractionation_timestamp(fecha)
        except ValueError as exc:
            raise ValueError("Formato de fecha invalido. Use YYYY-MM-DD.") from exc
        kg = _kg_requeridos(payload.gramaje, payload.paquetes)

        with connection("fraccionadora") as cn:
            product = cn.execute("SELECT name FROM products WHERE id = %s", (payload.product_id,)).fetchone()
            if not product:
                raise ValueError("Producto no encontrado.")
            product_name = product["name"] or ""

            lot_id = payload.lot_id
            if lot_id is not None:
                lot = cn.execute(
                    "SELECT product_id, kg_saldo, cerrado FROM raw_lots WHERE id = %s",
                    (lot_id,),
                ).fetchone()
                if not lot:
                    raise ValueError("Lote no encontrado.")
                if int(lot["product_id"]) != payload.product_id:
                    raise ValueError("El lote seleccionado no corresponde al producto.")
                if int(lot["cerrado"] or 0):
                    raise ValueError("El lote seleccionado esta cerrado.")
                cn.execute("UPDATE raw_lots SET kg_saldo = kg_saldo - %s WHERE id = %s", (kg, lot_id))

            cn.execute("UPDATE raw_stock SET kg = kg - %s WHERE product_id = %s", (kg, payload.product_id))
            cn.execute(
                """
                INSERT INTO package_stock(product_id, gramaje, paquetes)
                VALUES(%s, %s, %s)
                ON CONFLICT(product_id, gramaje)
                DO UPDATE SET paquetes = package_stock.paquetes + excluded.paquetes
                """,
                (payload.product_id, payload.gramaje, payload.paquetes),
            )
            row = cn.execute(
                """
                INSERT INTO fractionations(ts, product_id, gramaje, paquetes, kg_consumidos)
                VALUES(%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (fecha_sql, payload.product_id, payload.gramaje, payload.paquetes, kg),
            ).fetchone()
            frac_id = int(row["id"])
            if lot_id is not None:
                cn.execute(
                    """
                    INSERT INTO lot_fractionations(lot_id, fractionation_id, kg_consumidos)
                    VALUES(%s, %s, %s)
                    """,
                    (lot_id, frac_id, kg),
                )

        return FraccionamientoHistoryRow(
            id=frac_id,
            fecha=fecha_sql,
            product_id=payload.product_id,
            producto=product_name,
            gramaje=payload.gramaje,
            paquetes=payload.paquetes,
            kg_consumidos=kg,
            bolsas_eq=_bolsas_eq(kg, product_name),
            lote=str(lot_id or "-"),
            lot_id=lot_id,
        )

    def update(self, frac_id: int, payload: FraccionamientoUpdate) -> FraccionamientoHistoryRow:
        if payload.paquetes <= 0:
            raise ValueError("Paquetes invalidos.")
        kg_new = _kg_requeridos(payload.gramaje, payload.paquetes)

        with connection("fraccionadora") as cn:
            row = cn.execute(
                """
                SELECT f.id, f.product_id, p.name AS producto, f.gramaje, f.paquetes, f.kg_consumidos
                FROM fractionations f
                JOIN products p ON p.id = f.product_id
                WHERE f.id = %s
                """,
                (frac_id,),
            ).fetchone()
            if not row:
                raise ValueError("Fraccionamiento no encontrado.")

            product_id = int(row["product_id"])
            product_name = row["producto"] or ""
            gram_old = int(row["gramaje"] or 0)
            paquetes_old = int(row["paquetes"] or 0)
            kg_old = float(row["kg_consumidos"] or 0)
            delta_kg = kg_new - kg_old

            lot_row = cn.execute(
                "SELECT lot_id FROM lot_fractionations WHERE fractionation_id = %s LIMIT 1",
                (frac_id,),
            ).fetchone()
            lot_id_orig = int(lot_row["lot_id"]) if lot_row else None
            new_lot_id = payload.lot_id

            lot_orig_saldo: float | None = None
            if lot_id_orig is not None:
                lot_orig = cn.execute("SELECT kg_saldo FROM raw_lots WHERE id = %s", (lot_id_orig,)).fetchone()
                if not lot_orig:
                    raise ValueError("Lote vinculado no encontrado.")
                lot_orig_saldo = float(lot_orig["kg_saldo"] or 0)

            lot_dest = None
            if new_lot_id is not None:
                lot_dest = cn.execute(
                    "SELECT product_id, kg_saldo, COALESCE(cerrado, 0) AS cerrado FROM raw_lots WHERE id = %s",
                    (new_lot_id,),
                ).fetchone()
                if not lot_dest:
                    raise ValueError("Lote seleccionado no encontrado.")
                if int(lot_dest["product_id"]) != product_id:
                    raise ValueError("El lote seleccionado no corresponde al producto.")
                if int(lot_dest["cerrado"] or 0):
                    raise ValueError("El lote seleccionado esta cerrado.")

            if lot_id_orig == new_lot_id and lot_orig_saldo is not None:
                if lot_orig_saldo + kg_old - kg_new < -1e-9:
                    raise ValueError("Saldo insuficiente en el lote seleccionado.")
            elif new_lot_id is not None and lot_dest is not None:
                if float(lot_dest["kg_saldo"] or 0) < kg_new - 1e-9:
                    raise ValueError("Saldo insuficiente en el lote seleccionado.")

            cn.execute(
                "UPDATE package_stock SET paquetes = paquetes - %s WHERE product_id = %s AND gramaje = %s",
                (paquetes_old, product_id, gram_old),
            )
            cn.execute(
                """
                INSERT INTO package_stock(product_id, gramaje, paquetes)
                VALUES(%s, %s, %s)
                ON CONFLICT(product_id, gramaje)
                DO UPDATE SET paquetes = package_stock.paquetes + excluded.paquetes
                """,
                (product_id, payload.gramaje, payload.paquetes),
            )
            cn.execute("UPDATE raw_stock SET kg = kg - %s WHERE product_id = %s", (delta_kg, product_id))

            if lot_id_orig is not None:
                cn.execute("UPDATE raw_lots SET kg_saldo = kg_saldo + %s WHERE id = %s", (kg_old, lot_id_orig))
            if new_lot_id is not None:
                cn.execute("UPDATE raw_lots SET kg_saldo = kg_saldo - %s WHERE id = %s", (kg_new, new_lot_id))

            if lot_id_orig != new_lot_id:
                cn.execute("DELETE FROM lot_fractionations WHERE fractionation_id = %s", (frac_id,))
                if new_lot_id is not None:
                    cn.execute(
                        """
                        INSERT INTO lot_fractionations(lot_id, fractionation_id, kg_consumidos)
                        VALUES(%s, %s, %s)
                        """,
                        (new_lot_id, frac_id, kg_new),
                    )
            elif new_lot_id is None:
                cn.execute("DELETE FROM lot_fractionations WHERE fractionation_id = %s", (frac_id,))
            else:
                cn.execute(
                    """
                    UPDATE lot_fractionations
                    SET kg_consumidos = %s
                    WHERE lot_id = %s AND fractionation_id = %s
                    """,
                    (kg_new, new_lot_id, frac_id),
                )

            cn.execute(
                """
                UPDATE fractionations
                SET gramaje = %s, paquetes = %s, kg_consumidos = %s
                WHERE id = %s
                """,
                (payload.gramaje, payload.paquetes, kg_new, frac_id),
            )

        return FraccionamientoHistoryRow(
            id=frac_id,
            fecha="",
            product_id=product_id,
            producto=product_name,
            gramaje=payload.gramaje,
            paquetes=payload.paquetes,
            kg_consumidos=kg_new,
            bolsas_eq=_bolsas_eq(kg_new, product_name),
            lote=str(new_lot_id or "-"),
            lot_id=new_lot_id,
        )

    def delete(self, frac_id: int) -> FraccionamientoDeleteResponse:
        with connection("fraccionadora") as cn:
            row = cn.execute(
                "SELECT product_id, gramaje, paquetes, kg_consumidos FROM fractionations WHERE id = %s",
                (frac_id,),
            ).fetchone()
            if not row:
                raise ValueError("Fraccionamiento no encontrado.")

            product_id = int(row["product_id"])
            gramaje = int(row["gramaje"] or 0)
            paquetes = int(row["paquetes"] or 0)
            kg = float(row["kg_consumidos"] or 0)

            lot_row = cn.execute(
                "SELECT lot_id FROM lot_fractionations WHERE fractionation_id = %s LIMIT 1",
                (frac_id,),
            ).fetchone()
            lot_id = int(lot_row["lot_id"]) if lot_row else None

            updated = cn.execute(
                """
                UPDATE package_stock
                SET paquetes = paquetes - %s
                WHERE product_id = %s AND gramaje = %s
                RETURNING product_id
                """,
                (paquetes, product_id, gramaje),
            ).fetchone()
            if not updated:
                cn.execute(
                    "INSERT INTO package_stock(product_id, gramaje, paquetes) VALUES(%s, %s, %s)",
                    (product_id, gramaje, -paquetes),
                )
            cn.execute("UPDATE raw_stock SET kg = kg + %s WHERE product_id = %s", (kg, product_id))
            if lot_id is not None:
                cn.execute("UPDATE raw_lots SET kg_saldo = kg_saldo + %s WHERE id = %s", (kg, lot_id))
            cn.execute("DELETE FROM lot_fractionations WHERE fractionation_id = %s", (frac_id,))
            cn.execute("DELETE FROM fractionations WHERE id = %s", (frac_id,))

        return FraccionamientoDeleteResponse(
            id=frac_id,
            deleted=True,
            message="Fraccionamiento eliminado y stock revertido.",
        )

    def _product_name(self, product_id: int) -> str:
        with connection("fraccionadora") as cn:
            row = cn.execute("SELECT name FROM products WHERE id = %s", (product_id,)).fetchone()
        if not row:
            return ""
        return row["name"] or ""

    def _list_lotes(self, cn) -> list[LoteItem]:
        rows = cn.execute(
            """
            SELECT rl.id, rl.product_id, COALESCE(rl.lote, '') AS lote,
                   COALESCE(rl.kg_saldo, 0) AS kg_saldo,
                   COALESCE(rl.costo_kg_gs, 0) AS costo_kg_gs,
                   COALESCE(rl.proveedor, '') AS proveedor,
                   COALESCE(rl.factura, '') AS factura,
                   CAST(rl.ts AS TEXT) AS ts
            FROM raw_lots rl
            WHERE rl.kg_saldo > 1e-9 AND COALESCE(rl.cerrado, 0) = 0
            ORDER BY rl.ts DESC, rl.id DESC
            """
        ).fetchall()
        return [
            LoteItem(
                id=int(r["id"]),
                product_id=int(r["product_id"]),
                lote=r["lote"] or "",
                kg_saldo=float(r["kg_saldo"] or 0),
                costo_kg_gs=float(r["costo_kg_gs"] or 0),
                proveedor=r["proveedor"] or "",
                factura=r["factura"] or "",
                ts=r["ts"] or "",
            )
            for r in rows
        ]

    def _history(
        self,
        cn,
        product_id: int | None,
        desde: str | None,
        hasta: str | None,
        limit: int | None,
    ) -> list[FraccionamientoHistoryRow]:
        where: list[str] = []
        params: list[Any] = []
        if product_id is not None:
            where.append("f.product_id = %s")
            params.append(product_id)
        if desde:
            where.append("f.ts::date >= %s")
            params.append(desde)
        if hasta:
            where.append("f.ts::date <= %s")
            params.append(hasta)
        sql = """
            SELECT f.id, CAST(f.ts AS TEXT) AS fecha, f.product_id, p.name AS producto,
                   f.gramaje, f.paquetes, f.kg_consumidos,
                   COALESCE(rl.lote, '') AS lote,
                   rl.id AS lot_id
            FROM fractionations f
            JOIN products p ON p.id = f.product_id
            LEFT JOIN lot_fractionations lf ON lf.fractionation_id = f.id
            LEFT JOIN raw_lots rl ON rl.id = lf.lot_id
        """
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY f.ts DESC, f.id DESC"
        if limit:
            sql += " LIMIT %s"
            params.append(limit)
        rows = cn.execute(sql, params).fetchall()
        return [
            FraccionamientoHistoryRow(
                id=int(r["id"]),
                fecha=r["fecha"] or "",
                product_id=int(r["product_id"]),
                producto=r["producto"] or "",
                gramaje=int(r["gramaje"] or 0),
                paquetes=int(r["paquetes"] or 0),
                kg_consumidos=float(r["kg_consumidos"] or 0),
                bolsas_eq=_bolsas_eq(float(r["kg_consumidos"] or 0), r["producto"] or ""),
                lote=(f"{r['lote']} (ID {r['lot_id']})" if r["lot_id"] else "-"),
                lot_id=(int(r["lot_id"]) if r["lot_id"] else None),
            )
            for r in rows
        ]

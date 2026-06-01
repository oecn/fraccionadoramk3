from __future__ import annotations

import datetime as dt
import unicodedata
from collections import defaultdict

from core.database import connection
from modules.produccion.schemas import (
    ProduccionMesRow,
    ProduccionOptions,
    ProduccionProductoRow,
    ProduccionResumen,
)


def _normalize_name(name: str) -> str:
    text = (name or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    if "azucar" in text or "azukita" in text:
        return "azucar"
    if "arroz" in text:
        return "arroz"
    return text


def _maquina_for_producto(name: str) -> str:
    return "Maquina 1" if _normalize_name(name) in {"arroz", "azucar"} else "Maquina 2"


def _unidades_por_paquete(gramaje: int) -> int:
    return 20 if int(gramaje or 0) <= 250 else 10


def _month_bounds(year: int, month: int) -> tuple[str, str]:
    start = dt.date(year, month, 1)
    end = dt.date(year + 1, 1, 1) if month == 12 else dt.date(year, month + 1, 1)
    return start.isoformat(), end.isoformat()


def _month_range(end_year: int, end_month: int, count: int) -> list[str]:
    year = end_year
    month = end_month
    months: list[str] = []
    for _ in range(max(1, count)):
        months.append(f"{year:04d}-{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return list(reversed(months))


class ProduccionRepository:
    def options(self) -> ProduccionOptions:
        today = dt.date.today()
        with connection("fraccionadora") as cn:
            row = cn.execute(
                "SELECT MIN(EXTRACT(YEAR FROM ts)::int) AS min_y, MAX(EXTRACT(YEAR FROM ts)::int) AS max_y FROM fractionations"
            ).fetchone()
        min_y = int(row["min_y"]) if row and row["min_y"] is not None else today.year
        max_y = int(row["max_y"]) if row and row["max_y"] is not None else today.year
        years = list(range(min_y, max_y + 1))
        if today.year not in years:
            years.append(today.year)
            years.sort()
        return ProduccionOptions(years=years, current_year=today.year, current_month=today.month)

    def resumen(self, year: int, month: int, range_months: int = 12) -> ProduccionResumen:
        if month < 1 or month > 12:
            raise ValueError("Mes invalido.")
        range_months = min(max(int(range_months or 12), 1), 36)
        start, end = _month_bounds(year, month)
        trend_months = _month_range(year, month, range_months)
        trend_start = f"{trend_months[0]}-01"

        with connection("fraccionadora") as cn:
            rows = cn.execute(
                """
                SELECT p.name AS producto, f.gramaje,
                       SUM(f.paquetes) AS paquetes,
                       SUM(f.kg_consumidos) AS kg_consumidos,
                       COUNT(*) AS fraccionamientos
                FROM fractionations f
                JOIN products p ON p.id = f.product_id
                WHERE f.ts::date >= %s AND f.ts::date < %s
                GROUP BY p.name, f.gramaje
                """,
                (start, end),
            ).fetchall()
            trend_rows = cn.execute(
                """
                SELECT TO_CHAR(f.ts::date, 'YYYY-MM') AS ym, p.name AS producto, f.gramaje,
                       SUM(f.paquetes) AS paquetes
                FROM fractionations f
                JOIN products p ON p.id = f.product_id
                WHERE f.ts::date >= %s AND f.ts::date < %s
                GROUP BY ym, p.name, f.gramaje
                ORDER BY ym
                """,
                (trend_start, end),
            ).fetchall()

        by_product: dict[str, dict[str, object]] = {}
        for row in rows:
            producto = row["producto"] or ""
            paquetes = int(row["paquetes"] or 0)
            unidades = paquetes * _unidades_por_paquete(int(row["gramaje"] or 0))
            current = by_product.setdefault(
                producto,
                {
                    "producto": producto,
                    "maquina": _maquina_for_producto(producto),
                    "unidades": 0,
                    "paquetes": 0,
                    "kg_consumidos": 0.0,
                    "fraccionamientos": 0,
                },
            )
            current["unidades"] = int(current["unidades"]) + unidades
            current["paquetes"] = int(current["paquetes"]) + paquetes
            current["kg_consumidos"] = float(current["kg_consumidos"]) + float(row["kg_consumidos"] or 0)
            current["fraccionamientos"] = int(current["fraccionamientos"]) + int(row["fraccionamientos"] or 0)

        product_rows = [
            ProduccionProductoRow(
                producto=str(item["producto"]),
                maquina=str(item["maquina"]),
                unidades=int(item["unidades"]),
                paquetes=int(item["paquetes"]),
                kg_consumidos=float(item["kg_consumidos"]),
                fraccionamientos=int(item["fraccionamientos"]),
            )
            for item in by_product.values()
        ]
        product_rows.sort(key=lambda r: (0 if r.maquina == "Maquina 1" else 1, -r.unidades, r.producto.lower()))

        total_by_month: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "m1": 0, "m2": 0})
        for row in trend_rows:
            ym = row["ym"] or ""
            producto = row["producto"] or ""
            unidades = int(row["paquetes"] or 0) * _unidades_por_paquete(int(row["gramaje"] or 0))
            total_by_month[ym]["total"] += unidades
            if _maquina_for_producto(producto) == "Maquina 1":
                total_by_month[ym]["m1"] += unidades
            else:
                total_by_month[ym]["m2"] += unidades

        trend: list[ProduccionMesRow] = []
        totals_for_avg: list[int] = []
        for ym in trend_months:
            data = total_by_month[ym]
            total = int(data["total"])
            totals_for_avg.append(total)
            window = totals_for_avg[-3:]
            trend.append(
                ProduccionMesRow(
                    ym=ym,
                    total_unidades=total,
                    maquina1_unidades=int(data["m1"]),
                    maquina2_unidades=int(data["m2"]),
                    promedio_3m=sum(window) / len(window) if window else 0.0,
                )
            )

        total_m1 = sum(r.unidades for r in product_rows if r.maquina == "Maquina 1")
        total_m2 = sum(r.unidades for r in product_rows if r.maquina != "Maquina 1")
        return ProduccionResumen(
            year=year,
            month=month,
            range_months=range_months,
            total_unidades=total_m1 + total_m2,
            maquina1_unidades=total_m1,
            maquina2_unidades=total_m2,
            total_paquetes=sum(r.paquetes for r in product_rows),
            total_kg=sum(r.kg_consumidos for r in product_rows),
            total_fraccionamientos=sum(r.fraccionamientos for r in product_rows),
            rows=product_rows,
            trend=trend,
        )

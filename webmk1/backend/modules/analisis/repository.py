from __future__ import annotations

import datetime as dt
from collections import defaultdict
from typing import Any

from core.database import connection
from modules.analisis.schemas import (
    AnaliticaClienteRow,
    AnaliticaClientesResponse,
    ProyeccionCompraRow,
    ProyeccionComprasResponse,
)


class AnalisisRepository:
    def analitica_clientes(self, top_n: int = 15) -> AnaliticaClientesResponse:
        top_n = max(1, min(int(top_n or 15), 200))
        resumen: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "cliente": "",
                "facturas": 0,
                "ventas_bolsa": 0,
                "monto_facturas": 0.0,
                "monto_bolsa": 0.0,
                "last_ts": None,
            },
        )

        with connection("fraccionadora") as cn:
            invoice_rows = cn.execute(
                """
                SELECT COALESCE(NULLIF(TRIM(customer), ''), 'Sin cliente') AS cliente,
                       COUNT(*) AS n,
                       COALESCE(SUM(total_gs), 0) AS total,
                       MAX(ts) AS last_ts
                FROM sales_invoices
                GROUP BY cliente
                """
            ).fetchall()
            bag_rows = cn.execute(
                """
                SELECT COALESCE(NULLIF(TRIM(customer), ''), 'Sin cliente') AS cliente,
                       COUNT(*) AS n,
                       COALESCE(SUM(total_gs), 0) AS total,
                       MAX(ts) AS last_ts
                FROM bag_sales
                GROUP BY cliente
                """
            ).fetchall()

        for row in invoice_rows:
            current = resumen[row["cliente"]]
            current["cliente"] = row["cliente"]
            current["facturas"] += int(row["n"] or 0)
            current["monto_facturas"] += float(row["total"] or 0)
            current["last_ts"] = self._max_ts(current["last_ts"], row["last_ts"])

        for row in bag_rows:
            current = resumen[row["cliente"]]
            current["cliente"] = row["cliente"]
            current["ventas_bolsa"] += int(row["n"] or 0)
            current["monto_bolsa"] += float(row["total"] or 0)
            current["last_ts"] = self._max_ts(current["last_ts"], row["last_ts"])

        rows: list[AnaliticaClienteRow] = []
        for item in resumen.values():
            total_gs = float(item["monto_facturas"] or 0) + float(item["monto_bolsa"] or 0)
            ops = int(item["facturas"] or 0) + int(item["ventas_bolsa"] or 0)
            rows.append(
                AnaliticaClienteRow(
                    cliente=item["cliente"] or "Sin cliente",
                    ops=ops,
                    facturas=int(item["facturas"] or 0),
                    bolsas=int(item["ventas_bolsa"] or 0),
                    total_gs=total_gs,
                    ticket_prom=(total_gs / ops) if ops else 0.0,
                    last_ts=str(item["last_ts"]) if item["last_ts"] else None,
                )
            )

        rows.sort(key=lambda row: row.total_gs, reverse=True)
        return AnaliticaClientesResponse(
            top_n=top_n,
            total_clientes=len(rows),
            total_gs=sum(row.total_gs for row in rows),
            rows=rows[:top_n],
        )

    def proyeccion_compras(self, ventana_dias: int = 30, top_n: int | None = None) -> ProyeccionComprasResponse:
        ventana_dias = max(1, min(int(ventana_dias or 30), 365))
        if top_n is not None:
            top_n = max(1, min(int(top_n), 200))
        desde = (dt.datetime.now() - dt.timedelta(days=ventana_dias)).date().isoformat()

        with connection("fraccionadora") as cn:
            stock_rows = cn.execute(
                """
                SELECT p.id, p.name, COALESCE(rs.kg, 0) AS kg
                FROM products p
                JOIN raw_stock rs ON rs.product_id = p.id
                ORDER BY p.name
                """
            ).fetchall()
            consumo_frac_rows = cn.execute(
                """
                SELECT f.product_id, COALESCE(SUM(f.kg_consumidos), 0) AS kg
                FROM fractionations f
                WHERE f.ts::date >= %s
                GROUP BY f.product_id
                """,
                (desde,),
            ).fetchall()
            consumo_bolsas_rows = cn.execute(
                """
                SELECT bs.product_id, COALESCE(SUM(bs.kg_total), 0) AS kg
                FROM bag_sales bs
                WHERE bs.ts::date >= %s
                GROUP BY bs.product_id
                """,
                (desde,),
            ).fetchall()
            dias_rows = cn.execute(
                """
                SELECT t.product_id, COUNT(DISTINCT t.dia) AS dias_activos
                FROM (
                    SELECT product_id, ts::date AS dia FROM fractionations WHERE ts::date >= %s
                    UNION ALL
                    SELECT product_id, ts::date AS dia FROM bag_sales WHERE ts::date >= %s
                ) t
                GROUP BY t.product_id
                """,
                (desde, desde),
            ).fetchall()

        consumo_frac = {int(row["product_id"]): float(row["kg"] or 0) for row in consumo_frac_rows}
        consumo_bolsas = {int(row["product_id"]): float(row["kg"] or 0) for row in consumo_bolsas_rows}
        dias_activos = {int(row["product_id"]): int(row["dias_activos"] or 0) for row in dias_rows}

        rows: list[ProyeccionCompraRow] = []
        for row in stock_rows:
            product_id = int(row["id"])
            consumo_total = consumo_frac.get(product_id, 0.0) + consumo_bolsas.get(product_id, 0.0)
            activos = dias_activos.get(product_id, 0)
            consumo_diario = consumo_total / float(ventana_dias) if ventana_dias else 0.0
            stock_kg = float(row["kg"] or 0)
            rows.append(
                ProyeccionCompraRow(
                    producto=row["name"] or "",
                    stock_kg=stock_kg,
                    consumo_diario=consumo_diario,
                    dias_restantes=(stock_kg / consumo_diario) if consumo_diario > 0 else None,
                    consumo_total=consumo_total,
                    dias_activos=activos,
                )
            )

        rows.sort(key=lambda row: row.dias_restantes if row.dias_restantes is not None else 1e9)
        return ProyeccionComprasResponse(
            ventana_dias=ventana_dias,
            top_n=top_n,
            rows=rows[:top_n] if top_n else rows,
        )

    def _max_ts(self, left: Any, right: Any) -> Any:
        if not left:
            return right
        if not right:
            return left
        return max(left, right)

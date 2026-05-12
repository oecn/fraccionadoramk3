from __future__ import annotations

from core.database import connection
from modules.historial_compras.schemas import CompraRow, HistorialComprasSummary


class HistorialComprasRepository:
    def summary(
        self,
        search: str = "",
        from_date: str = "",
        to_date: str = "",
    ) -> HistorialComprasSummary:
        conditions: list[str] = []
        params: list[str] = []

        if from_date:
            conditions.append("ts::date >= %s")
            params.append(from_date)
        if to_date:
            conditions.append("ts::date <= %s")
            params.append(to_date)
        if search:
            conditions.append("(COALESCE(factura,'') ILIKE %s OR COALESCE(proveedor,'') ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        sql = f"""
            SELECT
                id,
                COALESCE(CAST(ts AS TEXT), '') AS ts,
                COALESCE(factura, '')           AS factura,
                COALESCE(proveedor, '')         AS proveedor,
                COALESCE(costo_total_gs, 0)     AS costo_total_gs
            FROM raw_lots
            {where}
            ORDER BY ts DESC
            LIMIT 500
        """

        with connection("fraccionadora") as cn:
            rows = cn.execute(sql, params).fetchall()

        compras = [
            CompraRow(
                id=r["id"],
                ts=r["ts"],
                factura=r["factura"],
                proveedor=r["proveedor"],
                costo_total_gs=float(r["costo_total_gs"]),
            )
            for r in rows
        ]

        total_gs = sum(c.costo_total_gs for c in compras)

        return HistorialComprasSummary(
            total_registros=len(compras),
            total_gs=total_gs,
            rows=compras,
        )

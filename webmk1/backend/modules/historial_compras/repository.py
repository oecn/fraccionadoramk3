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
                COALESCE(costo_total_gs, 0)     AS costo_total_gs,
                'Compra'                        AS tipo,
                ''                              AS motivo,
                ''                              AS producto
            FROM raw_lots
            {where}
        """

        with connection("fraccionadora") as cn:
            rows = cn.execute(sql, params).fetchall()
            table_row = cn.execute("SELECT to_regclass(%s) AS table_name", ("raw_lot_deletions",)).fetchone()
            if table_row and table_row["table_name"]:
                del_conditions: list[str] = []
                del_params: list[str] = []
                if from_date:
                    del_conditions.append("deleted_at::date >= %s")
                    del_params.append(from_date)
                if to_date:
                    del_conditions.append("deleted_at::date <= %s")
                    del_params.append(to_date)
                if search:
                    del_conditions.append("(COALESCE(factura,'') ILIKE %s OR COALESCE(proveedor,'') ILIKE %s OR COALESCE(motivo,'') ILIKE %s)")
                    del_params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])
                del_where = ("WHERE " + " AND ".join(del_conditions)) if del_conditions else ""
                deleted_rows = cn.execute(
                    f"""
                    SELECT
                        lot_id AS id,
                        COALESCE(CAST(deleted_at AS TEXT), '') AS ts,
                        COALESCE(factura, '') AS factura,
                        COALESCE(proveedor, '') AS proveedor,
                        -1 * COALESCE(costo_total_gs, 0) AS costo_total_gs,
                        'Eliminacion' AS tipo,
                        COALESCE(motivo, '') AS motivo,
                        COALESCE(producto, '') AS producto
                    FROM raw_lot_deletions
                    {del_where}
                    """,
                    del_params,
                ).fetchall()
            else:
                deleted_rows = []

        all_rows = list(rows) + list(deleted_rows)
        all_rows.sort(key=lambda r: str(r["ts"] or ""), reverse=True)
        all_rows = all_rows[:500]

        compras = [
            CompraRow(
                id=r["id"],
                ts=r["ts"],
                factura=r["factura"],
                proveedor=r["proveedor"],
                costo_total_gs=float(r["costo_total_gs"]),
                tipo=r["tipo"],
                motivo=r["motivo"],
                producto=r["producto"],
            )
            for r in all_rows
        ]

        total_gs = sum(c.costo_total_gs for c in compras if c.tipo == "Compra")
        total_eliminado_gs = abs(sum(c.costo_total_gs for c in compras if c.tipo == "Eliminacion"))

        return HistorialComprasSummary(
            total_registros=len(compras),
            total_gs=total_gs,
            total_eliminado_gs=total_eliminado_gs,
            rows=compras,
        )

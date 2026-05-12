from __future__ import annotations

from datetime import date, datetime

from core.database import connection
from modules.historial_ventas.schemas import (
    HistorialVentasSummary,
    ProductoItem,
    ReporteMensualData,
    SucursalRow,
    TopProductoRow,
    VentaDetalleRow,
    VentaRow,
    VentasDetalleResponse,
)


def _month_bounds(ym: str) -> tuple[str, str]:
    y, m = ym.split("-")
    year, month = int(y), int(m)
    first = date(year, month, 1)
    nxt = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    last = nxt.fromordinal(nxt.toordinal() - 1)
    return first.isoformat(), last.isoformat()


def _fmt_gs(x: float | int | None) -> str:
    try:
        return f"{float(x or 0):,.0f}".replace(",", ".")
    except Exception:
        return "0"


class HistorialVentasRepository:
    def summary(
        self,
        search: str = "",
        from_date: str = "",
        to_date: str = "",
        retencion_mode: bool = False,
    ) -> HistorialVentasSummary:
        conditions: list[str] = []
        params: list[str] = []

        if from_date:
            conditions.append("ts::date >= %s")
            params.append(from_date)
        if to_date:
            conditions.append("ts::date <= %s")
            params.append(to_date)
        if search:
            conditions.append("(COALESCE(invoice_no,'') ILIKE %s OR COALESCE(customer,'') ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        sql = f"""
            SELECT
                id,
                COALESCE(CAST(ts AS TEXT), '') AS ts,
                COALESCE(invoice_no, '')        AS invoice_no,
                COALESCE(customer, '')          AS customer,
                COALESCE(gravada5_gs, 0)        AS gravada5_gs,
                COALESCE(iva5_gs, 0)            AS iva5_gs,
                COALESCE(gravada10_gs, 0)       AS gravada10_gs,
                COALESCE(iva10_gs, 0)           AS iva10_gs,
                COALESCE(total_gs, 0)           AS total_gs
            FROM sales_invoices
            {where}
            ORDER BY ts DESC
            LIMIT 500
        """

        with connection("fraccionadora") as cn:
            rows = cn.execute(sql, params).fetchall()

        ventas = [
            VentaRow(
                id=r["id"],
                ts=r["ts"],
                invoice_no=r["invoice_no"],
                customer=r["customer"],
                gravada5_gs=float(r["gravada5_gs"]),
                iva5_gs=float(r["iva5_gs"]),
                gravada10_gs=float(r["gravada10_gs"]),
                iva10_gs=float(r["iva10_gs"]),
                total_gs=float(r["total_gs"]),
                total_con_retencion=float(r["total_gs"]) - 0.30 * (float(r["iva5_gs"]) + float(r["iva10_gs"])),
            )
            for r in rows
        ]

        return HistorialVentasSummary(
            total_registros=len(ventas),
            total_gs=sum(v.total_gs for v in ventas),
            total_con_retencion=sum(v.total_con_retencion for v in ventas),
            rows=ventas,
        )

    def reporte_mensual(self, ym: str, empresa: str = "Fraccionadora") -> ReporteMensualData:
        if len(ym) != 7 or ym[4] != "-":
            raise ValueError("Formato de mes inválido. Use YYYY-MM.")

        d1, d2 = _month_bounds(ym)

        with connection("fraccionadora") as cn:
            def scalar(sql: str, params: tuple = ()) -> float:
                try:
                    row = cn.execute(sql, params).fetchone()
                    return float((list(row.values())[0] if row else 0) or 0)
                except Exception:
                    return 0.0

            def count(sql: str, params: tuple = ()) -> int:
                try:
                    row = cn.execute(sql, params).fetchone()
                    return int((list(row.values())[0] if row else 0) or 0)
                except Exception:
                    return 0

            ventas_facturas = scalar(
                "SELECT COALESCE(SUM(total_gs),0) AS v FROM sales_invoices WHERE ts::date >= %s AND ts::date <= %s",
                (d1, d2),
            )
            ventas_bolsas = scalar(
                "SELECT COALESCE(SUM(total_gs),0) AS v FROM bag_sales WHERE ts::date >= %s AND ts::date <= %s",
                (d1, d2),
            )
            compras_total = scalar(
                "SELECT COALESCE(SUM(costo_total_gs),0) AS v FROM raw_lots WHERE ts::date >= %s AND ts::date <= %s",
                (d1, d2),
            )
            gastos_total = scalar(
                "SELECT COALESCE(SUM(monto_gs),0) AS v FROM expenses WHERE ts::date >= %s AND ts::date <= %s",
                (d1, d2),
            )
            cant_facturas = count(
                "SELECT COUNT(*) AS v FROM sales_invoices WHERE ts::date >= %s AND ts::date <= %s",
                (d1, d2),
            )
            cant_ventas_bolsa = count(
                "SELECT COUNT(*) AS v FROM bag_sales WHERE ts::date >= %s AND ts::date <= %s",
                (d1, d2),
            )

            top_rows: list[TopProductoRow] = []
            try:
                rows = cn.execute(
                    """
                    SELECT p.name AS producto, sii.gramaje,
                           SUM(sii.cantidad) AS paquetes,
                           SUM(sii.line_total) AS total_gs
                    FROM sales_invoice_items sii
                    JOIN sales_invoices si ON si.id = sii.invoice_id
                    JOIN products p ON p.id = sii.product_id
                    WHERE si.ts::date >= %s AND si.ts::date <= %s
                    GROUP BY p.name, sii.gramaje
                    ORDER BY total_gs DESC
                    LIMIT 10
                    """,
                    (d1, d2),
                ).fetchall()
                top_rows = [
                    TopProductoRow(
                        producto=r["producto"] or "",
                        gramaje=int(r["gramaje"] or 0),
                        paquetes=int(r["paquetes"] or 0),
                        total_gs=float(r["total_gs"] or 0),
                    )
                    for r in rows
                ]
            except Exception:
                pass

        ventas_total = ventas_facturas + ventas_bolsas
        margen_bruto = ventas_total - compras_total
        margen_bruto_pct = (margen_bruto / ventas_total * 100.0) if ventas_total else 0.0
        beneficio_operativo = ventas_total - compras_total - gastos_total
        beneficio_pct = (beneficio_operativo / ventas_total * 100.0) if ventas_total else 0.0

        now_txt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lines: list[str] = [
            "=" * 78,
            f"REPORTE MENSUAL - {empresa}",
            "=" * 78,
            f"Periodo: {ym} ({d1} a {d2})",
            f"Generado: {now_txt}",
            "",
            "RESUMEN CONTABLE",
            "-" * 78,
            f"Ventas facturas (Gs)..................: {_fmt_gs(ventas_facturas)}",
            f"Ventas bolsas (Gs)....................: {_fmt_gs(ventas_bolsas)}",
            f"VENTAS TOTALES (Gs)...................: {_fmt_gs(ventas_total)}",
            f"Compras MP del mes (Gs)...............: {_fmt_gs(compras_total)}",
            f"Gastos del mes (Gs)...................: {_fmt_gs(gastos_total)}",
            f"Margen bruto estimado (Gs)............: {_fmt_gs(margen_bruto)} ({margen_bruto_pct:.1f}%)",
            f"Beneficio operativo estimado (Gs).....: {_fmt_gs(beneficio_operativo)} ({beneficio_pct:.1f}%)",
            "",
            "OPERACION DEL MES",
            "-" * 78,
            f"Cantidad de facturas.................: {cant_facturas}",
            f"Cantidad de ventas de bolsa..........: {cant_ventas_bolsa}",
            "",
            "TOP PRODUCTOS (por venta del mes)",
            "-" * 78,
        ]

        if not top_rows:
            lines.append("Sin datos de items de factura disponibles.")
        else:
            for idx, r in enumerate(top_rows, 1):
                lines.append(
                    f"{idx:>2}. {r.producto} {r.gramaje} g"
                    f" | Paq: {r.paquetes}"
                    f" | Total Gs: {_fmt_gs(r.total_gs)}"
                )

        lines += [
            "",
            "NOTA",
            "-" * 78,
            "Margen/beneficio son estimados contables sobre caja del periodo (ventas - compras - gastos).",
            "=" * 78,
        ]

        return ReporteMensualData(
            ym=ym,
            empresa=empresa,
            d1=d1,
            d2=d2,
            ventas_facturas=ventas_facturas,
            ventas_bolsas=ventas_bolsas,
            ventas_total=ventas_total,
            compras_total=compras_total,
            gastos_total=gastos_total,
            margen_bruto=margen_bruto,
            margen_bruto_pct=margen_bruto_pct,
            beneficio_operativo=beneficio_operativo,
            beneficio_pct=beneficio_pct,
            cant_facturas=cant_facturas,
            cant_ventas_bolsa=cant_ventas_bolsa,
            top_productos=top_rows,
            reporte_txt="\n".join(lines) + "\n",
        )

    def list_productos(self) -> list[ProductoItem]:
        with connection("fraccionadora") as cn:
            rows = cn.execute("SELECT id, name FROM products ORDER BY name").fetchall()
        return [ProductoItem(id=r["id"], name=r["name"]) for r in rows]

    def list_meses(self) -> list[str]:
        sql = """
            SELECT DISTINCT ym FROM (
                SELECT TO_CHAR((ts)::timestamp, 'YYYY-MM') AS ym FROM sales_invoices
                UNION
                SELECT TO_CHAR((ts)::timestamp, 'YYYY-MM') AS ym FROM bag_sales
            ) m
            WHERE ym IS NOT NULL AND ym <> ''
            ORDER BY ym DESC
        """
        try:
            with connection("fraccionadora") as cn:
                rows = cn.execute(sql).fetchall()
            return [r["ym"] for r in rows]
        except Exception:
            return []

    def ventas_detalle(
        self,
        periodo: str = "mes",
        product_id: int | None = None,
        gramaje: int | None = None,
        ym: str | None = None,
        desde: str | None = None,
        hasta: str | None = None,
    ) -> VentasDetalleResponse:
        key_si = "TO_CHAR((si.ts)::timestamp, 'IYYY-\"W\"IW')" if periodo == "semana" else "TO_CHAR((si.ts)::timestamp, 'YYYY-MM')"
        key_bs = "TO_CHAR((bs.ts)::timestamp, 'IYYY-\"W\"IW')" if periodo == "semana" else "TO_CHAR((bs.ts)::timestamp, 'YYYY-MM')"
        order = "periodo ASC, producto, gramaje" if periodo == "semana" else "periodo DESC, importe_gs DESC, producto, gramaje"

        where_si: list[str] = ["1=1"]
        params_si: list = []
        where_bs: list[str] = ["1=1"]
        params_bs: list = []

        for where, params in ((where_si, params_si), (where_bs, params_bs)):
            if product_id is not None:
                where.append("sii.product_id=%s" if where is where_si else "bs.product_id=%s")
                params.append(product_id)
            if gramaje is not None:
                where.append("sii.gramaje=%s" if where is where_si else "CAST(bs.kg_por_bolsa*1000 AS INTEGER)=%s")
                params.append(gramaje)
            if ym:
                where.append("TO_CHAR((si.ts)::timestamp, 'YYYY-MM')=%s" if where is where_si else "TO_CHAR((bs.ts)::timestamp, 'YYYY-MM')=%s")
                params.append(ym)
            if desde:
                where.append("si.ts::date >= %s" if where is where_si else "bs.ts::date >= %s")
                params.append(desde)
            if hasta:
                where.append("si.ts::date <= %s" if where is where_si else "bs.ts::date <= %s")
                params.append(hasta)

        sql = f"""
            SELECT * FROM (
                SELECT {key_si} AS periodo,
                       p.name      AS producto,
                       sii.gramaje AS gramaje,
                       SUM(sii.cantidad)   AS paquetes,
                       SUM(sii.line_total) AS importe_gs,
                       COALESCE(SUM(sii.line_base), 0) AS base_gs,
                       COALESCE(SUM(sii.line_iva), 0)  AS iva_gs,
                       SUM(CASE WHEN position('luque'   IN lower(COALESCE(si.customer,''))) > 0 THEN sii.cantidad ELSE 0 END) AS paq_luque,
                       SUM(CASE WHEN position('aregua'  IN lower(COALESCE(si.customer,''))) > 0 THEN sii.cantidad ELSE 0 END) AS paq_aregua,
                       SUM(CASE WHEN position('itaugua' IN lower(COALESCE(si.customer,''))) > 0 THEN sii.cantidad ELSE 0 END) AS paq_itaugua
                FROM sales_invoice_items sii
                JOIN sales_invoices si ON si.id = sii.invoice_id
                JOIN products p        ON p.id = sii.product_id
                WHERE {' AND '.join(where_si)}
                GROUP BY 1, 2, 3
                UNION ALL
                SELECT {key_bs} AS periodo,
                       p.name AS producto,
                       CAST(bs.kg_por_bolsa * 1000 AS INTEGER) AS gramaje,
                       SUM(bs.bolsas)    AS paquetes,
                       SUM(bs.total_gs)  AS importe_gs,
                       0 AS base_gs, 0 AS iva_gs,
                       SUM(CASE WHEN position('luque'   IN lower(COALESCE(bs.customer,''))) > 0 THEN bs.bolsas ELSE 0 END) AS paq_luque,
                       SUM(CASE WHEN position('aregua'  IN lower(COALESCE(bs.customer,''))) > 0 THEN bs.bolsas ELSE 0 END) AS paq_aregua,
                       SUM(CASE WHEN position('itaugua' IN lower(COALESCE(bs.customer,''))) > 0 THEN bs.bolsas ELSE 0 END) AS paq_itaugua
                FROM bag_sales bs
                JOIN products p ON p.id = bs.product_id
                WHERE {' AND '.join(where_bs)}
                GROUP BY 1, 2, 3
            ) resumen
            ORDER BY {order}
        """

        suc_sql = f"""
            SELECT sucursal, SUM(importe_gs) AS importe_gs, SUM(paquetes) AS paquetes
            FROM (
                SELECT CASE
                           WHEN position('luque'   IN lower(COALESCE(si.customer,''))) > 0 THEN 'Luque'
                           WHEN position('aregua'  IN lower(COALESCE(si.customer,''))) > 0 THEN 'Aregua'
                           WHEN position('itaugua' IN lower(COALESCE(si.customer,''))) > 0 THEN 'Itaugua'
                           ELSE 'Otras'
                       END AS sucursal,
                       SUM(sii.line_total) AS importe_gs,
                       SUM(sii.cantidad)   AS paquetes
                FROM sales_invoice_items sii
                JOIN sales_invoices si ON si.id = sii.invoice_id
                WHERE {' AND '.join(where_si)}
                GROUP BY 1
                UNION ALL
                SELECT CASE
                           WHEN position('luque'   IN lower(COALESCE(bs.customer,''))) > 0 THEN 'Luque'
                           WHEN position('aregua'  IN lower(COALESCE(bs.customer,''))) > 0 THEN 'Aregua'
                           WHEN position('itaugua' IN lower(COALESCE(bs.customer,''))) > 0 THEN 'Itaugua'
                           ELSE 'Otras'
                       END AS sucursal,
                       SUM(bs.total_gs) AS importe_gs,
                       SUM(bs.bolsas)   AS paquetes
                FROM bag_sales bs
                WHERE {' AND '.join(where_bs)}
                GROUP BY 1
            ) suc
            GROUP BY sucursal
            ORDER BY importe_gs DESC
        """

        rows: list[VentaDetalleRow] = []
        sucursales: list[SucursalRow] = []

        try:
            with connection("fraccionadora") as cn:
                raw = cn.execute(sql, params_si + params_bs).fetchall()
                rows = [
                    VentaDetalleRow(
                        periodo=r["periodo"] or "",
                        producto=r["producto"] or "",
                        gramaje=int(r["gramaje"] or 0),
                        paquetes=float(r["paquetes"] or 0),
                        importe_gs=float(r["importe_gs"] or 0),
                        base_gs=float(r["base_gs"] or 0),
                        iva_gs=float(r["iva_gs"] or 0),
                        paq_luque=float(r["paq_luque"] or 0),
                        paq_aregua=float(r["paq_aregua"] or 0),
                        paq_itaugua=float(r["paq_itaugua"] or 0),
                    )
                    for r in raw
                ]
                suc_raw = cn.execute(suc_sql, params_si + params_bs).fetchall()
                sucursales = [
                    SucursalRow(
                        sucursal=r["sucursal"] or "",
                        importe_gs=float(r["importe_gs"] or 0),
                        paquetes=float(r["paquetes"] or 0),
                    )
                    for r in suc_raw
                ]
        except Exception:
            pass

        return VentasDetalleResponse(
            rows=rows,
            sucursales=sucursales,
            total_paquetes=sum(r.paquetes for r in rows),
            total_importe_gs=sum(r.importe_gs for r in rows),
        )

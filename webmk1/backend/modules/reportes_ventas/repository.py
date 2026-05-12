from __future__ import annotations

from datetime import date
from typing import Any

from core.database import connection
from modules.reportes_ventas.schemas import (
    FacturaDetalleRow,
    FacturaItemsResponse,
    KpiCard,
    ProductoItem,
    RankingRow,
    ReporteVentaRow,
    ReportesVentasOptions,
    ReportesVentasResumen,
)


def _prev_month(ym: str | None) -> str | None:
    if not ym or len(ym) != 7 or ym[4] != "-":
        return None
    try:
        year = int(ym[:4])
        month = int(ym[5:])
    except ValueError:
        return None
    month -= 1
    if month == 0:
        year -= 1
        month = 12
    return f"{year:04d}-{month:02d}"


def _pct(current: float, previous: float) -> float | None:
    if previous == 0:
        return None if current == 0 else 100.0
    return ((current - previous) / previous) * 100.0


class ReportesVentasRepository:
    def options(self) -> ReportesVentasOptions:
        with connection("fraccionadora") as cn:
            productos = cn.execute("SELECT id, name FROM products ORDER BY name").fetchall()
            meses = cn.execute(
                """
                SELECT DISTINCT ym FROM (
                    SELECT TO_CHAR((ts)::timestamp, 'YYYY-MM') AS ym FROM sales_invoices
                    UNION
                    SELECT TO_CHAR((ts)::timestamp, 'YYYY-MM') AS ym FROM bag_sales
                ) m
                WHERE ym IS NOT NULL AND ym <> ''
                ORDER BY ym DESC
                """
            ).fetchall()
            gramajes = cn.execute(
                """
                SELECT DISTINCT gramaje FROM (
                    SELECT gramaje FROM sales_invoice_items
                    UNION
                    SELECT CAST(kg_por_bolsa * 1000 AS INTEGER) AS gramaje FROM bag_sales
                ) g
                WHERE gramaje IS NOT NULL
                ORDER BY gramaje
                """
            ).fetchall()
        return ReportesVentasOptions(
            productos=[ProductoItem(id=r["id"], name=r["name"]) for r in productos],
            meses=[r["ym"] for r in meses],
            gramajes=[int(r["gramaje"]) for r in gramajes],
        )

    def resumen(
        self,
        periodo: str = "mes",
        product_id: int | None = None,
        gramaje: int | None = None,
        ym: str | None = None,
        desde: str | None = None,
        hasta: str | None = None,
        ranking_scope: str = "month",
        ranking_ym: str | None = None,
    ) -> ReportesVentasResumen:
        rows = self._resumen_rows(periodo, product_id, gramaje, ym, desde, hasta)
        if periodo == "mes":
            previous_rows = self._resumen_rows(periodo, product_id, gramaje, _prev_month(ym), None, None) if ym else rows
        else:
            previous_rows = []
        previous_by_key = {
            (r["periodo"], r["producto"], int(r["gramaje"] or 0)): r
            for r in previous_rows
        }

        total_importe = sum(float(r["importe_gs"] or 0) for r in rows)
        total_paquetes = sum(float(r["paquetes"] or 0) for r in rows)
        facturas = self._facturas_count(product_id, gramaje, ym, desde, hasta)
        prev_facturas = self._facturas_count(product_id, gramaje, _prev_month(ym), None, None) if ym else 0
        prev_importe = sum(float(r["importe_gs"] or 0) for r in previous_rows)
        prev_paquetes = sum(float(r["paquetes"] or 0) for r in previous_rows)
        ticket = total_importe / facturas if facturas else 0.0
        prev_ticket = prev_importe / prev_facturas if prev_facturas else 0.0

        data_rows: list[ReporteVentaRow] = []
        for r in rows:
            key = (_prev_month(r["periodo"]), r["producto"], int(r["gramaje"] or 0))
            prev = previous_by_key.get(key, {})
            importe = float(r["importe_gs"] or 0)
            paquetes = float(r["paquetes"] or 0)
            data_rows.append(
                ReporteVentaRow(
                    periodo=r["periodo"] or "",
                    producto=r["producto"] or "",
                    gramaje=int(r["gramaje"] or 0),
                    paquetes=paquetes,
                    importe_gs=importe,
                    base_gs=float(r["base_gs"] or 0),
                    iva_gs=float(r["iva_gs"] or 0),
                    paq_luque=float(r["paq_luque"] or 0),
                    paq_aregua=float(r["paq_aregua"] or 0),
                    paq_itaugua=float(r["paq_itaugua"] or 0),
                    share_pct=(importe / total_importe * 100.0) if total_importe else 0.0,
                    delta_paquetes_pct=_pct(paquetes, float(prev.get("paquetes") or 0)),
                    delta_importe_pct=_pct(importe, float(prev.get("importe_gs") or 0)),
                )
            )

        rank_ym = ranking_ym if ranking_scope == "month" else None
        return ReportesVentasResumen(
            periodo=periodo,
            filtros_label=ym or f"{desde or 'inicio'} a {hasta or 'hoy'}",
            kpis=[
                KpiCard(key="paquetes", label="Paquetes", value=total_paquetes, delta_pct=_pct(total_paquetes, prev_paquetes)),
                KpiCard(key="importe", label="Importe (Gs)", value=total_importe, delta_pct=_pct(total_importe, prev_importe)),
                KpiCard(key="facturas", label="Cantidad de facturas", value=float(facturas), delta_pct=_pct(float(facturas), float(prev_facturas))),
                KpiCard(key="ticket_promedio", label="Ticket promedio", value=ticket, delta_pct=_pct(ticket, prev_ticket)),
            ],
            top_productos=self._top_productos(product_id, gramaje, rank_ym),
            top_sucursales=self._top_sucursales(product_id, gramaje, rank_ym),
            rows=data_rows,
        )

    def detalle_facturas(
        self,
        product_id: int | None = None,
        gramaje: int | None = None,
        ym: str | None = None,
        desde: str | None = None,
        hasta: str | None = None,
    ) -> FacturaItemsResponse:
        where_si, params_si, where_bs, params_bs = self._where(product_id, gramaje, ym, desde, hasta)
        sql = f"""
            SELECT * FROM (
                SELECT CAST(si.ts AS TEXT) AS fecha,
                       COALESCE(si.invoice_no, '') AS nro_factura,
                       COALESCE(si.customer, '') AS cliente,
                       p.name AS producto,
                       sii.gramaje AS gramaje,
                       sii.cantidad AS paquetes,
                       sii.price_gs AS precio_unit,
                       sii.line_total AS importe_gs,
                       si.id AS invoice_id
                FROM sales_invoice_items sii
                JOIN sales_invoices si ON si.id = sii.invoice_id
                JOIN products p ON p.id = sii.product_id
                WHERE {' AND '.join(where_si)}
                UNION ALL
                SELECT CAST(bs.ts AS TEXT) AS fecha,
                       COALESCE(bs.invoice_no, '') AS nro_factura,
                       COALESCE(bs.customer, '') AS cliente,
                       p.name AS producto,
                       CAST(bs.kg_por_bolsa * 1000 AS INTEGER) AS gramaje,
                       bs.bolsas AS paquetes,
                       bs.price_bolsa_gs AS precio_unit,
                       bs.total_gs AS importe_gs,
                       NULL AS invoice_id
                FROM bag_sales bs
                JOIN products p ON p.id = bs.product_id
                WHERE {' AND '.join(where_bs)}
            ) d
            ORDER BY fecha DESC, producto ASC, gramaje ASC
            LIMIT 1000
        """
        with connection("fraccionadora") as cn:
            rows = cn.execute(sql, params_si + params_bs).fetchall()
        return FacturaItemsResponse(
            rows=[
                FacturaDetalleRow(
                    fecha=r["fecha"] or "",
                    nro_factura=r["nro_factura"] or "",
                    cliente=r["cliente"] or "",
                    producto=r["producto"] or "",
                    gramaje=int(r["gramaje"] or 0),
                    paquetes=float(r["paquetes"] or 0),
                    precio_unit=float(r["precio_unit"] or 0),
                    importe_gs=float(r["importe_gs"] or 0),
                    invoice_id=r["invoice_id"],
                )
                for r in rows
            ]
        )

    def _where(
        self,
        product_id: int | None,
        gramaje: int | None,
        ym: str | None,
        desde: str | None,
        hasta: str | None,
    ) -> tuple[list[str], list[Any], list[str], list[Any]]:
        where_si: list[str] = ["1=1"]
        params_si: list[Any] = []
        where_bs: list[str] = ["1=1"]
        params_bs: list[Any] = []
        if product_id is not None:
            where_si.append("sii.product_id = %s")
            params_si.append(product_id)
            where_bs.append("bs.product_id = %s")
            params_bs.append(product_id)
        if gramaje is not None:
            where_si.append("sii.gramaje = %s")
            params_si.append(gramaje)
            where_bs.append("CAST(bs.kg_por_bolsa * 1000 AS INTEGER) = %s")
            params_bs.append(gramaje)
        if ym:
            where_si.append("TO_CHAR((si.ts)::timestamp, 'YYYY-MM') = %s")
            params_si.append(ym)
            where_bs.append("TO_CHAR((bs.ts)::timestamp, 'YYYY-MM') = %s")
            params_bs.append(ym)
        if desde:
            where_si.append("si.ts::date >= %s")
            params_si.append(desde)
            where_bs.append("bs.ts::date >= %s")
            params_bs.append(desde)
        if hasta:
            where_si.append("si.ts::date <= %s")
            params_si.append(hasta)
            where_bs.append("bs.ts::date <= %s")
            params_bs.append(hasta)
        return where_si, params_si, where_bs, params_bs

    def _resumen_rows(
        self,
        periodo: str,
        product_id: int | None,
        gramaje: int | None,
        ym: str | None,
        desde: str | None,
        hasta: str | None,
    ) -> list[dict[str, Any]]:
        key_si = "TO_CHAR((si.ts)::timestamp, 'IYYY-\"W\"IW')" if periodo == "semana" else "TO_CHAR((si.ts)::timestamp, 'YYYY-MM')"
        key_bs = "TO_CHAR((bs.ts)::timestamp, 'IYYY-\"W\"IW')" if periodo == "semana" else "TO_CHAR((bs.ts)::timestamp, 'YYYY-MM')"
        order = "periodo ASC, producto, gramaje" if periodo == "semana" else "periodo DESC, importe_gs DESC, producto, gramaje"
        where_si, params_si, where_bs, params_bs = self._where(product_id, gramaje, ym, desde, hasta)
        sql = f"""
            SELECT periodo, producto, gramaje,
                   SUM(paquetes) AS paquetes,
                   SUM(importe_gs) AS importe_gs,
                   SUM(base_gs) AS base_gs,
                   SUM(iva_gs) AS iva_gs,
                   SUM(paq_luque) AS paq_luque,
                   SUM(paq_aregua) AS paq_aregua,
                   SUM(paq_itaugua) AS paq_itaugua
            FROM (
                SELECT {key_si} AS periodo,
                       p.name AS producto,
                       sii.gramaje AS gramaje,
                       SUM(sii.cantidad) AS paquetes,
                       SUM(sii.line_total) AS importe_gs,
                       COALESCE(SUM(sii.line_base), 0) AS base_gs,
                       COALESCE(SUM(sii.line_iva), 0) AS iva_gs,
                       SUM(CASE WHEN position('luque' IN lower(COALESCE(si.customer, ''))) > 0 THEN sii.cantidad ELSE 0 END) AS paq_luque,
                       SUM(CASE WHEN position('aregua' IN lower(COALESCE(si.customer, ''))) > 0 THEN sii.cantidad ELSE 0 END) AS paq_aregua,
                       SUM(CASE WHEN position('itaugua' IN lower(COALESCE(si.customer, ''))) > 0 THEN sii.cantidad ELSE 0 END) AS paq_itaugua
                FROM sales_invoice_items sii
                JOIN sales_invoices si ON si.id = sii.invoice_id
                JOIN products p ON p.id = sii.product_id
                WHERE {' AND '.join(where_si)}
                GROUP BY 1, 2, 3
                UNION ALL
                SELECT {key_bs} AS periodo,
                       p.name AS producto,
                       CAST(bs.kg_por_bolsa * 1000 AS INTEGER) AS gramaje,
                       SUM(bs.bolsas) AS paquetes,
                       SUM(bs.total_gs) AS importe_gs,
                       0 AS base_gs,
                       0 AS iva_gs,
                       SUM(CASE WHEN position('luque' IN lower(COALESCE(bs.customer, ''))) > 0 THEN bs.bolsas ELSE 0 END) AS paq_luque,
                       SUM(CASE WHEN position('aregua' IN lower(COALESCE(bs.customer, ''))) > 0 THEN bs.bolsas ELSE 0 END) AS paq_aregua,
                       SUM(CASE WHEN position('itaugua' IN lower(COALESCE(bs.customer, ''))) > 0 THEN bs.bolsas ELSE 0 END) AS paq_itaugua
                FROM bag_sales bs
                JOIN products p ON p.id = bs.product_id
                WHERE {' AND '.join(where_bs)}
                GROUP BY 1, 2, 3
            ) resumen
            WHERE periodo IS NOT NULL AND periodo <> ''
            GROUP BY periodo, producto, gramaje
            ORDER BY {order}
        """
        with connection("fraccionadora") as cn:
            return list(cn.execute(sql, params_si + params_bs).fetchall())

    def _facturas_count(self, product_id: int | None, gramaje: int | None, ym: str | None, desde: str | None, hasta: str | None) -> int:
        where_si, params_si, where_bs, params_bs = self._where(product_id, gramaje, ym, desde, hasta)
        sql = f"""
            SELECT COUNT(*) AS qty
            FROM (
                SELECT DISTINCT 'SI-' || si.id AS doc_key
                FROM sales_invoice_items sii
                JOIN sales_invoices si ON si.id = sii.invoice_id
                WHERE {' AND '.join(where_si)}
                UNION
                SELECT DISTINCT 'BS-' || bs.id AS doc_key
                FROM bag_sales bs
                WHERE {' AND '.join(where_bs)}
            ) docs
        """
        with connection("fraccionadora") as cn:
            row = cn.execute(sql, params_si + params_bs).fetchone()
        return int(row["qty"] or 0) if row else 0

    def _top_productos(self, product_id: int | None, gramaje: int | None, ym: str | None) -> list[RankingRow]:
        rows = self._resumen_rows("mes", product_id, gramaje, ym, None, None)
        grouped: dict[str, dict[str, float]] = {}
        for r in rows:
            label = f"{r['producto']} {int(r['gramaje'] or 0)} g"
            item = grouped.setdefault(label, {"paquetes": 0.0, "importe_gs": 0.0})
            item["paquetes"] += float(r["paquetes"] or 0)
            item["importe_gs"] += float(r["importe_gs"] or 0)
        ordered = sorted(grouped.items(), key=lambda kv: kv[1]["importe_gs"], reverse=True)[:3]
        return [RankingRow(label=k, paquetes=v["paquetes"], importe_gs=v["importe_gs"]) for k, v in ordered]

    def _top_sucursales(self, product_id: int | None, gramaje: int | None, ym: str | None) -> list[RankingRow]:
        where_si, params_si, where_bs, params_bs = self._where(product_id, gramaje, ym, None, None)
        sql = f"""
            SELECT sucursal, SUM(importe_gs) AS importe_gs, SUM(paquetes) AS paquetes
            FROM (
                SELECT CASE
                           WHEN position('luque' IN lower(COALESCE(si.customer, ''))) > 0 THEN 'Luque'
                           WHEN position('aregua' IN lower(COALESCE(si.customer, ''))) > 0 THEN 'Aregua'
                           WHEN position('itaugua' IN lower(COALESCE(si.customer, ''))) > 0 THEN 'Itaugua'
                           ELSE 'Otras'
                       END AS sucursal,
                       SUM(sii.line_total) AS importe_gs,
                       SUM(sii.cantidad) AS paquetes
                FROM sales_invoice_items sii
                JOIN sales_invoices si ON si.id = sii.invoice_id
                WHERE {' AND '.join(where_si)}
                GROUP BY 1
                UNION ALL
                SELECT CASE
                           WHEN position('luque' IN lower(COALESCE(bs.customer, ''))) > 0 THEN 'Luque'
                           WHEN position('aregua' IN lower(COALESCE(bs.customer, ''))) > 0 THEN 'Aregua'
                           WHEN position('itaugua' IN lower(COALESCE(bs.customer, ''))) > 0 THEN 'Itaugua'
                           ELSE 'Otras'
                       END AS sucursal,
                       SUM(bs.total_gs) AS importe_gs,
                       SUM(bs.bolsas) AS paquetes
                FROM bag_sales bs
                WHERE {' AND '.join(where_bs)}
                GROUP BY 1
            ) s
            GROUP BY sucursal
            ORDER BY importe_gs DESC, paquetes DESC
            LIMIT 3
        """
        with connection("fraccionadora") as cn:
            rows = cn.execute(sql, params_si + params_bs).fetchall()
        return [
            RankingRow(label=r["sucursal"] or "", paquetes=float(r["paquetes"] or 0), importe_gs=float(r["importe_gs"] or 0))
            for r in rows
        ]

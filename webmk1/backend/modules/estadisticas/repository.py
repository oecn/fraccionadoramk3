from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from core.database import connection
from modules.estadisticas.schemas import (
    EstadisticaAlerta,
    EstadisticaAlertaDetalle,
    EstadisticaCajaRow,
    EstadisticaClienteRow,
    EstadisticaInventarioRow,
    EstadisticaKpi,
    EstadisticaProductoRow,
    EstadisticasResumen,
)


def _unidades_por_paquete(gramaje: int) -> int:
    return 20 if int(gramaje or 0) <= 250 else 10


class EstadisticasRepository:
    @staticmethod
    def _fmt_gs(value: float) -> str:
        return f"{float(value or 0):,.0f}".replace(",", ".")

    @staticmethod
    def _date_or_default(value: str, fallback: date) -> str:
        try:
            return datetime.strptime((value or "").strip()[:10], "%Y-%m-%d").date().isoformat()
        except Exception:
            return fallback.isoformat()

    @staticmethod
    def _parse_date(value: Any) -> date | None:
        try:
            return datetime.strptime(str(value or "")[:10], "%Y-%m-%d").date()
        except Exception:
            return None

    def _safe_rows(self, sql: str, params: tuple[Any, ...] = ()) -> list[Any]:
        try:
            with connection("fraccionadora") as cn:
                return cn.execute(sql, params).fetchall()
        except Exception:
            return []

    def _safe_scalar(self, sql: str, params: tuple[Any, ...] = ()) -> float:
        rows = self._safe_rows(sql, params)
        if not rows:
            return 0.0
        row = rows[0]
        try:
            return float(row[0] if not hasattr(row, "keys") else row["total"] or 0)
        except Exception:
            return 0.0

    def _avg_costs(self) -> dict[int, float]:
        rows = self._safe_rows(
            """
            SELECT product_id,
                   COALESCE(SUM(costo_total_gs) / NULLIF(SUM(kg_inicial), 0), 0) AS costo_kg
            FROM raw_lots
            GROUP BY product_id
            """
        )
        return {int(r["product_id"]): float(r["costo_kg"] or 0) for r in rows}

    def productos(self, from_date: str, to_date: str, top_n: int) -> list[EstadisticaProductoRow]:
        costs = self._avg_costs()
        merma_rows = self._safe_rows(
            """
            SELECT rl.product_id, COALESCE(SUM(lm.kg), 0) AS kg
            FROM lot_mermas lm
            JOIN raw_lots rl ON rl.id = lm.lot_id
            WHERE lm.ts::date >= CAST(%s AS date) AND lm.ts::date <= CAST(%s AS date)
            GROUP BY rl.product_id
            """,
            (from_date, to_date),
        )
        mermas = {int(r["product_id"]): float(r["kg"] or 0) for r in merma_rows}
        rows = self._safe_rows(
            """
            SELECT p.id AS product_id, p.name AS producto,
                   COALESCE(SUM(sii.line_total), 0) AS venta_gs,
                   COALESCE(SUM(sii.cantidad), 0) AS paquetes,
                   COALESCE(SUM(sii.cantidad * CASE WHEN sii.gramaje <= 250 THEN 20 ELSE 10 END), 0) AS unidades,
                   COALESCE(SUM(sii.cantidad * CASE WHEN sii.gramaje <= 250 THEN 20 ELSE 10 END * sii.gramaje / 1000.0), 0) AS kg_vendidos
            FROM sales_invoice_items sii
            JOIN sales_invoices si ON si.id = sii.invoice_id
            JOIN products p ON p.id = sii.product_id
            WHERE si.ts::date >= CAST(%s AS date) AND si.ts::date <= CAST(%s AS date)
            GROUP BY p.id, p.name
            ORDER BY venta_gs DESC
            LIMIT %s
            """,
            (from_date, to_date, int(top_n)),
        )
        out: list[EstadisticaProductoRow] = []
        for r in rows:
            pid = int(r["product_id"])
            kg_vendidos = float(r["kg_vendidos"] or 0)
            costo_kg = costs.get(pid, 0.0)
            costo = kg_vendidos * costo_kg
            venta = float(r["venta_gs"] or 0)
            margen = venta - costo
            merma_kg = mermas.get(pid, 0.0)
            out.append(
                EstadisticaProductoRow(
                    producto=str(r["producto"] or ""),
                    venta_gs=venta,
                    costo_estimado_gs=costo,
                    margen_gs=margen,
                    margen_pct=(margen / venta * 100) if venta > 0 else 0,
                    paquetes=int(r["paquetes"] or 0),
                    unidades=int(r["unidades"] or 0),
                    kg_vendidos=kg_vendidos,
                    merma_kg=merma_kg,
                    merma_gs=merma_kg * costo_kg,
                )
            )
        return out

    def total_ventas_y_costo(self, from_date: str, to_date: str) -> tuple[float, float]:
        costs = self._avg_costs()
        rows = self._safe_rows(
            """
            SELECT sii.product_id,
                   COALESCE(SUM(sii.line_total), 0) AS venta_gs,
                   COALESCE(SUM(sii.cantidad * CASE WHEN sii.gramaje <= 250 THEN 20 ELSE 10 END * sii.gramaje / 1000.0), 0) AS kg_vendidos
            FROM sales_invoice_items sii
            JOIN sales_invoices si ON si.id = sii.invoice_id
            WHERE si.ts::date >= CAST(%s AS date) AND si.ts::date <= CAST(%s AS date)
            GROUP BY sii.product_id
            """,
            (from_date, to_date),
        )
        ventas = 0.0
        costo = 0.0
        for r in rows:
            pid = int(r["product_id"] or 0)
            ventas += float(r["venta_gs"] or 0)
            costo += float(r["kg_vendidos"] or 0) * costs.get(pid, 0.0)
        ventas += self._safe_scalar(
            "SELECT COALESCE(SUM(total_gs), 0) AS total FROM bag_sales WHERE ts::date >= CAST(%s AS date) AND ts::date <= CAST(%s AS date)",
            (from_date, to_date),
        )
        return ventas, costo

    def inventario(self, from_date: str, to_date: str) -> list[EstadisticaInventarioRow]:
        days = max((self._parse_date(to_date) - self._parse_date(from_date)).days + 1, 1) if self._parse_date(to_date) and self._parse_date(from_date) else 30
        consumo_rows = self._safe_rows(
            """
            SELECT product_id, COALESCE(SUM(kg), 0) AS kg
            FROM (
                SELECT product_id, kg_consumidos AS kg FROM fractionations
                WHERE ts::date >= CAST(%s AS date) AND ts::date <= CAST(%s AS date)
                UNION ALL
                SELECT product_id, kg_total AS kg FROM bag_sales
                WHERE ts::date >= CAST(%s AS date) AND ts::date <= CAST(%s AS date)
            ) t
            GROUP BY product_id
            """,
            (from_date, to_date, from_date, to_date),
        )
        consumo = {int(r["product_id"]): float(r["kg"] or 0) / days for r in consumo_rows}
        rows = self._safe_rows(
            """
            SELECT p.id AS product_id, p.name AS producto,
                   COALESCE(rs.kg, 0) AS stock_kg,
                   COALESCE(SUM(rl.kg_saldo * rl.costo_kg_gs), 0) AS valor_mp,
                   COALESCE(MAX(pkg.valor_pt), 0) AS valor_pt
            FROM products p
            LEFT JOIN raw_stock rs ON rs.product_id = p.id
            LEFT JOIN raw_lots rl ON rl.product_id = p.id AND rl.kg_saldo > 1e-9 AND COALESCE(rl.cerrado, 0) = 0
            LEFT JOIN (
                SELECT ps.product_id, SUM(ps.paquetes * COALESCE(pp.price_gs, 0)) AS valor_pt
                FROM package_stock ps
                LEFT JOIN package_prices pp ON pp.product_id = ps.product_id AND pp.gramaje = ps.gramaje
                GROUP BY ps.product_id
            ) pkg ON pkg.product_id = p.id
            GROUP BY p.id, p.name, rs.kg
            ORDER BY p.name
            """
        )
        out: list[EstadisticaInventarioRow] = []
        for r in rows:
            pid = int(r["product_id"])
            stock = float(r["stock_kg"] or 0)
            diario = consumo.get(pid, 0.0)
            cobertura = (stock / diario) if diario > 0 else None
            estado = "Sin consumo"
            if cobertura is not None:
                estado = "Critico" if cobertura <= 7 else "Atencion" if cobertura <= 15 else "Normal"
            out.append(
                EstadisticaInventarioRow(
                    producto=str(r["producto"] or ""),
                    stock_kg=stock,
                    valor_materia_prima_gs=float(r["valor_mp"] or 0),
                    valor_producto_terminado_gs=float(r["valor_pt"] or 0),
                    consumo_diario_kg=diario,
                    dias_cobertura=cobertura,
                    estado=estado,
                )
            )
        return out

    def clientes(self, from_date: str, to_date: str, top_n: int) -> list[EstadisticaClienteRow]:
        rows = self._safe_rows(
            """
            SELECT COALESCE(NULLIF(TRIM(customer), ''), 'Sin cliente') AS cliente,
                   COUNT(*) AS facturas,
                   COALESCE(SUM(total_gs), 0) AS total_gs,
                   MAX(ts)::text AS ultima_compra
            FROM sales_invoices
            WHERE ts::date >= CAST(%s AS date) AND ts::date <= CAST(%s AS date)
            GROUP BY cliente
            ORDER BY total_gs DESC
            LIMIT %s
            """,
            (from_date, to_date, int(top_n)),
        )
        today = date.today()
        out: list[EstadisticaClienteRow] = []
        for r in rows:
            ultima = self._parse_date(r["ultima_compra"])
            facturas = int(r["facturas"] or 0)
            total = float(r["total_gs"] or 0)
            out.append(
                EstadisticaClienteRow(
                    cliente=str(r["cliente"] or "Sin cliente"),
                    facturas=facturas,
                    total_gs=total,
                    ticket_promedio_gs=(total / facturas) if facturas else 0,
                    ultima_compra=str(r["ultima_compra"] or "")[:10] or None,
                    dias_sin_comprar=(today - ultima).days if ultima else None,
                )
            )
        return out

    def _facturas_vencidas_alerta(self, credit_days: int = 15) -> EstadisticaAlerta | None:
        rows = self._safe_rows(
            """
            SELECT si.id, COALESCE(si.invoice_no, '') AS invoice_no,
                   COALESCE(NULLIF(TRIM(si.customer), ''), 'Sin cliente') AS cliente,
                   CAST(si.ts AS TEXT) AS ts,
                   CURRENT_DATE - si.ts::date AS dias,
                   COALESCE(si.total_gs, 0) - 0.30 * (COALESCE(si.iva5_gs, 0) + COALESCE(si.iva10_gs, 0)) AS total_ret
            FROM sales_invoices si
            WHERE si.ts::date < CURRENT_DATE - (%s * INTERVAL '1 day')
              AND NOT EXISTS (SELECT 1 FROM invoice_collection_items ici WHERE ici.invoice_id = si.id)
              AND NOT EXISTS (
                SELECT 1 FROM dashboard_collection_flags dcf
                WHERE dcf.invoice_id = si.id AND COALESCE(dcf.collected, 0) = 1
              )
            ORDER BY total_ret DESC
            LIMIT 20
            """,
            (credit_days,),
        )
        total = sum(float(r["total_ret"] or 0) for r in rows)
        if not rows:
            return None
        return EstadisticaAlerta(
            tipo="cobro",
            severidad="alta",
            titulo="Facturas vencidas",
            detalle=f"{len(rows)} facturas con mas de {credit_days} dias sin cobro.",
            monto_gs=total,
            accion="Priorizar cobranza por cliente y factura.",
            detalles=[
                EstadisticaAlertaDetalle(
                    label=f"{r['cliente']} / {r['invoice_no'] or 's/factura'}",
                    value=f"{str(r['ts'] or '')[:10]} / {int(r['dias'] or 0)} dias",
                    monto_gs=float(r["total_ret"] or 0),
                )
                for r in rows
            ],
        )

    def _stock_critico_alerta(self, inventario: list[EstadisticaInventarioRow], min_days: int = 7) -> EstadisticaAlerta | None:
        rows = [r for r in inventario if r.dias_cobertura is not None and r.dias_cobertura <= min_days]
        rows.sort(key=lambda r: r.dias_cobertura or 0)
        if not rows:
            return None
        return EstadisticaAlerta(
            tipo="stock",
            severidad="alta",
            titulo="Stock critico",
            detalle=f"{len(rows)} productos con menos de {min_days} dias de cobertura.",
            accion="Revisar compra de materia prima o plan de produccion.",
            detalles=[
                EstadisticaAlertaDetalle(
                    label=r.producto,
                    value=f"{r.dias_cobertura:.1f} dias / {r.stock_kg:.3f} kg",
                    monto_gs=r.valor_materia_prima_gs,
                )
                for r in rows[:20]
            ],
        )

    def _concentracion_deuda_alerta(self) -> EstadisticaAlerta | None:
        rows = self._safe_rows(
            """
            SELECT COALESCE(NULLIF(TRIM(si.customer), ''), 'Sin cliente') AS cliente,
                   COUNT(*) AS facturas,
                   COALESCE(SUM(si.total_gs - 0.30 * (COALESCE(si.iva5_gs, 0) + COALESCE(si.iva10_gs, 0))), 0) AS total
            FROM sales_invoices si
            WHERE NOT EXISTS (SELECT 1 FROM invoice_collection_items ici WHERE ici.invoice_id = si.id)
              AND NOT EXISTS (
                SELECT 1 FROM dashboard_collection_flags dcf
                WHERE dcf.invoice_id = si.id AND COALESCE(dcf.collected, 0) = 1
              )
            GROUP BY cliente
            ORDER BY total DESC
            LIMIT 10
            """
        )
        total_cxc = sum(float(r["total"] or 0) for r in rows)
        if not rows or total_cxc <= 0:
            return None
        top = rows[0]
        top_total = float(top["total"] or 0)
        pct = top_total / total_cxc * 100
        if pct < 30:
            return None
        return EstadisticaAlerta(
            tipo="cliente",
            severidad="media" if pct < 45 else "alta",
            titulo="Concentracion de deuda",
            detalle=f"{top['cliente']} concentra {pct:.1f}% de cuentas por cobrar.",
            monto_gs=top_total,
            accion="Reducir riesgo de credito concentrado.",
            detalles=[
                EstadisticaAlertaDetalle(
                    label=f"{r['cliente']} / {int(r['facturas'] or 0)} facturas",
                    value=f"{(float(r['total'] or 0) / total_cxc * 100):.1f}% del total",
                    monto_gs=float(r["total"] or 0),
                )
                for r in rows
            ],
        )

    def _caja_proyectada_alerta(self, caja_neta: float) -> EstadisticaAlerta | None:
        today = date.today()
        to_date = (today + timedelta(days=15)).isoformat()
        cobros = self._safe_scalar(
            """
            SELECT COALESCE(SUM(si.total_gs - 0.30 * (COALESCE(si.iva5_gs, 0) + COALESCE(si.iva10_gs, 0))), 0) AS total
            FROM sales_invoices si
            WHERE si.ts::date <= CAST(%s AS date)
              AND NOT EXISTS (SELECT 1 FROM invoice_collection_items ici WHERE ici.invoice_id = si.id)
              AND NOT EXISTS (
                SELECT 1 FROM dashboard_collection_flags dcf
                WHERE dcf.invoice_id = si.id AND COALESCE(dcf.collected, 0) = 1
              )
            """,
            (to_date,),
        )
        pagos = self._safe_scalar(
            """
            SELECT COALESCE(SUM(rl.costo_total_gs), 0) AS total
            FROM raw_lots rl
            LEFT JOIN dashboard_payment_flags dpf ON dpf.lot_id = rl.id AND dpf.paid = 1
            WHERE dpf.lot_id IS NULL
              AND (rl.ts::date + INTERVAL '15 days') <= CAST(%s AS date)
            """,
            (to_date,),
        )
        proyectada = caja_neta + cobros - pagos
        if proyectada >= 0:
            return None
        return EstadisticaAlerta(
            tipo="caja",
            severidad="alta",
            titulo="Caja proyectada negativa",
            detalle=f"Riesgo de caja negativa a 15 dias: {self._fmt_gs(proyectada)} Gs.",
            monto_gs=proyectada,
            accion="Acelerar cobros o reprogramar pagos.",
            detalles=[
                EstadisticaAlertaDetalle(label="Caja neta periodo", value="Base de calculo", monto_gs=caja_neta),
                EstadisticaAlertaDetalle(label="Cobros abiertos estimados", value="Proximos 15 dias", monto_gs=cobros),
                EstadisticaAlertaDetalle(label="Pagos proveedores estimados", value="Proximos 15 dias", monto_gs=pagos),
            ],
        )

    def _gasto_fuera_promedio_alerta(self) -> EstadisticaAlerta | None:
        today = date.today()
        month_start = today.replace(day=1)
        prev_start = (month_start - timedelta(days=1)).replace(day=1)
        prev3_start = (prev_start - timedelta(days=70)).replace(day=1)
        rows = self._safe_rows(
            """
            WITH actual AS (
                SELECT tipo, COALESCE(SUM(monto_gs), 0) AS total
                FROM expenses
                WHERE ts::date >= CAST(%s AS date) AND ts::date <= CURRENT_DATE
                GROUP BY tipo
            ),
            hist AS (
                SELECT tipo, COALESCE(SUM(monto_gs), 0) / 3.0 AS promedio
                FROM expenses
                WHERE ts::date >= CAST(%s AS date) AND ts::date < CAST(%s AS date)
                GROUP BY tipo
            )
            SELECT actual.tipo, actual.total, COALESCE(hist.promedio, 0) AS promedio
            FROM actual
            LEFT JOIN hist ON hist.tipo = actual.tipo
            WHERE actual.total > 0 AND COALESCE(hist.promedio, 0) > 0 AND actual.total > hist.promedio * 1.30
            ORDER BY actual.total - hist.promedio DESC
            LIMIT 10
            """,
            (month_start.isoformat(), prev3_start.isoformat(), month_start.isoformat()),
        )
        if not rows:
            return None
        top = rows[0]
        pct = (float(top["total"] or 0) / float(top["promedio"] or 1) - 1) * 100
        return EstadisticaAlerta(
            tipo="gasto",
            severidad="media",
            titulo="Gasto fuera de promedio",
            detalle=f"{top['tipo']} subio {pct:.1f}% contra el promedio de 3 meses.",
            monto_gs=float(top["total"] or 0),
            accion="Revisar categoria y comprobantes del mes.",
            detalles=[
                EstadisticaAlertaDetalle(
                    label=str(r["tipo"] or "Sin tipo"),
                    value=f"Prom. {self._fmt_gs(float(r['promedio'] or 0))} Gs",
                    monto_gs=float(r["total"] or 0),
                )
                for r in rows
            ],
        )

    def alertas(self, inventario: list[EstadisticaInventarioRow], caja_neta: float) -> list[EstadisticaAlerta]:
        candidates = [
            self._caja_proyectada_alerta(caja_neta),
            self._facturas_vencidas_alerta(),
            self._stock_critico_alerta(inventario),
            self._gasto_fuera_promedio_alerta(),
            self._concentracion_deuda_alerta(),
        ]
        order = {"alta": 0, "media": 1, "baja": 2}
        return sorted([item for item in candidates if item is not None], key=lambda item: order.get(item.severidad, 9))

    def resumen(self, from_date: str = "", to_date: str = "", top_n: int = 12) -> EstadisticasResumen:
        today = date.today()
        from_date = self._date_or_default(from_date, today - timedelta(days=30))
        to_date = self._date_or_default(to_date, today)
        top_n = max(1, min(int(top_n or 12), 50))

        productos = self.productos(from_date, to_date, top_n)
        inventario = self.inventario(from_date, to_date)
        clientes = self.clientes(from_date, to_date, top_n)

        ventas, costo = self.total_ventas_y_costo(from_date, to_date)
        margen = ventas - costo
        gastos = self._safe_scalar(
            "SELECT COALESCE(SUM(monto_gs), 0) AS total FROM expenses WHERE ts::date >= CAST(%s AS date) AND ts::date <= CAST(%s AS date)",
            (from_date, to_date),
        )
        utilidad = margen - gastos
        cxc = self._safe_scalar(
            """
            SELECT COALESCE(SUM(si.total_gs - 0.30 * (COALESCE(si.iva5_gs, 0) + COALESCE(si.iva10_gs, 0))), 0) AS total
            FROM sales_invoices si
            WHERE NOT EXISTS (SELECT 1 FROM invoice_collection_items ici WHERE ici.invoice_id = si.id)
              AND NOT EXISTS (
                SELECT 1
                FROM dashboard_collection_flags dcf
                WHERE dcf.invoice_id = si.id AND COALESCE(dcf.collected, 0) = 1
              )
            """
        )
        cxp = self._safe_scalar(
            """
            SELECT COALESCE(SUM(rl.costo_total_gs), 0) AS total
            FROM raw_lots rl
            LEFT JOIN dashboard_payment_flags dpf ON dpf.lot_id = rl.id AND dpf.paid = 1
            WHERE dpf.lot_id IS NULL
            """
        )
        cobrado = self._safe_scalar(
            "SELECT COALESCE(SUM(total_gs), 0) AS total FROM invoice_collections WHERE fecha_cobro >= CAST(%s AS date) AND fecha_cobro <= CAST(%s AS date)",
            (from_date, to_date),
        )
        pagos_mp = self._safe_scalar(
            "SELECT COALESCE(SUM(monto_gs), 0) AS total FROM dashboard_payment_details WHERE fecha_pago >= CAST(%s AS date) AND fecha_pago <= CAST(%s AS date)",
            (from_date, to_date),
        )
        inventario_total = sum(r.valor_materia_prima_gs + r.valor_producto_terminado_gs for r in inventario)
        caja_neta = cobrado - pagos_mp - gastos

        caja = [
            EstadisticaCajaRow(concepto="Cobrado", monto_gs=cobrado, tipo="Ingreso", detalle="Cobros registrados"),
            EstadisticaCajaRow(concepto="Pagos materia prima", monto_gs=pagos_mp, tipo="Egreso", detalle="Facturas de proveedores pagadas"),
            EstadisticaCajaRow(concepto="Gastos", monto_gs=gastos, tipo="Egreso", detalle="IPS, salarios y otros egresos"),
            EstadisticaCajaRow(concepto="Caja neta periodo", monto_gs=caja_neta, tipo="Resultado", detalle="Cobrado - pagos - gastos"),
        ]

        return EstadisticasResumen(
            from_date=from_date,
            to_date=to_date,
            kpis=[
                EstadisticaKpi(title="Ventas", value=self._fmt_gs(ventas), numeric=ventas, subtitle="Gs facturados", tone="neutral"),
                EstadisticaKpi(title="Margen bruto", value=self._fmt_gs(margen), numeric=margen, subtitle=f"{(margen / ventas * 100) if ventas else 0:.1f}%", tone="good" if margen >= 0 else "bad"),
                EstadisticaKpi(title="Utilidad operativa", value=self._fmt_gs(utilidad), numeric=utilidad, subtitle="Margen - gastos", tone="good" if utilidad >= 0 else "bad"),
                EstadisticaKpi(title="Caja neta", value=self._fmt_gs(caja_neta), numeric=caja_neta, subtitle="Periodo", tone="good" if caja_neta >= 0 else "bad"),
                EstadisticaKpi(title="Cuentas por cobrar", value=self._fmt_gs(cxc), numeric=cxc, subtitle="Facturas abiertas", tone="warn" if cxc > 0 else "good"),
                EstadisticaKpi(title="Cuentas por pagar", value=self._fmt_gs(cxp), numeric=cxp, subtitle="Compras abiertas", tone="warn" if cxp > 0 else "good"),
                EstadisticaKpi(title="Inventario valorizado", value=self._fmt_gs(inventario_total), numeric=inventario_total, subtitle="MP + terminado", tone="neutral"),
            ],
            alertas=self.alertas(inventario, caja_neta),
            productos=productos,
            inventario=inventario,
            clientes=clientes,
            caja=caja,
            updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

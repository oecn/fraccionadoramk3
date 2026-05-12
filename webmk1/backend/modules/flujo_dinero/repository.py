from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

from core.database import connection
from modules.flujo_dinero.schemas import FlujoKpi, FlujoMonthRow, FlujoSummary


MONTHS = [
    ("01", "Enero"),
    ("02", "Febrero"),
    ("03", "Marzo"),
    ("04", "Abril"),
    ("05", "Mayo"),
    ("06", "Junio"),
    ("07", "Julio"),
    ("08", "Agosto"),
    ("09", "Septiembre"),
    ("10", "Octubre"),
    ("11", "Noviembre"),
    ("12", "Diciembre"),
]

QUARTERS = {
    "T1": {"01", "02", "03"},
    "T2": {"04", "05", "06"},
    "T3": {"07", "08", "09"},
    "T4": {"10", "11", "12"},
}


class FlujoDineroRepository:
    @staticmethod
    def _fmt_gs(value: float) -> str:
        return f"{float(value or 0):,.0f}".replace(",", ".")

    @staticmethod
    def _parse_date(value: str) -> date | None:
        try:
            return datetime.strptime((value or "").strip()[:10], "%Y-%m-%d").date()
        except Exception:
            return None

    @staticmethod
    def _root_dir() -> Path:
        return Path(__file__).resolve().parents[4]

    def _saldo_inicial(self, year: int, from_date: str, to_date: str) -> float:
        path = self._root_dir() / "GCMK8" / "datos_iniciales.json"
        if not path.exists():
            return 0.0
        d1 = self._parse_date(from_date)
        d2 = self._parse_date(to_date)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return 0.0

        total = 0.0
        for item in raw.get("saldo_inicial", []):
            if not isinstance(item, dict):
                continue
            fecha_txt = str(item.get("fecha") or item.get("date") or "")
            fecha = self._parse_date(fecha_txt)
            if fecha and fecha.year != int(year):
                continue
            if fecha and d1 and fecha < d1:
                continue
            if fecha and d2 and fecha > d2:
                continue
            try:
                total += float(item.get("monto", item.get("valor")) or 0)
            except Exception:
                continue
        return total

    def _monthly_sum(
        self,
        table: str,
        value_expr: str,
        year: int,
        from_date: str,
        to_date: str,
    ) -> dict[str, float]:
        where = ["TO_CHAR((ts)::timestamp, 'YYYY') = %s"]
        params: list[Any] = [str(year)]
        if from_date:
            where.append("ts::date >= CAST(%s AS date)")
            params.append(from_date)
        if to_date:
            where.append("ts::date <= CAST(%s AS date)")
            params.append(to_date)
        sql = f"""
            SELECT TO_CHAR((ts)::timestamp, 'MM') AS month_no, SUM({value_expr}) AS total
            FROM {table}
            WHERE {" AND ".join(where)}
            GROUP BY 1;
        """
        with connection("fraccionadora") as cn:
            rows = cn.execute(sql, tuple(params)).fetchall()
        return {str(r["month_no"]): float(r["total"] or 0.0) for r in rows}

    def _monthly_sum_optional(
        self,
        table: str,
        value_expr: str,
        year: int,
        from_date: str,
        to_date: str,
    ) -> dict[str, float]:
        try:
            return self._monthly_sum(table, value_expr, year, from_date, to_date)
        except Exception:
            return {}

    def available_years(self) -> list[int]:
        years: set[int] = set()
        for table in ("sales_invoices", "raw_lots", "bag_sales", "expenses", "credit_notes"):
            try:
                with connection("fraccionadora") as cn:
                    rows = cn.execute(
                        f"""
                        SELECT DISTINCT TO_CHAR((ts)::timestamp, 'YYYY') AS year
                        FROM {table}
                        WHERE ts IS NOT NULL
                        ORDER BY 1;
                        """
                    ).fetchall()
                years.update(int(r["year"]) for r in rows if r["year"])
            except Exception:
                continue
        if not years:
            years.add(date.today().year)
        return sorted(years)

    def summary(
        self,
        year: int | None = None,
        quarter: str = "Todos",
        retencion_mode: bool = False,
        from_date: str = "",
        to_date: str = "",
    ) -> FlujoSummary:
        selected_year = int(year or date.today().year)
        selected_quarter = (quarter or "Todos").strip()
        if selected_quarter not in {"Todos", "T1", "T2", "T3", "T4"}:
            selected_quarter = "Todos"

        ventas_gross = self._monthly_sum("sales_invoices", "total_gs", selected_year, from_date, to_date)
        ventas_retencion = self._monthly_sum(
            "sales_invoices",
            "total_gs - 0.30 * (COALESCE(iva5_gs, 0) + COALESCE(iva10_gs, 0))",
            selected_year,
            from_date,
            to_date,
        )
        ventas_bolsas = self._monthly_sum_optional("bag_sales", "total_gs", selected_year, from_date, to_date)
        for month_no, amount in ventas_bolsas.items():
            ventas_gross[month_no] = ventas_gross.get(month_no, 0.0) + amount
            ventas_retencion[month_no] = ventas_retencion.get(month_no, 0.0) + amount

        compras = self._monthly_sum("raw_lots", "costo_total_gs", selected_year, from_date, to_date)
        gastos = self._monthly_sum_optional("expenses", "monto_gs", selected_year, from_date, to_date)
        notas_credito_gross = self._monthly_sum_optional("credit_notes", "total_gs", selected_year, from_date, to_date)
        notas_credito_retencion = self._monthly_sum_optional(
            "credit_notes",
            "total_gs - 0.30 * (COALESCE(iva5_gs, 0) + COALESCE(iva10_gs, 0))",
            selected_year,
            from_date,
            to_date,
        )

        ventas = ventas_retencion if retencion_mode else ventas_gross
        notas_credito = notas_credito_retencion if retencion_mode else notas_credito_gross
        saldo_inicial = self._saldo_inicial(selected_year, from_date, to_date)

        allowed_months = QUARTERS.get(selected_quarter)
        month_items = [(m, n) for m, n in MONTHS if allowed_months is None or m in allowed_months]
        acumulado = saldo_inicial
        rows: list[FlujoMonthRow] = []

        for month_no, month_name in month_items:
            v = float(ventas.get(month_no, 0.0))
            v_ret = float(ventas_retencion.get(month_no, 0.0))
            c = float(compras.get(month_no, 0.0))
            nc = float(notas_credito.get(month_no, 0.0))
            g = float(gastos.get(month_no, 0.0))
            flujo = v - nc - (c + g)
            margen = (flujo / v * 100.0) if v > 0 else 0.0
            acumulado += flujo
            rows.append(
                FlujoMonthRow(
                    month=month_name,
                    month_no=month_no,
                    compras=c,
                    ventas=v,
                    ventas_retencion=v_ret,
                    notas_credito=nc,
                    gastos=g,
                    flujo=flujo,
                    margen=margen,
                    acumulado=acumulado,
                )
            )

        ventas_total = sum(r.ventas for r in rows)
        ventas_ret_total = sum(r.ventas_retencion for r in rows)
        notas_total = sum(r.notas_credito for r in rows)
        egresos_total = sum(r.compras + r.gastos for r in rows)
        flujo_total = sum(r.flujo for r in rows)
        banco_estimado = saldo_inicial + flujo_total

        return FlujoSummary(
            year=selected_year,
            from_date=from_date,
            to_date=to_date,
            quarter=selected_quarter,
            retencion_mode=retencion_mode,
            saldo_inicial=saldo_inicial,
            kpis=[
                FlujoKpi(title="Total ventas", value=self._fmt_gs(ventas_total), subtitle="Gs"),
                FlujoKpi(title="Ventas con retencion", value=self._fmt_gs(ventas_ret_total), subtitle="Gs"),
                FlujoKpi(title="Notas de credito", value=self._fmt_gs(notas_total), subtitle="Gs"),
                FlujoKpi(title="Compras + gastos", value=self._fmt_gs(egresos_total), subtitle="Gs"),
                FlujoKpi(title="Flujo neto", value=self._fmt_gs(flujo_total), subtitle="Gs"),
                FlujoKpi(title="Estimado en banco", value=self._fmt_gs(banco_estimado), subtitle=f"Saldo inicial {self._fmt_gs(saldo_inicial)}"),
            ],
            rows=rows,
            updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )


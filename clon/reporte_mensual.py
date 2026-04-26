# -*- coding: utf-8 -*-
"""
Generador de reporte mensual en TXT para fraccionadora.db.

Incluye KPIs contables y operativos:
- Ventas del mes (facturas + ventas de bolsas)
- Compras del mes (lotes de materia prima)
- Gastos del mes
- Margen y beneficio estimado
- Inventario valuado en Gs:
  - Producto terminado (paquetes * precio vigente)
  - Materia prima (kg * ultimo costo/kg)
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path
import sys
from typing import Optional
import unicodedata


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import db

DB_CANDIDATES = [
    ROOT_DIR / "GCMK8" / "fraccionadora.db",
    ROOT_DIR / "fraccionadora.db",
]
OUT_DIR_DEFAULT = ROOT_DIR / "reportes_mensuales"


def _fmt_gs(x: float | int | None) -> str:
    try:
        return f"{float(x or 0):,.0f}".replace(",", ".")
    except Exception:
        return "0"


def _month_bounds(ym: str) -> tuple[str, str]:
    y, m = ym.split("-")
    year, month = int(y), int(m)
    first = date(year, month, 1)
    if month == 12:
        nxt = date(year + 1, 1, 1)
    else:
        nxt = date(year, month + 1, 1)
    last = nxt.fromordinal(nxt.toordinal() - 1)
    return first.isoformat(), last.isoformat()


def _resolve_db(db_arg: Optional[str]) -> Path:
    if db_arg:
        p = Path(db_arg).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"No existe la base: {p}")
        return p
    for p in DB_CANDIDATES:
        if p.exists():
            return p
    raise FileNotFoundError("No se encontro fraccionadora.db en rutas esperadas.")


def _scalar(cur, sql: str, params: tuple = ()) -> float:
    try:
        cur.execute(sql, params)
        row = cur.fetchone()
        return float((row[0] if row else 0) or 0)
    except Exception:
        return 0.0


def _count(cur, sql: str, params: tuple = ()) -> int:
    try:
        cur.execute(sql, params)
        row = cur.fetchone()
        return int((row[0] if row else 0) or 0)
    except Exception:
        return 0


def _bag_kg_default(name: str) -> float:
    n = (name or "").strip().lower()
    if n == "arroz":
        return 50.0
    if n == "galleta molida":
        return 25.0
    return 50.0


def _normalize_name(name: str) -> str:
    s = (name or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.replace("\ufffd", "o").replace("3", "o")
    alias = {
        "azÂ£car": "azucar",
        "azÂ§car": "azucar",
        "az?car": "azucar",
        "aza?car": "azucar",
        "azocar": "azucar",
        "azaocar": "azucar",
    }
    return alias.get(s, s)


def _maquina_for_producto(name: str) -> str:
    if _normalize_name(name) in ("arroz", "azucar"):
        return "Maquina 1"
    return "Maquina 2"


def _unidades_por_paquete(gramaje: int) -> int:
    return 20 if gramaje <= 250 else 10


def _price_at_cutoff(cur, product_id: int, gramaje: int, cutoff_dt: str) -> float:
    # 1) Historial de precios (si existe y hay datos hasta cutoff)
    try:
        cur.execute(
            """
            SELECT price_gs
            FROM package_price_history
            WHERE product_id=%s AND gramaje=%s AND datetime(ts) <= datetime(%s)
            ORDER BY datetime(ts) DESC, id DESC
            LIMIT 1;
            """,
            (product_id, gramaje, cutoff_dt),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0] or 0)
    except Exception:
        pass

    # 2) Fallback: precio vigente actual
    try:
        cur.execute(
            """
            SELECT price_gs
            FROM package_prices
            WHERE product_id=%s AND gramaje=%s
            LIMIT 1;
            """,
            (product_id, gramaje),
        )
        row = cur.fetchone()
        return float((row[0] if row else 0) or 0)
    except Exception:
        return 0.0


def _cost_kg_at_cutoff(cur, product_id: int, cutoff_dt: str) -> float:
    try:
        cur.execute(
            """
            SELECT costo_kg_gs
            FROM raw_lots
            WHERE product_id=%s AND datetime(ts) <= datetime(%s)
            ORDER BY datetime(ts) DESC, id DESC
            LIMIT 1;
            """,
            (product_id, cutoff_dt),
        )
        row = cur.fetchone()
        return float((row[0] if row else 0) or 0)
    except Exception:
        return 0.0


def _inventory_snapshot_values(cur, d2: str) -> tuple[float, float]:
    """
    Devuelve (inventario_terminado_gs, inventario_mp_gs) al cierre de la fecha d2.
    Reconstruye stock por movimientos hasta el cutoff.
    """
    cutoff_dt = f"{d2} 23:59:59"

    # ----- Stock de materia prima al cutoff (kg) -----
    raw_by_pid: dict[int, float] = {}

    # + Compras por lote
    cur.execute(
        """
        SELECT product_id, COALESCE(SUM(kg_inicial),0)
        FROM raw_lots
        WHERE datetime(ts) <= datetime(%s)
        GROUP BY product_id;
        """,
        (cutoff_dt,),
    )
    for pid, kg in cur.fetchall():
        raw_by_pid[int(pid)] = raw_by_pid.get(int(pid), 0.0) + float(kg or 0)

    # - Consumo por fraccionamiento
    cur.execute(
        """
        SELECT product_id, COALESCE(SUM(kg_consumidos),0)
        FROM fractionations
        WHERE datetime(ts) <= datetime(%s)
        GROUP BY product_id;
        """,
        (cutoff_dt,),
    )
    for pid, kg in cur.fetchall():
        raw_by_pid[int(pid)] = raw_by_pid.get(int(pid), 0.0) - float(kg or 0)

    # - Ventas de bolsas
    cur.execute(
        """
        SELECT product_id, COALESCE(SUM(kg_total),0)
        FROM bag_sales
        WHERE datetime(ts) <= datetime(%s)
        GROUP BY product_id;
        """,
        (cutoff_dt,),
    )
    for pid, kg in cur.fetchall():
        raw_by_pid[int(pid)] = raw_by_pid.get(int(pid), 0.0) - float(kg or 0)

    # +/- Ajustes manuales de MP (delta)
    try:
        cur.execute(
            """
            SELECT product_id, COALESCE(SUM(delta),0)
            FROM stock_adjustments
            WHERE kind='raw' AND datetime(ts) <= datetime(%s)
            GROUP BY product_id;
            """,
            (cutoff_dt,),
        )
        for pid, delta in cur.fetchall():
            raw_by_pid[int(pid)] = raw_by_pid.get(int(pid), 0.0) + float(delta or 0)
    except Exception:
        pass

    # Valorización MP al cutoff
    inv_mp = 0.0
    for pid, kg in raw_by_pid.items():
        kg = max(0.0, float(kg or 0))
        cost_kg = _cost_kg_at_cutoff(cur, pid, cutoff_dt)
        inv_mp += kg * cost_kg

    # ----- Stock de producto terminado al cutoff (paquetes) -----
    pkg_by_key: dict[tuple[int, int], float] = {}

    # + Fraccionamientos
    cur.execute(
        """
        SELECT product_id, gramaje, COALESCE(SUM(paquetes),0)
        FROM fractionations
        WHERE datetime(ts) <= datetime(%s)
        GROUP BY product_id, gramaje;
        """,
        (cutoff_dt,),
    )
    for pid, gram, paq in cur.fetchall():
        key = (int(pid), int(gram))
        pkg_by_key[key] = pkg_by_key.get(key, 0.0) + float(paq or 0)

    # - Facturas emitidas
    cur.execute(
        """
        SELECT sii.product_id, sii.gramaje, COALESCE(SUM(sii.cantidad),0)
        FROM sales_invoice_items sii
        JOIN sales_invoices si ON si.id = sii.invoice_id
        WHERE datetime(si.ts) <= datetime(%s)
        GROUP BY sii.product_id, sii.gramaje;
        """,
        (cutoff_dt,),
    )
    for pid, gram, paq in cur.fetchall():
        key = (int(pid), int(gram))
        pkg_by_key[key] = pkg_by_key.get(key, 0.0) - float(paq or 0)

    # +/- Ajustes manuales de paquetes (delta)
    try:
        cur.execute(
            """
            SELECT product_id, gramaje, COALESCE(SUM(delta),0)
            FROM stock_adjustments
            WHERE kind='package' AND datetime(ts) <= datetime(%s)
            GROUP BY product_id, gramaje;
            """,
            (cutoff_dt,),
        )
        for pid, gram, delta in cur.fetchall():
            key = (int(pid), int(gram))
            pkg_by_key[key] = pkg_by_key.get(key, 0.0) + float(delta or 0)
    except Exception:
        pass

    # Valorización producto terminado al cutoff
    inv_terminado = 0.0
    for (pid, gram), paq in pkg_by_key.items():
        paq = max(0.0, float(paq or 0))
        price = _price_at_cutoff(cur, pid, gram, cutoff_dt)
        inv_terminado += paq * price

    return inv_terminado, inv_mp


def build_report(db_path: Path, ym: str, empresa: str) -> str:
    d1, d2 = _month_bounds(ym)
    cn = db.connect("fraccionadora")
    cur = cn.cursor()

    ventas_facturas = _scalar(
        cur,
        """
        SELECT COALESCE(SUM(total_gs),0)
        FROM sales_invoices
        WHERE ts::date >= %s AND ts::date <= %s;
        """,
        (d1, d2),
    )
    ventas_bolsas = _scalar(
        cur,
        """
        SELECT COALESCE(SUM(total_gs),0)
        FROM bag_sales
        WHERE ts::date >= %s AND ts::date <= %s;
        """,
        (d1, d2),
    )
    ventas_total = ventas_facturas + ventas_bolsas

    compras_total = _scalar(
        cur,
        """
        SELECT COALESCE(SUM(costo_total_gs),0)
        FROM raw_lots
        WHERE ts::date >= %s AND ts::date <= %s;
        """,
        (d1, d2),
    )
    gastos_total = _scalar(
        cur,
        """
        SELECT COALESCE(SUM(monto_gs),0)
        FROM expenses
        WHERE ts::date >= %s AND ts::date <= %s;
        """,
        (d1, d2),
    )

    cant_facturas = _count(
        cur,
        "SELECT COUNT(*) FROM sales_invoices WHERE ts::date >= %s AND ts::date <= %s;",
        (d1, d2),
    )
    cant_ventas_bolsa = _count(
        cur,
        "SELECT COUNT(*) FROM bag_sales WHERE ts::date >= %s AND ts::date <= %s;",
        (d1, d2),
    )

    # Produccion del mes en unidades por maquina.
    cur.execute(
        """
        SELECT p.name, f.gramaje, f.paquetes
        FROM fractionations f
        JOIN products p ON p.id = f.product_id
        WHERE f.ts::date >= %s AND f.ts::date <= %s;
        """,
        (d1, d2),
    )
    prod_m1_unid = 0
    prod_m2_unid = 0
    for name, gramaje, paquetes in cur.fetchall():
        try:
            unidades = int(paquetes or 0) * _unidades_por_paquete(int(gramaje or 0))
        except Exception:
            unidades = 0
        maquina = _maquina_for_producto(name)
        if maquina == "Maquina 1":
            prod_m1_unid += unidades
        else:
            prod_m2_unid += unidades
    prod_total_unid = prod_m1_unid + prod_m2_unid

    # Inventario valuado al CIERRE del mes consultado (foto histórica)
    inv_terminado, inv_mp = _inventory_snapshot_values(cur, d2)

    inv_total = inv_terminado + inv_mp

    margen_bruto = ventas_total - compras_total
    margen_bruto_pct = (margen_bruto / ventas_total * 100.0) if ventas_total else 0.0
    beneficio_operativo = ventas_total - compras_total - gastos_total
    beneficio_pct = (beneficio_operativo / ventas_total * 100.0) if ventas_total else 0.0

    # Top productos por ventas del mes
    cur.execute(
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
        LIMIT 10;
        """,
        (d1, d2),
    )
    top_rows = cur.fetchall()

    cn.close()

    now_txt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: list[str] = []
    lines.append("=" * 78)
    lines.append(f"REPORTE MENSUAL - {empresa}")
    lines.append("=" * 78)
    lines.append(f"Periodo: {ym} ({d1} a {d2})")
    lines.append(f"Generado: {now_txt}")
    lines.append(f"Base de datos: {db_path}")
    lines.append("")
    lines.append("RESUMEN CONTABLE")
    lines.append("-" * 78)
    lines.append(f"Ventas facturas (Gs)..................: {_fmt_gs(ventas_facturas)}")
    lines.append(f"Ventas bolsas (Gs)....................: {_fmt_gs(ventas_bolsas)}")
    lines.append(f"VENTAS TOTALES (Gs)...................: {_fmt_gs(ventas_total)}")
    lines.append(f"Compras MP del mes (Gs)...............: {_fmt_gs(compras_total)}")
    lines.append(f"Gastos del mes (Gs)...................: {_fmt_gs(gastos_total)}")
    lines.append(f"Margen bruto estimado (Gs)............: {_fmt_gs(margen_bruto)} ({margen_bruto_pct:.1f}%)")
    lines.append(f"Beneficio operativo estimado (Gs).....: {_fmt_gs(beneficio_operativo)} ({beneficio_pct:.1f}%)")
    lines.append("")
    lines.append("INVENTARIO VALUADO")
    lines.append("-" * 78)
    lines.append(f"Producto terminado (Gs)...............: {_fmt_gs(inv_terminado)}")
    lines.append(f"Materia prima (Gs)....................: {_fmt_gs(inv_mp)}")
    lines.append(f"Inventario total (Gs).................: {_fmt_gs(inv_total)}")
    lines.append("")
    lines.append("OPERACION DEL MES")
    lines.append("-" * 78)
    lines.append(f"Cantidad de facturas.................: {cant_facturas}")
    lines.append(f"Cantidad de ventas de bolsa..........: {cant_ventas_bolsa}")
    lines.append(f"Produccion Maquina 1 (unid)..........: {_fmt_gs(prod_m1_unid)}")
    lines.append(f"Produccion Maquina 2 (unid)..........: {_fmt_gs(prod_m2_unid)}")
    lines.append(f"PRODUCCION TOTAL (unid)..............: {_fmt_gs(prod_total_unid)}")
    lines.append("")
    lines.append("TOP PRODUCTOS (por venta del mes)")
    lines.append("-" * 78)
    if not top_rows:
        lines.append("Sin ventas registradas en el periodo.")
    else:
        for idx, r in enumerate(top_rows, 1):
            lines.append(
                f"{idx:>2}. {r['producto']} {int(r['gramaje'])} g"
                f" | Paq: {int(r['paquetes'] or 0)}"
                f" | Total Gs: {_fmt_gs(r['total_gs'])}"
            )
    lines.append("")
    lines.append("NOTA")
    lines.append("-" * 78)
    lines.append(
        "Margen/beneficio son estimados contables sobre caja del periodo "
        "(ventas - compras - gastos)."
    )
    lines.append("=" * 78)
    return "\n".join(lines) + "\n"


def save_report_for_month(db_path: Path, ym: str, empresa: str, out_dir: Path | str = OUT_DIR_DEFAULT) -> Path:
    out = Path(out_dir).expanduser().resolve()
    out.mkdir(parents=True, exist_ok=True)
    txt = build_report(db_path=db_path, ym=ym, empresa=empresa)
    out_path = out / f"reporte_mensual_{ym}.txt"
    out_path.write_text(txt, encoding="utf-8")
    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Genera reporte mensual TXT contable.")
    parser.add_argument("--db", help="Ruta a fraccionadora.db")
    parser.add_argument("--month", help="Periodo YYYY-MM (por defecto: mes actual)")
    parser.add_argument("--empresa", default="Fraccionadora", help="Nombre de empresa en el encabezado")
    parser.add_argument("--out-dir", default=str(OUT_DIR_DEFAULT), help="Carpeta de salida")
    args = parser.parse_args()

    ym = (args.month or datetime.now().strftime("%Y-%m")).strip()
    if len(ym) != 7 or ym[4] != "-":
        raise SystemExit("Formato de --month invalido. Use YYYY-MM.")

    db_path = _resolve_db(args.db)
    out_path = save_report_for_month(
        db_path=db_path,
        ym=ym,
        empresa=args.empresa.strip() or "Fraccionadora",
        out_dir=args.out_dir,
    )
    print(f"OK: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

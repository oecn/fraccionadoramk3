# -*- coding: utf-8 -*-
"""
ProyecciÃ³n de compras de materia prima.

Calcula consumo promedio diario reciente y estima cuÃ¡ntos dÃ­as
durarÃ¡ el stock actual por producto.

Uso:
    python proyeccion_compras.py          # ventana 30 dÃ­as, top faltantes
    python proyeccion_compras.py 60 5     # ventana 60 dÃ­as, mostrar top 5
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db


def fmt_gs(x: float) -> str:
    try:
        return f"{float(x):,.0f}".replace(",", ".")
    except Exception:
        return "-"


def fmt_kg(x: float) -> str:
    try:
        return f"{float(x):,.3f}"
    except Exception:
        return "-"


def cargar_proyeccion(ventana_dias: int = 30, top_n: int | None = None):

    cn = db.connect("fraccionadora")
    cur = cn.cursor()

    d1 = (dt.datetime.now() - dt.timedelta(days=ventana_dias)).date().isoformat()

    # Stock actual por producto
    cur.execute("""
        SELECT p.id, p.name, rs.kg
        FROM products p
        JOIN raw_stock rs ON rs.product_id = p.id
        ORDER BY p.name
    """)
    stock = {row["id"]: {"name": row["name"], "kg": row["kg"]} for row in cur.fetchall()}

    # Consumo por fraccionamiento (kg_consumidos)
    cur.execute("""
        SELECT f.product_id, SUM(f.kg_consumidos) as kg
        FROM fractionations f
        WHERE f.ts::date >= %s
        GROUP BY f.product_id
    """, (d1,))
    consumo_frac = {row["product_id"]: row["kg"] or 0.0 for row in cur.fetchall()}

    # Ventas de bolsas (kg_total)
    cur.execute("""
        SELECT bs.product_id, SUM(bs.kg_total) as kg
        FROM bag_sales bs
        WHERE bs.ts::date >= %s
        GROUP BY bs.product_id
    """, (d1,))
    consumo_bolsas = {row["product_id"]: row["kg"] or 0.0 for row in cur.fetchall()}

    # Dias activos (union fraccionamientos + bolsas)
    cur.execute("""
        SELECT t.product_id, COUNT(DISTINCT t.dia) AS dias_activos
        FROM (
            SELECT product_id, ts::date AS dia FROM fractionations WHERE ts::date >= %s
            UNION ALL
            SELECT product_id, ts::date AS dia FROM bag_sales WHERE ts::date >= %s
        ) t
        GROUP BY t.product_id
    """, (d1, d1))
    dias_activos = {row["product_id"]: row["dias_activos"] or 0 for row in cur.fetchall()}

    proy = []
    for pid, info in stock.items():
        kg_frac = consumo_frac.get(pid, 0.0)
        kg_bolsa = consumo_bolsas.get(pid, 0.0)
        consumo_total = kg_frac + kg_bolsa
        activos = dias_activos.get(pid, 0)
        consumo_diario = consumo_total / float(activos) if activos else 0.0
        dias_restantes = None
        if consumo_diario > 0:
            dias_restantes = info["kg"] / consumo_diario
        proy.append({
            "producto": info["name"],
            "stock_kg": info["kg"],
            "consumo_diario": consumo_diario,
            "dias_restantes": dias_restantes,
            "consumo_total": consumo_total,
            "dias_activos": activos,
        })

    # Orden: menos dÃ­as restantes primero
    proy.sort(key=lambda x: (x["dias_restantes"] if x["dias_restantes"] is not None else 1e9))
    if top_n:
        proy = proy[:top_n]
    cn.close()
    return proy


def render_tabla(rows, ventana_dias: int):
    headers = ["Producto", "Stock kg", f"Cons./dÃ­a (kg) (Ãºlt {ventana_dias}d)", "DÃ­as restantes", f"Consumo {ventana_dias}d"]
    widths = [22, 12, 18, 14, 18]

    def pad(val, w):
        s = str(val)
        return (s[: w - 1] + "â€¦") if len(s) > w else s.ljust(w)

    line = " | ".join(pad(h, w) for h, w in zip(headers, widths))
    print(line)
    print("-" * len(line))

    for r in rows:
        dias_txt = "-"
        if r["dias_restantes"] is not None:
            dias_txt = f"{r['dias_restantes']:.1f}"
        cols = [
            r["producto"],
            fmt_kg(r["stock_kg"]),
            fmt_kg(r["consumo_diario"]),
            dias_txt,
            fmt_kg(r["consumo_total"]),
        ]
        print(" | ".join(pad(c, w) for c, w in zip(cols, widths)))


def main():
    ventana = 30
    top_n = None
    if len(sys.argv) > 1:
        try:
            ventana = max(1, int(sys.argv[1]))
        except Exception:
            print("Uso: python proyeccion_compras.py [ventana_dias] [top_n]")
            return
    if len(sys.argv) > 2:
        try:
            top_n = max(1, int(sys.argv[2]))
        except Exception:
            print("Uso: python proyeccion_compras.py [ventana_dias] [top_n]")
            return

    rows = cargar_proyeccion(ventana, top_n)
    print(f"ProyecciÃ³n de compras (ventana {ventana} dÃ­as)")
    render_tabla(rows, ventana)


if __name__ == "__main__":
    main()

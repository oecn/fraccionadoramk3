# -*- coding: utf-8 -*-
"""
AnalÃ­tica de clientes basada en facturas y ventas de bolsas.

Uso rÃ¡pido:
    python analitica_clientes.py          # Top 15 clientes
    python analitica_clientes.py 30       # Top 30 clientes
"""
from __future__ import annotations

import datetime as dt
import sys
from collections import defaultdict
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


def fmt_date(s: str | None) -> str:
    if not s:
        return "-"
    return str(s)[:19]


def cargar_resumen(top_n: int = 15):

    cn = db.connect("fraccionadora")
    cur = cn.cursor()

    resumen = defaultdict(lambda: {
        "cliente": "",
        "facturas": 0,
        "ventas_bolsa": 0,
        "monto_facturas": 0.0,
        "monto_bolsa": 0.0,
        "last_ts": None,
    })

    # Facturas (ventas de paquetes)
    cur.execute("""
        SELECT COALESCE(NULLIF(TRIM(customer), ''), 'Sin cliente') AS cliente,
               COUNT(*) AS n,
               SUM(total_gs) AS total,
               MAX(ts) AS last_ts
        FROM sales_invoices
        GROUP BY cliente
    """)
    for row in cur.fetchall():
        c = row["cliente"]
        r = resumen[c]
        r["cliente"] = c
        r["facturas"] += row["n"] or 0
        r["monto_facturas"] += row["total"] or 0.0
        r["last_ts"] = max(r["last_ts"], row["last_ts"]) if r["last_ts"] else row["last_ts"]

    # Ventas de bolsas (materia prima sin fraccionar)
    cur.execute("""
        SELECT COALESCE(NULLIF(TRIM(customer), ''), 'Sin cliente') AS cliente,
               COUNT(*) AS n,
               SUM(total_gs) AS total,
               MAX(ts) AS last_ts
        FROM bag_sales
        GROUP BY cliente
    """)
    for row in cur.fetchall():
        c = row["cliente"]
        r = resumen[c]
        r["cliente"] = c
        r["ventas_bolsa"] += row["n"] or 0
        r["monto_bolsa"] += row["total"] or 0.0
        r["last_ts"] = max(r["last_ts"], row["last_ts"]) if r["last_ts"] else row["last_ts"]

    cn.close()

    filas = []
    for r in resumen.values():
        total_gs = r["monto_facturas"] + r["monto_bolsa"]
        ops = r["facturas"] + r["ventas_bolsa"]
        filas.append({
            "cliente": r["cliente"],
            "ops": ops,
            "facturas": r["facturas"],
            "bolsas": r["ventas_bolsa"],
            "total_gs": total_gs,
            "ticket_prom": (total_gs / ops) if ops else 0.0,
            "last_ts": r["last_ts"],
        })

    filas.sort(key=lambda x: x["total_gs"], reverse=True)
    return filas[:top_n]


def render_tabla(filas):
    headers = ["Cliente", "Ops", "Fact", "Bolsas", "Monto (Gs)", "Ticket prom.", "Ãšltima compra"]
    col_widths = [25, 5, 6, 7, 14, 14, 20]

    def pad(txt, width):
        s = str(txt)
        return (s[:width - 1] + "â€¦") if len(s) > width else s.ljust(width)

    linea = " | ".join(pad(h, w) for h, w in zip(headers, col_widths))
    print(linea)
    print("-" * len(linea))

    for r in filas:
        cols = [
            r["cliente"],
            r["ops"],
            r["facturas"],
            r["bolsas"],
            fmt_gs(r["total_gs"]),
            fmt_gs(r["ticket_prom"]),
            fmt_date(r["last_ts"]),
        ]
        print(" | ".join(pad(c, w) for c, w in zip(cols, col_widths)))


def main():
    top_n = 15
    if len(sys.argv) > 1:
        try:
            top_n = max(1, int(sys.argv[1]))
        except Exception:
            print("Uso: python analitica_clientes.py [top_n]")
            return
    filas = cargar_resumen(top_n)
    print(f"Top {len(filas)} clientes por monto total (facturas + ventas de bolsas)")
    render_tabla(filas)


if __name__ == "__main__":
    main()

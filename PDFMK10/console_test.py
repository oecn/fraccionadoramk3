# -*- coding: utf-8 -*-
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db
from parser.pdf_parser import parse_pdf
from config import PDF_PATH, DB_PATH, SCHEMA
import pdfplumber, os
print("pdfplumber version:", pdfplumber.__version__)
print("pdfplumber file   :", pdfplumber.__file__)
print("Python executable :", sys.executable)


def ensure_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SCHEMA, "r", encoding="utf-8") as f:
        schema_sql = f.read()
    conn = db.connect("pedidos")
    try:
        db.run_ddl(conn, schema_sql)
    finally:
        conn.close()

def insert_oc(meta, items):
    conn = db.connect("pedidos")
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO orden_compra (nro_oc, sucursal, fecha_pedido, raw_text) VALUES (%s,%s,%s,%s) RETURNING id",
            (meta.get("nro_oc"), meta.get("sucursal"), meta.get("fecha_pedido"), meta.get("raw_text"))
        )
        cur.execute("SELECT id FROM orden_compra WHERE nro_oc = %s", (meta.get("nro_oc"),))
        row = cur.fetchone()
        if row is None:
            # Si no encontró nro_oc, crea un registro temporal
            cur.execute(
                "INSERT INTO orden_compra (nro_oc, sucursal, fecha_pedido, raw_text) VALUES (%s,%s,%s,%s) RETURNING id",
                (meta.get("nro_oc") or 'SIN_OC', meta.get("sucursal"), meta.get("fecha_pedido"), meta.get("raw_text"))
            )
            oc_id = cur.fetchone()[0]
        else:
            oc_id = row["id"]

        cur.execute("DELETE FROM orden_item WHERE oc_id = %s", (oc_id,))
        for idx, it in enumerate(items, start=1):
            cur.execute(
                "INSERT INTO orden_item (oc_id, linea, descripcion, cantidad, unidad) VALUES (%s,%s,%s,%s,%s)",
                (oc_id, idx, it.get("descripcion"), it.get("cantidad"), it.get("unidad"))
            )
        conn.commit()
        return oc_id
    finally:
        conn.close()

def main():
    ensure_db()
    assert PDF_PATH.exists(), f"No encuentro el PDF en {PDF_PATH}"
    result = parse_pdf(PDF_PATH)
    meta, items = result["meta"], result["items"]

    print("— META —")
    print("Nro OC       :", meta.get("nro_oc"))
    print("Sucursal     :", meta.get("sucursal"))
    print("Fecha Pedido :", meta.get("fecha_pedido"))
    print(f"Items detectados: {len(items)}\n")

    for i, it in enumerate(items[:20], start=1):
        print(f"{i:>2}. {it.get('descripcion')} | Cant: {it.get('cantidad')}")

    oc_id = insert_oc(meta, items)
    print(f"\nGuardado en SQLite. oc_id={oc_id} DB={DB_PATH}")

if __name__ == "__main__":
    main()

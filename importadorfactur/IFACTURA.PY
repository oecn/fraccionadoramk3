# -*- coding: utf-8 -*-
"""
Importador local de facturas en SQLite.
- Muestra en consola lo detectado del PDF.
- Solo guarda en DB si el usuario confirma.
- Calcula bolsas equivalentes a un peso configurable.

Ejecutar desde esta carpeta:
    python IFACTURA.PY
"""
import math
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db

from factura_parser import parse_invoice
from config_factura import PDF_PATH, DB_PATH, SCHEMA

BOLSA_KG = 50.0  # peso por bolsa para equivalencias


def ensure_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SCHEMA, "r", encoding="utf-8-sig") as f:
        schema_sql = f.read()
    conn = db.connect("facturas")
    try:
        db.run_ddl(conn, schema_sql)
        # Migración ligera: añade columna proveedor si falta
        cur = conn.cursor()
        cols = {c.lower() for c in db.table_columns(conn, "factura")}
        if "proveedor" not in cols:
            cur.execute("ALTER TABLE factura ADD COLUMN proveedor TEXT;")
            conn.commit()
        if "pdf_path" not in cols:
            cur.execute("ALTER TABLE factura ADD COLUMN pdf_path TEXT;")
            conn.commit()
    finally:
        conn.close()


def insert_factura(meta, items, pdf_path=None):
    conn = db.connect("facturas")
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO factura
            (numero, sucursal, fecha_emision, cliente, ruc_cliente, condicion_venta,
             proveedor, total, total_exentas, total_iva5, total_iva10, pdf_path, raw_text)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                meta.get("numero"), meta.get("sucursal"), meta.get("fecha_emision"),
                meta.get("cliente"), meta.get("ruc_cliente"), meta.get("condicion_venta"),
                meta.get("proveedor"),
                meta.get("total"), meta.get("total_exentas"), meta.get("total_iva5"), meta.get("total_iva10"),
                str(pdf_path) if pdf_path else None,
                meta.get("raw_text"),
            )
        )
        cur.execute("SELECT id FROM factura WHERE numero = %s", (meta.get("numero"),))
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO factura (numero, sucursal, fecha_emision, cliente, ruc_cliente, condicion_venta, proveedor, total, total_exentas, total_iva5, total_iva10, pdf_path, raw_text) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                (
                    meta.get("numero") or "SIN_NUMERO", meta.get("sucursal"), meta.get("fecha_emision"),
                    meta.get("cliente"), meta.get("ruc_cliente"), meta.get("condicion_venta"),
                    meta.get("proveedor"),
                    meta.get("total"), meta.get("total_exentas"), meta.get("total_iva5"), meta.get("total_iva10"),
                    str(pdf_path) if pdf_path else None,
                    meta.get("raw_text"),
                )
            )
            factura_id = cur.fetchone()[0]
        else:
            factura_id = row["id"]
            if pdf_path:
                cur.execute("UPDATE factura SET pdf_path=%s WHERE id=%s;", (str(pdf_path), factura_id))

        cur.execute("DELETE FROM factura_item WHERE factura_id = %s", (factura_id,))
        for idx, it in enumerate(items, start=1):
            cur.execute(
                "INSERT INTO factura_item (factura_id, linea, descripcion, kg, precio_unitario, total_linea) VALUES (%s,%s,%s,%s,%s,%s)",
                (
                    factura_id, idx, it.get("descripcion"), it.get("kg"),
                    it.get("precio_unitario"), it.get("total_linea"),
                )
            )
        conn.commit()
        return factura_id
    finally:
        conn.close()


def bolsas_equivalentes(kg: float | None, bolsa_kg: float = BOLSA_KG) -> int | None:
    if kg is None or kg <= 0 or bolsa_kg <= 0:
        return None
    return int(math.ceil(kg / bolsa_kg))


def peso_bolsa_estandar(desc: str | None, default: float = BOLSA_KG) -> float:
    """
    Determina el peso de bolsa segun la descripcion.
    - Arroz: 50 kg
    - Galleta molida: 25 kg
    - Otros: default
    """
    s = (desc or "").lower()
    if "arroz" in s:
        return 50.0
    if "gallet" in s and "molid" in s:
        return 25.0
    return default


def main():
    ensure_db()
    assert PDF_PATH.exists(), f"No encuentro el PDF en {PDF_PATH}"
    result = parse_invoice(PDF_PATH)
    meta, items = result["meta"], result["items"]

    print("=== META ===")
    print("Numero        :", meta.get("numero"))
    print("Fecha emision :", meta.get("fecha_emision"))
    print("Sucursal      :", meta.get("sucursal"))
    print("Cliente       :", meta.get("cliente"))
    print("RUC Cliente   :", meta.get("ruc_cliente"))
    print("Proveedor     :", meta.get("proveedor"))
    print("Cond. venta   :", meta.get("condicion_venta"))
    print("Total         :", meta.get("total"))
    print("IVA 5         :", meta.get("total_iva5"))
    print("IVA 10        :", meta.get("total_iva10"))
    print("Exentas       :", meta.get("total_exentas"))
    print(f"Items detectados: {len(items)} (bolsa ~ {BOLSA_KG} kg)\n")

    total_bolsas = 0
    total_kg = 0.0
    for i, it in enumerate(items, start=1):
        kg = it.get("kg")
        bag_w = peso_bolsa_estandar(it.get("descripcion"), BOLSA_KG)
        bolsas = bolsas_equivalentes(kg, bag_w)
        kg_por_bolsa = (kg / bolsas) if (kg and bolsas) else None
        if kg:
            total_kg += kg
        if bolsas:
            total_bolsas += bolsas
        print(f"{i:>2}. {it.get('descripcion')} | kg: {kg} | unit: {it.get('precio_unitario')} | total: {it.get('total_linea')} | bolsas: {bolsas} | kg/bolsa aprox: {kg_por_bolsa}")

    print(f"\nTotal kg: {total_kg} | Bolsas equivalentes: {total_bolsas}")

    ans = input("\nGuardar en base local facturas.db? (s/N): ").strip().lower()
    if ans != "s":
        print("No se guardo en la base.")
        return

    factura_id = insert_factura(meta, items, pdf_path=PDF_PATH)
    print(f"Guardado en SQLite. factura_id={factura_id} DB={DB_PATH}")


if __name__ == "__main__":
    main()

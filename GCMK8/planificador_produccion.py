# -*- coding: utf-8 -*-
"""
Planificador de producción usando:
- Stock y consumo reciente (ventana en días, sólo días con consumo).
- Kg pendientes de órdenes en facturas importadas (facturas.db).

Devuelve sugerencias por producto.
"""
from __future__ import annotations

import re
from pathlib import Path
import sys
from typing import List, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db
import proyeccion_compras as pc

# Ruta DB de pedidos/OC (PDFMK10)
try:
    from PDFMK10.config import DB_PATH as OC_DB_PATH
except Exception:
    OC_DB_PATH = Path(__file__).resolve().parent.parent / "PDFMK10" / "db" / "pedidos.db"


def normalize_product_key(name: str) -> str:
    text = (name or "").strip().lower()
    text = text.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    text = text.replace("ñ", "n")
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def unidades_por_paquete(gramaje: int) -> int:
    return 20 if int(gramaje or 0) <= 300 else 10


def bag_kg_por_defecto(product_name: str) -> float:
    n = normalize_product_key(product_name)
    if n == "galleta molida":
        return 25.0
    return 50.0


def _map_products() -> Dict[str, str]:
    """Mapea clave normalizada -> nombre de producto."""
    cn = db.connect("fraccionadora")
    cur = cn.cursor()
    cur.execute("SELECT name FROM products;")
    m = {}
    for (name,) in cur.fetchall():
        m[normalize_product_key(name)] = name
    cn.close()
    return m


def _gramaje_en_gramos(desc: str) -> int | None:
    if not desc:
        return None
    s = desc.lower()
    m = re.search(r"(\d+)\s*(?:kg|kilo|kilos)\b", s)
    if m:
        return int(m.group(1)) * 1000
    m = re.search(r"(\d+)\s*(?:g|gr|gramo|gramos)\b", s)
    if m:
        return int(m.group(1))
    m = re.search(r"\*\s*(\d{2,4})\b", s)
    if m:
        try:
            val = int(m.group(1))
            if 50 <= val <= 5000:
                return val
        except Exception:
            return None
    return None


def _match_producto(desc: str, prod_map: Dict[str, str]) -> str:
    """Busca un producto conocido dentro de la descripción; fallback key normalizada completa."""
    base = (desc or "").lower()
    for name in prod_map.values():
        if not name:
            continue
        n = name.lower()
        if n and n in base:
            return normalize_product_key(name)
    return normalize_product_key(desc or "")


def _kg_oc_pendientes(prod_map: Dict[str, str]) -> Dict[str, float]:
    """
    Kg pendientes por producto desde ordenes de compra importadas (enviado=0) en pedidos.db.
    Interpreta 'cantidad' como paquetes; si se puede extraer gramaje, convierte a kg (paquetes*gram/1000).
    Si no se puede extraer gramaje, se salta la línea.
    """
    cn = db.connect("pedidos")
    cur = cn.cursor()
    cur.execute("""
        SELECT TRIM(CAST(oi.descripcion AS TEXT)) AS descripcion,
               COALESCE(SUM(oi.cantidad),0) AS cantidad,
               LOWER(TRIM(CAST(oi.unidad AS TEXT))) AS unidad
        FROM orden_item oi
        JOIN orden_compra oc ON oc.id = oi.oc_id
        WHERE COALESCE(oi.enviado,0)=0
          AND COALESCE(oc.completada,0)=0
          AND oi.descripcion IS NOT NULL
        GROUP BY descripcion, unidad
        HAVING COALESCE(SUM(oi.cantidad),0) > 0
    """)
    res: Dict[str, float] = {}
    for desc, cant, unidad in cur.fetchall():
        try:
            qty = float(cant or 0)
        except Exception:
            qty = 0.0
        if qty <= 0:
            continue
        gram = _gramaje_en_gramos(desc)
        unidad_txt = (unidad or "").strip().lower()
        kg = None
        if gram:
            unidades_pkg = unidades_por_paquete(gram)
            kg = (qty * unidades_pkg * gram) / 1000.0
        elif unidad_txt in {"kg", "kilo", "kilos"}:
            kg = qty
        if kg is None:
            continue
        key = _match_producto(str(desc or ""), prod_map)
        res[key] = res.get(key, 0.0) + kg
    cn.close()
    return res


def _stock_paquetes_por_producto() -> dict[tuple[str, int], float]:
    """
    Devuelve stock de paquetes por producto (key normalizada) y gramaje.
    """
    res: dict[tuple[str, int], float] = {}
    try:
        cn = db.connect("fraccionadora")
        cur = cn.cursor()
        cur.execute("""
            SELECT p.name, ps.gramaje, ps.paquetes
            FROM package_stock ps
            JOIN products p ON p.id = ps.product_id
        """)
        for name, gram, paq in cur.fetchall():
            key = normalize_product_key(name or "")
            try:
                gram_val = int(gram)
                paq_val = float(paq or 0)
                res[(key, gram_val)] = res.get((key, gram_val), 0.0) + paq_val
            except Exception:
                continue
    except Exception:
        pass
    finally:
        try:
            cn.close()
        except Exception:
            pass
    return res


def pendientes_por_gramaje(prod_map: Dict[str, str]) -> list[dict]:
    """
    Detalle de pendientes agrupados por producto+gramaje (solo líneas enviadas=0 y OC no completadas).
    Retorna lista de dict con: producto, gramaje, paquetes pendientes (neto), kg, bolsas_eq.
    """
    stock_paquetes = _stock_paquetes_por_producto()
    cn = db.connect("pedidos")
    cur = cn.cursor()
    cur.execute("""
        SELECT TRIM(CAST(oi.descripcion AS TEXT)) AS descripcion,
               COALESCE(SUM(oi.cantidad),0) AS cantidad,
               LOWER(TRIM(CAST(oi.unidad AS TEXT))) AS unidad
        FROM orden_item oi
        JOIN orden_compra oc ON oc.id = oi.oc_id
        WHERE COALESCE(oi.enviado,0)=0
          AND COALESCE(oc.completada,0)=0
          AND oi.descripcion IS NOT NULL
        GROUP BY descripcion, unidad
        HAVING COALESCE(SUM(oi.cantidad),0) > 0
    """)
    items = []
    for desc, cant, unidad in cur.fetchall():
        try:
            qty = float(cant or 0)
        except Exception:
            qty = 0.0
        if qty <= 0:
            continue
        gram = _gramaje_en_gramos(desc)
        unidad_txt = (unidad or "").strip().lower()
        prod_key = _match_producto(str(desc or ""), prod_map)
        prod_name = prod_map.get(prod_key, prod_key)

        # restar paquetes disponibles del mismo gramaje
        disp_paq = 0.0
        try:
            gram_int = int(gram) if gram is not None else None
            if gram_int is not None:
                disp_paq = float(stock_paquetes.get((prod_key, gram_int), 0.0))
        except Exception:
            disp_paq = 0.0
        missing_paq = max(0.0, qty - disp_paq)
        if missing_paq <= 0:
            continue

        kg = None
        bolsas_eq = None
        if gram:
            unidades_pkg = unidades_por_paquete(gram)
            kg = (missing_paq * unidades_pkg * gram) / 1000.0
            bolsa_peso = None
            # aproximar peso de bolsa base según producto (usar productos de fraccionadora)
            bolsa_peso = float(bag_kg_por_defecto(prod_name))
            if bolsa_peso and bolsa_peso > 0:
                bolsas_eq = kg / bolsa_peso
        elif unidad_txt in {"kg", "kilo", "kilos"}:
            kg = missing_paq

        if kg is None:
            continue
        bolsas_eq = None if bolsas_eq is None else float(bolsas_eq)
        items.append({
            "producto": prod_name,
            "producto_key": prod_key,
            "gramaje": gram,
            "paquetes": missing_paq,
            "kg": kg,
            "bolsas_eq": bolsas_eq,
        })
    cn.close()
    # ordenar por producto y gramaje asc
    items.sort(key=lambda x: (x["producto_key"], int(x["gramaje"] or 999999)))
    return items


def cargar_plan(ventana_dias: int = 30) -> List[Dict[str, Any]]:
    base = pc.cargar_proyeccion(ventana_dias)
    prod_map = _map_products()
    oc_map = _kg_oc_pendientes(prod_map)

    plan = []
    for row in base:
        key = normalize_product_key(row.get("producto", ""))
        oc_kg = oc_map.get(key, 0.0)
        dias_oc = None
        cons = float(row.get("consumo_diario") or 0.0)
        stock = float(row.get("stock_kg") or 0.0)
        if cons > 0:
            dias_oc = (stock + oc_kg) / cons
        dias_activos = float(row.get("dias_activos") or 0.0)
        prod_dia_est = 0.0
        if dias_activos > 0:
            prod_dia_est = float(row.get("consumo_total") or 0.0) / dias_activos
        elif cons > 0:
            prod_dia_est = cons
        plan.append({
            "producto": prod_map.get(key, row.get("producto")),
            "stock_kg": stock,
            "consumo_diario": cons,
            "consumo_total": row.get("consumo_total", 0.0),
            "dias_restantes": row.get("dias_restantes"),
            "dias_activos": row.get("dias_activos"),
            "oc_kg": oc_kg,
            "dias_con_oc": dias_oc,
            "prod_dia_est": prod_dia_est,
        })
    plan.sort(key=lambda r: (r["dias_con_oc"] if r["dias_con_oc"] is not None else 1e9))
    return plan

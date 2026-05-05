# fraccionadora.py

# -*- coding: utf-8 -*-

import os
import csv

import subprocess

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import db
import psycopg

import tkinter as tk

import tkinter.font as tkfont

from tkinter import ttk, messagebox, simpledialog, colorchooser

import datetime as _dt

import requests


from zoneinfo import ZoneInfo

import unicodedata

from historial_ventas import TabHistorialVentas

from resumen_compras import TabResumenCompras

from tab_flujo_dinero import TabFlujoDinero

from tab_gastos import TabGastos

from merma import TabMerma

from venta_bolsas import TabVentaBolsas
from produccion import TabProduccion
from sqlite_backup_sync import backup_project_sqlite_from_postgres








GRAMAJES = [200, 250, 400, 500, 800, 1000]  # gramos

BOLSAS_PREDEF = [25, 30, 50]                 # kg



MATERIAS_PRIMAS_INICIALES = [

    "Azúcar", "Arroz", "Galleta molida", "Pororó", "Poroto Rojo",

    "Locro", "Locrillo", "Lenteja"

]

# Orden de productos como en la factura física

PRODUCT_ORDER = [

    "Arroz", "Azúcar", "Pororó", "Poroto Rojo", "Galleta molida",

    "Locro", "Locrillo", "Lenteja"

]



# Normaliza nombres para que variantes mal codificadas sigan funcionando

ALIAS_MAP = {

    "azucar": "azucar",

    "azúcar": "azucar",

    "azºcar": "azucar",

    "az?car": "azucar",

    "aza?car": "azucar",

    "azocar": "azucar",

    "azaocar": "azucar",

    "pororo": "pororo",

    "pororó": "pororo",

    "poror³": "pororo",

    "poror?": "pororo",

    "porora3": "pororo",

    "poroto tojo": "poroto rojo",

    "gall molida": "galleta molida",

}

# Reglas de palabra clave para descripciones largas (ej: "azucar a granel x kilo").
KEYWORD_MAP = (
    ("azucar", "azucar"),
    ("azukita", "azucar"),
    ("arroz", "arroz"),
    ("pororo", "pororo"),
    ("poroto", "poroto rojo"),
    ("lenteja", "lenteja"),
    ("gallet", "galleta molida"),
    ("molida", "galleta molida"),
    ("locro", "locro"),
    ("locrillo", "locrillo"),
)




def normalize_product_key(name: str) -> str:

    s = (name or "").strip().lower()

    s = unicodedata.normalize("NFKD", s)

    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

    s = s.replace("\ufffd", "o").replace("ã", "a").replace("³", "o")

    s = ALIAS_MAP.get(s, s)

    # Si contiene una palabra clave conocida, forzamos el producto base.

    for kw, target in KEYWORD_MAP:

        if kw in s:

            s = target

            break

    return s



PRODUCT_ORDER_MAP = {normalize_product_key(n): i for i, n in enumerate(PRODUCT_ORDER)}



def product_order_idx(name: str) -> int:

    return PRODUCT_ORDER_MAP.get(normalize_product_key(name), 10_000)



def gram_order_idx(product_name: str, gramaje: int) -> int:

    orden = gramajes_permitidos(product_name)

    try:    return orden.index(gramaje)

    except: return 999





def gramajes_permitidos(product_name: str):

    n = normalize_product_key(product_name)

    if n in {"arroz", "azucar"}:

        return [250, 500, 1000]

    if n == "lenteja":

        return [250, 500]

    return [200, 400, 800]

def unidades_por_paquete(gramaje: int) -> int:

    # 200 y 250 -> 20 unidades/paquete; 400-1000 -> 10 unidades/paquete

    return 20 if gramaje <= 250 else 10



def kg_requeridos_para_paquetes(gramaje_g: int, paquetes: int) -> float:

    # Consumo en kg = paquetes * (unidades_por_paquete * gramaje_g) / 1000

    return paquetes * (unidades_por_paquete(gramaje_g) * gramaje_g) / 1000.0


def format_bag_equivalences(kg: float, product_name: str = "") -> str:

    product_key = normalize_product_key(product_name)

    if product_key in {"arroz", "azucar"}:
        return f"Cantidad bolsas equivalente: 50kg = {kg / 50.0:.2f}"

    if product_key == "galleta molida":
        return f"Cantidad bolsas equivalente: 25kg = {kg / 25.0:.2f}"

    return (
        "Cantidad bolsas equivalente: "
        f"50kg = {kg / 50.0:.2f} | "
        f"25kg = {kg / 25.0:.2f}"
    )


def format_single_bag_equivalence(kg: float, bag_kg: float) -> str:

    bag_txt = f"{bag_kg:.0f}" if float(bag_kg).is_integer() else f"{bag_kg:.2f}".rstrip("0").rstrip(".")

    return f"{bag_txt}kg: {kg / bag_kg:.2f}"

def bag_kg_por_defecto(product_name: str) -> float:

    n = normalize_product_key(product_name)

    if n == "arroz":

        return 50.0

    if n == "galleta molida":

        return 25.0

    return 50.0

def fetch_and_store_weather(repo, lat: float, lon: float):

    url = ("https://api.open-meteo.com/v1/forecast"

           f"?latitude={lat}&longitude={lon}"

           "&current=temperature_2m,relative_humidity_2m,precipitation,cloud_cover"

           "&timezone=America/Asuncion")

    r = requests.get(url, timeout=10)

    r.raise_for_status()

    data = r.json().get("current", {})



    ts_local_s = data.get("time")  # p.ej. "2025-09-17T17:45" en America/Asuncion

    temp_c = data.get("temperature_2m")

    rh = data.get("relative_humidity_2m")

    rain = data.get("precipitation")

    cloud = data.get("cloud_cover")



    # Convertimos a UTC para guardar en DB

    ts_utc_iso = None

    if ts_local_s:

        try:

            tz_asu = ZoneInfo("America/Asuncion")

            dt_local = _dt.datetime.fromisoformat(ts_local_s).replace(tzinfo=tz_asu)

            dt_utc = dt_local.astimezone(_dt.timezone.utc)

            ts_utc_iso = dt_utc.isoformat().replace("+00:00", "Z")

        except Exception:

            ts_utc_iso = ts_local_s  # fallback



    if ts_utc_iso:

        repo.insert_weather(ts_utc_iso, temp_c, rh, rain, cloud, "open-meteo")

    return ts_utc_iso, temp_c, rh, rain, cloud





class Repo:

    def __init__(self):

        self.cn = db.connect("fraccionadora")


        self._init_schema()

        self._ensure_schema_updates()

        self._seed()

        



    def ajustar_raw_kg(self, product_id: int, new_kg: float):
        if new_kg < 0:
            raise ValueError("No se admite negativo.")
        cur = self._cursor()
        cur.execute("SELECT kg FROM raw_stock WHERE product_id=%s;", (product_id,))
        row = cur.fetchone()
        old_kg = row[0] if row else 0.0
        delta = float(new_kg) - float(old_kg)
        cur.execute("UPDATE raw_stock SET kg=%s WHERE product_id=%s;", (new_kg, product_id))
        cur.execute(
            """INSERT INTO stock_adjustments(kind, product_id, gramaje, stock_before, stock_after, delta, motivo)
               VALUES(%s,%s,%s,%s,%s,%s,%s);""",
            ("raw", product_id, None, old_kg, new_kg, delta, "Ajuste inventario"),
        )
        self.cn.commit()





    def _init_schema(self):

        cur = self._cursor()

        db.run_ddl(self.cn, """

        CREATE TABLE IF NOT EXISTS products(

            id BIGSERIAL PRIMARY KEY,

            name TEXT UNIQUE NOT NULL

        );

        CREATE TABLE IF NOT EXISTS raw_stock(  -- stock de materia prima (kg)

            product_id INTEGER PRIMARY KEY,

            kg REAL NOT NULL DEFAULT 0,

            FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE

        );

        CREATE TABLE IF NOT EXISTS raw_alerts( -- minimos de bolsas para aviso de MP baja

            product_id INTEGER PRIMARY KEY,

            min_bolsas REAL NOT NULL DEFAULT 0,

            FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE

        );

        CREATE TABLE IF NOT EXISTS product_bag_display( -- kg/bolsa preferido para mostrar equivalencias

            product_id INTEGER PRIMARY KEY,

            bag_kg REAL NOT NULL,

            FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE

        );

        CREATE TABLE IF NOT EXISTS package_stock( -- stock en paquetes

            product_id INTEGER NOT NULL,

            gramaje INTEGER NOT NULL,

            paquetes INTEGER NOT NULL DEFAULT 0,

            PRIMARY KEY(product_id, gramaje),

            FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE

        );

        CREATE TABLE IF NOT EXISTS product_colors( -- colores personalizados por producto en historial

            product_id INTEGER PRIMARY KEY,

            fg_hex TEXT NOT NULL DEFAULT '#12326B',

            bg_hex TEXT NOT NULL DEFAULT '#E6EEF9',

            FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE

        );


        CREATE TABLE IF NOT EXISTS stock_adjustments( -- log de ajustes manuales
            id BIGSERIAL PRIMARY KEY,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP,
            kind TEXT NOT NULL CHECK (kind IN ('raw','package')),
            product_id INTEGER NOT NULL,
            gramaje INTEGER,
            stock_before REAL NOT NULL,
            stock_after  REAL NOT NULL,
            delta        REAL NOT NULL,
            motivo TEXT NOT NULL,
            FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS purchases(   -- log compras de bolsas

            id BIGSERIAL PRIMARY KEY,

            ts DATETIME DEFAULT CURRENT_TIMESTAMP,

            product_id INTEGER NOT NULL,

            kg REAL NOT NULL,           -- total kg comprados

            bolsa_kg REAL NOT NULL,     -- kg por bolsa

            bolsas INTEGER NOT NULL,    -- cantidad de bolsas

            FOREIGN KEY(product_id) REFERENCES products(id)

        );

        CREATE TABLE IF NOT EXISTS fractionations( -- log fraccionamientos

            id BIGSERIAL PRIMARY KEY,

            ts DATETIME DEFAULT CURRENT_TIMESTAMP,

            product_id INTEGER NOT NULL,

            gramaje INTEGER NOT NULL,

            paquetes INTEGER NOT NULL,

            kg_consumidos REAL NOT NULL,

            FOREIGN KEY(product_id) REFERENCES products(id)

        );

        CREATE TABLE IF NOT EXISTS sales( -- log ventas

            id BIGSERIAL PRIMARY KEY,

            ts DATETIME DEFAULT CURRENT_TIMESTAMP,

            product_id INTEGER NOT NULL,

            gramaje INTEGER NOT NULL,

            paquetes INTEGER NOT NULL,

            FOREIGN KEY(product_id) REFERENCES products(id)

        );

        -- Lista de precios por producto+gramaje (precio unitario IVA incluido)

        CREATE TABLE IF NOT EXISTS package_prices(

            product_id INTEGER NOT NULL,

            gramaje    INTEGER NOT NULL,

            price_gs   REAL NOT NULL,

            iva        INTEGER NOT NULL CHECK (iva IN (5,10)),

            PRIMARY KEY(product_id, gramaje),

            FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE

        );
        CREATE TABLE IF NOT EXISTS package_price_history(
            id BIGSERIAL PRIMARY KEY,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP,
            product_id INTEGER NOT NULL,
            gramaje INTEGER NOT NULL,
            price_gs REAL NOT NULL,
            iva INTEGER NOT NULL CHECK (iva IN (5,10)),
            FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_price_hist_prod_gram_ts
            ON package_price_history(product_id, gramaje, ts, id);



        -- Cabecera de factura

        CREATE TABLE IF NOT EXISTS sales_invoices(

            id BIGSERIAL PRIMARY KEY,

            ts DATETIME DEFAULT CURRENT_TIMESTAMP,

            invoice_no TEXT,

            customer   TEXT,

            gravada5_gs  REAL NOT NULL,

            iva5_gs      REAL NOT NULL,

            gravada10_gs REAL NOT NULL,

            iva10_gs     REAL NOT NULL,

            total_gs     REAL NOT NULL

        );



        -- Detalle de factura

        CREATE TABLE IF NOT EXISTS sales_invoice_items(

            id BIGSERIAL PRIMARY KEY,

            invoice_id INTEGER NOT NULL,

            product_id INTEGER NOT NULL,

            gramaje    INTEGER NOT NULL,

            cantidad   INTEGER NOT NULL,

            price_gs   REAL NOT NULL,    -- precio unitario (IVA incl.)

            iva        INTEGER NOT NULL, -- 5 o 10

            line_total REAL NOT NULL,    -- cantidad * price_gs (IVA incl.)

            line_base  REAL NOT NULL,    -- base gravada (si precio es IVA incl.)

            line_iva   REAL NOT NULL,    -- IVA de la línea

            FOREIGN KEY(invoice_id) REFERENCES sales_invoices(id)

        );
        CREATE TABLE IF NOT EXISTS sales_invoice_item_edits(
            id BIGSERIAL PRIMARY KEY,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP,
            invoice_id INTEGER NOT NULL,
            item_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            gramaje INTEGER NOT NULL,
            old_qty INTEGER NOT NULL,
            new_qty INTEGER NOT NULL,
            old_price_gs REAL NOT NULL,
            new_price_gs REAL NOT NULL,
            old_line_total REAL NOT NULL,
            new_line_total REAL NOT NULL,
            motivo TEXT NOT NULL,
            FOREIGN KEY(invoice_id) REFERENCES sales_invoices(id) ON DELETE CASCADE,
            FOREIGN KEY(item_id) REFERENCES sales_invoice_items(id) ON DELETE CASCADE,
            FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS credit_notes(
            id BIGSERIAL PRIMARY KEY,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP,
            credit_no TEXT,
            invoice_id INTEGER NOT NULL,
            customer TEXT,
            motivo TEXT,
            reingresa_stock INTEGER NOT NULL DEFAULT 1,
            gravada5_gs REAL NOT NULL,
            iva5_gs REAL NOT NULL,
            gravada10_gs REAL NOT NULL,
            iva10_gs REAL NOT NULL,
            total_gs REAL NOT NULL,
            FOREIGN KEY(invoice_id) REFERENCES sales_invoices(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS credit_note_items(
            id BIGSERIAL PRIMARY KEY,
            credit_note_id INTEGER NOT NULL,
            invoice_item_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            gramaje INTEGER NOT NULL,
            cantidad INTEGER NOT NULL,
            price_gs REAL NOT NULL,
            iva INTEGER NOT NULL,
            line_total REAL NOT NULL,
            line_base REAL NOT NULL,
            line_iva REAL NOT NULL,
            FOREIGN KEY(credit_note_id) REFERENCES credit_notes(id) ON DELETE CASCADE,
            FOREIGN KEY(invoice_item_id) REFERENCES sales_invoice_items(id) ON DELETE CASCADE,
            FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
        );

        -- Ventas de bolsas (materia prima sin fraccionar)

        CREATE TABLE IF NOT EXISTS bag_sales(

            id BIGSERIAL PRIMARY KEY,

            ts DATETIME DEFAULT CURRENT_TIMESTAMP,

            product_id INTEGER NOT NULL,

            bolsas INTEGER NOT NULL,

            kg_por_bolsa REAL NOT NULL,

            kg_total REAL NOT NULL,

            price_bolsa_gs REAL NOT NULL,

            total_gs REAL NOT NULL,

            customer TEXT,

            invoice_no TEXT,

            notes TEXT,

            FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE

        );

        -- Lotes de materia prima (compra por lote)

        CREATE TABLE IF NOT EXISTS raw_lots(

            id BIGSERIAL PRIMARY KEY,

            ts DATETIME DEFAULT CURRENT_TIMESTAMP,

            product_id INTEGER NOT NULL,

            lote TEXT,                 -- nro/lote (opcional)

            proveedor TEXT,            -- proveedor (opcional)

            factura TEXT,              -- nro factura (opcional)

            kg_inicial REAL NOT NULL,  -- kg comprados para el lote

            kg_saldo  REAL NOT NULL,   -- kg aún disponibles en el lote

            costo_total_gs REAL NOT NULL DEFAULT 0,

            costo_kg_gs    REAL NOT NULL DEFAULT 0,

            cerrado INTEGER NOT NULL DEFAULT 0,

            FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE

        );



        -- Vínculo fraccionamiento <- lote (para saber de qué lote se consumió)

        CREATE TABLE IF NOT EXISTS lot_fractionations(

            id BIGSERIAL PRIMARY KEY,

            lot_id INTEGER NOT NULL,

            fractionation_id INTEGER NOT NULL,

            kg_consumidos REAL NOT NULL,

            FOREIGN KEY(lot_id) REFERENCES raw_lots(id) ON DELETE CASCADE,

            FOREIGN KEY(fractionation_id) REFERENCES fractionations(id) ON DELETE CASCADE

        );



        CREATE TABLE IF NOT EXISTS lot_mermas(

            id BIGSERIAL PRIMARY KEY,

            lot_id INTEGER NOT NULL,

            ts DATETIME DEFAULT CURRENT_TIMESTAMP,

            kg REAL NOT NULL,

            motivo TEXT,

            FOREIGN KEY(lot_id) REFERENCES raw_lots(id) ON DELETE CASCADE

        );

        CREATE INDEX IF NOT EXISTS idx_lot_mermas_lot ON lot_mermas(lot_id);



        CREATE TABLE IF NOT EXISTS weather_readings(

            id BIGSERIAL PRIMARY KEY,

            ts DATETIME NOT NULL,          -- fecha/hora UTC del dato

            temp_c REAL,

            rh_pct REAL,                   -- humedad relativa %

            rain_mm REAL,                  -- lluvia última hora estimada

            cloud_pct REAL,                -- nubosidad %

            src TEXT                       -- fuente (p.ej. 'open-meteo')

        );

        CREATE TABLE IF NOT EXISTS expenses(

            id BIGSERIAL PRIMARY KEY,

            ts DATETIME NOT NULL,

            tipo TEXT NOT NULL,

            descripcion TEXT,

            monto_gs REAL NOT NULL

        );

                           

                           """)

        self.cn.commit()



    def _ensure_schema_updates(self):

        cur = self._cursor()

        cols = db.table_columns(self.cn, "raw_lots")

        if "cerrado" not in cols:

            cur.execute("ALTER TABLE raw_lots ADD COLUMN cerrado INTEGER NOT NULL DEFAULT 0;")

            self.cn.commit()

        cols = db.table_columns(self.cn, "lot_mermas")

        if cols and "motivo" not in cols:

            cur.execute("ALTER TABLE lot_mermas ADD COLUMN motivo TEXT;")

            self.cn.commit()

        cols = db.table_columns(self.cn, "product_colors")
        if cols:
            if "fg_hex" not in cols:
                cur.execute("ALTER TABLE product_colors ADD COLUMN fg_hex TEXT NOT NULL DEFAULT '#12326B';")
                self.cn.commit()
            if "bg_hex" not in cols:
                cur.execute("ALTER TABLE product_colors ADD COLUMN bg_hex TEXT NOT NULL DEFAULT '#E6EEF9';")
                self.cn.commit()
        self._dedupe_products_by_name()
        self._ensure_products_name_unique_index()

    def _dedupe_products_by_name(self):

        cur = self._cursor()
        cur.execute("SELECT id, name FROM products ORDER BY id;")
        rows = cur.fetchall()

        grouped: dict[str, list[tuple[int, str]]] = {}
        for pid, name in rows:
            clean_name = str(name or "").strip()
            grouped.setdefault(clean_name, []).append((int(pid), str(name or "")))

        changed = False
        for clean_name, items in grouped.items():
            if not items:
                continue
            canonical_id = min(pid for pid, _name in items)
            original_name = next(name for pid, name in items if pid == canonical_id)

            if clean_name and original_name != clean_name:
                cur.execute("UPDATE products SET name=%s WHERE id=%s;", (clean_name, canonical_id))
                changed = True

            duplicate_ids = [pid for pid, _name in items if pid != canonical_id]
            if duplicate_ids:
                self._merge_product_duplicates(cur, canonical_id, duplicate_ids)
                changed = True

        if changed:
            self.cn.commit()

    def _merge_product_duplicates(self, cur, canonical_id: int, duplicate_ids: list[int]):

        for duplicate_id in duplicate_ids:
            self._merge_single_value_row(cur, "raw_stock", "kg", canonical_id, duplicate_id, merge_mode="sum")
            self._merge_single_value_row(cur, "raw_alerts", "min_bolsas", canonical_id, duplicate_id, merge_mode="max")
            self._merge_single_value_row(cur, "product_bag_display", "bag_kg", canonical_id, duplicate_id, merge_mode="prefer_existing")
            self._merge_product_color_row(cur, canonical_id, duplicate_id)
            self._merge_package_stock(cur, canonical_id, duplicate_id)
            self._merge_package_prices(cur, canonical_id, duplicate_id)
            self._repoint_product_references(cur, canonical_id, duplicate_id)
            cur.execute("DELETE FROM products WHERE id=%s;", (duplicate_id,))

    def _merge_single_value_row(self, cur, table: str, value_col: str, canonical_id: int, duplicate_id: int,
                                merge_mode: str = "sum"):

        cur.execute(f"SELECT {value_col} FROM {table} WHERE product_id=%s;", (canonical_id,))
        keep_row = cur.fetchone()
        cur.execute(f"SELECT {value_col} FROM {table} WHERE product_id=%s;", (duplicate_id,))
        dup_row = cur.fetchone()

        if not dup_row:
            return
        if not keep_row:
            cur.execute(
                f"UPDATE {table} SET product_id=%s WHERE product_id=%s;",
                (canonical_id, duplicate_id),
            )
            return

        keep_value = keep_row[0]
        dup_value = dup_row[0]
        if merge_mode == "sum":
            merged_value = float(keep_value or 0) + float(dup_value or 0)
        elif merge_mode == "max":
            merged_value = max(float(keep_value or 0), float(dup_value or 0))
        else:
            merged_value = keep_value if keep_value not in (None, "", 0) else dup_value

        cur.execute(
            f"UPDATE {table} SET {value_col}=%s WHERE product_id=%s;",
            (merged_value, canonical_id),
        )
        cur.execute(f"DELETE FROM {table} WHERE product_id=%s;", (duplicate_id,))

    def _merge_product_color_row(self, cur, canonical_id: int, duplicate_id: int):

        cur.execute("SELECT fg_hex, bg_hex FROM product_colors WHERE product_id=%s;", (canonical_id,))
        keep_row = cur.fetchone()
        cur.execute("SELECT fg_hex, bg_hex FROM product_colors WHERE product_id=%s;", (duplicate_id,))
        dup_row = cur.fetchone()

        if not dup_row:
            return
        if not keep_row:
            cur.execute("UPDATE product_colors SET product_id=%s WHERE product_id=%s;", (canonical_id, duplicate_id))
            return

        keep_fg, keep_bg = keep_row
        dup_fg, dup_bg = dup_row
        merged_fg = keep_fg if keep_fg and keep_fg != "#12326B" else dup_fg
        merged_bg = keep_bg if keep_bg and keep_bg != "#E6EEF9" else dup_bg
        cur.execute(
            "UPDATE product_colors SET fg_hex=%s, bg_hex=%s WHERE product_id=%s;",
            (merged_fg, merged_bg, canonical_id),
        )
        cur.execute("DELETE FROM product_colors WHERE product_id=%s;", (duplicate_id,))

    def _merge_package_stock(self, cur, canonical_id: int, duplicate_id: int):

        cur.execute("SELECT gramaje, paquetes FROM package_stock WHERE product_id=%s;", (duplicate_id,))
        for gramaje, paquetes in cur.fetchall():
            cur.execute("SELECT paquetes FROM package_stock WHERE product_id=%s AND gramaje=%s;", (canonical_id, gramaje))
            keep_row = cur.fetchone()
            if keep_row:
                merged_paq = int(keep_row[0] or 0) + int(paquetes or 0)
                cur.execute(
                    "UPDATE package_stock SET paquetes=%s WHERE product_id=%s AND gramaje=%s;",
                    (merged_paq, canonical_id, gramaje),
                )
                cur.execute("DELETE FROM package_stock WHERE product_id=%s AND gramaje=%s;", (duplicate_id, gramaje))
            else:
                cur.execute(
                    "UPDATE package_stock SET product_id=%s WHERE product_id=%s AND gramaje=%s;",
                    (canonical_id, duplicate_id, gramaje),
                )

    def _merge_package_prices(self, cur, canonical_id: int, duplicate_id: int):

        cur.execute("SELECT gramaje, price_gs, iva FROM package_prices WHERE product_id=%s;", (duplicate_id,))
        for gramaje, price_gs, iva in cur.fetchall():
            cur.execute("SELECT 1 FROM package_prices WHERE product_id=%s AND gramaje=%s;", (canonical_id, gramaje))
            if cur.fetchone():
                cur.execute("DELETE FROM package_prices WHERE product_id=%s AND gramaje=%s;", (duplicate_id, gramaje))
            else:
                cur.execute(
                    "UPDATE package_prices SET product_id=%s WHERE product_id=%s AND gramaje=%s;",
                    (canonical_id, duplicate_id, gramaje),
                )

        cur.execute("UPDATE package_price_history SET product_id=%s WHERE product_id=%s;", (canonical_id, duplicate_id))

    def _repoint_product_references(self, cur, canonical_id: int, duplicate_id: int):

        ref_tables = [
            "stock_adjustments",
            "purchases",
            "fractionations",
            "sales",
            "sales_invoice_items",
            "sales_invoice_item_edits",
            "credit_note_items",
            "bag_sales",
            "raw_lots",
        ]
        for table in ref_tables:
            cur.execute(f"UPDATE {table} SET product_id=%s WHERE product_id=%s;", (canonical_id, duplicate_id))

    def _ensure_products_name_unique_index(self):

        cur = self._cursor()
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_products_name_unique ON products(name);")
        self.cn.commit()

    def insert_weather(self, ts: str, temp_c: float|None, rh_pct: float|None,

                    rain_mm: float|None, cloud_pct: float|None, src: str="open-meteo"):

        cur = self._cursor()

        cur.execute("""INSERT INTO weather_readings(ts,temp_c,rh_pct,rain_mm,cloud_pct,src)

                    VALUES(%s,%s,%s,%s,%s,%s);""", (ts, temp_c, rh_pct, rain_mm, cloud_pct, src))

        self.cn.commit()



    def latest_weather(self):

        cur = self._cursor()

        cur.execute("""SELECT ts, temp_c, rh_pct, rain_mm, cloud_pct, src

                    FROM weather_readings ORDER BY ts DESC LIMIT 1;""")

        return cur.fetchone()

    def _seed(self):

        cur = self._cursor()

        # Insertar materias primas iniciales si no existen

        for nombre in MATERIAS_PRIMAS_INICIALES:

            cur.execute("INSERT INTO products(name) VALUES(%s) ON CONFLICT DO NOTHING;", (nombre,))

        # Asegurar raw_stock para cada producto

        cur.execute("SELECT id FROM products;")

        for (pid,) in cur.fetchall():

            cur.execute("INSERT INTO raw_stock(product_id, kg) VALUES(%s, 0) ON CONFLICT DO NOTHING;", (pid,))

        self.cn.commit()



    # CRUD Productos

    def _fetch_returning_id(self, cur, operation: str) -> int:
        row = db.fetchone_required(cur, f"No se pudo obtener el ID generado en {operation}.")
        return int(row[0])

    def add_product(self, name: str):

        self._recover_if_aborted()
        cur = self._cursor()

        try:

            cur.execute("INSERT INTO products(name) VALUES(%s) RETURNING id", (name.strip(),))
            pid = self._fetch_returning_id(cur, "alta de producto")

            cur.execute("INSERT INTO raw_stock(product_id, kg) VALUES(%s, 0);", (pid,))

            self.cn.commit()

            return pid

        except Exception:

            self.cn.rollback()

            raise



    def _recover_if_aborted(self):
        """Roll back any aborted transaction so the connection stays usable."""
        try:
            if self.cn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR:
                self.cn.rollback()
        except Exception:
            pass

    def _cursor(self):
        """Devuelve cursor usable; reconecta si la conexion persistente caduco."""
        self._recover_if_aborted()
        try:
            return self.cn.cursor()
        except Exception:
            try:
                self.cn.close()
            except Exception:
                pass
            self.cn = db.connect("fraccionadora")
            return self.cn.cursor()

    def list_products(self):

        cur = self._cursor()

        cur.execute("SELECT id, name FROM products ORDER BY name;")

        return cur.fetchall()



    def get_product_id_by_name(self, name: str):

        cur = self._cursor()

        cur.execute("SELECT id FROM products WHERE name=%s;", (name,))

        row = cur.fetchone()

        return row[0] if row else None

    def _validate_hex_color(self, color_hex: str, field_name: str = "Color") -> str:

        c = (color_hex or "").strip().upper()

        if len(c) != 7 or not c.startswith("#"):

            raise ValueError(f"{field_name} inválido.")

        try:

            int(c[1:], 16)

        except Exception as exc:

            raise ValueError(f"{field_name} inválido.") from exc

        return c

    def list_product_color_styles(self) -> dict[int, dict[str, str]]:

        cur = self._cursor()
        cols = db.table_columns(self.cn, "product_colors")

        out: dict[int, dict[str, str]] = {}

        if not cols:
            return out

        if "fg_hex" in cols and "bg_hex" in cols:
            cur.execute("SELECT product_id, fg_hex, bg_hex FROM product_colors;")
            rows = cur.fetchall()
            for pid, fg_hex, bg_hex in rows:
                try:
                    fg = self._validate_hex_color(fg_hex, "Color de letra")
                    bg = self._validate_hex_color(bg_hex, "Color de fondo")
                except Exception:
                    continue
                out[int(pid)] = {"fg": fg, "bg": bg}
            return out

        if "color_hex" in cols:
            cur.execute("SELECT product_id, color_hex FROM product_colors;")
            rows = cur.fetchall()
            for pid, color_hex in rows:
                try:
                    fg = self._validate_hex_color(color_hex, "Color de letra")
                except Exception:
                    continue
                out[int(pid)] = {"fg": fg, "bg": "#E6EEF9"}
        return out

    def set_product_color_style(self, product_id: int, fg_hex: str, bg_hex: str):

        fg = self._validate_hex_color(fg_hex, "Color de letra")
        bg = self._validate_hex_color(bg_hex, "Color de fondo")

        cur = self._cursor()
        cols = db.table_columns(self.cn, "product_colors")

        if "color_hex" in cols and "fg_hex" in cols and "bg_hex" in cols:
            cur.execute("""
                INSERT INTO product_colors(product_id, color_hex, fg_hex, bg_hex)
                VALUES(%s, %s, %s, %s)
                ON CONFLICT(product_id) DO UPDATE SET
                    color_hex=excluded.color_hex,
                    fg_hex=excluded.fg_hex,
                    bg_hex=excluded.bg_hex;
            """, (product_id, fg, fg, bg))
        elif "fg_hex" in cols and "bg_hex" in cols:
            cur.execute("""
                INSERT INTO product_colors(product_id, fg_hex, bg_hex)
                VALUES(%s, %s, %s)
                ON CONFLICT(product_id) DO UPDATE SET fg_hex=excluded.fg_hex, bg_hex=excluded.bg_hex;
            """, (product_id, fg, bg))
        elif "color_hex" in cols:
            cur.execute("""
                INSERT INTO product_colors(product_id, color_hex)
                VALUES(%s, %s)
                ON CONFLICT(product_id) DO UPDATE SET color_hex=excluded.color_hex;
            """, (product_id, fg))
        else:
            raise ValueError("No se encontró la tabla de colores de productos.")

        self.cn.commit()

    def clear_product_color_style(self, product_id: int):

        cur = self._cursor()

        cur.execute("DELETE FROM product_colors WHERE product_id=%s;", (product_id,))

        self.cn.commit()



    # Compras de bolsas (materia prima)

    def comprar_bolsas(self, product_id: int, bolsa_kg: float, bolsas:int):

        if bolsas <= 0 or bolsa_kg <= 0:

            raise ValueError("Valores de compra inválidos.")

        total_kg = bolsa_kg * bolsas

        cur = self._cursor()

        cur.execute("UPDATE raw_stock SET kg = kg + %s WHERE product_id=%s;", (total_kg, product_id))

        cur.execute("""INSERT INTO purchases(product_id, kg, bolsa_kg, bolsas)

                       VALUES(%s,%s,%s,%s);""", (product_id, total_kg, bolsa_kg, bolsas))

        self.cn.commit()



    # Ventas de bolsas (materia prima sin fraccionar)

    def registrar_venta_bolsas(self, product_id:int, bolsas:int, kg_por_bolsa:float,

                               price_por_bolsa:float, customer:str="", invoice_no:str="",

                               fecha:str|None=None, notas:str=""):

        if bolsas <= 0 or kg_por_bolsa <= 0:

            raise ValueError("Cantidad o kg por bolsa inválidos.")

        if price_por_bolsa < 0:

            raise ValueError("El precio por bolsa no puede ser negativo.")



        kg_total = bolsas * kg_por_bolsa

        cur = self._cursor()

        cur.execute("SELECT kg FROM raw_stock WHERE product_id=%s;", (product_id,))

        row = cur.fetchone()

        disp = row[0] if row else 0.0

        if disp < kg_total - 1e-9:

            raise ValueError(f"Stock insuficiente: disponible {disp:.3f} kg.")



        total_gs = price_por_bolsa * bolsas

        notas = (notas or "").strip()

        customer = (customer or "").strip()

        invoice_no = (invoice_no or "").strip()



        try:

            self.cn.execute("BEGIN")

            if fecha:

                fecha = fecha.strip()

                if len(fecha) == 10:

                    fecha = fecha + " 00:00:00"

                cur.execute("""

                    INSERT INTO bag_sales(ts, product_id, bolsas, kg_por_bolsa, kg_total,

                                          price_bolsa_gs, total_gs, customer, invoice_no, notes)

                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);

                """, (fecha, product_id, bolsas, kg_por_bolsa, kg_total,

                      price_por_bolsa, total_gs, customer, invoice_no, notas))

            else:

                cur.execute("""

                    INSERT INTO bag_sales(product_id, bolsas, kg_por_bolsa, kg_total,

                                          price_bolsa_gs, total_gs, customer, invoice_no, notes)

                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);

                """, (product_id, bolsas, kg_por_bolsa, kg_total,

                      price_por_bolsa, total_gs, customer, invoice_no, notas))



            cur.execute("UPDATE raw_stock SET kg = kg - %s WHERE product_id=%s;",

                        (kg_total, product_id))

            self.cn.commit()

        except Exception:

            self.cn.rollback()

            raise



    def listar_ventas_bolsas(self, desde:str|None=None, hasta:str|None=None,

                             texto:str|None=None, limit:int=200):

        cur = self._cursor()

        where = []

        params = []

        if desde:

            where.append("date(bs.ts) >= date(%s)")

            params.append(desde)

        if hasta:

            where.append("date(bs.ts) <= date(%s)")

            params.append(hasta)

        if texto:

            like = f"%{texto}%"

            where.append("(p.name ILIKE %s OR COALESCE(bs.customer,'') ILIKE %s OR COALESCE(bs.invoice_no,'') ILIKE %s)")

            params.extend([like, like, like])

        sql = """

            SELECT bs.id, bs.ts, p.name, bs.bolsas, bs.kg_por_bolsa, bs.kg_total,

                   bs.price_bolsa_gs, bs.total_gs, bs.customer, bs.invoice_no, bs.notes

            FROM bag_sales bs

            JOIN products p ON p.id = bs.product_id

        """

        if where:

            sql += " WHERE " + " AND ".join(where)

        sql += " ORDER BY bs.ts DESC"

        if limit:

            sql += " LIMIT %s"

            params.append(limit)

        cur.execute(sql + ";", params)

        return cur.fetchall()



    def get_bag_sale(self, sale_id:int):

        cur = self._cursor()

        cur.execute("""

            SELECT bs.id, bs.ts, p.name, bs.bolsas, bs.kg_por_bolsa, bs.kg_total,

                   bs.price_bolsa_gs, bs.total_gs, bs.customer, bs.invoice_no, bs.notes

            FROM bag_sales bs

            JOIN products p ON p.id = bs.product_id

            WHERE bs.id=%s;

        """, (sale_id,))

        return cur.fetchone()



    # ---------- Lotes: compras y consumo ----------

    def comprar_lote(self, product_id:int, lote:str, kg:float,

                     proveedor:str="", factura:str="", costo_total_gs:float=0.0):

        """

        Registra una compra por lote, suma a raw_stock y deja saldo de lote.

        """

        if kg <= 0:

            raise ValueError("Kg del lote inválidos.")

        if costo_total_gs < 0:

            raise ValueError("Costo total inválido.")



        costo_kg = (costo_total_gs / kg) if kg > 0 else 0.0



        cur = self._cursor()

        # Sumar a stock total de materia prima

        cur.execute("UPDATE raw_stock SET kg = kg + %s WHERE product_id=%s;", (kg, product_id))

        # Insertar lote

        cur.execute("""

            INSERT INTO raw_lots(product_id, lote, proveedor, factura, kg_inicial, kg_saldo, costo_total_gs, costo_kg_gs)

            VALUES(%s,%s,%s,%s,%s,%s,%s,%s);

        """, (product_id, (lote or "").strip(), (proveedor or "").strip(), (factura or "").strip(),

              kg, kg, costo_total_gs, costo_kg))

        self.cn.commit()

    def listar_lotes_abiertos(self, product_id:int|None=None, solo_abiertos:bool=True):

        cur = self._cursor()

        where = []

        params = []

        if product_id is not None:

            where.append("rl.product_id=%s"); params.append(product_id)

        if solo_abiertos:

            where.append("rl.kg_saldo > 1e-9")

        where.append("rl.cerrado = 0")



        sql = """

            SELECT rl.id, rl.product_id, p.name, rl.lote, rl.kg_saldo, rl.costo_kg_gs

            FROM raw_lots rl JOIN products p ON p.id=rl.product_id

        """

        if where:

            sql += " WHERE " + " AND ".join(where)

        sql += " ORDER BY p.name, rl.ts;"

        cur.execute(sql, params)

        return cur.fetchall()



    # Fraccionamiento: materia prima -> paquetes (SIN lote)

    def fraccionar(self, product_id: int, gramaje: int, paquetes: int, fecha: str|None=None):

        if paquetes <= 0:

            raise ValueError("Cantidad de paquetes inválida.")



        kg_need = kg_requeridos_para_paquetes(gramaje, paquetes)

        cur = self._cursor()



        # Descontar materia prima

        cur.execute("UPDATE raw_stock SET kg = kg - %s WHERE product_id=%s;", (kg_need, product_id))



        # Sumar paquetes terminados

        cur.execute("""

            INSERT INTO package_stock(product_id, gramaje, paquetes)

            VALUES(%s,%s,%s)

            ON CONFLICT(product_id,gramaje)

            DO UPDATE SET paquetes = package_stock.paquetes + excluded.paquetes;

        """, (product_id, gramaje, paquetes))



        # Log de fraccionamiento con fecha personalizada si se indicó

        if fecha:

            # Convertimos fecha a formato ISO si viene como solo "YYYY-MM-DD"

            if len(fecha.strip()) == 10:

                fecha = fecha.strip() + " 00:00:00"

            cur.execute("""

                INSERT INTO fractionations(ts, product_id, gramaje, paquetes, kg_consumidos)

                VALUES(%s,%s,%s,%s,%s) RETURNING id;

            """, (fecha, product_id, gramaje, paquetes, kg_need))

        else:

            cur.execute("""

                INSERT INTO fractionations(product_id, gramaje, paquetes, kg_consumidos)

                VALUES(%s,%s,%s,%s) RETURNING id;

            """, (product_id, gramaje, paquetes, kg_need))



        self.cn.commit()





    # Ventas: descuenta paquetes

    def vender_paquetes(self, ventas: list[tuple[int,int,int]]):

        """

        ventas = [(product_id, gramaje, paquetes), ...]

        """

        cur = self._cursor()

        # Verificar stock

        for pid, g, p in ventas:

            if p <= 0:

                raise ValueError("Cantidad de paquetes inválida en ventas.")

            cur.execute("""SELECT paquetes FROM package_stock

                           WHERE product_id=%s AND gramaje=%s;""", (pid, g))

            row = cur.fetchone()

            disp = row[0] if row else 0

            if disp < p:

                raise ValueError(f"Stock insuficiente de {p} paquetes para producto {pid} {g} g (disp: {disp}).")

        # Aplicar

        for pid, g, p in ventas:

            cur.execute("""UPDATE package_stock SET paquetes = paquetes - %s

                           WHERE product_id=%s AND gramaje=%s;""", (p, pid, g))

            cur.execute("""INSERT INTO sales(product_id, gramaje, paquetes) VALUES(%s,%s,%s);""", (pid, g, p))

        self.cn.commit()



    # Inventario

    def listar_raw_stock(self):

        cur = self._cursor()

        cur.execute("""

            SELECT p.id, p.name, rs.kg

            FROM raw_stock rs JOIN products p ON p.id=rs.product_id

            ORDER BY p.name;

        """)

        return cur.fetchall()



    def set_raw_alert(self, product_id:int, min_bolsas:float):
        """
        Guarda el minimo de bolsas para aviso de materia prima baja.
        """
        val = max(0.0, float(min_bolsas))
        cur = self._cursor()
        cur.execute("""
            INSERT INTO raw_alerts(product_id, min_bolsas)
            VALUES(%s,%s)
            ON CONFLICT(product_id)
            DO UPDATE SET min_bolsas=excluded.min_bolsas;
        """, (product_id, val))
        self.cn.commit()


    def get_raw_alerts_map(self) -> dict[int, float]:
        """
        Devuelve {product_id: min_bolsas}.
        """
        cur = self._cursor()
        cur.execute("SELECT product_id, min_bolsas FROM raw_alerts;")
        return {int(pid): float(val or 0.0) for pid, val in cur.fetchall()}


    def set_product_bag_display_kg(self, product_id: int, bag_kg: float | None):
        cur = self._cursor()
        if bag_kg is None:
            cur.execute("DELETE FROM product_bag_display WHERE product_id=%s;", (product_id,))
        else:
            val = float(bag_kg)
            if val <= 0:
                raise ValueError("Kg por bolsa invalido.")
            cur.execute("""
                INSERT INTO product_bag_display(product_id, bag_kg)
                VALUES(%s,%s)
                ON CONFLICT(product_id)
                DO UPDATE SET bag_kg=excluded.bag_kg;
            """, (product_id, val))
        self.cn.commit()


    def get_product_bag_display_map(self) -> dict[int, float]:
        cur = self._cursor()
        cur.execute("SELECT product_id, bag_kg FROM product_bag_display;")
        return {int(pid): float(val or 0.0) for pid, val in cur.fetchall()}


    def listar_package_stock(self):

        cur = self._cursor()

        cur.execute("""

            SELECT p.id, p.name, ps.gramaje, ps.paquetes

            FROM package_stock ps JOIN products p ON p.id=ps.product_id

            ORDER BY p.name, ps.gramaje;

        """)

        return cur.fetchall()



    # ------- PRECIOS -------

    def upsert_price(self, product_id:int, gramaje:int, price_gs:float, iva:int):

        if price_gs < 0 or iva not in (5,10):

            raise ValueError("Precio/IVA inválidos.")

        cur = self._cursor()
        cur.execute(
            "SELECT price_gs, iva FROM package_prices WHERE product_id=%s AND gramaje=%s;",
            (product_id, gramaje),
        )
        old = cur.fetchone()
        if old:
            try:
                if float(old[0] or 0) == float(price_gs) and int(old[1] or 0) == int(iva):
                    return
            except Exception:
                pass

        cur.execute("""

            INSERT INTO package_prices(product_id, gramaje, price_gs, iva)

            VALUES(%s,%s,%s,%s)

            ON CONFLICT(product_id,gramaje)

            DO UPDATE SET price_gs=excluded.price_gs, iva=excluded.iva;

        """, (product_id, gramaje, price_gs, iva))
        cur.execute(
            """
            INSERT INTO package_price_history(product_id, gramaje, price_gs, iva)
            VALUES(%s,%s,%s,%s);
            """,
            (product_id, gramaje, price_gs, iva),
        )

        self.cn.commit()



    def get_price(self, product_id:int, gramaje:int):

        cur = self._cursor()

        cur.execute("SELECT price_gs, iva FROM package_prices WHERE product_id=%s AND gramaje=%s;",

                    (product_id, gramaje))

        row = cur.fetchone()

        return (row[0], row[1]) if row else (None, None)



    def list_all_prices(self):

        out = []

        prods = self.list_products()

        prods.sort(key=lambda r: (product_order_idx(r[1]), r[1]))

        for pid, name in prods:

            for g in gramajes_permitidos(name):

                price, iva = self.get_price(pid, g)

                out.append((pid, name, g, price if price else 0.0, iva if iva else 10))

        return out

    def list_price_history(self, product_id:int):
        cur = self._cursor()
        cur.execute(
            """
            SELECT ts, gramaje, price_gs, iva
            FROM package_price_history
            WHERE product_id=%s
            ORDER BY datetime(ts) ASC, id ASC;
            """,
            (product_id,),
        )
        rows = cur.fetchall()
        if rows:
            return rows
        cur.execute(
            """
            SELECT CURRENT_TIMESTAMP as ts, gramaje, price_gs, iva
            FROM package_prices
            WHERE product_id=%s
            ORDER BY gramaje ASC;
            """,
            (product_id,),
        )
        return cur.fetchall()

    def get_product_name(self, product_id:int) -> str:

        cur = self._cursor()

        cur.execute("SELECT name FROM products WHERE id=%s;", (product_id,))

        row = cur.fetchone()

        return row[0] if row else ""





    # ------- FACTURAS (precio IVA incluido) -------

    def crear_factura(self, invoice_no:str, customer:str, items:list[tuple[int,int,int]], fecha:str|None=None):

        """

        items = [(product_id, gramaje, cantidad), ...]

        Usa el precio de package_prices (IVA incluido).

        Valida stock, descuenta, guarda cabecera y detalle.

        Si se pasa 'fecha', se usa para el campo ts en sales_invoices.

        Devuelve: (invoice_id, resumen_dict)

        """

        if not items:

            raise ValueError("No hay ítems para facturar.")



        self._recover_if_aborted()

        cur = self._cursor()



        try:

            # 1) Validar stock y precios configurados

            faltan_precios = []

            for pid, g, qty in items:

                if qty <= 0:

                    raise ValueError("Cantidades inválidas en la factura.")

                cur.execute("""SELECT paquetes FROM package_stock WHERE product_id=%s AND gramaje=%s;""",(pid,g))

                row = cur.fetchone(); disp = row[0] if row else 0

                if disp < qty:

                    raise ValueError(f"Stock insuficiente para producto {pid} {g} g. Disp:{disp}, pide:{qty}.")

                price, iva = self.get_price(pid, g)

                if price is None or iva not in (5,10):

                    faltan_precios.append((pid,g))

            if faltan_precios:

                raise ValueError("Faltan precios/IVA para: " + ", ".join([f"{pid}-{g}g" for pid,g in faltan_precios]))



            # 2) Calcular totales (precio IVA incluido)

            grav5 = iva5 = grav10 = iva10 = total = 0.0

            lineas = []  # (pid,g,qty,price,iva,line_total,base,iva_monto)

            for pid, g, qty in items:

                price, iva = self.get_price(pid, g)

                line_total = price * qty

                base       = line_total / (1.0 + iva/100.0)

                iva_monto  = line_total - base



                total += line_total

                if iva == 5:

                    grav5 += base; iva5 += iva_monto

                else:

                    grav10 += base; iva10 += iva_monto

                lineas.append((pid, g, qty, price, iva, line_total, base, iva_monto))



            # 3) Transacción completa

            self.cn.execute("BEGIN")



            # Cabecera: si hay fecha manual, usarla

            if fecha:

                if len(fecha.strip()) == 10:

                    fecha = fecha.strip() + " 00:00:00"

                cur.execute("""

                    INSERT INTO sales_invoices(ts, invoice_no, customer,

                                               gravada5_gs, iva5_gs, gravada10_gs, iva10_gs, total_gs)

                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id;

                """, (fecha, invoice_no or "", customer or "", grav5, iva5, grav10, iva10, total))

            else:

                cur.execute("""

                    INSERT INTO sales_invoices(invoice_no, customer,

                                               gravada5_gs, iva5_gs, gravada10_gs, iva10_gs, total_gs)

                    VALUES(%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id;

                """, (invoice_no or "", customer or "", grav5, iva5, grav10, iva10, total))



            invoice_id = self._fetch_returning_id(cur, "alta de factura")



            # Detalle + descuento stock + log ventas

            for pid, g, qty, price, iva, lt, base, iva_monto in lineas:

                cur.execute("""INSERT INTO sales_invoice_items

                               (invoice_id, product_id, gramaje, cantidad,

                                price_gs, iva, line_total, line_base, line_iva)

                               VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);""",

                            (invoice_id, pid, g, qty, price, iva, lt, base, iva_monto))



                cur.execute("""UPDATE package_stock

                               SET paquetes = paquetes - %s

                               WHERE product_id=%s AND gramaje=%s;""",

                            (qty, pid, g))

                cur.execute("""INSERT INTO sales(product_id, gramaje, paquetes)

                               VALUES(%s,%s,%s);""", (pid, g, qty))



            self.cn.commit()

            resumen = {

                "gravada5": grav5, "iva5": iva5,

                "gravada10": grav10, "iva10": iva10,

                "total": total

            }

            return invoice_id, resumen



        except Exception:

            self.cn.rollback()

            raise


    def get_invoice_header(self, invoice_id:int):

        cur = self._cursor()
        cur.execute("""
            SELECT id, ts, invoice_no, customer,
                   gravada5_gs, iva5_gs, gravada10_gs, iva10_gs, total_gs
            FROM sales_invoices
            WHERE id=%s;
        """, (invoice_id,))
        return cur.fetchone()


    def find_invoice_for_credit(self, lookup:str):

        txt = (lookup or "").strip()
        if not txt:
            raise ValueError("Ingrese un ID o número de factura.")

        if "|" in txt:
            txt = txt.split("|", 1)[0].strip()

        cur = self._cursor()
        if txt.isdigit():
            cur.execute("""
                SELECT id, ts, invoice_no, customer,
                       gravada5_gs, iva5_gs, gravada10_gs, iva10_gs, total_gs
                FROM sales_invoices
                WHERE id=%s
                LIMIT 1;
            """, (int(txt),))
            row = cur.fetchone()
            if row:
                return row

        cur.execute("""
            SELECT id, ts, invoice_no, customer,
                   gravada5_gs, iva5_gs, gravada10_gs, iva10_gs, total_gs
            FROM sales_invoices
            WHERE TRIM(COALESCE(invoice_no,'')) = TRIM(%s)
            ORDER BY datetime(ts) DESC, id DESC
            LIMIT 1;
        """, (txt,))
        row = cur.fetchone()
        if row:
            return row
        raise ValueError("Factura no encontrada.")


    def list_invoice_lookup_values(self, limit:int=200):

        cur = self._cursor()
        cur.execute("""
            SELECT id, ts, invoice_no, customer
            FROM sales_invoices
            ORDER BY datetime(ts) DESC, id DESC
            LIMIT %s;
        """, (int(limit),))
        out = []
        for fid, ts, invoice_no, customer in cur.fetchall():
            nro = (invoice_no or "-").strip() or "-"
            cli = (customer or "-").strip() or "-"
            out.append(f"{int(fid)} | {nro} | {cli} | {ts or '-'}")
        return out


    def listar_items_factura_para_credito(self, invoice_id:int):

        cur = self._cursor()
        cur.execute("""
            SELECT sii.id,
                   sii.product_id,
                   p.name,
                   sii.gramaje,
                   sii.cantidad,
                   sii.price_gs,
                   sii.iva,
                   sii.line_total,
                   COALESCE((
                       SELECT SUM(cni.cantidad)
                       FROM credit_note_items cni
                       JOIN credit_notes cn ON cn.id = cni.credit_note_id
                       WHERE cni.invoice_item_id = sii.id
                   ), 0)
            FROM sales_invoice_items sii
            JOIN products p ON p.id = sii.product_id
            WHERE sii.invoice_id = %s
            ORDER BY p.name, sii.gramaje;
        """, (invoice_id,))
        return cur.fetchall()


    def create_credit_note(self, credit_no:str, invoice_id:int, customer:str, motivo:str,
                           items:list[tuple[int,int,int,int,float,int]], fecha:str|None=None,
                           reingresa_stock:bool=True):

        if not items:
            raise ValueError("No hay ítems para acreditar.")

        cur = self._cursor()
        grav5 = iva5 = grav10 = iva10 = total = 0.0
        lineas = []

        for invoice_item_id, product_id, gramaje, cantidad, price_gs, iva in items:
            qty = int(cantidad or 0)
            if qty <= 0:
                raise ValueError("Cantidades inválidas en la nota de crédito.")

            cur.execute("""
                SELECT sii.cantidad,
                       COALESCE((
                           SELECT SUM(cni.cantidad)
                           FROM credit_note_items cni
                           JOIN credit_notes cn ON cn.id = cni.credit_note_id
                           WHERE cni.invoice_item_id = sii.id
                       ), 0)
                FROM sales_invoice_items sii
                WHERE sii.id = %s AND sii.invoice_id = %s;
            """, (invoice_item_id, invoice_id))
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Ítem de factura no encontrado: {invoice_item_id}.")

            fact_qty = int(row[0] or 0)
            cred_qty = int(row[1] or 0)
            disp_qty = fact_qty - cred_qty
            if qty > disp_qty:
                raise ValueError(f"La cantidad supera lo disponible para acreditar en el ítem {invoice_item_id}.")

            line_total = float(price_gs or 0) * qty
            base = line_total / (1.0 + int(iva) / 100.0)
            iva_monto = line_total - base

            total += line_total
            if int(iva) == 5:
                grav5 += base
                iva5 += iva_monto
            else:
                grav10 += base
                iva10 += iva_monto

            lineas.append((invoice_item_id, product_id, gramaje, qty, float(price_gs or 0), int(iva), line_total, base, iva_monto))

        try:
            self.cn.execute("BEGIN")

            if fecha:
                if len(fecha.strip()) == 10:
                    fecha = fecha.strip() + " 00:00:00"
                cur.execute("""
                    INSERT INTO credit_notes(
                        ts, credit_no, invoice_id, customer, motivo, reingresa_stock,
                        gravada5_gs, iva5_gs, gravada10_gs, iva10_gs, total_gs
                    )
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""", (fecha, credit_no or "", invoice_id, customer or "", motivo or "", 1 if reingresa_stock else 0,
                      grav5, iva5, grav10, iva10, total))
            else:
                cur.execute("""
                    INSERT INTO credit_notes(
                        credit_no, invoice_id, customer, motivo, reingresa_stock,
                        gravada5_gs, iva5_gs, gravada10_gs, iva10_gs, total_gs
                    )
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""", (credit_no or "", invoice_id, customer or "", motivo or "", 1 if reingresa_stock else 0,
                      grav5, iva5, grav10, iva10, total))
            credit_note_id = self._fetch_returning_id(cur, "alta de nota de credito")

            for invoice_item_id, product_id, gramaje, qty, price_gs, iva, line_total, base, iva_monto in lineas:
                cur.execute("""
                    INSERT INTO credit_note_items(
                        credit_note_id, invoice_item_id, product_id, gramaje, cantidad,
                        price_gs, iva, line_total, line_base, line_iva
                    )
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """, (credit_note_id, invoice_item_id, product_id, gramaje, qty,
                      price_gs, iva, line_total, base, iva_monto))

                if reingresa_stock:
                    cur.execute("""
                        INSERT INTO package_stock(product_id, gramaje, paquetes)
                        VALUES(%s,%s,%s)
                        ON CONFLICT(product_id, gramaje)
                        DO UPDATE SET paquetes = paquetes + excluded.paquetes;
                    """, (product_id, gramaje, qty))

            self.cn.commit()
            resumen = {
                "gravada5": grav5, "iva5": iva5,
                "gravada10": grav10, "iva10": iva10,
                "total": total
            }
            return credit_note_id, resumen

        except Exception:

            self.cn.rollback()

            raise



    def actualizar_factura_header(self, invoice_id:int, invoice_no:str|None, customer:str|None):

        """

        Permite corregir el nro de factura y/o cliente luego de emitirla.

        """

        cur = self._cursor()

        cur.execute("""

            UPDATE sales_invoices

               SET invoice_no = %s, customer = %s

             WHERE id = %s;

        """, ((invoice_no or "").strip(), (customer or "").strip(), invoice_id))

        if cur.rowcount <= 0:

            cur.execute("SELECT 1 FROM sales_invoices WHERE id=%s;", (invoice_id,))

            if not cur.fetchone():

                raise ValueError(f"Factura {invoice_id} no encontrada.")

        self.cn.commit()

    def actualizar_factura_item(self, item_id:int, nueva_cantidad:int, nuevo_precio_gs:float, motivo:str):
        """
        Edita un item de factura (cantidad y/o precio) y recalcula:
        - stock de paquetes (delta)
        - totales de cabecera de factura
        Registra auditoria con motivo obligatorio.
        """
        if int(nueva_cantidad or 0) <= 0:
            raise ValueError("La cantidad debe ser mayor a 0.")
        if float(nuevo_precio_gs or 0) < 0:
            raise ValueError("El precio no puede ser negativo.")
        motivo_txt = (motivo or "").strip()
        if not motivo_txt:
            raise ValueError("Debe indicar el motivo de la modificación.")

        cur = self._cursor()
        cur.execute("""
            SELECT id, invoice_id, product_id, gramaje, cantidad, price_gs, iva, line_total
            FROM sales_invoice_items
            WHERE id=%s;
        """, (item_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError("Ítem de factura no encontrado.")

        _id, invoice_id, product_id, gramaje, qty_old, price_old, iva_old, line_total_old = row
        qty_old = int(qty_old or 0)
        price_old = float(price_old or 0)
        iva_old = int(iva_old or 10)
        line_total_old = float(line_total_old or 0)

        qty_new = int(nueva_cantidad)
        price_new = float(nuevo_precio_gs)
        delta_qty = qty_new - qty_old
        stock_ajuste = -delta_qty  # +suma stock, -resta stock

        # Si aumenta cantidad vendida, validar stock disponible adicional.
        if delta_qty > 0:
            cur.execute("""
                SELECT paquetes
                FROM package_stock
                WHERE product_id=%s AND gramaje=%s;
            """, (product_id, gramaje))
            rr = cur.fetchone()
            disp = int(rr[0] if rr and rr[0] is not None else 0)
            if disp < delta_qty:
                raise ValueError(
                    f"Stock insuficiente para aumentar cantidad. "
                    f"Disponible: {disp}, adicional requerido: {delta_qty}."
                )

        line_total_new = price_new * qty_new
        base_new = line_total_new / (1.0 + iva_old / 100.0)
        iva_new = line_total_new - base_new

        try:
            self.cn.execute("BEGIN")

            # Ajustar stock de paquetes por delta de cantidad.
            cur.execute("""
                INSERT INTO package_stock(product_id, gramaje, paquetes)
                VALUES(%s,%s,%s)
                ON CONFLICT(product_id,gramaje)
                DO UPDATE SET paquetes = package_stock.paquetes + excluded.paquetes;
            """, (product_id, gramaje, stock_ajuste))

            # Actualizar línea de factura.
            cur.execute("""
                UPDATE sales_invoice_items
                   SET cantidad=%s,
                       price_gs=%s,
                       line_total=%s,
                       line_base=%s,
                       line_iva=%s
                 WHERE id=%s;
            """, (qty_new, price_new, line_total_new, base_new, iva_new, item_id))

            # Recalcular cabecera completa.
            cur.execute("""
                SELECT
                  COALESCE(SUM(CASE WHEN iva=5  THEN line_base ELSE 0 END),0) AS grav5,
                  COALESCE(SUM(CASE WHEN iva=5  THEN line_iva  ELSE 0 END),0) AS iva5,
                  COALESCE(SUM(CASE WHEN iva=10 THEN line_base ELSE 0 END),0) AS grav10,
                  COALESCE(SUM(CASE WHEN iva=10 THEN line_iva  ELSE 0 END),0) AS iva10,
                  COALESCE(SUM(line_total),0) AS total
                FROM sales_invoice_items
                WHERE invoice_id=%s;
            """, (invoice_id,))
            grav5, iva5, grav10, iva10, total = cur.fetchone()
            cur.execute("""
                UPDATE sales_invoices
                   SET gravada5_gs=%s,
                       iva5_gs=%s,
                       gravada10_gs=%s,
                       iva10_gs=%s,
                       total_gs=%s
                 WHERE id=%s;
            """, (grav5, iva5, grav10, iva10, total, invoice_id))

            # Auditoría.
            cur.execute("""
                INSERT INTO sales_invoice_item_edits(
                    invoice_id, item_id, product_id, gramaje,
                    old_qty, new_qty,
                    old_price_gs, new_price_gs,
                    old_line_total, new_line_total,
                    motivo
                ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """, (
                invoice_id, item_id, product_id, gramaje,
                qty_old, qty_new,
                price_old, price_new,
                line_total_old, line_total_new,
                motivo_txt
            ))

            self.cn.commit()
        except Exception:
            self.cn.rollback()
            raise



    def actualizar_venta_bolsa_header(self, sale_id:int, invoice_no:str|None, customer:str|None):

        """

        Actualiza encabezado de una venta de bolsas (materia prima sin fraccionar).

        """

        cur = self._cursor()

        cur.execute("""

            UPDATE bag_sales

               SET invoice_no = %s, customer = %s

             WHERE id = %s;

        """, ((invoice_no or "").strip(), (customer or "").strip(), sale_id))

        if cur.rowcount <= 0:

            cur.execute("SELECT 1 FROM bag_sales WHERE id=%s;", (sale_id,))

            if not cur.fetchone():

                raise ValueError(f"Venta de bolsas {sale_id} no encontrada.")

        self.cn.commit()



    def ajustar_paquetes(self, product_id: int, gramaje: int, new_paq: int):
        if new_paq < 0: raise ValueError("No se admite negativo.")
        cur = self._cursor()
        cur.execute("SELECT paquetes FROM package_stock WHERE product_id=%s AND gramaje=%s;", (product_id, gramaje))
        row = cur.fetchone()
        old_paq = int(row[0]) if row else 0
        delta = int(new_paq) - int(old_paq)
        cur.execute("""INSERT INTO package_stock(product_id, gramaje, paquetes)
                       VALUES(%s,%s,%s)
                       ON CONFLICT(product_id,gramaje) DO UPDATE SET paquetes=excluded.paquetes;""",
                    (product_id, gramaje, new_paq))
        cur.execute(
            """INSERT INTO stock_adjustments(kind, product_id, gramaje, stock_before, stock_after, delta, motivo)
               VALUES(%s,%s,%s,%s,%s,%s,%s);""",
            ("package", product_id, gramaje, old_paq, new_paq, delta, "Ajuste inventario"),
        )
        self.cn.commit()

    def listar_ajustes(self, desde:str|None=None, hasta:str|None=None,
                        product_id:int|None=None, kind:str|None=None, limit:int=500):
        cur = self._cursor()
        where = ["1=1"]
        params = []
        if product_id:
            where.append("sa.product_id=%s")
            params.append(product_id)
        if kind in ("raw", "package"):
            where.append("sa.kind=%s")
            params.append(kind)
        if desde:
            where.append("date(sa.ts) >= date(%s)")
            params.append(desde)
        if hasta:
            where.append("date(sa.ts) <= date(%s)")
            params.append(hasta)
        sql = """
            SELECT sa.id, sa.ts, sa.kind, sa.product_id, p.name, sa.gramaje,
                   sa.stock_before, sa.stock_after, sa.delta, sa.motivo
            FROM stock_adjustments sa
            JOIN products p ON p.id = sa.product_id
            WHERE {where}
            ORDER BY sa.ts DESC, sa.id DESC
        """.format(where=" AND ".join(where))
        if limit and limit > 0:
            sql += " LIMIT %s"
            params.append(limit)
        cur.execute(sql, params)
        return cur.fetchall()

    def listar_lotes(self, product_id:int|None=None, solo_abiertos:bool=False):

        """

        Maestro de lotes: devuelve filas con info básica y saldos.

        Salida: [(id, lote, producto, kg_total, kg_usado, kg_disp, costo_total, costo_kg, proveedor, factura, ts)]

        """

        cur = self._cursor()

        sql = """

        SELECT rl.id, rl.lote, p.name,

            rl.kg_inicial,

            (rl.kg_inicial - rl.kg_saldo) AS kg_usado,

            rl.kg_saldo,

            rl.costo_total_gs, rl.costo_kg_gs,

            rl.proveedor, rl.factura, rl.ts

        FROM raw_lots rl

        JOIN products p ON p.id=rl.product_id

        """

        where, params = [], []

        if product_id is not None:

            where.append("rl.product_id=%s"); params.append(product_id)

        if solo_abiertos:

            where.append("rl.kg_saldo > 1e-9")

            where.append("rl.cerrado = 0")

        if where:

            sql += " WHERE " + " AND ".join(where)

        sql += " ORDER BY rl.ts DESC, rl.id DESC;"

        cur.execute(sql, params)

        return cur.fetchall()



    def historial_costos_lotes(self, product_id:int, limit:int|None=None):
        """
        Devuelve historial de costos por kg de lotes de un producto, ordenado por fecha.
        """
        cur = self._cursor()
        sql = """
            SELECT rl.id, rl.ts, rl.lote, rl.costo_kg_gs,
                   rl.kg_inicial, rl.costo_total_gs
            FROM raw_lots rl
            WHERE rl.product_id=%s
            ORDER BY rl.ts ASC, rl.id ASC
        """
        params = [product_id]
        if limit:
            sql += " LIMIT %s"
            params.append(limit)
        cur.execute(sql, params)
        return cur.fetchall()

    def lot_detail(self, lot_id:int):

        """

        Detalle de un lote (para el panel derecho del resumen).

        """

        cur = self._cursor()

        cur.execute("""

            SELECT rl.id, rl.lote, p.id AS product_id, p.name AS product,

                rl.kg_inicial, rl.kg_saldo,

                rl.costo_total_gs, rl.costo_kg_gs,

                rl.proveedor, rl.factura, rl.ts, rl.cerrado

            FROM raw_lots rl

            JOIN products p ON p.id=rl.product_id

            WHERE rl.id=%s;

        """, (lot_id,))

        return cur.fetchone()

    def get_last_cost_kg(self, product_id:int):

        cur = self._cursor()

        cur.execute("""
            SELECT rl.costo_kg_gs
            FROM raw_lots rl
            WHERE rl.product_id=%s
            ORDER BY rl.ts DESC, rl.id DESC
            LIMIT 1;
        """, (product_id,))

        row = cur.fetchone()

        if not row or row[0] is None:

            return None

        try:

            return float(row[0])

        except Exception:

            return None



    def listar_fraccionamientos_de_lote(self, lot_id:int):

        """

        Historial de fraccionamientos que consumieron este lote.

        """

        cur = self._cursor()

        cur.execute("""

            SELECT f.ts, f.gramaje, f.paquetes, lf.kg_consumidos

            FROM lot_fractionations lf

            JOIN fractionations f ON f.id = lf.fractionation_id

            WHERE lf.lot_id=%s

            ORDER BY f.ts ASC, lf.id ASC;

        """, (lot_id,))

        return cur.fetchall()



    def fraccionar_desde_lote(self, product_id: int, lot_id: int, gramaje: int, paquetes: int, fecha: str|None=None):

        """

        Consume materia prima desde un lote específico,

        registrando la fecha indicada (si la hay).

        """

        if paquetes <= 0:

            raise ValueError("Cantidad de paquetes inválida.")



        kg_need = kg_requeridos_para_paquetes(gramaje, paquetes)

        self._recover_if_aborted()

        cur = self._cursor()


        try:

            # Validar lote

            cur.execute("SELECT product_id, kg_saldo, cerrado FROM raw_lots WHERE id=%s;", (lot_id,))

            row = cur.fetchone()

            if not row:

                raise ValueError(f"Lote {lot_id} no encontrado.")

            prod_lot, kg_saldo, cerrado = int(row[0]), float(row[1] or 0.0), int(row[2] or 0)

            if prod_lot != product_id:

                raise ValueError("El lote seleccionado no corresponde al producto.")

            if cerrado:

                raise ValueError("El lote seleccionado está cerrado.")



            # Actualizar saldo del lote y stock global

            nuevo_saldo = kg_saldo - kg_need

            cur.execute("UPDATE raw_lots SET kg_saldo=%s WHERE id=%s;", (nuevo_saldo, lot_id))

            cur.execute("UPDATE raw_stock SET kg = kg - %s WHERE product_id=%s;", (kg_need, product_id))



            # Registrar fraccionamiento con fecha

            if fecha:

                if len(fecha.strip()) == 10:

                    fecha = fecha.strip() + " 00:00:00"

                cur.execute("""

                    INSERT INTO fractionations(ts, product_id, gramaje, paquetes, kg_consumidos)

                    VALUES(%s,%s,%s,%s,%s) RETURNING id;

                """, (fecha, product_id, gramaje, paquetes, kg_need))

            else:

                cur.execute("""

                    INSERT INTO fractionations(product_id, gramaje, paquetes, kg_consumidos)

                    VALUES(%s,%s,%s,%s) RETURNING id;

                """, (product_id, gramaje, paquetes, kg_need))



            frac_id = self._fetch_returning_id(cur, "alta de fraccionamiento")



            # Registrar vínculo lote <-> fraccionamiento

            cur.execute("""

                INSERT INTO lot_fractionations(lot_id, fractionation_id, kg_consumidos)

                VALUES(%s,%s,%s);

            """, (lot_id, frac_id, kg_need))



            # Sumar paquetes terminados

            cur.execute("""

                INSERT INTO package_stock(product_id, gramaje, paquetes)

                VALUES(%s,%s,%s)

                ON CONFLICT(product_id,gramaje)

                DO UPDATE SET paquetes = package_stock.paquetes + excluded.paquetes;

            """, (product_id, gramaje, paquetes))



            self.cn.commit()

        except Exception:

            self.cn.rollback()

            raise



    def cerrar_lote(self, lot_id:int):

        cur = self._cursor()

        cur.execute("UPDATE raw_lots SET cerrado=1 WHERE id=%s;", (lot_id,))

        if cur.rowcount == 0:

            raise ValueError("Lote no encontrado.")

        self.cn.commit()

    def abrir_lote(self, lot_id:int):

        cur = self._cursor()

        cur.execute("UPDATE raw_lots SET cerrado=0 WHERE id=%s;", (lot_id,))

        if cur.rowcount == 0:

            raise ValueError("Lote no encontrado.")

        self.cn.commit()



    def renombrar_lote(self, lot_id:int, nuevo_nombre:str):

        nuevo_nombre = (nuevo_nombre or "").strip()

        if not nuevo_nombre:

            raise ValueError("El nombre del lote no puede estar vacío.")

        cur = self._cursor()

        cur.execute("UPDATE raw_lots SET lote=%s WHERE id=%s;", (nuevo_nombre, lot_id))

        if cur.rowcount == 0:

            raise ValueError("Lote no encontrado.")

        self.cn.commit()



    # --------- Mermas de lote ----------

    def registrar_merma_lote(self, lot_id:int, kg:float, fecha:str|None=None, motivo:str=""):

        if kg <= 0:

            raise ValueError("Kg de merma invAï¿½lidos.")



        cur = self._cursor()

        cur.execute("SELECT id FROM raw_lots WHERE id=%s;", (lot_id,))

        if not cur.fetchone():

            raise ValueError("Lote no encontrado.")



        motivo = (motivo or "").strip()

        if fecha:

            fecha = fecha.strip()

            if len(fecha) == 10:

                fecha += " 00:00:00"

            cur.execute("""

                INSERT INTO lot_mermas(ts, lot_id, kg, motivo)

                VALUES(%s,%s,%s,%s);

            """, (fecha, lot_id, kg, motivo))

        else:

            cur.execute("""

                INSERT INTO lot_mermas(lot_id, kg, motivo)

                VALUES(%s,%s,%s);

            """, (lot_id, kg, motivo))

        self.cn.commit()



    def listar_mermas_de_lote(self, lot_id:int):

        cur = self._cursor()

        cur.execute("""

            SELECT id, ts, kg, motivo

            FROM lot_mermas

            WHERE lot_id=%s

            ORDER BY ts DESC, id DESC;

        """, (lot_id,))

        return cur.fetchall()



    def total_merma_por_lote(self, lot_id:int):

        cur = self._cursor()

        cur.execute("SELECT COALESCE(SUM(kg), 0) FROM lot_mermas WHERE lot_id=%s;", (lot_id,))

        row = cur.fetchone()

        return float(row[0] or 0.0)



    def listar_lotes_con_merma(self, product_id:int|None=None, solo_abiertos:bool=False):

        cur = self._cursor()

        sql = """

            SELECT rl.id,

                   p.name      AS product_name,

                   rl.lote,

                   rl.kg_inicial,

                   rl.kg_saldo,

                   COALESCE(SUM(lm.kg), 0) AS merma_kg,

                   rl.cerrado,

                   rl.ts

            FROM raw_lots rl

            JOIN products p ON p.id = rl.product_id

            LEFT JOIN lot_mermas lm ON lm.lot_id = rl.id

        """

        where, params = [], []

        if product_id is not None:

            where.append("rl.product_id=%s"); params.append(product_id)

        if solo_abiertos:

            where.append("rl.cerrado = 0")

            where.append("rl.kg_saldo > 1e-9")

        if where:

            sql += " WHERE " + " AND ".join(where)

        sql += """

            GROUP BY rl.id, p.name, rl.lote, rl.kg_inicial, rl.kg_saldo, rl.cerrado, rl.ts

            ORDER BY rl.ts DESC, rl.id DESC;

        """

        cur.execute(sql, params)

        return cur.fetchall()



    def listar_todos_pkg_stock(self):

        cur = self._cursor()

        cur.execute("SELECT id, name FROM products;")

        prods = cur.fetchall()

        # orden maestro como en la factura

        prods.sort(key=lambda r: (product_order_idx(r[1]), r[1]))



        out = []

        for pid, name in prods:

            for g in gramajes_permitidos(name):

                cur.execute("""SELECT paquetes FROM package_stock

                            WHERE product_id=%s AND gramaje=%s;""", (pid, g))

                row = cur.fetchone()

                paq = row[0] if row else 0

                out.append((pid, name, g, paq))

        return out

        # -------- HISTORIAL DE FRACCIONAMIENTO --------

    def listar_fraccionamientos(self, dt_desde:str|None=None, dt_hasta:str|None=None,

                                product_id:int|None=None):

        """

        Devuelve filas para historial (más recientes primero).

        Salida: [(id, ts, product_id, product_name, gramaje, paquetes, kg_consumidos, lote_txt)]

        """

        cur = self._cursor()

        where, params = [], []

        sql = """

            SELECT f.id,

                f.ts,

                p.id        AS product_id,

                p.name      AS product_name,

                f.gramaje,

                f.paquetes,

                f.kg_consumidos,

                rl.lote     AS lote_txt

            FROM fractionations f

            JOIN products p ON p.id = f.product_id

            LEFT JOIN lot_fractionations lf ON lf.fractionation_id = f.id

            LEFT JOIN raw_lots rl          ON rl.id = lf.lot_id

        """

        if dt_desde:

            where.append("f.ts >= %s"); params.append(dt_desde)

        if dt_hasta:

            where.append("f.ts <= %s"); params.append(dt_hasta)

        if product_id is not None:

            where.append("f.product_id=%s"); params.append(product_id)

        if where:

            sql += " WHERE " + " AND ".join(where)

        sql += " ORDER BY f.ts DESC, f.id DESC;"

        cur.execute(sql, params)

        return cur.fetchall()



    def get_frac_info(self, frac_id:int):

        cur = self._cursor()

        cur.execute("""

            SELECT f.id, f.ts, f.product_id, p.name, f.gramaje, f.paquetes, f.kg_consumidos,

                lf.lot_id, rl.lote AS lote_txt

            FROM fractionations f

            JOIN products p ON p.id=f.product_id

            LEFT JOIN lot_fractionations lf ON lf.fractionation_id = f.id

            LEFT JOIN raw_lots rl          ON rl.id = lf.lot_id

            WHERE f.id=%s;

        """, (frac_id,))

        return cur.fetchone()



    def actualizar_fraccionamiento(self, frac_id:int, nuevo_gram:int, nuevos_paq:int, nuevo_lot_id:int|None=None):

        """

        Modifica gramaje/paquetes y, opcionalmente, cambia el lote vinculado:

        - package_stock (resta lo viejo, suma lo nuevo; si cambió gramaje, en dos filas)

        - raw_stock (ajusta por delta de kg)

        - raw_lots / lot_fractionations (restituye al lote anterior y descuenta del nuevo)

        """

        if nuevos_paq <= 0:

            raise ValueError("Paquetes inválidos.")



        cur = self._cursor()

        # 1) Traer fracc original (+ verificar)

        cur.execute("""

            SELECT product_id, gramaje, paquetes, kg_consumidos

            FROM fractionations WHERE id=%s;

        """, (frac_id,))

        row = cur.fetchone()

        if not row:

            raise ValueError("Fraccionamiento no encontrado.")

        prod_id, gram_old, paq_old, kg_old = int(row[0]), int(row[1]), int(row[2]), float(row[3] or 0.0)



        # 2) Calcular nuevos kg

        kg_new = kg_requeridos_para_paquetes(nuevo_gram, nuevos_paq)

        delta_kg = kg_new - kg_old



        # 3) Lote original (si exist?a)

        cur.execute("SELECT lot_id FROM lot_fractionations WHERE fractionation_id=%s LIMIT 1;", (frac_id,))

        lot_row = cur.fetchone()

        lot_id_orig = int(lot_row[0]) if lot_row else None

        lot_orig_saldo = None

        if lot_id_orig is not None:

            cur.execute("SELECT kg_saldo FROM raw_lots WHERE id=%s;", (lot_id_orig,))

            row = cur.fetchone()

            if not row:

                raise ValueError("Lote vinculado no encontrado.")

            lot_orig_saldo = float(row[0] or 0.0)



        # 4) Validaciones del lote destino (si se cambiara)

        lot_dest_info = None

        if nuevo_lot_id is not None:

            cur.execute("SELECT product_id, kg_saldo, cerrado FROM raw_lots WHERE id=%s;", (nuevo_lot_id,))

            lot_dest_info = cur.fetchone()

            if not lot_dest_info:

                raise ValueError("Lote seleccionado no encontrado.")

            lot_prod, lot_saldo_dest, lot_cerrado = int(lot_dest_info[0]), float(lot_dest_info[1] or 0.0), int(lot_dest_info[2] or 0)

            if lot_prod != prod_id:

                raise ValueError("El lote seleccionado no corresponde al producto.")

            if lot_cerrado:

                raise ValueError("El lote seleccionado est? cerrado.")



        # 5) Validar saldo en el lote destino

        if lot_id_orig == nuevo_lot_id and lot_orig_saldo is not None:

            nuevo_saldo = lot_orig_saldo + kg_old - kg_new

            if nuevo_saldo < -1e-9:

                raise ValueError("Saldo insuficiente en el lote seleccionado.")

        elif nuevo_lot_id is not None and lot_dest_info is not None:

            lot_saldo_dest = float(lot_dest_info[1] or 0.0)

            if lot_saldo_dest < kg_new - 1e-9:

                raise ValueError("Saldo insuficiente en el lote seleccionado.")



        try:

            self.cn.execute("BEGIN")



            # 6) Ajustar package_stock (primero revertir lo viejo)

            cur.execute("""

                UPDATE package_stock

                SET paquetes = paquetes - %s

                WHERE product_id=%s AND gramaje=%s;

            """, (paq_old, prod_id, gram_old))

            cur.execute("""

                INSERT INTO package_stock(product_id, gramaje, paquetes)

                VALUES(%s,%s,%s)

                ON CONFLICT(product_id,gramaje)

                DO UPDATE SET paquetes = package_stock.paquetes + excluded.paquetes;

            """, (prod_id, nuevo_gram, nuevos_paq))



            # 7) Ajustar raw_stock con delta (permite negativo, igual que el resto del sistema)

            cur.execute("UPDATE raw_stock SET kg = kg - %s WHERE product_id=%s;", (delta_kg, prod_id))



            # 8) Restituir al lote original y descontar del nuevo

            if lot_id_orig is not None:

                cur.execute("UPDATE raw_lots SET kg_saldo = kg_saldo + %s WHERE id=%s;", (kg_old, lot_id_orig))

            if nuevo_lot_id is not None:

                cur.execute("UPDATE raw_lots SET kg_saldo = kg_saldo - %s WHERE id=%s;", (kg_new, nuevo_lot_id))



            # 9) Actualizar vínculo lote <-> fraccionamiento

            if lot_id_orig != nuevo_lot_id:

                cur.execute("DELETE FROM lot_fractionations WHERE fractionation_id=%s;", (frac_id,))

                if nuevo_lot_id is not None:

                    cur.execute("""

                        INSERT INTO lot_fractionations(lot_id, fractionation_id, kg_consumidos)

                        VALUES(%s,%s,%s);

                    """, (nuevo_lot_id, frac_id, kg_new))

            else:

                if nuevo_lot_id is None:

                    cur.execute("DELETE FROM lot_fractionations WHERE fractionation_id=%s;", (frac_id,))

                else:

                    cur.execute("""

                        UPDATE lot_fractionations

                        SET kg_consumidos=%s

                        WHERE lot_id=%s AND fractionation_id=%s;

                    """, (kg_new, nuevo_lot_id, frac_id))



            # 10) Actualizar fraccionamiento

            cur.execute("""

                UPDATE fractionations

                SET gramaje=%s, paquetes=%s, kg_consumidos=%s

                WHERE id=%s;

            """, (nuevo_gram, nuevos_paq, kg_new, frac_id))



            self.cn.commit()

        except Exception:

            self.cn.rollback()

            raise



    def eliminar_fraccionamiento(self, frac_id:int):

        """

        Borra un fraccionamiento y revierte sus efectos en paquetes, materia prima y lotes.

        """

        cur = self._cursor()

        cur.execute("SELECT product_id, gramaje, paquetes, kg_consumidos FROM fractionations WHERE id=%s;", (frac_id,))

        row = cur.fetchone()

        if not row:

            raise ValueError("Fraccionamiento no encontrado.")

        prod_id, gram, paq, kg = int(row[0]), int(row[1]), int(row[2]), float(row[3] or 0.0)



        cur.execute("SELECT lot_id FROM lot_fractionations WHERE fractionation_id=%s LIMIT 1;", (frac_id,))

        lot_row = cur.fetchone()

        lot_id = int(lot_row[0]) if lot_row else None



        try:

            self.cn.execute("BEGIN")



            cur.execute("""

                UPDATE package_stock

                SET paquetes = paquetes - %s

                WHERE product_id=%s AND gramaje=%s;

            """, (paq, prod_id, gram))

            if cur.rowcount == 0:

                cur.execute("""

                    INSERT INTO package_stock(product_id, gramaje, paquetes)

                    VALUES(%s,%s,%s);

                """, (prod_id, gram, -paq))



            cur.execute("UPDATE raw_stock SET kg = kg + %s WHERE product_id=%s;", (kg, prod_id))



            if lot_id is not None:

                cur.execute("UPDATE raw_lots SET kg_saldo = kg_saldo + %s WHERE id=%s;", (kg, lot_id))



            cur.execute("DELETE FROM lot_fractionations WHERE fractionation_id=%s;", (frac_id,))

            cur.execute("DELETE FROM fractionations WHERE id=%s;", (frac_id,))



            self.cn.commit()

        except Exception:

            self.cn.rollback()

            raise









class App(tk.Tk):

    def __init__(self):

        super().__init__()

        self.title("Fraccionadora de granos - SQLite + Tkinter")

        self.geometry("1050x650")

        try:
            self.state("zoomed")  # arranca en ventana completa (Windows)
        except Exception:
            try:
                self.attributes("-zoomed", True)
            except Exception:
                pass

        self.resizable(True, True)

        self.repo = Repo()
        self._hist_product_color_styles = self.repo.list_product_color_styles()
        self._bag_eq_display_map = self.repo.get_product_bag_display_map()
        self._hist_prod_labels = {}
        self._hist_iid_pid = {}
        self._hist_iid_pname = {}

        self._setup_styles()



        self._build_toolbar()

        nb_main = ttk.Notebook(self)

        nb_main.pack(fill="both", expand=True, padx=8, pady=8)



        # ------------ Operaciones (ventas, compras, fraccionar, productos, bolsas) ------------

        ops_tab = ttk.Frame(nb_main)

        nb_main.add(ops_tab, text="Operaciones")

        ops_nb = ttk.Notebook(ops_tab)

        ops_nb.pack(fill="both", expand=True)

        self._build_tab_ventas(ops_nb)
        self._build_tab_notas_credito(ops_nb)

        self._build_tab_fraccionamiento(ops_nb)

        self._build_tab_compras(ops_nb)

        self._build_tab_productos(ops_nb)

        self._tab_venta_bolsas = TabVentaBolsas(self, ops_nb, BOLSAS_PREDEF)



        # ------------ Inventario ------------

        inv_tab = ttk.Frame(nb_main)

        nb_main.add(inv_tab, text="Inventario")

        inv_nb = ttk.Notebook(inv_tab)

        inv_nb.pack(fill="both", expand=True)

        self._build_tab_inventario(inv_nb)



        # ------------ Historial (fracc + ventas) ------------

        hist_tab = ttk.Frame(nb_main)

        nb_main.add(hist_tab, text="Historial")

        hist_nb = ttk.Notebook(hist_tab)

        hist_nb.pack(fill="both", expand=True)

        self._build_tab_hist_fracc(hist_nb)
        self._build_tab_hist_ajustes(hist_nb)

        self._tab_hist_ventas = TabHistorialVentas(self, hist_nb)



        # ------------ Panel (resúmenes y gastos/flujo/merma) ------------

        panel_tab = ttk.Frame(nb_main)

        nb_main.add(panel_tab, text="Panel")

        panel_nb = ttk.Notebook(panel_tab)

        panel_nb.pack(fill="both", expand=True)

        self._build_tab_resumenes(panel_nb)

        TabResumenCompras(self, panel_nb)

        TabFlujoDinero(self, panel_nb, self.repo)

        TabGastos(self, panel_nb, self.repo)

        self._tab_merma = TabMerma(self, panel_nb, self.repo)
        self._build_tab_analisis(panel_nb)



    def _setup_styles(self):

        style = ttk.Style(self)

        if "clam" in style.theme_names():

            style.theme_use("clam")



        base_bg = "#f7fbf5"         # fondo claro inspirado en el blanco del logo

        card_bg = "#ffffff"

        primary_text = "#12326b"    # azul corporativo

        muted_text = "#4e5b68"

        accent_blue = "#0d4ba0"

        accent_green = "#4cb050"

        accent_yellow = "#f6c746"

        zebra_alt = "#f0f4ff"



        self.configure(bg=base_bg)

        for fname in ("TkDefaultFont", "TkTextFont", "TkHeadingFont", "TkMenuFont"):

            try:

                tkfont.nametofont(fname).configure(family="Segoe UI", size=10)

            except tk.TclError:

                pass

        style.configure("TFrame", background=base_bg)

        style.configure("TLabel", foreground=primary_text, background=base_bg)

        style.configure("TNotebook", background=base_bg, padding=6)

        style.configure(

            "TNotebook.Tab",

            padding=(18, 8),

            foreground=primary_text,

            background=card_bg)

        style.map(

            "TNotebook.Tab",

            background=[("selected", card_bg), ("active", "#eef3ff")],

            foreground=[("selected", accent_blue)])

        style.configure("TButton", padding=(14, 6), background=accent_blue, foreground="#ffffff")

        style.map("TButton",

                  background=[("active", "#125ec9"), ("pressed", "#0b3f79")])
        style.configure("Export.TButton", padding=(14, 6), background="#2e7d32", foreground="#ffffff")
        style.map("Export.TButton",
                  background=[("active", "#2b6f2d"), ("pressed", "#1f5a24")])

        style.configure("TLabelframe", background=card_bg, padding=10, borderwidth=1, relief="solid")

        style.configure("TLabelframe.Label", background=card_bg, foreground=primary_text)

        style.configure(

            "Treeview",

            background=card_bg,

            fieldbackground=card_bg,

            borderwidth=0,

            rowheight=24,

            font=("Segoe UI", 10),

        )

        style.configure("Treeview.Heading",

                        font=("Segoe UI", 10, "bold"),

                        padding=6,

                        background=card_bg,

                        foreground=accent_blue)

        style.map(

            "Treeview",

            background=[("selected", "#d9e5ff")],

            foreground=[("selected", primary_text)],

        )

        style.configure(

            "WeatherValue.TLabel",

            background=card_bg,

            foreground=accent_green,

            font=("Segoe UI", 13, "bold"),

        )

        style.configure("WeatherInfo.TLabel", background=card_bg, foreground=muted_text)

        style.configure(

            "Usage.Horizontal.TProgressbar",

            troughcolor="#dfe7db",

            background=accent_green,

            thickness=12,

        )



        self._style = style

        self._stripe_colors = (card_bg, zebra_alt)



    def _build_toolbar(self):
        bar = ttk.Frame(self)
        bar.pack(fill="x", padx=8, pady=(8, 0))
        ttk.Button(bar, text="Respaldar SQLite", command=self._backup_sqlite_now, style="Export.TButton")\
            .pack(side="right")
        ttk.Button(bar, text="Reportes de ventas", command=self._launch_reportes_ventas)\
            .pack(side="left", padx=(0, 6))
        ttk.Button(bar, text="Reporte mensual", command=self._launch_reporte_mensual)\
            .pack(side="left", padx=(0, 6))
        ttk.Button(bar, text="Reporte trimestral PDF", command=self._launch_reporte_trimestral)\
            .pack(side="left", padx=(0, 6))
        ttk.Button(bar, text="Auditoria", command=self._launch_auditoria)\
            .pack(side="left", padx=(0, 6))
        ttk.Button(bar, text="RRHH", command=self._launch_rrhh)\
            .pack(side="left", padx=(0, 6))
        ttk.Button(bar, text="Importador de Facturas", command=self._launch_facturas_tabs)\
            .pack(side="left", padx=(0, 6))
        ttk.Button(bar, text="Importador OC", command=self._launch_oc_importer)\
            .pack(side="left", padx=(0, 6))
        ttk.Button(bar, text="Bancos / Chequeras", command=self._launch_bancos_chequeras)\
            .pack(side="left")

    def _backup_sqlite_now(self):
        self.config(cursor="watch")
        self.update_idletasks()
        try:
            results = backup_project_sqlite_from_postgres()
            parts = []
            for schema, table_counts in results.items():
                parts.append(f"{schema}: {sum(table_counts.values())} filas")
            messagebox.showinfo("Respaldo SQLite", "Respaldo generado correctamente.\n\n" + "\n".join(parts))
        except Exception as exc:
            messagebox.showerror("Respaldo SQLite", f"No se pudo generar el respaldo:\n{exc}")
        finally:
            self.config(cursor="")
            self.update_idletasks()

    def _launch_facturas_tabs(self):

        project_root = Path(__file__).resolve().parent.parent

        script = project_root / "importadorfactur" / "facturas_tabs.py"

        if script.exists():

            cmd = [sys.executable, str(script)]

            cwd = str(script.parent)

        else:

            # Fallback: ejecutar como modulo para evitar problemas de ruta

            cmd = [sys.executable, "-m", "importadorfactur.facturas_tabs"]

            cwd = str(project_root)

        try:

            subprocess.Popen(cmd, cwd=cwd)

        except Exception as exc:

            messagebox.showerror("Importador de Facturas", f"No se pudo abrir ({cmd}): {exc}")



    def _launch_oc_importer(self):

        script = Path(__file__).resolve().parent.parent / "PDFMK10" / "app_tk.py"

        if not script.exists():

            messagebox.showerror("Importador OC", f"No se encontro {script}.")

            return

        try:

            subprocess.Popen([sys.executable, str(script)], cwd=str(script.parent))

        except Exception as exc:

            messagebox.showerror("Importador OC", f"No se pudo abrir: {exc}")

    def _launch_bancos_chequeras(self):

        script = Path(__file__).resolve().parent / "bancos_chequeras_qt.py"

        if not script.exists():

            messagebox.showerror("Bancos y chequeras", f"No se encontro {script}.")

            return

        try:

            subprocess.Popen([sys.executable, str(script)], cwd=str(script.parent))

        except Exception as exc:

            messagebox.showerror("Bancos y chequeras", f"No se pudo abrir: {exc}")



    def _launch_auditoria(self):
        script = Path(__file__).resolve().parent / "auditoria.py"
        if not script.exists():
            messagebox.showerror("Auditoria", f"No se encontro {script}.")
            return
        try:
            subprocess.Popen([sys.executable, str(script)], cwd=str(script.parent))
        except Exception as exc:
            messagebox.showerror("Auditoria", f"No se pudo abrir: {exc}")

    def _launch_rrhh(self):
        script = Path(__file__).resolve().parent / "rrhh" / "RRHH.py"
        if not script.exists():
            messagebox.showerror("RRHH", f"No se encontro {script}.")
            return
        try:
            subprocess.Popen([sys.executable, str(script)], cwd=str(script.parent))
        except Exception as exc:
            messagebox.showerror("RRHH", f"No se pudo abrir: {exc}")

    def _launch_reportes_ventas(self):

        script = Path(__file__).resolve().parent.parent / "clon" / "reportes_ventas_qt.py"

        if not script.exists():

            messagebox.showerror("Reportes de ventas", f"No se encontro {script}.")

            return

        try:

            subprocess.Popen([sys.executable, str(script)], cwd=str(script.parent))

        except Exception as exc:

            messagebox.showerror("Reportes de ventas", f"No se pudo abrir: {exc}")

    def _launch_reporte_mensual(self):

        script = Path(__file__).resolve().parent.parent / "clon" / "reporte_mensual_qt.py"

        if not script.exists():

            messagebox.showerror("Reporte mensual", f"No se encontro {script}.")

            return

        try:

            subprocess.Popen([sys.executable, str(script)], cwd=str(script.parent))

        except Exception as exc:

            messagebox.showerror("Reporte mensual", f"No se pudo abrir: {exc}")

    def _launch_reporte_trimestral(self):

        script = Path(__file__).resolve().parent.parent / "clon" / "reporte_trimestral_qt.py"

        if not script.exists():

            messagebox.showerror("Reporte trimestral", f"No se encontro {script}.")

            return

        try:

            subprocess.Popen([sys.executable, str(script)], cwd=str(script.parent))

        except Exception as exc:

            messagebox.showerror("Reporte trimestral", f"No se pudo abrir: {exc}")



    def _apply_treeview_striping(self, tree):

        if not tree:

            return

        colors = getattr(self, "_stripe_colors", ("#ffffff", "#f6f8fc"))

        tree.tag_configure("evenrow", background=colors[0])

        tree.tag_configure("oddrow", background=colors[1])

        for idx, iid in enumerate(tree.get_children()):

            tags = [t for t in tree.item(iid, "tags") if t not in ("evenrow", "oddrow")]

            tags.append("evenrow" if idx % 2 == 0 else "oddrow")

            tree.item(iid, tags=tuple(tags))



    def _fmt_gs(self, x):

        try:

            return f"{float(x):,.0f}".replace(",", ".")

        except:

            return "-"

    def _render_weather(self):

        row = self.repo.latest_weather()

        if not row:

            self.lbl_w_time.config(text="Sin datos de clima")

            self.lbl_w_main.config(text="Pulsa 'Actualizar clima'")

            if hasattr(self, "lbl_w_detail"):

                self.lbl_w_detail.config(text="")

            return

        ts_utc, temp_c, rh, rain, cloud, src = row

        try:

            dt_utc = _dt.datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))

            dt_asu = dt_utc.astimezone(ZoneInfo("America/Asuncion"))

            ts_show = dt_asu.strftime("%Y-%m-%d %H:%M")

        except Exception:

            ts_show = ts_utc



        self.lbl_w_time.config(text=f"Ultima lectura: {ts_show} (America/Asuncion) - fuente: {src}")

        temp_txt = "--"

        if temp_c is not None:

            temp_txt = f"{temp_c:.1f} C"

        self.lbl_w_main.config(text=f"Temperatura: {temp_txt}")



        details = []

        if rh is not None:

            details.append(f"Humedad: {rh:.0f}%")

        if rain is not None:

            details.append(f"Lluvia/h: {rain:.2f} mm")

        if cloud is not None:

            details.append(f"Nubosidad: {cloud:.0f}%")

        detail_txt = " | ".join(details) or "Sin detalles meteorologicos"

        if hasattr(self, "lbl_w_detail"):

            self.lbl_w_detail.config(text=detail_txt)



    def _update_weather_auto(self):

        try:

            self._do_fetch_weather()

        except:

            pass

        self._render_weather()

        self.after(30*60*1000, self._update_weather_auto)  # 30 min



    def _do_fetch_weather(self):

        # Coordenadas de tu planta/depósito

        lat, lon = -25.267, -57.484

        try:

            fetch_and_store_weather(self.repo, lat, lon)

        except Exception as e:

            messagebox.showerror("Clima", f"No se pudo obtener el clima: {e}")



    def _update_weather_manual(self):

        self._do_fetch_weather()

        self._render_weather()





    def _build_tab_resumenes(self, nb):

        frame = ttk.Frame(nb)

        nb.add(frame, text="Resúmenes")

        boxW = ttk.Labelframe(frame, text="Clima (ahora)")

        boxW.pack(fill="x", padx=6, pady=6)



        self.lbl_w_time = ttk.Label(boxW, text="    Sin datos de clima", style="WeatherInfo.TLabel")

        self.lbl_w_time.pack(anchor="w")

        self.lbl_w_main = ttk.Label(boxW, text="Pulsa 'Actualizar clima'", style="WeatherValue.TLabel")

        self.lbl_w_main.pack(anchor="w", pady=(2, 0))

        self.lbl_w_detail = ttk.Label(boxW, text="", style="WeatherInfo.TLabel")

        self.lbl_w_detail.pack(anchor="w")



        frm_w = ttk.Frame(boxW); frm_w.pack(fill="x", pady=4)

        ttk.Button(frm_w, text="Actualizar clima", command=self._update_weather_manual).pack(side="left")



        # auto-refresh cada 30 min

        self.after(1000, self._update_weather_auto)



        # Filtros

        filtros = ttk.Frame(frame); filtros.pack(fill="x", pady=6)

        ttk.Label(filtros, text="Producto:").pack(side="left")

        self.cb_res_prod = ttk.Combobox(filtros, state="readonly", width=30, values=[])

        self.cb_res_prod.pack(side="left", padx=6)

        self.var_res_abiertos = tk.BooleanVar(value=False)

        ttk.Checkbutton(filtros, text="Solo abiertos", variable=self.var_res_abiertos,

                        command=self._refresh_resumenes).pack(side="left", padx=6)

        ttk.Button(filtros, text="Refrescar", command=self._refresh_resumenes).pack(side="left", padx=6)



        pan = ttk.Panedwindow(frame, orient="horizontal"); pan.pack(fill="both", expand=True, pady=6)



        # Maestro: lotes

        boxL = ttk.Labelframe(pan, text="Lotes (maestro)")

        pan.add(boxL, weight=1)

        cols_l = ("lote","producto","kg_total","kg_usado","kg_disp","costo_kg")

        self.tv_res_lots = ttk.Treeview(boxL, columns=cols_l, show="headings", height=14)

        self.tv_res_lots.heading("lote",      text="Lote")

        self.tv_res_lots.heading("producto",  text="Producto")

        self.tv_res_lots.heading("kg_total",  text="Kg total")

        self.tv_res_lots.heading("kg_usado",  text="Kg usados")

        self.tv_res_lots.heading("kg_disp",   text="Kg disp.")

        self.tv_res_lots.heading("costo_kg",  text="Costo/kg (Gs)")

        self.tv_res_lots.column("lote",       width=140)

        self.tv_res_lots.column("producto",   width=160)

        self.tv_res_lots.column("kg_total",   width=90,  anchor="center")

        self.tv_res_lots.column("kg_usado",   width=90,  anchor="center")

        self.tv_res_lots.column("kg_disp",    width=90,  anchor="center")

        self.tv_res_lots.column("costo_kg",   width=110, anchor="center")

        self.tv_res_lots.pack(fill="both", expand=True, padx=6, pady=6)

        self.tv_res_lots.bind("<<TreeviewSelect>>", self._on_res_lot_select)
        self.tv_res_lots.bind("<Double-1>", self._on_res_lot_double_click)
        self._res_lot_iid2id = {}



        # Detalle a la derecha

        boxR = ttk.Labelframe(pan, text="Detalle del lote")

        pan.add(boxR, weight=2)



        hdr = ttk.Frame(boxR); hdr.pack(fill="x", padx=6, pady=(6,0))

        self.lbl_res_header = ttk.Label(hdr, text="-", font=("TkDefaultFont", 10, "bold"))

        self.lbl_res_header.pack(side="left", anchor="w", expand=True)

        self.btn_close_lot = ttk.Button(hdr, text="Cerrar lote", command=self._close_selected_lot, state=tk.DISABLED)

        self.btn_close_lot.pack(side="right")

        self.btn_open_lot = ttk.Button(hdr, text="Abrir lote", command=self._open_selected_lot, state=tk.DISABLED)

        self.btn_open_lot.pack(side="right", padx=(0, 6))

        self.btn_edit_lot = ttk.Button(hdr, text="Modificar lote", command=self._edit_selected_lot, state=tk.DISABLED)

        self.btn_edit_lot.pack(side="right", padx=(0, 6))

        self.btn_lot_metric = ttk.Button(hdr, text="Metrica", command=self._open_selected_lot_metric, state=tk.DISABLED)

        self.btn_lot_metric.pack(side="right", padx=(0, 6))

        self._res_selected_lot = None



        grid = ttk.Frame(boxR); grid.pack(fill="x", padx=6, pady=6)

        self._res_lbls = {}

        def add_field(r, c, key, title):

            col_label = c * 2

            ttk.Label(grid, text=title + ":").grid(row=r, column=col_label, sticky="w", pady=2, padx=(0,2))

            lbl = ttk.Label(grid, text="???")

            lbl.grid(row=r, column=col_label + 1, sticky="w", padx=6)

            self._res_lbls[key] = lbl



        # Columna izquierda: compra -> beneficio

        add_field(0, 0, "fecha",     "Fecha compra")

        add_field(1, 0, "proveedor", "Proveedor")

        add_field(2, 0, "factura",   "N° factura")

        add_field(3, 0, "monto",     "Monto compra (Gs)")

        add_field(4, 0, "venta",     "Venta estimada (Gs)")

        add_field(5, 0, "benef",     "Beneficio estimado (Gs)")



        # Columna derecha: costos y stock del lote

        add_field(0, 1, "costo_kg",  "Costo por kg (Gs/kg)")

        add_field(1, 1, "kg_tot",    "Kg totales")

        add_field(2, 1, "bolsas_eq", "Cant. bolsas (eq)")

        add_field(3, 1, "kg_used",   "Kg usados (bolsas eq)")
        add_field(4, 1, "kg_disp",   "Kg disponibles (bolsas eq)")
        add_field(5, 1, "merma",     "Merma total")
        add_field(6, 1, "benef_pct", "Beneficio estimado (%)")

        self._res_lbls["merma"].config(foreground="#b91c1c")



        prog = ttk.Frame(boxR)

        prog.pack(fill="x", padx=6, pady=(0, 6))

        ttk.Label(prog, text="Consumo del lote").pack(anchor="w")

        self.pg_lot_usage = tk.Canvas(prog, height=20, background="#f4f7ef", highlightthickness=0)

        self.pg_lot_usage.pack(fill="x", pady=2)

        self._usage_state = (0.0, 0.0)

        self.pg_lot_usage.bind("<Configure>", lambda e: self._draw_usage_bar())

        self.lbl_lot_usage = ttk.Label(prog, text="Sin datos de consumo")

        self.lbl_lot_usage.pack(anchor="w")



        boxH = ttk.Labelframe(boxR, text="Fraccionamientos de este lote")

        boxH.pack(fill="both", expand=True, padx=6, pady=(6,8))

        cols_h = ("fecha","gramaje","paquetes","kg","bolsas_eq","costo_kg","costo_total","precio","benef")

        self.tv_res_hist = ttk.Treeview(boxH, columns=cols_h, show="headings", height=10)

        for c, t, w, a in [

            ("fecha","Fecha",160,"w"),

            ("gramaje","Gramaje (g)",100,"center"),

            ("paquetes","Paquetes",100,"center"),

            ("kg","Kg consumidos",120,"center"),

            ("bolsas_eq","Bolsas eq",110,"center"),

            ("costo_kg","Costo kg",100,"center"),

            ("costo_total","Costo total",120,"center"),

            ("precio","Precio venta (Gs)",140,"center"),

            ("benef","Beneficio (Gs)",140,"center"),

        ]:

            self.tv_res_hist.heading(c, text=t)

            self.tv_res_hist.column(c, width=w, anchor=a)

        self.tv_res_hist.pack(fill="both", expand=True, padx=6, pady=6)

        self._refresh_resumenes()



    # --- en tu sección de UI donde refrescás el Treeview ---





    def _fmt_kg(self, x):

        try:    return f"{float(x):.3f}"

        except: return "-"



    def _refresh_resumenes(self, *_):

        # cargar productos (+ "Todos")

        prods = [name for _, name in self.repo.list_products()]

        vals = ["Todos"] + prods

        self.cb_res_prod["values"] = vals

        if not self.cb_res_prod.get():

            self.cb_res_prod.set("Todos")

        self._load_lot_list()

        self._clear_lot_detail()



    def _load_lot_list(self):

        # limpiar tabla

        for i in self.tv_res_lots.get_children():

            self.tv_res_lots.delete(i)

        self._res_lot_iid2id.clear()



        sel = (self.cb_res_prod.get() or "Todos").strip()

        pid = None if sel == "Todos" else self.repo.get_product_id_by_name(sel)

        solo_abiertos = bool(self.var_res_abiertos.get())



        # tag visual para negativos

        self.tv_res_lots.tag_configure("neg", foreground="#b91c1c")



        for (lot_id, lot_no, product, kg_total, kg_used, kg_disp,

            amount_total, costo_kg, proveedor, factura, ts) in self.repo.listar_lotes(pid, solo_abiertos):



            tags = ("neg",) if (kg_disp is not None and float(kg_disp) < 0) else ()

            iid = self.tv_res_lots.insert(

                "", "end",

                values=(

                    lot_no or f"L{lot_id}",

                    product,

                    self._fmt_kg(kg_total),

                    self._fmt_kg(kg_used),

                    self._fmt_kg(kg_disp),

                    self._fmt_gs(costo_kg),

                ),

                tags=tags

            )

            self._res_lot_iid2id[iid] = lot_id

        self._apply_treeview_striping(self.tv_res_lots)



    def _clear_lot_detail(self):

        self.lbl_res_header.config(text="-")

        for k, lbl in getattr(self, "_res_lbls", {}).items():

            lbl.config(text="-")

        if hasattr(self, "pg_lot_usage"):

            self._usage_state = (0.0, 0.0)

            self._draw_usage_bar()

        if hasattr(self, "lbl_lot_usage"):

            self.lbl_lot_usage.config(text="Sin datos de consumo")

        if hasattr(self, "tv_res_hist"):

            for i in self.tv_res_hist.get_children():

                self.tv_res_hist.delete(i)

        self._res_selected_lot = None

        if hasattr(self, "btn_close_lot"):

            self.btn_close_lot.config(state=tk.DISABLED)

        if hasattr(self, "btn_open_lot"):

            self.btn_open_lot.config(state=tk.DISABLED)

        if hasattr(self, "btn_edit_lot"):

            self.btn_edit_lot.config(state=tk.DISABLED)
        if hasattr(self, "btn_lot_metric"):

            self.btn_lot_metric.config(state=tk.DISABLED)



    def _set_usage_bar(self, used_pct: float, merma_pct: float):

        used_pct = max(0.0, min(100.0, used_pct))

        merma_pct = max(0.0, min(100.0, merma_pct))

        self._usage_state = (used_pct, merma_pct)

        self._draw_usage_bar()



    def _draw_usage_bar(self):

        canvas = getattr(self, "pg_lot_usage", None)

        if not isinstance(canvas, tk.Canvas):

            return

        canvas.delete("all")

        used_pct, merma_pct = getattr(self, "_usage_state", (0.0, 0.0))

        width = canvas.winfo_width()

        height = canvas.winfo_height()

        if width <= 1 or height <= 1:

            canvas.after(30, self._draw_usage_bar)

            return

        used_w = width * (used_pct / 100.0)

        total_w = width * ((used_pct + merma_pct) / 100.0)

        if used_w > 0:

            canvas.create_rectangle(0, 0, used_w, height, fill="#4cb050", outline="")

        if total_w > used_w:

            canvas.create_rectangle(used_w, 0, total_w, height, fill="#c53030", outline="")

        canvas.create_rectangle(0, 0, width - 1, height - 1, outline="#8aa07a")



    def _on_res_lot_select(self, *_):

        sel = self.tv_res_lots.selection()

        if not sel:

            self._clear_lot_detail(); return

        lot_id = self._res_lot_iid2id.get(sel[0])

        if not lot_id:

            self._clear_lot_detail(); return



        row = self.repo.lot_detail(lot_id)

        if not row:

            self._clear_lot_detail(); return



        (lot_id, lot_no, product_id, product,

        kg_total, kg_saldo, amount_total, costo_kg,

        proveedor, factura, ts, cerrado) = row



        status = " (CERRADO)" if cerrado else ""

        header = f"Lote {lot_no or f'L{lot_id}'} - {product}{status}"

        self.lbl_res_header.config(text=header)



        self._res_selected_lot = lot_id

        if hasattr(self, "btn_close_lot"):

            self.btn_close_lot.config(state=(tk.DISABLED if cerrado else tk.NORMAL))

        if hasattr(self, "btn_open_lot"):

            self.btn_open_lot.config(state=(tk.NORMAL if cerrado else tk.DISABLED))

        if hasattr(self, "btn_edit_lot"):

            self.btn_edit_lot.config(state=tk.NORMAL if not cerrado else tk.DISABLED)
        if hasattr(self, "btn_lot_metric"):

            self.btn_lot_metric.config(state=tk.NORMAL)



        kg_used = (kg_total or 0) - (kg_saldo or 0)

        costo_kg_calc = float(costo_kg or 0)



        self._res_lbls["fecha"].config(     text=str(ts or "-"))

        self._res_lbls["proveedor"].config( text=str(proveedor or "-"))

        self._res_lbls["factura"].config(   text=str(factura or "-"))

        self._res_lbls["monto"].config(     text=self._fmt_gs(amount_total or 0))

        self._res_lbls["costo_kg"].config(  text=self._fmt_gs(costo_kg_calc))

        self._res_lbls["kg_tot"].config(    text=self._fmt_kg(kg_total or 0))

        bag_kg = None
        bolsas_eq_txt = "-"
        bolsas_used_txt = "-"
        bolsas_disp_txt = "-"
        try:
            bag_kg = float(bag_kg_por_defecto(product))
            if bag_kg > 0:
                bolsas_eq = (kg_total or 0) / bag_kg
                bolsas_eq_txt = f"{bolsas_eq:,.3f}".replace(",", ".")
                bolsas_used = (kg_used or 0) / bag_kg
                bolsas_disp = (kg_saldo or 0) / bag_kg
                bolsas_used_txt = f"{bolsas_used:,.3f}".replace(",", ".")
                bolsas_disp_txt = f"{bolsas_disp:,.3f}".replace(",", ".")
        except Exception:
            bolsas_eq_txt = "-"
        if "bolsas_eq" in self._res_lbls:
            self._res_lbls["bolsas_eq"].config(text=bolsas_eq_txt)

        kg_used_txt = f"{self._fmt_kg(kg_used)} ({bolsas_used_txt} bolsas)"
        kg_disp_txt = f"{self._fmt_kg(kg_saldo or 0)} ({bolsas_disp_txt} bolsas)"
        self._res_lbls["kg_used"].config(   text=kg_used_txt)
        self._res_lbls["kg_disp"].config(   text=kg_disp_txt)

        merma_total = float(self.repo.total_merma_por_lote(lot_id) or 0.0)

        if "merma" in self._res_lbls:

            self._res_lbls["merma"].config(text=self._fmt_kg(merma_total))



        if hasattr(self, "pg_lot_usage"):

            total_val = float(kg_total or 0)

            used_val = float(kg_used or 0)

            total_consumed = used_val + merma_total

            pct_used = pct_merma = pct_total = 0.0

            if total_val > 0:

                pct_used = max(0.0, min(100.0, (used_val / total_val) * 100))

                pct_total = max(0.0, min(100.0, (total_consumed / total_val) * 100))

                pct_merma = max(0.0, pct_total - pct_used)

            self._set_usage_bar(pct_used, pct_merma)

            if hasattr(self, "lbl_lot_usage"):

                self.lbl_lot_usage.config(

                    text=("Consumo: {total:.0f}% ({used} kg + {merma} kg merma = {tot_used} / {total_kg} kg)"

                          .format(

                              total=pct_total,

                              used=self._fmt_kg(used_val),

                              merma=self._fmt_kg(merma_total),

                              tot_used=self._fmt_kg(total_consumed),

                              total_kg=self._fmt_kg(total_val)

                          ))

                )



        # Historial

        for i in self.tv_res_hist.get_children():

            self.tv_res_hist.delete(i)

        total_gs = 0.0

        total_venta = 0.0

        for (ts_f, g, paq, kgc) in self.repo.listar_fraccionamientos_de_lote(lot_id):

            kg_val = float(kgc or 0)
            bolsas_eq_row = "-"
            if bag_kg and bag_kg > 0:
                bolsas_eq_row = f"{(kg_val / bag_kg):,.3f}".replace(",", ".")

            price, _iva = self.repo.get_price(product_id, int(g))

            price = float(price or 0)

            paq_val = int(paq or 0)

            total_venta += price * paq_val

            self.tv_res_hist.insert("", "end",

                values=(str(ts_f), int(g), int(paq),

                        self._fmt_kg(kg_val),
                        bolsas_eq_row,

                        self._fmt_gs(costo_kg_calc),

                        self._fmt_gs(costo_kg_calc * kg_val),

                        self._fmt_gs(price),

                        self._fmt_gs(price * paq_val)))

        self._apply_treeview_striping(self.tv_res_hist)

        venta_lbl = self._res_lbls.get("venta")

        if venta_lbl:

            venta_lbl.config(text=self._fmt_gs(total_venta))

        benef_lbl = self._res_lbls.get("benef")
        benef_val = total_venta - float(amount_total or 0)
        if benef_lbl:
            benef_lbl.config(text=self._fmt_gs(benef_val))
        benef_pct_lbl = self._res_lbls.get("benef_pct")
        if benef_pct_lbl:
            compra_val = float(amount_total or 0)
            pct_txt = "-"
            if compra_val:
                pct_txt = f"{(benef_val / compra_val) * 100:.1f}%"
            benef_pct_lbl.config(text=pct_txt)



    def _build_tab_analisis(self, nb):
        frame = ttk.Frame(nb)
        nb.add(frame, text="Analisis")

        sub_nb = ttk.Notebook(frame)
        sub_nb.pack(fill="both", expand=True, padx=6, pady=6)

        self._build_tab_analitica_clientes(sub_nb)
        self._build_tab_proyeccion_compras(sub_nb)
        TabProduccion(self, sub_nb, self.repo)

    # --- Analítica de clientes (resumen) ---
    def _build_tab_analitica_clientes(self, nb):
        frame = ttk.Frame(nb)
        nb.add(frame, text="Analitica clientes")

        topbar = ttk.Frame(frame); topbar.pack(fill="x", pady=6, padx=6)
        ttk.Label(topbar, text="Top:").pack(side="left")
        self.ent_cli_top = ttk.Entry(topbar, width=6)
        self.ent_cli_top.insert(0, "15")
        self.ent_cli_top.pack(side="left", padx=4)
        ttk.Button(topbar, text="Refrescar", command=self._refresh_analitica_clientes).pack(side="left")
        ttk.Label(topbar, text="(monto total facturas + ventas de bolsas)").pack(side="left", padx=8)

        cols = ("cliente","ops","fact","bolsa","monto","ticket","ultima")
        self.tv_cli = ttk.Treeview(frame, columns=cols, show="headings", height=14)
        self.tv_cli.heading("cliente", text="Cliente")
        self.tv_cli.heading("ops",     text="Ops")
        self.tv_cli.heading("fact",    text="Facturas")
        self.tv_cli.heading("bolsa",   text="Bolsas")
        self.tv_cli.heading("monto",   text="Monto (Gs)")
        self.tv_cli.heading("ticket",  text="Ticket prom.")
        self.tv_cli.heading("ultima",  text="Ultima compra")

        self.tv_cli.column("cliente", width=260, anchor="w")
        self.tv_cli.column("ops",     width=60,  anchor="center")
        self.tv_cli.column("fact",    width=80,  anchor="center")
        self.tv_cli.column("bolsa",   width=80,  anchor="center")
        self.tv_cli.column("monto",   width=120, anchor="e")
        self.tv_cli.column("ticket",  width=110, anchor="e")
        self.tv_cli.column("ultima",  width=160, anchor="w")

        self.tv_cli.pack(fill="both", expand=True, padx=6, pady=(0,6))
        self._apply_treeview_striping(self.tv_cli)
        self._refresh_analitica_clientes()

    def _refresh_analitica_clientes(self):
        try:
            import analitica_clientes as ac
        except Exception as exc:
            messagebox.showerror("Analitica", f"No se pudo cargar analitica_clientes.py: {exc}")
            return
        try:
            top = int((self.ent_cli_top.get() or "15").strip())
            if top <= 0:
                top = 15
        except Exception:
            top = 15
            self.ent_cli_top.delete(0, tk.END)
            self.ent_cli_top.insert(0, "15")

        try:
            rows = ac.cargar_resumen(top)
        except Exception as exc:
            messagebox.showerror("Analitica", str(exc))
            return

        for i in self.tv_cli.get_children():
            self.tv_cli.delete(i)
        for r in rows:
            self.tv_cli.insert("", "end", values=(
                r.get("cliente","-"),
                r.get("ops",0),
                r.get("facturas",0),
                r.get("bolsas",0),
                self._fmt_gs(r.get("total_gs",0)),
                self._fmt_gs(r.get("ticket_prom",0)),
                str(r.get("last_ts",""))[:19] or "-",
            ))
        self._apply_treeview_striping(self.tv_cli)

    # --- Proyeccion de compras ---
    def _build_tab_proyeccion_compras(self, nb):
        frame = ttk.Frame(nb)
        nb.add(frame, text="Proyeccion compras")

        topbar = ttk.Frame(frame); topbar.pack(fill="x", pady=6, padx=6)
        ttk.Label(topbar, text="Ventana dias:").pack(side="left")
        self.ent_proj_window = ttk.Entry(topbar, width=6)
        self.ent_proj_window.insert(0, "30")
        self.ent_proj_window.pack(side="left", padx=4)
        ttk.Label(topbar, text="Top (opcional):").pack(side="left", padx=(8,2))
        self.ent_proj_top = ttk.Entry(topbar, width=6)
        self.ent_proj_top.pack(side="left", padx=4)
        ttk.Button(topbar, text="Refrescar", command=self._refresh_proyeccion_compras).pack(side="left", padx=6)
        ttk.Label(topbar, text="Consumo promedio solo en dias con movimiento. Ordenado por menos dias restantes.").pack(side="left", padx=6)

        cols = ("producto","stock","cons_dia","dias_rest","cons_total","dias_act")
        self.tv_proj = ttk.Treeview(frame, columns=cols, show="headings", height=14)
        self.tv_proj.heading("producto",  text="Producto")
        self.tv_proj.heading("stock",     text="Stock kg")
        self.tv_proj.heading("cons_dia",  text="Cons/dia (kg)")
        self.tv_proj.heading("dias_rest", text="Dias restantes")
        self.tv_proj.heading("cons_total",text="Consumo ventana")
        self.tv_proj.heading("dias_act",  text="Dias con consumo")

        self.tv_proj.column("producto",  width=220, anchor="w")
        self.tv_proj.column("stock",     width=100, anchor="center")
        self.tv_proj.column("cons_dia",  width=130, anchor="center")
        self.tv_proj.column("dias_rest", width=110, anchor="center")
        self.tv_proj.column("cons_total",width=130, anchor="center")
        self.tv_proj.column("dias_act",  width=130, anchor="center")

        self.tv_proj.pack(fill="both", expand=True, padx=6, pady=(0,6))
        self.lbl_proj_info = ttk.Label(frame, text="Carga inicial...", foreground="#374151")
        self.lbl_proj_info.pack(anchor="w", padx=8, pady=(0,6))
        self._apply_treeview_striping(self.tv_proj)
        self._refresh_proyeccion_compras()

    def _refresh_proyeccion_compras(self):
        try:
            import proyeccion_compras as pc
        except Exception as exc:
            messagebox.showerror("Proyeccion compras", f"No se pudo cargar proyeccion_compras.py: {exc}")
            return
        try:
            ventana = int((self.ent_proj_window.get() or "30").strip())
            if ventana <= 0:
                ventana = 30
        except Exception:
            ventana = 30
            self.ent_proj_window.delete(0, tk.END)
            self.ent_proj_window.insert(0, "30")
        try:
            top_txt = (self.ent_proj_top.get() or "").strip()
            top_n = int(top_txt) if top_txt else None
            if top_n is not None and top_n <= 0:
                top_n = None
        except Exception:
            top_n = None
            self.ent_proj_top.delete(0, tk.END)

        try:
            rows = pc.cargar_proyeccion(ventana, top_n)
        except Exception as exc:
            messagebox.showerror("Proyeccion compras", str(exc))
            return

        for i in self.tv_proj.get_children():
            self.tv_proj.delete(i)
        self.tv_proj.tag_configure("crit", foreground="#b91c1c")
        self.tv_proj.tag_configure("warn", foreground="#d97706")
        self.tv_proj.tag_configure("ok", foreground="#0f766e")

        crit = warn = 0
        for r in rows:
            dias_txt = "-"
            tag = "ok"
            if r.get("dias_restantes") is not None:
                try:
                    dias_txt = f"{float(r['dias_restantes']):.1f}"
                    if float(r["dias_restantes"]) <= 7:
                        tag = "crit"; crit += 1
                    elif float(r["dias_restantes"]) <= 15:
                        tag = "warn"; warn += 1
                except Exception:
                    dias_txt = str(r.get("dias_restantes"))
                    tag = "ok"
            self.tv_proj.insert("", "end", values=(
                r.get("producto","-"),
                self._fmt_kg(r.get("stock_kg",0)),
                self._fmt_kg(r.get("consumo_diario",0)),
                dias_txt,
                self._fmt_kg(r.get("consumo_total",0)),
                str(r.get("dias_activos","-")),
            ), tags=(tag,))
        self._apply_treeview_striping(self.tv_proj)
        total = len(rows)
        self.lbl_proj_info.config(
            text=f"{total} productos. Criticos (<=7d): {crit} | Aviso (<=15d): {warn} | Consumo promedio calculado solo en dias con consumo."
        )


    def _on_res_lot_double_click(self, event):
        tree = getattr(self, "tv_res_lots", None)
        if not tree:
            return

        col_id = tree.identify_column(event.x)
        try:
            col_idx = int(col_id.replace("#", "")) - 1
        except Exception:
            return

        cols = list(tree["columns"])
        if col_idx < 0 or col_idx >= len(cols):
            return
        if cols[col_idx] != "costo_kg":
            return

        iid = tree.identify_row(event.y)
        if not iid:
            return

        vals = tree.item(iid, "values")
        if not vals or len(vals) < 2:
            return

        producto = str(vals[1] or "")
        lot_id = self._res_lot_iid2id.get(iid)
        if not lot_id:
            return

        pid = self.repo.get_product_id_by_name(producto)
        if not pid:
            messagebox.showerror("Lotes", "No se pudo cargar el producto.")
            return

        data = self.repo.historial_costos_lotes(pid)
        if not data:
            messagebox.showinfo("Lotes", "No hay historial de costos para mostrar.")
            return

        self._show_cost_history_chart(producto, lot_id, data)

    def _show_cost_history_chart(self, product_name: str, selected_lot_id: int, data):
        win = tk.Toplevel(self)
        win.title(f"Historial costo/kg - {product_name}")
        win.resizable(True, False)

        container = ttk.Frame(win)
        container.pack(fill="both", expand=True, padx=10, pady=10)

        ttk.Label(container, text="Evolucion del costo por kg (lotes comprados)").pack(anchor="w")

        canvas = tk.Canvas(container, width=760, height=380, background="#f8fafc", highlightthickness=0)
        canvas.pack(fill="both", expand=True, pady=(8, 0))

        w = int(canvas.cget("width") or 760)
        h = int(canvas.cget("height") or 360)
        left, right, top, bottom = 70, 30, 30, 70
        plot_w = max(1, w - left - right)
        plot_h = max(1, h - top - bottom)

        months = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]
        points = []
        for lot_id, ts, lote, costo_kg, kg_ini, costo_total in data:
            try:
                cost_val = float(costo_kg)
            except Exception:
                continue
            label = str(lote or f"L{lot_id}")
            if len(label) > 14:
                label = label[:13] + "..."
            month_txt = ""
            try:
                dt = _dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                month_txt = months[dt.month - 1]
            except Exception:
                try:
                    dt = _dt.datetime.strptime(str(ts)[:10], "%Y-%m-%d")
                    month_txt = months[dt.month - 1]
                except Exception:
                    month_txt = ""
            xlabel = f"{month_txt} {label}".strip() if month_txt else label
            points.append({
                "id": int(lot_id),
                "ts": ts,
                "label": label,
                "xlabel": xlabel,
                "costo": cost_val,
            })

        if not points:
            messagebox.showinfo("Lotes", "No hay costos para graficar.")
            win.destroy()
            return

        min_y = min(p["costo"] for p in points)
        max_y = max(p["costo"] for p in points)
        if abs(max_y - min_y) < 1e-6:
            pad = max(1.0, max_y * 0.05 or 1.0)
            min_y -= pad
            max_y += pad
        else:
            delta = (max_y - min_y) * 0.08
            min_y -= delta
            max_y += delta

        step = plot_w / max(1, len(points) - 1)

        def y_for(val: float) -> float:
            if max_y == min_y:
                return top + plot_h / 2
            ratio = (val - min_y) / (max_y - min_y)
            ratio = max(0.0, min(1.0, ratio))
            return (h - bottom) - (ratio * plot_h)

        # Ejes
        canvas.create_line(left, top, left, h - bottom, fill="#475569")
        canvas.create_line(left, h - bottom, left + plot_w, h - bottom, fill="#475569")

        # Grid horizontal y labels de valores
        for i in range(5):
            frac = i / 4 if i else 0
            y = top + (plot_h * frac)
            val = max_y - ((max_y - min_y) * frac)
            canvas.create_line(left, y, left + plot_w, y, fill="#e5e7eb")
            canvas.create_text(8, y, text=self._fmt_gs(val), anchor="w", font=("TkDefaultFont", 8), fill="#475569")

        # Líneas de referencia: promedio y último costo
        avg_cost = sum(p["costo"] for p in points) / len(points)
        last_cost = points[-1]["costo"]
        for val, color, label_txt in [
            (avg_cost, "#0ea5e9", "Promedio"),
            (last_cost, "#f59e0b", "Último"),
        ]:
            y = y_for(val)
            canvas.create_line(left, y, left + plot_w, y, fill=color, dash=(4, 3))
            canvas.create_text(left + plot_w - 4, y - 10, text=f"{label_txt}: {self._fmt_gs(val)}", anchor="se", fill=color, font=("TkDefaultFont", 8))

        last_x = last_y = None
        selected_cost = None
        hitboxes = []  # guarda coords y data para hover
        for idx, pt in enumerate(points):
            x = left + (step * idx if len(points) > 1 else plot_w / 2)
            y = y_for(pt["costo"])
            if last_x is not None:
                canvas.create_line(last_x, last_y, x, y, fill="#2563eb", width=2)
            color = "#dc2626" if pt["id"] == selected_lot_id else "#2563eb"
            radius = 5 if pt["id"] == selected_lot_id else 3
            canvas.create_oval(x - radius, y - radius, x + radius, y + radius, fill=color, outline="")
            canvas.create_text(x, y - 12, text=self._fmt_gs(pt["costo"]), fill=color, font=("TkDefaultFont", 8))
            canvas.create_text(x, h - bottom + 14, text=pt.get("xlabel") or pt["label"], angle=45, anchor="nw", font=("TkDefaultFont", 8))
            hitboxes.append((x, y, pt))
            last_x, last_y = x, y
            if pt["id"] == selected_lot_id:
                selected_cost = pt["costo"]

        if selected_cost is not None:
            ttk.Label(
                container,
                text=f"Lote actual: {self._fmt_gs(selected_cost)} Gs/kg",
                foreground="#dc2626"
            ).pack(anchor="w", pady=(6, 0))

        info_lbl = ttk.Label(container, text="", foreground="#111827")
        info_lbl.pack(anchor="w", pady=(4, 0))

        def _on_move(ev):
            if not hitboxes:
                return
            nearest = None
            min_dist2 = 64  # 8 px radio
            for x, y, pt in hitboxes:
                dx = ev.x - x
                dy = ev.y - y
                d2 = dx * dx + dy * dy
                if d2 <= min_dist2:
                    min_dist2 = d2
                    nearest = pt
            if not nearest:
                info_lbl.config(text="")
                return
            info_lbl.config(
                text=f"{nearest.get('ts') or ''} | Lote {nearest.get('label')} | {self._fmt_gs(nearest['costo'])} Gs/kg"
            )

        canvas.bind("<Motion>", _on_move)
        canvas.bind("<Leave>", lambda _e: info_lbl.config(text=""))

    def _edit_selected_lot(self):

        lot_id = getattr(self, "_res_selected_lot", None)

        if not lot_id:

            messagebox.showinfo("Lotes", "Seleccione un lote primero.")

            return

        row = self.repo.lot_detail(lot_id)

        if not row:

            messagebox.showerror("Lotes", "No se pudo cargar la información del lote.")

            return

        _, lot_no, *_ = row

        nuevo = simpledialog.askstring("Modificar lote", "Nuevo identificador del lote:", initialvalue=lot_no or "")

        if nuevo is None:

            return

        nuevo = (nuevo or "").strip()

        if not nuevo:

            messagebox.showwarning("Lotes", "El identificador no puede estar vacío.")

            return

        try:

            self.repo.renombrar_lote(lot_id, nuevo)

        except Exception as exc:

            messagebox.showerror("Lotes", f"No se pudo modificar el lote: {exc}")

            return



        self._load_lot_list()

        for iid, rid in self._res_lot_iid2id.items():

            if rid == lot_id:

                self.tv_res_lots.selection_set(iid)

                self.tv_res_lots.focus(iid)

                break

        self._on_res_lot_select()

        if hasattr(self, "_refresh_lotes_abiertos"):

            self._refresh_lotes_abiertos()

        if hasattr(self, "_tab_merma"):

            self._tab_merma.refresh_lotes()

        messagebox.showinfo("Lotes", "Lote modificado correctamente.")

    def _open_selected_lot_metric(self):

        lot_id = getattr(self, "_res_selected_lot", None)

        if not lot_id:

            messagebox.showinfo("Lotes", "Seleccione un lote primero.")

            return

        metric_path = (Path(__file__).resolve().parent.parent / "clon" / "metrica.py")

        if not metric_path.exists():

            messagebox.showerror("Metrica", f"No se encontró el archivo:\n{metric_path}")

            return

        try:

            subprocess.Popen([sys.executable, str(metric_path), "--lot-id", str(int(lot_id))])

        except Exception as exc:

            messagebox.showerror("Metrica", f"No se pudo abrir Métrica: {exc}")

    def _close_selected_lot(self):

        lot_id = getattr(self, "_res_selected_lot", None)

        if not lot_id:

            messagebox.showinfo("Lotes", "Seleccione un lote abierto primero.")

            return

        if not messagebox.askyesno("Lotes", "Seguro que deseas cerrar el lote seleccionado?"):

            return

        try:

            self.repo.cerrar_lote(lot_id)

            messagebox.showinfo("Lotes", "Lote cerrado correctamente.")

        except Exception as e:

            messagebox.showerror("Lotes", str(e))

            return

        self._refresh_resumenes()

        if hasattr(self, "_refresh_lotes_abiertos"):

            self._refresh_lotes_abiertos()

        if hasattr(self, "_on_frac_producto_seleccionado"):

            try:

                self._on_frac_producto_seleccionado()

            except Exception:

                pass

    def _open_selected_lot(self):

        lot_id = getattr(self, "_res_selected_lot", None)

        if not lot_id:

            messagebox.showinfo("Lotes", "Seleccione un lote cerrado primero.")

            return

        if not messagebox.askyesno("Lotes", "Seguro que deseas abrir el lote seleccionado?"):

            return

        try:

            self.repo.abrir_lote(lot_id)

            messagebox.showinfo("Lotes", "Lote abierto correctamente.")

        except Exception as e:

            messagebox.showerror("Lotes", str(e))

            return

        self._refresh_resumenes()

        if hasattr(self, "_refresh_lotes_abiertos"):

            self._refresh_lotes_abiertos()

        if hasattr(self, "_on_frac_producto_seleccionado"):

            try:

                self._on_frac_producto_seleccionado()

            except Exception:

                pass



    # ---------- Pestaña: Productos ----------

    def _build_tab_productos(self, nb):

        frame = ttk.Frame(nb)

        nb.add(frame, text="Productos")



        top = ttk.Frame(frame)

        top.pack(fill="x", pady=6)

        ttk.Label(top, text="Nuevo producto:").pack(side="left")

        self.ent_prod = ttk.Entry(top, width=40)

        self.ent_prod.pack(side="left", padx=6)

        ttk.Button(top, text="Agregar", command=self._add_producto).pack(side="left")



        self.tv_prod = ttk.Treeview(frame, columns=("id", "name"), show="headings", height=10)

        self.tv_prod.heading("id", text="ID")

        self.tv_prod.heading("name", text="Nombre")

        self.tv_prod.column("id", width=60, anchor="center")

        self.tv_prod.column("name", width=300)

        self.tv_prod.pack(fill="both", expand=True, pady=6)



        ttk.Label(frame, text=f"Gramajes disponibles (fijos): {', '.join(map(str, GRAMAJES))} g").pack(anchor="w", padx=4)

        ttk.Label(frame, text="Regla de empaque: 200-250 g - 20 unidades/paquete; 400-1000 g - 10 unidades/paquete.").pack(anchor="w", padx=4)



        self._refresh_productos()

                # --- Editor de Precios por paquete (IVA incl.) ---

        boxp = ttk.LabelFrame(frame, text="Precios por paquete (IVA incluido)")

        boxp.pack(fill="both", expand=True, pady=8)



        cols = ("producto","gram","precio","iva")

        self.tv_precios = ttk.Treeview(boxp, columns=cols, show="headings", height=8)

        self.tv_precios.heading("producto", text="Producto")

        self.tv_precios.heading("gram",     text="g")

        self.tv_precios.heading("precio",   text="Precio (Gs)")

        self.tv_precios.heading("iva",      text="IVA %")

        self.tv_precios.column("producto", width=240)

        self.tv_precios.column("gram",     width=70, anchor="center")

        self.tv_precios.column("precio",   width=120, anchor="center")

        self.tv_precios.column("iva",      width=80,  anchor="center")

        self.tv_precios.pack(fill="both", expand=True, padx=6, pady=6)



        self.tv_precios.bind("<Double-1>", self._editar_precio)

        self._refresh_precios()

    def _refresh_precios(self):

        if not hasattr(self, "tv_precios"): return

        for i in self.tv_precios.get_children():

            self.tv_precios.delete(i)

        for pid, name, g, price, iva in self.repo.list_all_prices():

            self.tv_precios.insert("", "end",

                values=(name, g, self._fmt_gs(price), iva),

                tags=(f"{pid}|{g}",))

        self._apply_treeview_striping(self.tv_precios)



    def _editar_precio(self, event=None):

        row_id = self.tv_precios.identify_row(event.y) if event else self.tv_precios.focus()
        if not row_id:
            return
        col_id = self.tv_precios.identify_column(event.x) if event else "#3"

        self.tv_precios.focus(row_id)
        self.tv_precios.selection_set(row_id)

        name, gram, _precio_txt, _iva_txt = self.tv_precios.item(row_id, "values")
        if col_id == "#1":
            self._show_price_history_window(str(name or ""))
            return
        # Solo permitir edicion por doble clic en Precio o IVA.
        if col_id not in ("#3", "#4"):
            return

        try:
            gram = int(gram)
        except Exception:
            return

        pid = self.repo.get_product_id_by_name(name)
        if pid is None:
            return

        price_cur, iva_cur = self.repo.get_price(pid, gram)
        price = float(price_cur or 0.0)
        iva = int(iva_cur) if iva_cur in (5, 10) else 10

        if col_id == "#3":
            sprice = simpledialog.askstring(
                "Editar precio",
                f"{name} {gram} g\nPrecio (Gs, IVA incluido):",
                parent=self,
                initialvalue=f"{price:,.0f}".replace(",", "."),
            )
            if sprice is None:
                return
            try:
                price = float(sprice.replace(".", "").replace(",", "."))
                if price < 0:
                    raise ValueError
            except Exception:
                messagebox.showerror("Error", "Precio inválido.")
                return
        else:
            siva = simpledialog.askstring(
                "IVA",
                "IVA % (5 o 10):",
                parent=self,
                initialvalue=str(iva),
            )
            if siva is None:
                return
            try:
                iva = int(siva)
                if iva not in (5, 10):
                    raise ValueError
            except Exception:
                messagebox.showerror("Error", "IVA debe ser 5 o 10.")
                return

        try:
            self.repo.upsert_price(pid, gram, price, iva)
            self._refresh_precios()
            # si estamos en ventas, refrescar allí también para que "estire" el precio
            if hasattr(self, "_refresh_ventas_grid"):
                self._refresh_ventas_grid()
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _show_price_history_window(self, product_name: str):
        pname = (product_name or "").strip()
        if not pname:
            return
        pid = self.repo.get_product_id_by_name(pname)
        if not pid:
            messagebox.showerror("Precios", "No se pudo identificar el producto.")
            return
        rows = self.repo.list_price_history(pid)
        if not rows:
            messagebox.showinfo("Precios", "No hay historial de precios para mostrar.")
            return

        win = tk.Toplevel(self)
        win.title(f"Historial de precios - {pname}")
        win.geometry("980x560")
        win.minsize(860, 460)

        root = ttk.Frame(win)
        root.pack(fill="both", expand=True, padx=10, pady=10)

        ttk.Label(root, text=f"Linea de tiempo de precios por gramaje: {pname}").pack(anchor="w")

        canvas = tk.Canvas(root, height=300, background="#f8fafc", highlightthickness=0)
        canvas.pack(fill="x", expand=False, pady=(8, 6))

        table_box = ttk.LabelFrame(root, text="Registro de cambios")
        table_box.pack(fill="both", expand=True)

        cols = ("fecha", "gramaje", "precio", "iva")
        tv = ttk.Treeview(table_box, columns=cols, show="headings", height=10)
        tv.heading("fecha", text="Fecha")
        tv.heading("gramaje", text="Gramaje (g)")
        tv.heading("precio", text="Precio (Gs)")
        tv.heading("iva", text="IVA %")
        tv.column("fecha", width=180, anchor="w")
        tv.column("gramaje", width=120, anchor="center")
        tv.column("precio", width=150, anchor="e")
        tv.column("iva", width=80, anchor="center")
        tv.pack(fill="both", expand=True, padx=6, pady=6)

        for ts, gram, price, iva in rows:
            tv.insert(
                "",
                "end",
                values=(
                    str(ts or ""),
                    int(gram),
                    self._fmt_gs(price),
                    int(iva),
                ),
            )
        self._apply_treeview_striping(tv)

        # Preparar puntos (orden cronologico del query)
        points = []
        for idx, (ts, gram, price, iva) in enumerate(rows):
            try:
                points.append({
                    "idx": idx,
                    "ts": str(ts or ""),
                    "gram": int(gram),
                    "price": float(price or 0),
                    "iva": int(iva or 0),
                })
            except Exception:
                continue
        if not points:
            return

        gramajes = sorted({p["gram"] for p in points})
        palette = ["#2563eb", "#dc2626", "#0f766e", "#d97706", "#7c3aed", "#0ea5e9"]
        gram_color = {g: palette[i % len(palette)] for i, g in enumerate(gramajes)}

        def _draw_chart(_event=None):
            canvas.delete("all")
            w = canvas.winfo_width()
            h = canvas.winfo_height()
            if w < 120 or h < 120:
                return
            left, right, top, bottom = 65, 20, 18, 50
            plot_w = max(1, w - left - right)
            plot_h = max(1, h - top - bottom)

            min_y = min(p["price"] for p in points)
            max_y = max(p["price"] for p in points)
            if abs(max_y - min_y) < 1e-6:
                pad = max(1.0, max_y * 0.05 or 1.0)
                min_y -= pad
                max_y += pad
            else:
                pad = (max_y - min_y) * 0.1
                min_y -= pad
                max_y += pad

            def y_for(val):
                ratio = (val - min_y) / (max_y - min_y) if max_y != min_y else 0.5
                ratio = max(0.0, min(1.0, ratio))
                return (h - bottom) - ratio * plot_h

            def x_for(i):
                if len(points) <= 1:
                    return left + plot_w / 2
                return left + (plot_w * (i / (len(points) - 1)))

            canvas.create_line(left, top, left, h - bottom, fill="#475569")
            canvas.create_line(left, h - bottom, w - right, h - bottom, fill="#475569")
            for i in range(5):
                frac = i / 4 if i else 0
                y = top + (plot_h * frac)
                val = max_y - ((max_y - min_y) * frac)
                canvas.create_line(left, y, w - right, y, fill="#e5e7eb")
                canvas.create_text(8, y, text=self._fmt_gs(val), anchor="w", fill="#475569", font=("TkDefaultFont", 8))

            by_gram = {}
            for p in points:
                by_gram.setdefault(p["gram"], []).append(p)

            legend_x = w - right - 6
            legend_y = top + 4
            for g in gramajes:
                color = gram_color[g]
                canvas.create_text(legend_x, legend_y, text=f"{g} g", anchor="ne", fill=color, font=("TkDefaultFont", 9, "bold"))
                legend_y += 14

            for g, plist in by_gram.items():
                color = gram_color[g]
                last_x = last_y = None
                for p in plist:
                    x = x_for(p["idx"])
                    y = y_for(p["price"])
                    if last_x is not None:
                        canvas.create_line(last_x, last_y, x, y, fill=color, width=2)
                    canvas.create_oval(x - 3, y - 3, x + 3, y + 3, fill=color, outline="")
                    last_x, last_y = x, y

            # Labels de referencia temporal (inicio/fin)
            first_ts = points[0]["ts"][:19]
            last_ts = points[-1]["ts"][:19]
            canvas.create_text(left, h - bottom + 18, text=first_ts, anchor="nw", fill="#475569", font=("TkDefaultFont", 8))
            canvas.create_text(w - right, h - bottom + 18, text=last_ts, anchor="ne", fill="#475569", font=("TkDefaultFont", 8))

        canvas.bind("<Configure>", _draw_chart)
        _draw_chart()





    def _add_producto(self):

        name = self.ent_prod.get().strip()

        if not name:

            messagebox.showwarning("Atención", "Ingrese un nombre de producto.")

            return

        try:

            self.repo.add_product(name)

            self.ent_prod.delete(0, tk.END)

            self._refresh_productos()

            self._refresh_comboboxes()

            messagebox.showinfo("OK", f"Producto '{name}' agregado.")

        except Exception as e:

            self.repo.cn.rollback()

            if isinstance(e, psycopg.errors.UniqueViolation) or "unique" in str(e).lower():

                messagebox.showerror("Error", "El producto ya existe.")

            else:

                messagebox.showerror("Error", str(e))



    def _refresh_productos(self):

        for i in self.tv_prod.get_children():

            self.tv_prod.delete(i)

        for pid, name in self.repo.list_products():

            self.tv_prod.insert("", "end", values=(pid, name))

        self._apply_treeview_striping(self.tv_prod)



    # ---------- Pestaña: Compras (Materia Prima) ----------

    def _build_tab_compras(self, nb):

        frame = ttk.Frame(nb)

        nb.add(frame, text="Compras (lote)")



        form = ttk.Frame(frame); form.pack(fill="x", pady=6)



        ttk.Label(form, text="Producto:").grid(row=0, column=0, sticky="w")

        self.cb_comp_prod = ttk.Combobox(form, state="readonly", width=30, values=[])

        self.cb_comp_prod.grid(row=0, column=1, padx=6, pady=2)



        ttk.Label(form, text="Proveedor:").grid(row=0, column=2, sticky="w")

        self.ent_lote_prov = ttk.Entry(form, width=20)

        self.ent_lote_prov.grid(row=0, column=3, padx=6)



        ttk.Label(form, text="Factura:").grid(row=0, column=4, sticky="w")

        self.ent_lote_fact = ttk.Entry(form, width=14)

        self.ent_lote_fact.grid(row=0, column=5, padx=6)



        ttk.Label(form, text="N° Lote:").grid(row=1, column=0, sticky="w")

        self.ent_lote_nro = ttk.Entry(form, width=20)

        self.ent_lote_nro.grid(row=1, column=1, padx=6, pady=2)



        ttk.Label(form, text="Kg por bolsa:").grid(row=1, column=2, sticky="w")

        self.cb_comp_bolsa = ttk.Combobox(form, state="readonly", width=8,

                                        values=[str(x) for x in BOLSAS_PREDEF]+["Otro"])

        self.cb_comp_bolsa.grid(row=1, column=3, padx=6)

        self.cb_comp_bolsa.bind("<<ComboboxSelected>>", self._toggle_otro_bolsa)



        self.ent_bolsa_otro = ttk.Entry(form, width=8, state="disabled")
        self.ent_bolsa_otro.grid(row=1, column=4, padx=6)
        ttk.Label(form, text="Bolsas:").grid(row=1, column=5, sticky="w")
        self.ent_comp_bolsas = ttk.Entry(form, width=8)
        self.ent_comp_bolsas.grid(row=1, column=6, padx=6)
        ttk.Label(form, text="Monto total (Gs):").grid(row=1, column=7, sticky="w")
        self.ent_lote_costo = ttk.Entry(form, width=12)
        self.ent_lote_costo.grid(row=1, column=8, padx=6)
        ttk.Button(form, text="Registrar lote", command=self._registrar_lote)\
            .grid(row=1, column=9, padx=8)



        # Stock MP

        box = ttk.LabelFrame(frame, text="Stock de materia prima (kg)")

        box.pack(fill="both", expand=True, pady=8)

        self.tv_raw = ttk.Treeview(box, columns=("id","name","kg"), show="headings")

        for c, t, w in [("id","ID",60), ("name","Producto",240), ("kg","Kg disp." ,120)]:

            self.tv_raw.heading(c, text=t)

            self.tv_raw.column(c, width=w, anchor="center" if c!="name" else "w")

        self.tv_raw.pack(fill="both", expand=True, padx=6, pady=6)



        # Lotes abiertos del producto

        boxLA = ttk.LabelFrame(frame, text="Lotes abiertos del producto")

        boxLA.pack(fill="both", expand=True, padx=6, pady=6)

        self.tv_lotes = ttk.Treeview(boxLA,

            columns=("prod","lote","saldo","costo"), show="headings", height=8)

        self.tv_lotes.heading("prod",  text="Producto")

        self.tv_lotes.heading("lote",  text="Lote")

        self.tv_lotes.heading("saldo", text="Saldo kg")

        self.tv_lotes.heading("costo", text="Costo/kg (Gs)")

        self.tv_lotes.column("prod", width=220)

        self.tv_lotes.column("lote", width=120)

        self.tv_lotes.column("saldo", width=100, anchor="center")

        self.tv_lotes.column("costo", width=120, anchor="center")

        self.tv_lotes.pack(fill="both", expand=True, padx=6, pady=6)

        exp = ttk.Frame(boxLA)
        exp.pack(fill="x", padx=6, pady=(0, 6))
        ttk.Button(exp, text="Exportar lotes CSV", command=self._export_lotes_abiertos_csv, style="Export.TButton").pack(side="left", padx=4)
        ttk.Button(exp, text="Exportar lotes Excel", command=self._export_lotes_abiertos_excel, style="Export.TButton").pack(side="left", padx=4)



        # refrescar lotes al cambiar producto

        self.cb_comp_prod.bind("<<ComboboxSelected>>", lambda *_: self._refresh_lotes_abiertos())



        # Inicializar combos/tabla

        self._refresh_comboboxes()

        self._refresh_raw()

        self._refresh_lotes_abiertos()



    def _toggle_otro_bolsa(self, *_):

        if self.cb_comp_bolsa.get() == "Otro":

            self.ent_bolsa_otro.configure(state="normal")

        else:

            self.ent_bolsa_otro.configure(state="disabled")



    def _registrar_compra(self):

        prod = self.cb_comp_prod.get().strip()

        if not prod:

            messagebox.showwarning("Atención", "Seleccione un producto.")

            return

        pid = self.repo.get_product_id_by_name(prod)



        sel = self.cb_comp_bolsa.get()

        if not sel:

            messagebox.showwarning("Atención", "Seleccione kg por bolsa.")

            return

        if sel == "Otro":

            try:

                bolsa_kg = float(self.ent_bolsa_otro.get().replace(",", "."))

            except:

                messagebox.showerror("Error", "Ingrese kg por bolsa válidos.")

                return

        else:

            bolsa_kg = float(sel)



        try:

            bolsas = int(self.ent_comp_bolsas.get())

        except:

            messagebox.showerror("Error", "Ingrese cantidad de bolsas válida.")

            return



        try:

            self.repo.comprar_bolsas(pid, bolsa_kg, bolsas)

            self._refresh_raw()

            messagebox.showinfo("OK", f"Compra registrada: {bolsas} bolsas x {bolsa_kg:.0f} kg de {prod}.")

            self.ent_comp_bolsas.delete(0, tk.END)

        except Exception as e:

            messagebox.showerror("Error", str(e))

    # --- Lotes: acciones de UI ---



    def _registrar_lote(self):

        prod = (self.cb_comp_prod.get() or "").strip()

        if not prod:

            messagebox.showwarning("Atención", "Seleccione un producto."); return

        pid = self.repo.get_product_id_by_name(prod)



        # kg/bolsa

        sel = self.cb_comp_bolsa.get()

        if not sel:

            messagebox.showwarning("Atención", "Seleccione kg por bolsa."); return

        if sel == "Otro":

            try:

                bag_kg = float((self.ent_bolsa_otro.get() or "0").replace(",", "."))

                if bag_kg <= 0: raise ValueError

            except:

                messagebox.showerror("Error", "Kg por bolsa inválidos."); return

        else:

            bag_kg = float(sel)



        # bolsas y costo

        try:

            bolsas = int((self.ent_comp_bolsas.get() or "0"))

            if bolsas <= 0: raise ValueError

            costo  = float((self.ent_lote_costo.get() or "0").replace(".", "").replace(",", "."))

            if costo < 0: raise ValueError

        except:

            messagebox.showerror("Error", "Bolsas o monto inválidos."); return



        # datos lote

        lote = (self.ent_lote_nro.get()  or "").strip()

        prov = (self.ent_lote_prov.get() or "").strip()

        fact = (self.ent_lote_fact.get() or "").strip()



        kg_total = bag_kg * bolsas

        try:

            self.repo.comprar_lote(pid, lote, kg_total, prov, fact, costo)

            self._refresh_raw()

            self._refresh_lotes_abiertos()

            # limpiar campos

            for w in (self.ent_lote_nro, self.ent_comp_bolsas, self.ent_lote_prov,

                    self.ent_lote_fact, self.ent_lote_costo, self.ent_bolsa_otro):

                w.delete(0, tk.END)

            self.cb_comp_bolsa.set("")

            messagebox.showinfo("OK", f"Lote registrado: {prod} lote '{lote or 's/lote'}' ({kg_total:.3f} kg).")

            if hasattr(self, "_refresh_resumenes"):

                self._refresh_resumenes()

        except Exception as e:

            messagebox.showerror("Error", str(e))





    def _refresh_lotes_abiertos(self):

        if not hasattr(self, "tv_lotes"):

            return

        for i in self.tv_lotes.get_children():

            self.tv_lotes.delete(i)



        # filtra por producto seleccionado

        prod = (self.cb_comp_prod.get() or "").strip()

        pid = self.repo.get_product_id_by_name(prod) if prod else None



        for lot_id, pid_row, name, lote, saldo, ckg in self.repo.listar_lotes_abiertos(pid):

            self.tv_lotes.insert("", "end",

                                values=(name, lote or "-", f"{saldo:.3f}", self._fmt_gs(ckg)))

        self._apply_treeview_striping(self.tv_lotes)

        if hasattr(self, "_tab_merma"):

            self._tab_merma.refresh_lotes()





    # ---------- Pestaña: Fraccionamiento ----------

    def _build_tab_fraccionamiento(self, nb):

        frame = ttk.Frame(nb)

        nb.add(frame, text="Fraccionamiento")



        form = ttk.Frame(frame); form.pack(fill="x", pady=6)

        ttk.Label(form, text="Producto:").grid(row=0, column=0, sticky="w")

        self.cb_frac_prod = ttk.Combobox(form, state="readonly", width=35, values=[])

        self.cb_frac_prod.grid(row=0, column=1, padx=6)

        self.cb_frac_prod.bind("<<ComboboxSelected>>", self._on_frac_producto_seleccionado)





        ttk.Label(form, text="Gramaje (g):").grid(row=0, column=2, sticky="w")

        self.cb_frac_gram = ttk.Combobox(form, state="readonly", width=8, values=[str(x) for x in GRAMAJES])

        self.cb_frac_gram.grid(row=0, column=3, padx=6)



        ttk.Label(form, text="Paquetes a producir:").grid(row=0, column=4, sticky="w")

        self.ent_frac_paq = ttk.Entry(form, width=10)

        self.ent_frac_paq.grid(row=0, column=5, padx=6)

        # --- dentro de _build_tab_fraccionamiento(), justo antes del botón "Fraccionar" ---



# Campo de fecha

# Campo de fecha

        ttk.Label(form, text="Fecha:").grid(row=0, column=6, sticky="w")

        self.ent_frac_fecha = ttk.Entry(form, width=12)

        self.ent_frac_fecha.grid(row=0, column=7, padx=6)



        # Valor por defecto = hoy

        hoy = _dt.date.today().strftime("%Y-%m-%d")

        self.ent_frac_fecha.insert(0, hoy)



        # Botón Fraccionar (moverlo una columna más)

        ttk.Button(form, text="Fraccionar", command=self._do_fraccionar).grid(row=0, column=8, padx=8)

                # Selección de lote (opcional)

        frm_lote = ttk.Frame(frame); frm_lote.pack(fill="x", pady=(2,0))

        ttk.Label(frm_lote, text="Lote (opcional):").grid(row=0, column=0, sticky="w")

        self.cb_frac_lote = ttk.Combobox(frm_lote, state="readonly", width=42, values=[])

        self.cb_frac_lote.grid(row=0, column=1, padx=6)

        self._lote_map = {}   # display -> lot_id





        # Indicador de consumo

        self.lbl_consumo = ttk.Label(frame, text="Consumo estimado: -", justify="left")

        self.lbl_consumo.pack(anchor="w", padx=6)

        self.ent_frac_paq.bind("<KeyRelease>", self._update_consumo_label)

        self.cb_frac_gram.bind("<<ComboboxSelected>>", self._update_consumo_label)



        # Vistas de stock

        pan = ttk.Panedwindow(frame, orient="horizontal"); pan.pack(fill="both", expand=True, pady=8)

        box1 = ttk.Labelframe(pan, text="Materia prima (kg)")

        box2 = ttk.Labelframe(pan, text="Stock de paquetes")

        pan.add(box1, weight=1); pan.add(box2, weight=1)



        self.tv_raw2 = ttk.Treeview(box1, columns=("id","name","kg"), show="headings", height=12)

        for c, t, w in [("id","ID",60), ("name","Producto",220), ("kg","Kg disp." ,120)]:

            self.tv_raw2.heading(c, text=t)

            self.tv_raw2.column(c, width=w, anchor="center" if c!="name" else "w")

        self.tv_raw2.pack(fill="both", expand=True, padx=6, pady=6)



        self.tv_pkg = ttk.Treeview(box2, columns=("id","name","gram","paq"), show="headings", height=12)

        for c, t, w in [("id","ID",60), ("name","Producto",200), ("gram","g",80), ("paq","Paquetes",120)]:

            self.tv_pkg.heading(c, text=t)

            self.tv_pkg.column(c, width=w, anchor="center" if c!="name" else "w")

        self.tv_pkg.pack(fill="both", expand=True, padx=6, pady=6)



        self._refresh_comboboxes()

        self._refresh_raw2()

        self._refresh_pkg()



    def _update_consumo_label(self, *_):

        try:

            prod = self.cb_frac_prod.get().strip()
            g = int(self.cb_frac_gram.get())

            p = int(self.ent_frac_paq.get())

            kg = kg_requeridos_para_paquetes(g, p)
            bag_kg = self._bag_eq_kg_for_product(prod)

            self.lbl_consumo.config(
                text=(
                    f"Consumo estimado: {kg:.3f} kg ( {unidades_por_paquete(g)} unid/paq )\n"
                    f"Cantidad bolsas equivalente: {format_single_bag_equivalence(kg, bag_kg)}"
                )
            )

        except:

            self.lbl_consumo.config(text="Consumo estimado: -")



    def _do_fraccionar(self):

        prod = self.cb_frac_prod.get().strip()

        if not prod:

            messagebox.showwarning("Atención", "Seleccione un producto.")

            return



        pid = self.repo.get_product_id_by_name(prod)



        try:

            gram = int(self.cb_frac_gram.get())

            paq = int(self.ent_frac_paq.get())

        except:

            messagebox.showerror("Error", "Ingrese gramaje y paquetes válidos.")

            return



        # Leer la fecha del campo (si se modificó)

        fecha_frac = (self.ent_frac_fecha.get() or "").strip()

        if not fecha_frac:

            fecha_frac = _dt.date.today().strftime("%Y-%m-%d")



        # Validar formato básico (YYYY-MM-DD)

        try:

            _ = _dt.datetime.strptime(fecha_frac, "%Y-%m-%d")

        except ValueError:

            messagebox.showerror("Error", "Formato de fecha inválido. Use AAAA-MM-DD.")

            return



        lot_display = (self.cb_frac_lote.get() or "").strip()



        try:

            # --- Si se seleccionó un lote ---

            if lot_display:

                lot_id = self._lote_map.get(lot_display)

                if not lot_id:

                    messagebox.showwarning("Atención", "Seleccione un lote válido.")

                    return

                self.repo.fraccionar_desde_lote(pid, lot_id, gram, paq, fecha_frac)

            else:

                # --- Sin lote, fraccionamiento general ---

                self.repo.fraccionar(pid, gram, paq, fecha_frac)



            # Refrescar vistas de stock y lotes

            self._refresh_raw()

            self._refresh_raw2()

            self._refresh_pkg()

            self._refresh_lotes_abiertos()

            self._on_frac_producto_seleccionado()



            # Confirmación

            messagebox.showinfo(

                "OK",

                f"Fraccionado el {fecha_frac}: {paq} paquetes de {gram} g en {prod}."

            )



            # Limpiar campo de paquetes y actualizar etiqueta

            self.ent_frac_paq.delete(0, tk.END)

            self._update_consumo_label()



        except Exception as e:

            messagebox.showerror("Error", str(e))



    # ---------- Pestaña: Ventas ----------

    def _build_tab_ventas(self, nb):

        frame = ttk.Frame(nb)

        nb.add(frame, text="Ventas / Facturación")



        # Encabezado

        top = ttk.Frame(frame); top.pack(fill="x", pady=4)

        ttk.Label(top, text="N° Factura:").pack(side="left")

        self.ent_fac_no = ttk.Entry(top, width=18); self.ent_fac_no.pack(side="left", padx=6)

        ttk.Label(top, text="Cliente:").pack(side="left")

        self.ent_fac_cli = ttk.Entry(top, width=28); self.ent_fac_cli.pack(side="left", padx=6)

        ttk.Label(top, text="Fecha (AAAA-MM-DD):").pack(side="left", padx=(10,0))

        self.ent_fac_fecha = ttk.Entry(top, width=12)

        self.ent_fac_fecha.pack(side="left", padx=6)



        # Valor por defecto = hoy

        hoy = _dt.date.today().strftime("%Y-%m-%d")

        self.ent_fac_fecha.insert(0, hoy)
        ttk.Label(frame, text="Ingrese SOLO las cantidades a facturar; precios se toman automaticamente.")\
            .pack(anchor="w", padx=6, pady=2)



        # Grilla

        cols = ("name","gram","stock","precio","iva","vender","importe")

        self.tv_ventas = ttk.Treeview(frame, columns=cols, show="headings", height=16)

        self.tv_ventas.heading("name",   text="Producto")

        self.tv_ventas.heading("gram",   text="g")

        self.tv_ventas.heading("stock",  text="Paquetes disp.")

        self.tv_ventas.heading("precio", text="Precio (Gs)")

        self.tv_ventas.heading("iva",    text="IVA %")

        self.tv_ventas.heading("vender", text="Vender (paq)")

        self.tv_ventas.heading("importe",text="Importe (Gs)")

        self.tv_ventas.column("name",   width=240)

        self.tv_ventas.column("gram",   width=70,  anchor="center")

        self.tv_ventas.column("stock",  width=110, anchor="center")

        self.tv_ventas.column("precio", width=110, anchor="center")

        self.tv_ventas.column("iva",    width=70,  anchor="center")

        self.tv_ventas.column("vender", width=110, anchor="center")

        self.tv_ventas.column("importe",width=120, anchor="center")

        self.tv_ventas.bind("<Configure>", self._relayout_ventas)

        self.tv_ventas.bind("<ButtonRelease-1>", self._relayout_ventas)

        self.tv_ventas.bind("<Motion>", self._relayout_ventas)

        self.tv_ventas.bind("<MouseWheel>", self._relayout_ventas)

        self.tv_ventas.pack(fill="both", expand=True, padx=6, pady=6)



        # ð??´ Mover los TOTALES antes del refresh

        tot = ttk.Frame(frame); tot.pack(fill="x", padx=6, pady=(0,6))

        self.lbl_tot = ttk.Label(

            tot,

            text="Gravada 5%: 0 | IVA 5%: 0 | Gravada 10%: 0 | IVA 10%: 0 | TOTAL: 0",

            font=("TkDefaultFont", 10, "bold")

        )

        self.lbl_tot.pack(side="left")



        # Botones

        btns = ttk.Frame(frame); btns.pack(fill="x", pady=2)
        ttk.Button(btns, text="Emitir factura", command=self._emitir_factura)\
            .pack(side="left", padx=6)
        ttk.Button(btns, text="Refrescar", command=self._refresh_ventas_grid)\
            .pack(side="left")



        # Entradas y mapas + primer llenado

        self._ventas_entries = {}

        self._ventas_iidkey  = {}

        self._ventas_keyiid  = {}

        self._refresh_ventas_grid()   # ahora sí, lbl_tot ya existe



    def _refresh_ventas_grid(self):

        # Limpiar filas

        for i in self.tv_ventas.get_children():

            self.tv_ventas.delete(i)

        # Destruir entradas anteriores

        if getattr(self, "_ventas_entries", None):

            for ent in self._ventas_entries.values():

                try: ent.destroy()

                except: pass

        self._ventas_entries = {}

        self._ventas_iidkey  = {}

        self._ventas_keyiid  = {}



        # Traer combinaciones permitidas

        data = self.repo.listar_todos_pkg_stock()  # [(pid, name, gram, paq_stock)]

        for pid, name, gram, stock_paq in data:

            price, iva = self.repo.get_price(pid, gram)

            price_txt = self._fmt_gs(price) if price is not None else "-"

            iva_txt   = str(iva) if iva in (5,10) else "-"

            iid = self.tv_ventas.insert("", "end",

                values=(name, gram, stock_paq, price_txt, iva_txt, "", "0"))

            self._ventas_iidkey[iid] = (pid, gram)

            self._ventas_keyiid[(pid,gram)] = iid



            ent = ttk.Entry(self.tv_ventas, width=8)

            ent.bind("<KeyRelease>", lambda _e, key=(pid,gram): self._update_totales_venta())

            self._ventas_entries[(pid, gram)] = ent



        self.after_idle(self._relayout_ventas)

        self._update_totales_venta()

        self._apply_treeview_striping(self.tv_ventas)

    def _emitir_factura(self):

        items = []

        for (pid, gram), ent in self._ventas_entries.items():

            s = (ent.get() or "").strip()

            if not s:

                continue

            try:

                qty = int(s)

                if qty <= 0:

                    continue

            except:

                continue

            items.append((pid, gram, qty))



        if not items:

            messagebox.showinfo("Info", "No hay cantidades cargadas para facturar.")

            return



        invoice_no = (self.ent_fac_no.get() or "").strip()

        customer   = (self.ent_fac_cli.get() or "").strip()

        # ordenar ítems como en la factura física

        _name_cache = {}

        def pname(pid):

            if pid not in _name_cache:

                _name_cache[pid] = self.repo.get_product_name(pid)

            return _name_cache[pid]



        items.sort(key=lambda t: (product_order_idx(pname(t[0])),

                                gram_order_idx(pname(t[0]), t[1])))



        # Leer la fecha ingresada

        fecha_txt = (self.ent_fac_fecha.get() or "").strip()

        if not fecha_txt:

            fecha_txt = _dt.date.today().strftime("%Y-%m-%d")

        try:

            _dt.datetime.strptime(fecha_txt, "%Y-%m-%d")

        except ValueError:

            messagebox.showerror("Error", "Formato de fecha inválido. Use AAAA-MM-DD.")

            return



        try:

            inv_id, res = self.repo.crear_factura(invoice_no, customer, items, fecha_txt)

            # Refrescos

            self._refresh_pkg()

            self._refresh_ventas_grid()
            if hasattr(self, "_refresh_credit_invoice_suggestions"):
                self._refresh_credit_invoice_suggestions()

            if hasattr(self, "_refresh_inventarios"):

                self._refresh_inventarios()

            if hasattr(self, "_refresh_resumenes"):

                self._refresh_resumenes()



            mensaje = (f"Factura emitida (ID {inv_id}).\n\n"

                       f"Gravada 5%: {self._fmt_gs(res['gravada5'])}  |  IVA 5%: {self._fmt_gs(res['iva5'])}\n"

                       f"Gravada 10%: {self._fmt_gs(res['gravada10'])}  |  IVA 10%: {self._fmt_gs(res['iva10'])}\n"

                       f"TOTAL: {self._fmt_gs(res['total'])}")

            messagebox.showinfo("OK", mensaje)

            tab_hist = getattr(self, "_tab_hist_ventas", None)
            if tab_hist and hasattr(tab_hist, "preview_send_sheet_for_invoice"):
                tab_hist.preview_send_sheet_for_invoice(inv_id)



            # limpiar encabezado de factura

            self.ent_fac_no.delete(0, tk.END)

            self.ent_fac_cli.delete(0, tk.END)

        except Exception as e:

            messagebox.showerror("Error", str(e))

    def _relayout_ventas(self, *_):

        # Si aún no armamos el mapeo (se crea en _refresh_ventas_grid), salir

        if not hasattr(self, "_ventas_iidkey"):

            return



        # Reubicar/ocultar cada Entry según el bbox de la columna 'vender'

        for iid, key in self._ventas_iidkey.items():

            ent = self._ventas_entries.get(key)

            if not ent:

                continue

            try:

                # Usar el NOMBRE de la columna (más robusto que el índice)

                bbox = self.tv_ventas.bbox(iid, column='vender')

            except Exception:

                bbox = None



            if bbox:

                x, y, w, h = bbox

                # Margen pequeño para que se vea prolijo dentro de la celda

                ent.place(x=x+2, y=y+2, width=w-4, height=h-4)

            else:

                # Fila fuera de vista (por scroll): ocultar

                ent.place_forget()


    def _build_tab_notas_credito(self, nb):

        frame = ttk.Frame(nb)
        nb.add(frame, text="Notas de crédito")

        top = ttk.Frame(frame); top.pack(fill="x", pady=4)

        ttk.Label(top, text="N° Nota crédito:").pack(side="left")
        self.ent_nc_no = ttk.Entry(top, width=18); self.ent_nc_no.pack(side="left", padx=6)

        ttk.Label(top, text="Factura origen:").pack(side="left")
        self.ent_nc_fact_ref = ttk.Combobox(top, width=42)
        self.ent_nc_fact_ref.pack(side="left", padx=6)
        self.ent_nc_fact_ref.bind("<<ComboboxSelected>>", lambda _e: self._load_credit_invoice())
        self.ent_nc_fact_ref.bind("<Return>", lambda _e: self._load_credit_invoice())
        ttk.Button(top, text="Cargar factura", command=self._load_credit_invoice).pack(side="left", padx=4)

        ttk.Label(top, text="Cliente:").pack(side="left", padx=(10, 0))
        self.ent_nc_cli = ttk.Entry(top, width=28); self.ent_nc_cli.pack(side="left", padx=6)

        ttk.Label(top, text="Fecha (AAAA-MM-DD):").pack(side="left", padx=(10, 0))
        self.ent_nc_fecha = ttk.Entry(top, width=12); self.ent_nc_fecha.pack(side="left", padx=6)
        self.ent_nc_fecha.insert(0, _dt.date.today().strftime("%Y-%m-%d"))

        row2 = ttk.Frame(frame); row2.pack(fill="x", pady=(0, 4))
        ttk.Label(row2, text="Motivo:").pack(side="left")
        self.ent_nc_motivo = ttk.Entry(row2, width=70); self.ent_nc_motivo.pack(side="left", padx=6)
        self.var_nc_reingresa = tk.BooleanVar(value=True)
        ttk.Checkbutton(row2, text="Reingresar stock", variable=self.var_nc_reingresa).pack(side="left", padx=10)
        self.lbl_nc_ref = ttk.Label(row2, text="Factura cargada: -")
        self.lbl_nc_ref.pack(side="left", padx=10)

        ttk.Label(frame, text="Cargá la factura origen y escribí SOLO las cantidades a acreditar.").pack(anchor="w", padx=6, pady=2)

        cols = ("name","gram","fact","cred","disp","precio","iva","acreditar","importe")
        self.tv_nc = ttk.Treeview(frame, columns=cols, show="headings", height=16)
        self.tv_nc.heading("name", text="Producto")
        self.tv_nc.heading("gram", text="g")
        self.tv_nc.heading("fact", text="Facturado")
        self.tv_nc.heading("cred", text="Acreditado")
        self.tv_nc.heading("disp", text="Disponible")
        self.tv_nc.heading("precio", text="Precio (Gs)")
        self.tv_nc.heading("iva", text="IVA %")
        self.tv_nc.heading("acreditar", text="Acreditar (paq)")
        self.tv_nc.heading("importe", text="Importe (Gs)")

        self.tv_nc.column("name", width=220)
        self.tv_nc.column("gram", width=70, anchor="center")
        self.tv_nc.column("fact", width=85, anchor="center")
        self.tv_nc.column("cred", width=85, anchor="center")
        self.tv_nc.column("disp", width=90, anchor="center")
        self.tv_nc.column("precio", width=110, anchor="center")
        self.tv_nc.column("iva", width=70, anchor="center")
        self.tv_nc.column("acreditar", width=115, anchor="center")
        self.tv_nc.column("importe", width=120, anchor="center")

        for ev in ("<Configure>", "<ButtonRelease-1>", "<Motion>", "<MouseWheel>"):
            self.tv_nc.bind(ev, self._relayout_credito)

        self.tv_nc.pack(fill="both", expand=True, padx=6, pady=6)

        tot = ttk.Frame(frame); tot.pack(fill="x", padx=6, pady=(0,6))
        self.lbl_nc_tot = ttk.Label(
            tot,
            text="Gravada 5%: 0 | IVA 5%: 0 | Gravada 10%: 0 | IVA 10%: 0 | TOTAL: 0",
            font=("TkDefaultFont", 10, "bold")
        )
        self.lbl_nc_tot.pack(side="left")

        btns = ttk.Frame(frame); btns.pack(fill="x", pady=2)
        ttk.Button(btns, text="Emitir nota de crédito", command=self._emitir_nota_credito).pack(side="left", padx=6)
        ttk.Button(btns, text="Refrescar factura", command=self._refresh_credito_grid).pack(side="left")
        ttk.Button(btns, text="Limpiar", command=self._clear_credit_note_form).pack(side="left", padx=6)

        self._credito_invoice_id = None
        self._credito_entries = {}
        self._credito_iidkey = {}
        self._credito_keyiid = {}
        self._refresh_credit_invoice_suggestions()


    def _refresh_credit_invoice_suggestions(self):

        if hasattr(self, "ent_nc_fact_ref"):
            try:
                self.ent_nc_fact_ref["values"] = self.repo.list_invoice_lookup_values()
            except Exception:
                self.ent_nc_fact_ref["values"] = []


    def _clear_credit_note_form(self):

        self._credito_invoice_id = None
        self.lbl_nc_ref.config(text="Factura cargada: -")
        self.ent_nc_fact_ref.delete(0, tk.END)
        self.ent_nc_cli.delete(0, tk.END)
        self.ent_nc_motivo.delete(0, tk.END)

        for i in self.tv_nc.get_children():
            self.tv_nc.delete(i)
        for ent in getattr(self, "_credito_entries", {}).values():
            try:
                ent.destroy()
            except Exception:
                pass
        self._credito_entries = {}
        self._credito_iidkey = {}
        self._credito_keyiid = {}
        self._update_totales_credito()


    def _load_credit_invoice(self):

        lookup = (self.ent_nc_fact_ref.get() or "").strip()
        try:
            row = self.repo.find_invoice_for_credit(lookup)
        except Exception as e:
            messagebox.showerror("Error", str(e))
            return

        self._credito_invoice_id = int(row[0])
        invoice_no = row[2] or ""
        customer = row[3] or ""
        self.ent_nc_cli.delete(0, tk.END)
        self.ent_nc_cli.insert(0, customer)
        if not lookup:
            self.ent_nc_fact_ref.insert(0, invoice_no or str(self._credito_invoice_id))
        self.lbl_nc_ref.config(
            text=f"Factura cargada: ID {self._credito_invoice_id} | N° {invoice_no or '-'}"
        )
        self._refresh_credito_grid()


    def _refresh_credito_grid(self):

        for i in self.tv_nc.get_children():
            self.tv_nc.delete(i)

        if getattr(self, "_credito_entries", None):
            for ent in self._credito_entries.values():
                try:
                    ent.destroy()
                except Exception:
                    pass
        self._credito_entries = {}
        self._credito_iidkey = {}
        self._credito_keyiid = {}

        if not self._credito_invoice_id:
            self._update_totales_credito()
            return

        data = self.repo.listar_items_factura_para_credito(self._credito_invoice_id)

        for item_id, pid, name, gram, fact_qty, price_gs, iva, _line_total, cred_qty in data:
            disp_qty = max(0, int(fact_qty or 0) - int(cred_qty or 0))
            iid = self.tv_nc.insert(
                "",
                "end",
                values=(
                    name,
                    int(gram),
                    int(fact_qty or 0),
                    int(cred_qty or 0),
                    disp_qty,
                    self._fmt_gs(price_gs),
                    str(iva),
                    "",
                    "0",
                ),
            )
            self._credito_iidkey[iid] = (int(item_id), int(pid), int(gram), float(price_gs or 0), int(iva), disp_qty)
            self._credito_keyiid[int(item_id)] = iid

            ent = ttk.Entry(self.tv_nc, width=8)
            ent.bind("<KeyRelease>", lambda _e, item_key=int(item_id): self._update_totales_credito())
            self._credito_entries[int(item_id)] = ent

        self.after_idle(self._relayout_credito)
        self._update_totales_credito()
        self._apply_treeview_striping(self.tv_nc)


    def _update_totales_credito(self):

        grav5 = iva5 = grav10 = iva10 = total = 0.0

        for item_id, ent in getattr(self, "_credito_entries", {}).items():
            iid = self._credito_keyiid.get(item_id)
            if not iid:
                continue
            meta = self._credito_iidkey.get(iid)
            if not meta:
                continue
            _item_id, _pid, _gram, price_gs, iva, disp_qty = meta
            s = (ent.get() or "").strip()
            try:
                qty = int(s) if s else 0
            except Exception:
                qty = 0
            if qty < 0:
                qty = 0
            if qty > disp_qty:
                qty = disp_qty
            line_total = float(price_gs or 0) * qty
            self.tv_nc.set(iid, "importe", self._fmt_gs(line_total))
            base = line_total / (1.0 + int(iva) / 100.0) if int(iva) in (5, 10) else line_total
            iva_monto = line_total - base

            total += line_total
            if int(iva) == 5:
                grav5 += base
                iva5 += iva_monto
            else:
                grav10 += base
                iva10 += iva_monto

        self.lbl_nc_tot.config(
            text=f"Gravada 5%: {self._fmt_gs(grav5)} | IVA 5%: {self._fmt_gs(iva5)} | "
                 f"Gravada 10%: {self._fmt_gs(grav10)} | IVA 10%: {self._fmt_gs(iva10)} | "
                 f"TOTAL: {self._fmt_gs(total)}"
        )


    def _emitir_nota_credito(self):

        if not self._credito_invoice_id:
            messagebox.showinfo("Info", "Primero cargue una factura origen.")
            return

        items = []
        for item_id, ent in self._credito_entries.items():
            s = (ent.get() or "").strip()
            if not s:
                continue
            try:
                qty = int(s)
            except Exception:
                continue
            if qty <= 0:
                continue
            iid = self._credito_keyiid.get(item_id)
            meta = self._credito_iidkey.get(iid)
            if not meta:
                continue
            invoice_item_id, pid, gram, price_gs, iva, disp_qty = meta
            if qty > disp_qty:
                messagebox.showerror("Error", f"La cantidad excede lo disponible para el ítem {item_id}.")
                return
            items.append((invoice_item_id, pid, gram, qty, price_gs, iva))

        if not items:
            messagebox.showinfo("Info", "No hay cantidades cargadas para acreditar.")
            return

        fecha_txt = (self.ent_nc_fecha.get() or "").strip() or _dt.date.today().strftime("%Y-%m-%d")
        try:
            _dt.datetime.strptime(fecha_txt, "%Y-%m-%d")
        except ValueError:
            messagebox.showerror("Error", "Formato de fecha inválido. Use AAAA-MM-DD.")
            return

        credit_no = (self.ent_nc_no.get() or "").strip()
        customer = (self.ent_nc_cli.get() or "").strip()
        motivo = (self.ent_nc_motivo.get() or "").strip()
        reingresa_stock = bool(self.var_nc_reingresa.get())

        try:
            nc_id, res = self.repo.create_credit_note(
                credit_no, self._credito_invoice_id, customer, motivo, items, fecha_txt, reingresa_stock
            )

            self._refresh_pkg()
            self._refresh_ventas_grid()
            if hasattr(self, "_refresh_inventarios"):
                self._refresh_inventarios()
            if hasattr(self, "_refresh_resumenes"):
                self._refresh_resumenes()

            mensaje = (
                f"Nota de crédito emitida (ID {nc_id}).\n\n"
                f"Gravada 5%: {self._fmt_gs(res['gravada5'])}  |  IVA 5%: {self._fmt_gs(res['iva5'])}\n"
                f"Gravada 10%: {self._fmt_gs(res['gravada10'])}  |  IVA 10%: {self._fmt_gs(res['iva10'])}\n"
                f"TOTAL: {self._fmt_gs(res['total'])}"
            )
            messagebox.showinfo("OK", mensaje)

            self.ent_nc_no.delete(0, tk.END)
            self.ent_nc_motivo.delete(0, tk.END)
            self._refresh_credito_grid()

        except Exception as e:

            messagebox.showerror("Error", str(e))


    def _relayout_credito(self, *_):

        if not hasattr(self, "_credito_iidkey"):
            return

        for iid, meta in self._credito_iidkey.items():
            item_id = meta[0]
            ent = self._credito_entries.get(item_id)
            if not ent:
                continue
            try:
                bbox = self.tv_nc.bbox(iid, column="acreditar")
            except Exception:
                bbox = None

            if bbox:
                x, y, w, h = bbox
                ent.place(x=x+2, y=y+2, width=w-4, height=h-4)
            else:
                ent.place_forget()



    # ---------- Pestaña: Inventario ----------

    def _build_tab_inventario(self, nb):

        frame = ttk.Frame(nb)

        nb.add(frame, text="Inventario / Ajustes")



        # ---- Dos paneles: MP en kg (izq) y paquetes con entrada (der) ----

        pan = ttk.Panedwindow(frame, orient="horizontal")

        pan.pack(fill="both", expand=True, pady=8)



        box1 = ttk.Labelframe(pan, text="Materia prima (kg) - doble clic para ajustar")

        box2 = ttk.Labelframe(pan, text="Paquetes - Inventario (escribir y confirmar)")

        pan.add(box1, weight=1)

        pan.add(box2, weight=1)



        # Tabla de MP (kg) con ajuste por doble clic

        self.tv_raw_adj = ttk.Treeview(box1, columns=("id","name","kg","bags","valor"), show="headings", height=14)

        self.tv_raw_adj.heading("id",    text="ID")

        self.tv_raw_adj.heading("name",  text="Producto")

        self.tv_raw_adj.heading("kg",    text="Kg")

        self.tv_raw_adj.heading("bags",  text="Bolsas eq. (kg/bolsa)")
        self.tv_raw_adj.heading("valor", text="Valor bolsas (Gs)")

        self.tv_raw_adj.column("id",    width=60,  anchor="center")

        self.tv_raw_adj.column("name",  width=220, anchor="w")

        self.tv_raw_adj.column("kg",    width=100, anchor="center")

        self.tv_raw_adj.column("bags",  width=160, anchor="center")
        self.tv_raw_adj.column("valor", width=140, anchor="e")

        self.tv_raw_adj.pack(fill="both", expand=True, padx=6, pady=6)

        self.tv_raw_adj.bind("<Double-1>", self._ajustar_raw)
        self.tv_raw_adj.tag_configure("mp_baja", foreground="#b00020")
        self.lbl_raw_total = ttk.Label(box1, text="Total valor inventario: 0 Gs")
        self.lbl_raw_total.pack(anchor="e", padx=8, pady=(0, 4))





        # Grilla estilo "Ventas rápidas" para AJUSTAR stock de paquetes

        cols = ("name", "gram", "stock", "valor", "ajustar")

        self.tv_pkg_inv = ttk.Treeview(box2, columns=cols, show="headings", height=14)

        self.tv_pkg_inv.heading("name",   text="Producto")

        self.tv_pkg_inv.heading("gram",   text="g")

        self.tv_pkg_inv.heading("stock",  text="Paquetes actuales")

        self.tv_pkg_inv.heading("valor",  text="Valor (Gs)")

        self.tv_pkg_inv.heading("ajustar",text="Ajustar a (paq)")

        self.tv_pkg_inv.column("name",   width=260)

        self.tv_pkg_inv.column("gram",   width=80,  anchor="center")

        self.tv_pkg_inv.column("stock",  width=140, anchor="center")

        self.tv_pkg_inv.column("valor",  width=160, anchor="e")

        self.tv_pkg_inv.column("ajustar",width=140, anchor="center")

        self.tv_pkg_inv.pack(fill="both", expand=True, padx=6, pady=6)

        self.tv_pkg_inv.bind("<Double-1>", self._ajustar_pkg)



        # Entradas flotantes en la columna "ajustar"

        self._inv_entries = {}  # (pid, gram) -> Entry

        self._inv_iidkey  = {}  # iid -> (pid, gram)

        self.tv_pkg_inv.bind("<Configure>",        self._relayout_pkg_inventario)

        self.tv_pkg_inv.bind("<ButtonRelease-1>",  self._relayout_pkg_inventario)

        self.tv_pkg_inv.bind("<Motion>",           self._relayout_pkg_inventario)

        self.tv_pkg_inv.bind("<MouseWheel>",       self._relayout_pkg_inventario)



        self.lbl_pkg_total = ttk.Label(box2, text="Total valor inventario: 0 Gs")

        self.lbl_pkg_total.pack(anchor="e", padx=8, pady=(0, 4))



        # Botones de acción

        btns = ttk.Frame(frame)

        btns.pack(fill="x", pady=4)

        ttk.Button(btns, text="Confirmar inventario", command=self._confirmar_inventario_paquetes).pack(side="left", padx=6)

        ttk.Button(btns, text="Aviso MP baja",       command=self._abrir_alertas_mp).pack(side="left", padx=6)

        ttk.Button(btns, text="Refrescar",           command=self._refresh_inventarios).pack(side="left")

        exp = ttk.Frame(frame)
        exp.pack(fill="x", pady=(0, 6))
        ttk.Button(exp, text="Exportar MP CSV", command=self._export_stock_raw_csv, style="Export.TButton").pack(side="left", padx=6)
        ttk.Button(exp, text="Exportar MP Excel", command=self._export_stock_raw_excel, style="Export.TButton").pack(side="left", padx=6)
        ttk.Button(exp, text="Exportar Paquetes CSV", command=self._export_stock_pkg_csv, style="Export.TButton").pack(side="left", padx=6)
        ttk.Button(exp, text="Exportar Paquetes Excel", command=self._export_stock_pkg_excel, style="Export.TButton").pack(side="left", padx=6)


        # Cargar datos iniciales en ambas tablas

        self._refresh_inventarios()

    def _abrir_alertas_mp(self):
        dlg = tk.Toplevel(self)
        dlg.title("Aviso de materia prima baja")
        dlg.transient(self)
        dlg.grab_set()
        dlg.resizable(False, False)

        ttk.Label(dlg, text="Define el minimo de bolsas para avisar stock bajo (color rojo).").grid(
            row=0, column=0, columnspan=3, sticky="w", padx=8, pady=(8, 4)
        )

        entries = {}
        alertas = {}
        try:
            alertas = self.repo.get_raw_alerts_map()
        except Exception:
            alertas = {}

        for idx, (pid, name, kg) in enumerate(self.repo.listar_raw_stock(), start=1):
            bagkg = bag_kg_por_defecto(name)
            bolsas_eq = (kg / bagkg) if bagkg > 0 else 0.0
            ttk.Label(dlg, text=name).grid(row=idx, column=0, sticky="w", padx=8, pady=2)
            ttk.Label(dlg, text=f"{kg:.2f} kg  (~{bolsas_eq:.2f} bolsas)").grid(
                row=idx, column=1, sticky="w", padx=6, pady=2
            )
            ent = ttk.Entry(dlg, width=8)
            val = alertas.get(pid, 0.0)
            if val:
                ent.insert(0, f"{val:.2f}".rstrip("0").rstrip("."))
            ent.grid(row=idx, column=2, sticky="e", padx=8, pady=2)
            entries[pid] = ent

        def guardar():
            for pid, ent in entries.items():
                txt = (ent.get() or "").strip()
                if not txt:
                    self.repo.set_raw_alert(pid, 0)
                    continue
                try:
                    val = float(txt.replace(",", "."))
                except Exception:
                    continue
                self.repo.set_raw_alert(pid, val)
            dlg.destroy()
            self._refresh_inventarios()

        btns = ttk.Frame(dlg)
        btns.grid(row=len(entries)+1, column=0, columnspan=3, pady=(10, 10))
        ttk.Button(btns, text="Guardar", command=guardar).pack(side="left", padx=6)
        ttk.Button(btns, text="Cancelar", command=dlg.destroy).pack(side="left", padx=6)


    def _refresh_comboboxes(self):

        prods = [name for _, name in self.repo.list_products()]

        for name in ("cb_comp_prod", "cb_frac_prod", "cb_lote_prod", "cb_hist_prod"):

            cb = getattr(self, name, None)

            if cb is not None:

                cb["values"] = prods

        tab_bolsas = getattr(self, "_tab_venta_bolsas", None)

        if tab_bolsas and hasattr(tab_bolsas, "refresh_products"):

            try:

                tab_bolsas.refresh_products()

            except Exception:

                pass


    def _bag_eq_kg_for_product(self, product_name: str, product_id: int | None = None) -> float:

        pid = product_id

        if pid is None and product_name:

            pid = self.repo.get_product_id_by_name(product_name)

        if pid is not None:

            bag_kg = float(self._bag_eq_display_map.get(int(pid), 0.0) or 0.0)

            if bag_kg > 0:

                return bag_kg

        return bag_kg_por_defecto(product_name)





    def _refresh_raw(self):

        for i in self.tv_raw.get_children():

            self.tv_raw.delete(i)

        for pid, name, kg in self.repo.listar_raw_stock():

            self.tv_raw.insert("", "end", values=(pid, name, f"{kg:.3f}"))

        self._apply_treeview_striping(self.tv_raw)



    def _refresh_raw2(self):

        for i in self.tv_raw2.get_children():

            self.tv_raw2.delete(i)

        for pid, name, kg in self.repo.listar_raw_stock():

            self.tv_raw2.insert("", "end", values=(pid, name, f"{kg:.3f}"))

        self._apply_treeview_striping(self.tv_raw2)



    def _refresh_pkg(self):

        # limpiar

        if hasattr(self, "tv_pkg") and self.tv_pkg:

            for i in self.tv_pkg.get_children():

                self.tv_pkg.delete(i)

        if hasattr(self, "tv_pkg_adj") and self.tv_pkg_adj:

            for i in self.tv_pkg_adj.get_children():

                self.tv_pkg_adj.delete(i)



        # Fraccionamiento (stock existente)

        if hasattr(self, "tv_pkg") and self.tv_pkg:

            for pid, name, gram, paq in self.repo.listar_package_stock():

                self.tv_pkg.insert("", "end", values=(pid, name, gram, paq))

            self._apply_treeview_striping(self.tv_pkg)



        # Inventario (todas las combinaciones)

        if hasattr(self, "tv_pkg_adj") and self.tv_pkg_adj:

            for pid, name, gram, paq in self.repo.listar_todos_pkg_stock():

                self.tv_pkg_adj.insert("", "end", values=(pid, name, gram, paq))

            self._apply_treeview_striping(self.tv_pkg_adj)





    def _refresh_inventarios(self):

        # --- MP (kg) ---

        if hasattr(self, "tv_raw_adj"):

            for i in self.tv_raw_adj.get_children():

                self.tv_raw_adj.delete(i)

            cols = tuple(self.tv_raw_adj["columns"])

            show_bags = "bags" in cols
            show_val = "valor" in cols

            try:
                alertas = self.repo.get_raw_alerts_map()
            except Exception:
                alertas = {}
            self._alertas_mp = alertas

            total_raw_val = 0.0
            for pid, name, kg in self.repo.listar_raw_stock():

                tags = []

                bagkg = bag_kg_por_defecto(name)  # Arroz 50, Galleta molida 25, resto 50
                bolsas_eq = (kg / bagkg) if bagkg > 0 else 0.0

                if alertas.get(pid, 0) > 0 and bolsas_eq <= alertas.get(pid, 0):

                    tags.append("mp_baja")

                values = [pid, name, f"{kg:.3f}"]

                if show_bags:

                    values.append(f"{bolsas_eq:.3f}")

                if show_val:

                    costo_kg = self.repo.get_last_cost_kg(pid)
                    valor_gs = None if costo_kg is None else (kg * costo_kg)
                    if valor_gs is not None:
                        total_raw_val += float(valor_gs)
                    values.append(self._fmt_gs(valor_gs) if valor_gs is not None else "-")

                self.tv_raw_adj.insert("", "end",
                                    values=tuple(values), tags=tuple(tags))

            self._apply_treeview_striping(self.tv_raw_adj)
            if hasattr(self, "lbl_raw_total"):
                self.lbl_raw_total.config(text=f"Total valor inventario: {self._fmt_gs(total_raw_val)} Gs")



        # --- Paquetes (grilla editable estilo ventas rápidas) ---

        if hasattr(self, "tv_pkg_inv"):

            self._refresh_inventario_paquetes_grid()



        

    def _refresh_inventario_bolsas_grid(self):

        if not hasattr(self, "tv_bolsas"):

            return

        # limpiar filas

        for i in self.tv_bolsas.get_children():

            self.tv_bolsas.delete(i)

        # destruir entradas anteriores

        if hasattr(self, "_inv_bolsas_entries") and self._inv_bolsas_entries:

            for d in self._inv_bolsas_entries.values():

                for ent in d.values():

                    try: ent.destroy()

                    except: pass

        self._inv_bolsas_entries = {}   # key=pid -> {"bagkg": Entry, "bolsas": Entry}

        self._inv_bolsas_iid = {}       # iid -> pid



        # poblar filas

        for pid, name, kg in self.repo.listar_raw_stock():

            bagkg_def = bag_kg_por_defecto(name)

            iid = self.tv_bolsas.insert("", "end",

                                        values=(pid, name, f"{kg:.3f}", f"{bagkg_def:.0f}", "", "-"))



            # Entradas

            ent_bagkg = ttk.Entry(self.tv_bolsas, width=10)

            ent_bagkg.insert(0, f"{bagkg_def:.0f}")  # editable



            ent_bolsas = ttk.Entry(self.tv_bolsas, width=10)



            def recalc(_e=None, iid=iid, ent_bagkg=ent_bagkg, ent_bolsas=ent_bolsas):

                try:

                    bag = float((ent_bagkg.get() or "").replace(",", "."))

                    n   = float((ent_bolsas.get() or "").replace(",", "."))

                    if bag <= 0 or n < 0:

                        raise ValueError

                    self.tv_bolsas.set(iid, "kg_new", f"{n * bag:.3f}")

                except Exception:

                    self.tv_bolsas.set(iid, "kg_new", "-")



            ent_bagkg.bind("<KeyRelease>", recalc)

            ent_bolsas.bind("<KeyRelease>", recalc)



            self._inv_bolsas_entries[pid] = {"bagkg": ent_bagkg, "bolsas": ent_bolsas}

            self._inv_bolsas_iid[iid] = pid



        self._apply_treeview_striping(self.tv_bolsas)

        self.after_idle(self._relayout_inv_bolsas)



    def _relayout_inv_bolsas(self, *_):

        if not hasattr(self, "_inv_bolsas_iid"):

            return

        for iid, pid in self._inv_bolsas_iid.items():

            ents = self._inv_bolsas_entries.get(pid, {})

            e_bag = ents.get("bagkg")

            e_cnt = ents.get("bolsas")

            try:

                bbox_bag = self.tv_bolsas.bbox(iid, column="bagkg")

                bbox_cnt = self.tv_bolsas.bbox(iid, column="bolsas")

            except Exception:

                bbox_bag = bbox_cnt = None

            if e_bag:

                if bbox_bag:

                    x, y, w, h = bbox_bag; e_bag.place(x=x+2, y=y+2, width=w-4, height=h-4)

                else:

                    e_bag.place_forget()

            if e_cnt:

                if bbox_cnt:

                    x, y, w, h = bbox_cnt; e_cnt.place(x=x+2, y=y+2, width=w-4, height=h-4)

                else:

                    e_cnt.place_forget()



    def _aplicar_inventario_bolsas(self):

        cambios = []  # (pid, kg_calculados_a_partir_de_bolsas)

        for iid, pid in self._inv_bolsas_iid.items():

            ents = self._inv_bolsas_entries.get(pid, {})

            e_bag = ents.get("bagkg")

            e_cnt = ents.get("bolsas")

            if not (e_bag and e_cnt):

                continue

            sbag = (e_bag.get() or "").strip()

            scnt = (e_cnt.get() or "").strip()

            if not scnt:

                continue

            try:

                bag = float(sbag.replace(",", "."))

                n   = float(scnt.replace(",", "."))

                if bag <= 0 or n < 0:

                    continue

                cambios.append((pid, n * bag))

            except:

                continue



        if not cambios:

            messagebox.showinfo("Info", "No hay cantidades de bolsas cargadas para ajustar.")

            return



        try:

            # si vamos a sumar, cacheamos el stock actual

            raw_actual = {}

            if getattr(self, "var_inv_sumar", None) and self.var_inv_sumar.get():

                raw_actual = {pid: kg for pid, _, kg in self.repo.listar_raw_stock()}



            for pid, kg_calc in cambios:

                final_kg = kg_calc

                if getattr(self, "var_inv_sumar", None) and self.var_inv_sumar.get():

                    final_kg = raw_actual.get(pid, 0.0) + kg_calc

                self.repo.ajustar_raw_kg(pid, final_kg)



            self._refresh_inventarios()      # refresca kg y paquetes

            self._refresh_raw()              # compras

            self._refresh_raw2()             # fraccionamiento

            modo = "sumado al" if (getattr(self, "var_inv_sumar", None) and self.var_inv_sumar.get()) else "reemplazado en"

            messagebox.showinfo("OK", f"Inventario {modo} stock (bolsas - kg).")

        except Exception as e:

            messagebox.showerror("Error", str(e))

    def _update_totales_venta(self):

        grav5 = iva5 = grav10 = iva10 = total = 0.0



        for (pid, gram), ent in self._ventas_entries.items():

            txt = (ent.get() or "").strip()

            try:

                qty = int(txt) if txt else 0

                if qty < 0: qty = 0

            except:

                qty = 0



            price, iva = self.repo.get_price(pid, gram)

            line_total = 0.0

            if price is not None and iva in (5,10) and qty > 0:

                line_total = price * qty

                base = line_total / (1.0 + iva/100.0)

                iva_m = line_total - base

                total += line_total

                if iva == 5:

                    grav5 += base; iva5 += iva_m

                else:

                    grav10 += base; iva10 += iva_m



            # actualizar columna "importe"

            iid = self._ventas_keyiid.get((pid,gram))

            if iid:

                self.tv_ventas.set(iid, "importe", self._fmt_gs(line_total))



        self.lbl_tot.config(

            text=f"Gravada 5%: {self._fmt_gs(grav5)} | IVA 5%: {self._fmt_gs(iva5)} | "

                 f"Gravada 10%: {self._fmt_gs(grav10)} | IVA 10%: {self._fmt_gs(iva10)} | "

                 f"TOTAL: {self._fmt_gs(total)}"

        )



    # -------- Ajustes inventario (doble clic) --------

    def _ajustar_raw(self, event):

        item = self.tv_raw_adj.focus()

        if not item:

            return



        # Leer por nombre de columna (soporta 4 columnas)

        cols = list(self.tv_raw_adj["columns"])

        vals = list(self.tv_raw_adj.item(item, "values"))

        row  = {c: vals[i] for i, c in enumerate(cols) if i < len(vals)}



        try:

            pid  = int(row["id"])

            name = str(row["name"])

            kg_actual = float(str(row["kg"]).replace(",", "."))

        except Exception:

            return



        bagkg = bag_kg_por_defecto(name)  # Arroz 50, Galleta molida 25, resto 50



        s = simpledialog.askstring(

            "Ajuste por BOLSAS",

            f"{name}\nKg actual: {kg_actual:.3f}\nKg por bolsa (según producto): {bagkg:.0f} kg\n\n"

            "Ingrese la CANTIDAD DE BOLSAS:",

            parent=self

        )

        if s is None:

            return



        try:

            bolsas = float(s.replace(",", "."))

            if bolsas < 0:

                raise ValueError

            nuevo_kg = bolsas * bagkg

        except Exception:

            messagebox.showerror("Error", "Valor inválido.")

            return



        # Sumar o reemplazar según el check "Sumar en lugar de reemplazar"

        if getattr(self, "var_inv_sumar", None) and self.var_inv_sumar.get():

            actual_por_id = {p: k for p, _, k in self.repo.listar_raw_stock()}

            nuevo_kg = actual_por_id.get(pid, 0.0) + nuevo_kg



        self.repo.ajustar_raw_kg(pid, nuevo_kg)



        # Refrescos

        self._refresh_inventarios()

        self._refresh_raw()

        self._refresh_raw2()



        modo = "sumado al" if (getattr(self, "var_inv_sumar", None) and self.var_inv_sumar.get()) else "reemplazado en"

        messagebox.showinfo("OK", f"Stock {modo} inventario con {bolsas:.3f} bolsas x {bagkg:.0f} kg.")





    def _ajustar_pkg(self, event):

        item = self.tv_pkg_inv.focus()

        if not item:

            return

        name, gram, stock, _ = self.tv_pkg_inv.item(item, "values")

        nuevo = simpledialog.askstring(

            "Ajuste paquetes",

            f"{name} {gram} g\nPaquetes actual: {stock}\nNuevo valor:",

            parent=self

        )

        if nuevo is None:

            return

        try:

            nv = int(nuevo)

            if nv < 0:

                raise ValueError

            # id del producto desde el mapeo que armás en _refresh_inventario_paquetes_grid

            pid = self._inv_iidkey.get(item, (None, None))[0]

            if pid is None:

                return

            self.repo.ajustar_paquetes(int(pid), int(gram), nv)

            self._refresh_inventarios()

            if hasattr(self, "_refresh_ventas_grid"):

                self._refresh_ventas_grid()

        except Exception as e:

            messagebox.showerror("Error", str(e))

    def _on_frac_producto_seleccionado(self, *_):

        prod = self.cb_frac_prod.get().strip()

        grams = gramajes_permitidos(prod)

        self.cb_frac_gram["values"] = [str(x) for x in grams]

        try:

            gsel = int(self.cb_frac_gram.get())

        except:

            gsel = None

        if gsel not in grams:

            self.cb_frac_gram.set("")

        self._update_consumo_label()



        # ---- cargar lotes del producto seleccionado ----

        if hasattr(self, "cb_frac_lote"):

            self.cb_frac_lote.set("")

            self._lote_map = {}

            if prod:

                pid = self.repo.get_product_id_by_name(prod)

                opts = []

                for lot_id, _, _, lote, saldo, ckg in self.repo.listar_lotes_abiertos(pid):

                    disp = f"Lote {lote or 's/lote'} - saldo {saldo:.3f} kg @ {self._fmt_gs(ckg)}"

                    self._lote_map[disp] = lot_id

                    opts.append(disp)

                self.cb_frac_lote["values"] = opts

            else:

                self.cb_frac_lote["values"] = []

    def _build_tab_hist_fracc(self, nb):

        frame = ttk.Frame(nb)

        nb.add(frame, text="Historial fraccionamiento")



        # Filtros

        filtros = ttk.Frame(frame); filtros.pack(fill="x", pady=6)

        ttk.Label(filtros, text="Producto:").pack(side="left")

        self.cb_hist_prod = ttk.Combobox(filtros, state="readonly", width=30, values=["Todos"])

        self.cb_hist_prod.pack(side="left", padx=6)



        ttk.Label(filtros, text="Desde (YYYY-MM-DD):").pack(side="left")

        self.ent_hist_desde = ttk.Entry(filtros, width=12); self.ent_hist_desde.pack(side="left", padx=4)

        ttk.Label(filtros, text="Hasta (YYYY-MM-DD):").pack(side="left")

        self.ent_hist_hasta = ttk.Entry(filtros, width=12); self.ent_hist_hasta.pack(side="left", padx=4)



        ttk.Button(filtros, text="Refrescar", command=self._refresh_hist_fracc).pack(side="left", padx=6)
        ttk.Button(filtros, text="Kg/bolsa eq", command=self._abrir_config_bolsa_eq_hist).pack(side="left", padx=6)
        ttk.Button(filtros, text="Editar colores", command=self._edit_hist_producto_colores).pack(side="left", padx=6)
        ttk.Button(filtros, text="Modificar seleccionado", command=self._modif_hist_fracc).pack(side="left", padx=6)
        ttk.Button(filtros, text="Eliminar seleccionado", command=self._del_hist_fracc).pack(side="left", padx=6)



        # Grilla

        cols = ("id","fecha","producto","gramaje","paquetes","kg","bolsas_eq","lote")

        self.tv_hist = ttk.Treeview(frame, columns=cols, show="headings", height=16)

        self.tv_hist.heading("id",       text="ID")

        self.tv_hist.heading("fecha",    text="Fecha")

        self.tv_hist.heading("producto", text="Producto")

        self.tv_hist.heading("gramaje",  text="g")

        self.tv_hist.heading("paquetes", text="Paquetes")

        self.tv_hist.heading("kg",       text="Kg consumidos")
        self.tv_hist.heading("bolsas_eq", text="Eq. bolsas")

        self.tv_hist.heading("lote",     text="Lote (id)")



        self.tv_hist.column("id",       width=60,  anchor="center")

        self.tv_hist.column("fecha",    width=150, anchor="w")

        self.tv_hist.column("producto", width=180, anchor="w")

        self.tv_hist.column("gramaje",  width=80,  anchor="center")

        self.tv_hist.column("paquetes", width=90,  anchor="center")

        self.tv_hist.column("kg",       width=120, anchor="center")
        self.tv_hist.column("bolsas_eq", width=220, anchor="center")

        self.tv_hist.column("lote",     width=90,  anchor="center")



        self.tv_hist.pack(fill="both", expand=True, padx=6, pady=6)
        for ev in ("<Configure>", "<MouseWheel>", "<ButtonRelease-1>", "<KeyRelease>", "<Expose>"):
            self.tv_hist.bind(ev, self._schedule_hist_prod_labels)
        self.tv_hist.bind("<<TreeviewSelect>>", self._schedule_hist_prod_labels)



        # Combos iniciales

        prods = [n for _, n in self.repo.list_products()]

        self.cb_hist_prod["values"] = ["Todos"] + prods

        self.cb_hist_prod.set("Todos")



        self._refresh_hist_fracc()



    def _clean_hist_product_name(self, pname: str) -> str:

        txt = (pname or "").strip()
        return txt

    def _hist_color_target_product(self):

        sel = (self.cb_hist_prod.get() or "").strip()

        if sel and sel != "Todos":

            pid = self.repo.get_product_id_by_name(sel)

            if pid:

                return pid, sel

        item = self.tv_hist.focus()

        if item:

            vals = self.tv_hist.item(item, "values")

            if vals and len(vals) >= 3:

                pname = self._clean_hist_product_name(vals[2])

                pid = self.repo.get_product_id_by_name(pname)

                if pid:

                    return pid, pname

        return None, None

    def _schedule_hist_prod_labels(self, _evt=None):
        if hasattr(self, "tv_hist"):
            self.after_idle(self._relayout_hist_prod_labels)

    def _on_hist_prod_label_click(self, iid: str):
        if not hasattr(self, "tv_hist"):
            return
        try:
            self.tv_hist.selection_set(iid)
            self.tv_hist.focus(iid)
            self._schedule_hist_prod_labels()
        except Exception:
            pass

    def _draw_rounded_rect(self, canvas: tk.Canvas, x1: int, y1: int, x2: int, y2: int, r: int,
                           fill: str, outline: str = ""):
        r = max(0, min(r, (x2 - x1) // 2, (y2 - y1) // 2))
        if r <= 0:
            canvas.create_rectangle(x1, y1, x2, y2, fill=fill, outline=outline)
            return
        canvas.create_arc(x1, y1, x1 + 2 * r, y1 + 2 * r, start=90, extent=90, fill=fill, outline=outline)
        canvas.create_arc(x2 - 2 * r, y1, x2, y1 + 2 * r, start=0, extent=90, fill=fill, outline=outline)
        canvas.create_arc(x1, y2 - 2 * r, x1 + 2 * r, y2, start=180, extent=90, fill=fill, outline=outline)
        canvas.create_arc(x2 - 2 * r, y2 - 2 * r, x2, y2, start=270, extent=90, fill=fill, outline=outline)
        canvas.create_rectangle(x1 + r, y1, x2 - r, y2, fill=fill, outline=outline)
        canvas.create_rectangle(x1, y1 + r, x2, y2 - r, fill=fill, outline=outline)

    def _clear_hist_prod_labels(self):
        if hasattr(self, "_hist_prod_labels"):
            for lbl in self._hist_prod_labels.values():
                try:
                    lbl.destroy()
                except Exception:
                    pass
        self._hist_prod_labels = {}

    def _relayout_hist_prod_labels(self):
        if not hasattr(self, "tv_hist"):
            return
        if not hasattr(self, "_hist_prod_labels"):
            self._hist_prod_labels = {}
        if not hasattr(self, "_hist_iid_pid"):
            self._hist_iid_pid = {}
        if not hasattr(self, "_hist_iid_pname"):
            self._hist_iid_pname = {}

        children = list(self.tv_hist.get_children())
        valid_iids = set(children)

        for iid in list(self._hist_prod_labels.keys()):
            if iid not in valid_iids:
                try:
                    self._hist_prod_labels[iid].destroy()
                except Exception:
                    pass
                self._hist_prod_labels.pop(iid, None)

        for iid in children:
            pid = self._hist_iid_pid.get(iid)
            pname = self._hist_iid_pname.get(iid, "")
            style = (self._hist_product_color_styles or {}).get(pid)
            if not style:
                lbl = self._hist_prod_labels.get(iid)
                if lbl:
                    lbl.place_forget()
                continue

            try:
                bbox = self.tv_hist.bbox(iid, column="producto")
            except Exception:
                bbox = None

            if not bbox:
                lbl = self._hist_prod_labels.get(iid)
                if lbl:
                    lbl.place_forget()
                continue

            x, y, w, h = bbox
            row_tags = set(self.tv_hist.item(iid, "tags") or ())
            stripe_bg = self._stripe_colors[0] if "evenrow" in row_tags else self._stripe_colors[1]

            badge = self._hist_prod_labels.get(iid)
            if not badge:
                badge = tk.Canvas(self.tv_hist, highlightthickness=0, bd=0)
                badge.bind("<Button-1>", lambda _e, _iid=iid: self._on_hist_prod_label_click(_iid))
                self._hist_prod_labels[iid] = badge

            fg_col = style.get("fg", "#12326B")
            bg_col = style.get("bg", "#E6EEF9")
            bw = max(1, w - 2)
            bh = max(1, h - 2)
            badge.configure(bg=stripe_bg, width=bw, height=bh)
            badge.delete("all")
            self._draw_rounded_rect(badge, 1, 2, bw - 2, bh - 2, r=8, fill=bg_col, outline="")
            badge.create_text(10, bh // 2, text=pname, anchor="w", fill=fg_col, font=("Segoe UI", 10))
            badge.place(x=x+1, y=y+1, width=bw, height=bh)

    def _edit_hist_producto_colores(self):

        pid, pname = self._hist_color_target_product()

        if not pid:

            messagebox.showinfo("Info", "Seleccione un producto en el filtro o una fila del historial.")

            return

        current = (self._hist_product_color_styles or {}).get(pid, {"fg": "#12326B", "bg": "#E6EEF9"})
        fg = current.get("fg", "#12326B")
        bg = current.get("bg", "#E6EEF9")
        fg_var = tk.StringVar(value=fg)
        bg_var = tk.StringVar(value=bg)

        dlg = tk.Toplevel(self)
        dlg.title(f"Editar colores - {pname}")
        dlg.transient(self)
        dlg.grab_set()
        dlg.resizable(False, False)

        ttk.Label(dlg, text=f"Producto: {pname}").grid(row=0, column=0, columnspan=3, sticky="w", padx=10, pady=(10, 6))
        preview = tk.Label(dlg, text=pname, anchor="w", padx=8, fg=fg, bg=bg)
        preview.grid(row=1, column=0, columnspan=3, sticky="ew", padx=10, pady=(0, 10))

        def _normalize_hex(raw: str, field_name: str) -> str:
            c = (raw or "").strip().upper()
            if len(c) == 6 and not c.startswith("#"):
                c = f"#{c}"
            if len(c) != 7 or not c.startswith("#"):
                raise ValueError(f"{field_name} inválido. Use formato #RRGGBB.")
            try:
                int(c[1:], 16)
            except Exception as exc:
                raise ValueError(f"{field_name} inválido. Use formato #RRGGBB.") from exc
            return c

        def _refresh_preview():
            try:
                fg_preview = _normalize_hex(fg_var.get(), "Color de letra")
                bg_preview = _normalize_hex(bg_var.get(), "Color de fondo")
            except Exception:
                return
            preview.configure(fg=fg_preview, bg=bg_preview)

        def _pick_fg():
            _rgb, color_hex = colorchooser.askcolor(color=fg_var.get(), title=f"Color de letra para {pname}")
            if color_hex:
                fg_var.set(color_hex.upper())
                _refresh_preview()

        def _pick_bg():
            _rgb, color_hex = colorchooser.askcolor(color=bg_var.get(), title=f"Color de fondo para {pname}")
            if color_hex:
                bg_var.set(color_hex.upper())
                _refresh_preview()

        def _save():
            try:
                fg_hex = _normalize_hex(fg_var.get(), "Color de letra")
                bg_hex = _normalize_hex(bg_var.get(), "Color de fondo")
                self.repo.set_product_color_style(pid, fg_hex, bg_hex)
            except Exception as e:
                messagebox.showerror("Error", str(e), parent=dlg)
                return
            self._hist_product_color_styles[pid] = {"fg": fg_hex, "bg": bg_hex}
            self._refresh_hist_fracc()
            dlg.destroy()

        def _clear():
            try:
                self.repo.clear_product_color_style(pid)
            except Exception as e:
                messagebox.showerror("Error", str(e), parent=dlg)
                return
            self._hist_product_color_styles.pop(pid, None)
            self._refresh_hist_fracc()
            dlg.destroy()

        ttk.Label(dlg, text="HEX letra:").grid(row=2, column=0, sticky="w", padx=10)
        ent_fg = ttk.Entry(dlg, textvariable=fg_var, width=14)
        ent_fg.grid(row=2, column=1, sticky="w", padx=0)
        ttk.Button(dlg, text="Elegir letra", command=_pick_fg).grid(row=2, column=2, padx=10, pady=(0, 4), sticky="ew")

        ttk.Label(dlg, text="HEX fondo:").grid(row=3, column=0, sticky="w", padx=10)
        ent_bg = ttk.Entry(dlg, textvariable=bg_var, width=14)
        ent_bg.grid(row=3, column=1, sticky="w", padx=0)
        ttk.Button(dlg, text="Elegir fondo", command=_pick_bg).grid(row=3, column=2, padx=10, pady=(0, 4), sticky="ew")

        ent_fg.bind("<KeyRelease>", lambda _e: _refresh_preview())
        ent_bg.bind("<KeyRelease>", lambda _e: _refresh_preview())

        ttk.Button(dlg, text="Quitar estilo", command=_clear).grid(row=4, column=0, padx=10, pady=(6, 10), sticky="ew")
        ttk.Button(dlg, text="Guardar", command=_save).grid(row=4, column=1, padx=0, pady=(6, 10), sticky="ew")
        ttk.Button(dlg, text="Cancelar", command=dlg.destroy).grid(row=4, column=2, padx=10, pady=(6, 10), sticky="ew")

    def _refresh_hist_fracc(self):

        # limpiar

        self._clear_hist_prod_labels()
        self._hist_iid_pid = {}
        self._hist_iid_pname = {}

        for i in self.tv_hist.get_children():

            self.tv_hist.delete(i)



        sel = (self.cb_hist_prod.get() or "Todos").strip()

        pid = None if sel == "Todos" else self.repo.get_product_id_by_name(sel)



        d1 = (self.ent_hist_desde.get() or "").strip() or None

        d2 = (self.ent_hist_hasta.get() or "").strip() or None



        self._hist_product_color_styles = self.repo.list_product_color_styles()

        rows = self.repo.listar_fraccionamientos(d1, d2, pid)

        for fid, ts, pid_, pname, g, paq, kg, lote_txt in rows:

            bag_kg = self._bag_eq_kg_for_product(pname, pid_)
            bolsas_eq = format_single_bag_equivalence(kg, bag_kg)

            iid = self.tv_hist.insert("", "end",
                values=(fid, str(ts), pname, int(g), int(paq), self._fmt_kg(kg), bolsas_eq, (lote_txt or "-")))
            self._hist_iid_pid[iid] = pid_
            self._hist_iid_pname[iid] = pname

        self._apply_treeview_striping(self.tv_hist)
        self.after_idle(self._relayout_hist_prod_labels)


    def _abrir_config_bolsa_eq_hist(self):

        dlg = tk.Toplevel(self)
        dlg.title("Kg por bolsa equivalente")
        dlg.transient(self)
        dlg.grab_set()
        dlg.resizable(False, False)

        ttk.Label(
            dlg,
            text="Define el kg/bolsa que querés mostrar por producto. Si queda vacío, usa el valor por defecto.",
        ).grid(row=0, column=0, columnspan=3, sticky="w", padx=8, pady=(8, 4))

        ttk.Label(dlg, text="Producto").grid(row=1, column=0, sticky="w", padx=8)
        ttk.Label(dlg, text="Por defecto").grid(row=1, column=1, sticky="w", padx=8)
        ttk.Label(dlg, text="Mostrar kg/bolsa").grid(row=1, column=2, sticky="w", padx=8)

        entries = {}
        products = self.repo.list_products()

        for idx, (pid, name) in enumerate(products, start=2):
            bag_def = bag_kg_por_defecto(name)
            bag_cur = float(self._bag_eq_display_map.get(int(pid), 0.0) or 0.0)

            ttk.Label(dlg, text=name).grid(row=idx, column=0, sticky="w", padx=8, pady=2)
            ttk.Label(dlg, text=f"{bag_def:.0f} kg").grid(row=idx, column=1, sticky="w", padx=8, pady=2)

            ent = ttk.Entry(dlg, width=10)
            if bag_cur > 0:
                ent.insert(0, f"{bag_cur:.2f}".rstrip("0").rstrip("."))
            ent.grid(row=idx, column=2, sticky="w", padx=8, pady=2)
            entries[int(pid)] = ent

        def guardar():
            for pid, ent in entries.items():
                txt = (ent.get() or "").strip()
                if not txt:
                    self.repo.set_product_bag_display_kg(pid, None)
                    continue
                try:
                    val = float(txt.replace(",", "."))
                except Exception:
                    messagebox.showerror("Error", "Kg por bolsa invalido.", parent=dlg)
                    return
                if val <= 0:
                    messagebox.showerror("Error", "Kg por bolsa invalido.", parent=dlg)
                    return
                self.repo.set_product_bag_display_kg(pid, val)

            self._bag_eq_display_map = self.repo.get_product_bag_display_map()
            self._refresh_hist_fracc()
            try:
                self._update_consumo_label()
            except Exception:
                pass
            dlg.destroy()

        btns = ttk.Frame(dlg)
        btns.grid(row=len(products) + 2, column=0, columnspan=3, pady=(10, 10))
        ttk.Button(btns, text="Guardar", command=guardar).pack(side="left", padx=6)
        ttk.Button(btns, text="Cancelar", command=dlg.destroy).pack(side="left", padx=6)





    def _modif_hist_fracc(self):

        item = self.tv_hist.focus()

        if not item:

            messagebox.showinfo("Info", "Seleccione una fila del historial.")

            return



        # La tabla carga: (fid, ts, pname, g, paq, kg, bolsas_eq, lote_txt)

        try:

            fid, fecha, pname, gram, paq, kg, _bolsas_eq, lote_txt = self.tv_hist.item(item, "values")
            pname = self._clean_hist_product_name(pname)

            fid  = int(fid)

            gram = int(gram)

            paq  = int(paq)

        except Exception as e:

            messagebox.showerror("Error", f"Fila inválida: {e}")

            return



        # Traer info completa (incluye id de lote actual)

        info = self.repo.get_frac_info(fid)

        if not info:

            messagebox.showerror("Error", "No se pudo cargar el fraccionamiento.")

            return

        _fid, _ts, prod_id, _pname_db, _g_db, _p_db, _kg_db, lot_id_actual, lote_txt_db = info

        lote_actual_label = lote_txt_db or lote_txt or "-"

        if lot_id_actual:

            lote_actual_label = f"{lote_actual_label} (ID {lot_id_actual})"



        grams = gramajes_permitidos(pname)



        # Opciones de lote disponibles

        lote_opts = ["(sin lote)"]

        lote_map = {}

        for lid, lote_nombre, _prod_name_lote, _kg_tot, _kg_usado, kg_disp, *_rest in self.repo.listar_lotes(prod_id, solo_abiertos=True):

            desc = f"ID {lid} - {lote_nombre or 's/lote'} (saldo {kg_disp:.3f} kg)"

            lote_map[desc] = lid

            lote_opts.append(desc)

        if lot_id_actual and lot_id_actual not in lote_map.values():

            det = self.repo.lot_detail(lot_id_actual)

            if det:

                lid, lote_nombre, _pid_det, _pname_det, _kg_ini, kg_saldo, *_more, cerrado = det

                desc = f"ID {lid} - {lote_nombre or 's/lote'} (saldo {kg_saldo:.3f} kg"

                if cerrado:

                    desc += " - cerrado"

                desc += ")"

                lote_map[desc] = lid

                lote_opts.append(desc)



        # Diálogo

        dlg = tk.Toplevel(self)

        dlg.title(f"Modificar fraccionamiento #{fid}")

        dlg.transient(self); dlg.grab_set()

        dlg.resizable(False, False)



        ttk.Label(dlg, text=f"Producto: {pname}").grid(row=0, column=0, columnspan=2, sticky="w", pady=(8,4), padx=8)

        ttk.Label(dlg, text=f"Fecha: {fecha}").grid(row=1, column=0, columnspan=2, sticky="w", pady=2, padx=8)

        ttk.Label(dlg, text=f"Lote vinculado: {lote_actual_label}").grid(row=2, column=0, columnspan=2, sticky="w", pady=2, padx=8)



        ttk.Label(dlg, text="Cambiar lote:").grid(row=3, column=0, sticky="e", pady=6, padx=6)

        cb_lote = ttk.Combobox(dlg, state="readonly", values=lote_opts, width=55)

        cb_lote.grid(row=3, column=1, sticky="w", padx=6)

        if lot_id_actual:

            for txt, lid in lote_map.items():

                if lid == lot_id_actual:

                    cb_lote.set(txt); break

        else:

            cb_lote.set("(sin lote)")



        ttk.Label(dlg, text="Gramaje (g):").grid(row=4, column=0, sticky="e", pady=6, padx=6)

        cb_g = ttk.Combobox(dlg, state="readonly", values=[str(x) for x in grams], width=10)

        cb_g.set(str(gram))

        cb_g.grid(row=4, column=1, sticky="w", padx=6)



        ttk.Label(dlg, text="Paquetes:").grid(row=5, column=0, sticky="e", pady=6, padx=6)

        ent_p = ttk.Entry(dlg, width=12)

        ent_p.insert(0, str(paq))

        ent_p.grid(row=5, column=1, sticky="w", padx=6)



        btns = ttk.Frame(dlg); btns.grid(row=6, column=0, columnspan=2, pady=10)

        def _ok():

            # Validación y guardado

            try:

                new_g = int(cb_g.get()); new_p = int(ent_p.get())

                if new_p <= 0:

                    raise ValueError

            except Exception:

                messagebox.showerror("Error", "Valores inválidos.")

                return



            lot_sel = (cb_lote.get() or "").strip()

            new_lot_id = None

            if lot_sel and lot_sel != "(sin lote)":

                new_lot_id = lote_map.get(lot_sel)

                if not new_lot_id:

                    messagebox.showerror("Error", "Seleccione un lote válido.")

                    return

            try:

                self.repo.actualizar_fraccionamiento(fid, new_g, new_p, new_lot_id)

                # Refrescar vistas

                self._refresh_hist_fracc()

                if hasattr(self, "_refresh_raw"): self._refresh_raw()

                if hasattr(self, "_refresh_raw2"): self._refresh_raw2()

                if hasattr(self, "_refresh_pkg"): self._refresh_pkg()

                if hasattr(self, "_refresh_lotes_abiertos"): self._refresh_lotes_abiertos()

                if hasattr(self, "_refresh_resumenes"): self._refresh_resumenes()

                messagebox.showinfo("OK", "Fraccionamiento actualizado.")

                dlg.destroy()

            except Exception as e:

                messagebox.showerror("Error", str(e))



        ttk.Button(btns, text="Guardar cambios", command=_ok).pack(side="left", padx=6)

        ttk.Button(btns, text="Cancelar", command=dlg.destroy).pack(side="left", padx=6)


    def _del_hist_fracc(self):

        item = self.tv_hist.focus()

        if not item:

            messagebox.showinfo("Info", "Seleccione una fila del historial.")

            return



        try:

            fid, fecha, pname, gram, paq, _kg, _bolsas_eq, _lote_txt = self.tv_hist.item(item, "values")
            pname = self._clean_hist_product_name(pname)

            fid  = int(fid)

            gram = int(gram)

            paq  = int(paq)

        except Exception as e:

            messagebox.showerror("Error", f"Fila inválida: {e}")

            return



        msg = (f"¿Eliminar el fraccionamiento #{fid}?\n"

               f"{pname} - {gram} g x {paq} paquetes\n"

               "Se revertirán los paquetes y la materia prima, incluido el lote si aplica.")

        if not messagebox.askyesno("Confirmar eliminación", msg):

            return



        try:

            self.repo.eliminar_fraccionamiento(fid)



            self._refresh_hist_fracc()

            if hasattr(self, "_refresh_raw"): self._refresh_raw()

            if hasattr(self, "_refresh_raw2"): self._refresh_raw2()

            if hasattr(self, "_refresh_pkg"): self._refresh_pkg()

            if hasattr(self, "_refresh_lotes_abiertos"): self._refresh_lotes_abiertos()

            if hasattr(self, "_refresh_resumenes"): self._refresh_resumenes()



            messagebox.showinfo("Eliminado", "Fraccionamiento eliminado y stock revertido.")

        except Exception as e:

            messagebox.showerror("Error", str(e))



    def _build_tab_hist_ajustes(self, nb):
        frame = ttk.Frame(nb)
        nb.add(frame, text="Historial ajustes")

        filtros = ttk.Frame(frame); filtros.pack(fill="x", pady=6)
        ttk.Label(filtros, text="Producto:").pack(side="left")
        self.cb_hist_adj_prod = ttk.Combobox(filtros, state="readonly", width=30, values=["Todos"])
        self.cb_hist_adj_prod.pack(side="left", padx=6)

        ttk.Label(filtros, text="Tipo:").pack(side="left")
        self.cb_hist_adj_tipo = ttk.Combobox(
            filtros,
            state="readonly",
            width=16,
            values=["Todos", "Materia prima", "Paquetes"],
        )
        self.cb_hist_adj_tipo.pack(side="left", padx=6)
        self.cb_hist_adj_tipo.set("Todos")

        ttk.Label(filtros, text="Desde (YYYY-MM-DD):").pack(side="left")
        self.ent_hist_adj_desde = ttk.Entry(filtros, width=12); self.ent_hist_adj_desde.pack(side="left", padx=4)
        ttk.Label(filtros, text="Hasta (YYYY-MM-DD):").pack(side="left")
        self.ent_hist_adj_hasta = ttk.Entry(filtros, width=12); self.ent_hist_adj_hasta.pack(side="left", padx=4)

        ttk.Button(filtros, text="Refrescar", command=self._refresh_hist_ajustes).pack(side="left", padx=6)

        cols = ("id","fecha","producto","tipo","gramaje","antes","despues","delta","motivo")
        self.tv_hist_adj = ttk.Treeview(frame, columns=cols, show="headings", height=16)
        self.tv_hist_adj.heading("id",       text="ID")
        self.tv_hist_adj.heading("fecha",    text="Fecha")
        self.tv_hist_adj.heading("producto", text="Producto")
        self.tv_hist_adj.heading("tipo",     text="Tipo")
        self.tv_hist_adj.heading("gramaje",  text="g")
        self.tv_hist_adj.heading("antes",    text="Antes")
        self.tv_hist_adj.heading("despues",  text="Despues")
        self.tv_hist_adj.heading("delta",    text="Delta")
        self.tv_hist_adj.heading("motivo",   text="Motivo")

        self.tv_hist_adj.column("id",       width=60,  anchor="center")
        self.tv_hist_adj.column("fecha",    width=150, anchor="w")
        self.tv_hist_adj.column("producto", width=180, anchor="w")
        self.tv_hist_adj.column("tipo",     width=120, anchor="center")
        self.tv_hist_adj.column("gramaje",  width=80,  anchor="center")
        self.tv_hist_adj.column("antes",    width=100, anchor="center")
        self.tv_hist_adj.column("despues",  width=100, anchor="center")
        self.tv_hist_adj.column("delta",    width=100, anchor="center")
        self.tv_hist_adj.column("motivo",   width=260, anchor="w")

        self.tv_hist_adj.pack(fill="both", expand=True, padx=6, pady=6)

        prods = [n for _, n in self.repo.list_products()]
        self.cb_hist_adj_prod["values"] = ["Todos"] + prods
        self.cb_hist_adj_prod.set("Todos")

        self._refresh_hist_ajustes()

    def _refresh_hist_ajustes(self):
        for i in self.tv_hist_adj.get_children():
            self.tv_hist_adj.delete(i)

        sel = (self.cb_hist_adj_prod.get() or "Todos").strip()
        pid = None if sel == "Todos" else self.repo.get_product_id_by_name(sel)

        tipo = (self.cb_hist_adj_tipo.get() or "Todos").strip()
        kind = None
        if tipo == "Materia prima":
            kind = "raw"
        elif tipo == "Paquetes":
            kind = "package"

        d1 = (self.ent_hist_adj_desde.get() or "").strip() or None
        d2 = (self.ent_hist_adj_hasta.get() or "").strip() or None

        rows = self.repo.listar_ajustes(d1, d2, pid, kind)
        for rid, ts, kind_row, _pid, pname, gram, before, after, delta, motivo in rows:
            if kind_row == "raw":
                tipo_txt = "MP"
                gtxt = "-"
                before_txt = self._fmt_kg(before)
                after_txt = self._fmt_kg(after)
                delta_txt = self._fmt_kg(delta)
            else:
                tipo_txt = "Paquetes"
                gtxt = str(int(gram)) if gram is not None else "-"
                before_txt = str(int(before))
                after_txt = str(int(after))
                delta_txt = str(int(delta))
            self.tv_hist_adj.insert("", "end",
                values=(rid, str(ts), pname, tipo_txt, gtxt, before_txt, after_txt, delta_txt, motivo))
        self._apply_treeview_striping(self.tv_hist_adj)

    # -------- Exportaciones (lotes / stock) --------
    def _write_csv(self, headers, rows, fname):
        with open(fname, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(headers)
            for r in rows:
                w.writerow(r)
        return os.path.abspath(fname)

    def _write_xlsx(self, headers, rows, fname):
        try:
            import openpyxl
        except Exception as exc:
            return False, str(exc)
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(list(headers))
        for r in rows:
            ws.append(list(r))
        wb.save(fname)
        return True, os.path.abspath(fname)

    def _export_csv_table(self, headers, rows, base_name):
        stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        fname = f"{base_name}_{stamp}.csv"
        path = self._write_csv(headers, rows, fname)
        messagebox.showinfo("Exportado", f"Archivo guardado: {path}")

    def _export_excel_table(self, headers, rows, base_name):
        stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        fname = f"{base_name}_{stamp}.xlsx"
        ok, info = self._write_xlsx(headers, rows, fname)
        if ok:
            messagebox.showinfo("Exportado", f"Archivo guardado: {info}")
            return
        fname_csv = f"{base_name}_{stamp}.csv"
        path = self._write_csv(headers, rows, fname_csv)
        messagebox.showinfo("Exportado", f"No se pudo crear Excel ({info}). CSV guardado: {path}")

    def _export_lotes_abiertos_csv(self):
        headers, rows = self._build_lotes_abiertos_export()
        self._export_csv_table(headers, rows, "lotes_abiertos")

    def _export_lotes_abiertos_excel(self):
        headers, rows = self._build_lotes_abiertos_export()
        self._export_excel_table(headers, rows, "lotes_abiertos")

    def _build_lotes_abiertos_export(self):
        prod = (self.cb_comp_prod.get() or "").strip()
        pid = self.repo.get_product_id_by_name(prod) if prod else None
        rows = []
        for lot_id, _pid_row, name, lote, saldo, ckg in self.repo.listar_lotes_abiertos(pid):
            rows.append((lot_id, name, lote or "", saldo, ckg))
        headers = ["lote_id", "producto", "lote", "saldo_kg", "costo_kg_gs"]
        return headers, rows

    def _export_stock_raw_csv(self):
        headers, rows = self._build_stock_raw_export()
        self._export_csv_table(headers, rows, "stock_mp")

    def _export_stock_raw_excel(self):
        headers, rows = self._build_stock_raw_export()
        self._export_excel_table(headers, rows, "stock_mp")

    def _build_stock_raw_export(self):
        rows = []
        for pid, name, kg in self.repo.listar_raw_stock():
            bagkg = bag_kg_por_defecto(name)
            bolsas_eq = (kg / bagkg) if bagkg > 0 else 0.0
            costo_kg = self.repo.get_last_cost_kg(pid)
            valor = (kg * costo_kg) if costo_kg is not None else ""
            rows.append((pid, name, kg, bagkg, bolsas_eq, costo_kg if costo_kg is not None else "", valor))
        headers = ["producto_id", "producto", "kg", "kg_por_bolsa", "bolsas_eq", "costo_kg_gs", "valor_gs"]
        return headers, rows

    def _export_stock_pkg_csv(self):
        headers, rows = self._build_stock_pkg_export()
        self._export_csv_table(headers, rows, "stock_paquetes")

    def _export_stock_pkg_excel(self):
        headers, rows = self._build_stock_pkg_export()
        self._export_excel_table(headers, rows, "stock_paquetes")

    def _build_stock_pkg_export(self):
        rows = []
        for pid, name, gram, paq in self.repo.listar_todos_pkg_stock():
            price, iva = self.repo.get_price(pid, gram)
            valor = (price * paq) if price is not None else ""
            rows.append((pid, name, gram, paq, price if price is not None else "", iva if iva is not None else "", valor))
        headers = ["producto_id", "producto", "gramaje_g", "paquetes", "precio_gs", "iva", "valor_gs"]
        return headers, rows


    def _export_hist_csv(self):

        import csv, datetime, os

        # armar nombre simple con fecha

        stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

        fname = f"historial_fraccionamiento_{stamp}.csv"

        # exportar lo que está en la vista

        with open(fname, "w", newline="", encoding="utf-8") as f:

            w = csv.writer(f, delimiter=";")

            w.writerow(["ID","Fecha","Producto","Gramaje (g)","Paquetes","Kg consumidos","Eq. bolsas (kg configurado)","Lote"])

            for iid in self.tv_hist.get_children():

                w.writerow(self.tv_hist.item(iid, "values"))

        messagebox.showinfo("Exportado", f"Archivo guardado: {os.path.abspath(fname)}")



    def _refresh_inventario_paquetes_grid(self):

        # Limpia filas

        for i in self.tv_pkg_inv.get_children():

            self.tv_pkg_inv.delete(i)



        # Destruye entradas anteriores

        if hasattr(self, "_inv_entries") and self._inv_entries:

            for ent in self._inv_entries.values():

                try: ent.destroy()

                except: pass

        self._inv_entries = {}   # key=(pid, gram) -> Entry

        self._inv_iidkey  = {}   # iid -> (pid, gram)



        # Trae TODAS las combinaciones permitidas, incluso stock 0

        data = self.repo.listar_todos_pkg_stock()  # [(pid, name, gram, paq)]



        # Inserta filas y crea entradas

        total_valor = 0.0
    
        for pid, name, gram, paq in data:

            price, _iva = self.repo.get_price(pid, gram)

            valor = float(price or 0) * float(paq or 0)

            total_valor += valor

            iid = self.tv_pkg_inv.insert("", "end", values=(name, gram, paq, self._fmt_gs(valor), ""))

            ent = ttk.Entry(self.tv_pkg_inv, width=10)

            ent.insert(0, str(paq))  # editable: ajusta directo al valor que quieras

            self._inv_entries[(pid, gram)] = ent

            self._inv_iidkey[iid] = (pid, gram)



        self._apply_treeview_striping(self.tv_pkg_inv)

        if hasattr(self, "lbl_pkg_total"):

            self.lbl_pkg_total.config(text=f"Total valor inventario: {self._fmt_gs(total_valor)}")

        # Coloca las entradas cuando el Treeview ya esta renderizado

        self.after_idle(self._relayout_pkg_inventario)





    def _relayout_pkg_inventario(self, *_):

        if not hasattr(self, "_inv_iidkey"):

            return

        for iid, key in self._inv_iidkey.items():

            ent = self._inv_entries.get(key)

            if not ent:

                continue    

            try:

                bbox = self.tv_pkg_inv.bbox(iid, column="ajustar")

            except Exception:

                bbox = None



            if bbox:

                x, y, w, h = bbox

                ent.place(x=x+2, y=y+2, width=w-4, height=h-4)

            else:

                ent.place_forget()





    def _confirmar_inventario_paquetes(self):

        cambios = []  # (pid, gram, nuevo_paq)

        for (pid, gram), ent in self._inv_entries.items():

            s = ent.get().strip()

            if s == "":

                continue

            try:

                nv = int(s)

                if nv < 0:

                    raise ValueError

                cambios.append((pid, gram, nv))

            except Exception:

                # ignora filas con valores inválidos

                continue



        if not cambios:

            messagebox.showinfo("Info", "No hay cantidades cargadas para ajustar.")

            return



        try:

            for pid, gram, nv in cambios:

                self.repo.ajustar_paquetes(pid, gram, nv)



            # Refrescos

            self._refresh_inventarios()

            if hasattr(self, "_refresh_ventas_grid"):

                self._refresh_ventas_grid()



            messagebox.showinfo("OK", "Inventario de paquetes actualizado.")

        except Exception as e:

            messagebox.showerror("Error", str(e))

        





if __name__ == "__main__":

    App().mainloop()

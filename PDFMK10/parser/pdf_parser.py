# -*- coding: utf-8 -*-
"""
parser/pdf_parser.py
Extractor de metadatos e ítems desde una OC en PDF usando pdfplumber.
- Meta: nro_oc, fecha_pedido (ISO), sucursal, raw_text
- Ítems: usa catálogo por código (determinista); si no hay, cae a heurísticas.

Requisitos:
    pip install pdfplumber
"""

import re
import datetime as dt
from pathlib import Path
from typing import List, Dict, Any, Optional

# Importa el catálogo desde el paquete 'parser'
try:
    from .catalog import CATALOG, ALIASES
except ImportError:
    # fallback si ejecutas fuera del paquete (no recomendado)
    from catalog import CATALOG, ALIASES

try:
    import pdfplumber
except Exception as e:
    raise RuntimeError("Falta pdfplumber. Instálalo con `pip install pdfplumber`.") from e


# ==========================
# Utilidades generales
# ==========================

SPANISH_MONTHS = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04", "mayo": "05", "junio": "06",
    "julio": "07", "agosto": "08", "septiembre": "09", "setiembre": "09", "octubre": "10",
    "noviembre": "11", "diciembre": "12"
}

CANDIDATE_HEADERS = [
    ("Codigo", "Descripcion", "Cantidad"),
    ("Código", "Descripción", "Cantidad"),
    ("Descripcion", "Cantidad"),
    ("Descripción", "Cantidad"),
    ("ITEM", "Descripción", "Cantidad"),
]

def _to_iso(d: str) -> Optional[str]:
    if not d:
        return None
    s1 = d.strip().replace(".", "/").replace("-", "/")
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d"):
        try:
            return dt.datetime.strptime(s1, fmt).date().isoformat()
        except ValueError:
            pass
    m = re.search(r"(\d{1,2})\s+de\s+([A-Za-zÁÉÍÓÚáéíóú]+)\s+de\s+(\d{4})", d, flags=re.I)
    if m:
        dd = int(m.group(1))
        mm = SPANISH_MONTHS.get(m.group(2).lower())
        yyyy = int(m.group(3))
        if mm:
            try:
                return dt.date(yyyy, int(mm), dd).isoformat()
            except ValueError:
                return None
    return None

def _to_number(s: str) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip()
    # quitar símbolos de moneda y espacios internos
    s = re.sub(r"[^\d,.\-]", "", s)
    # si hay más de un separador, asumimos que son miles: quitar todos
    if s.count(",") + s.count(".") > 1:
        s_clean = re.sub(r"[.,]", "", s)
        try:
            return float(s_clean)
        except ValueError:
            pass
    # patrón típico de miles: 1.234.567 o 1,234,567
    if re.fullmatch(r"-?\d{1,3}(?:[.,]\d{3})+", s):
        try:
            return float(re.sub(r"[.,]", "", s))
        except ValueError:
            pass
    # un solo separador: tratar coma como decimal
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        m = re.match(r"(\d+(?:\.\d+)?)", s)
        return float(m.group(1)) if m else None

def _extract_amounts(line: str) -> List[float]:
    """Devuelve todos los montos numéricos positivos hallados en la línea."""
    nums: List[float] = []
    for m in re.finditer(r"(?:GS\.?|₲)?\s*(-?\d[\d\.\,\s]*\d)", line, flags=re.I):
        v = _to_number(m.group(1))
        if v is not None and v > 0:
            nums.append(v)
    return nums


def _find_monto_total(pages_text: List[str]) -> Optional[float]:
    """
    Busca el monto total a pagar priorizando líneas con 'total a pagar' y,
    en su defecto, cualquier línea con 'total' (excluye 'subtotal' y porcentajes).
    Devuelve None si no encuentra candidatos.
    """
    lines = [ln for ln in "\n".join(pages_text).splitlines() if ln and re.search(r"\d", ln)]

    def _best_value(candidatas: List[str]) -> Optional[float]:
        vals: List[float] = []
        for ln in candidatas:
            low = ln.lower()
            if "subtotal" in low:
                continue
            if re.search(r"\d\s*%", low):
                continue
            vals.extend(_extract_amounts(ln))
        return max(vals) if vals else None

    pri = [ln for ln in lines if re.search(r"total\s+a\s+pagar", ln, re.I)]
    val = _best_value(pri)
    if val is None:
        sec = [ln for ln in lines if re.search(r"\btotal\b", ln, re.I)]
        val = _best_value(sec)
    return val

# ==========================
# Metadatos
# ==========================

def _find_meta_fields(pages_text: List[str]) -> Dict[str, Any]:
    text_all = "\n".join(pages_text)

    nro_oc = None
    if re.search(r"\bORDEN\s+DE\s+COMPRA\b", text_all, re.I) or \
       re.search(r"O\s*R\s*D\s*E\s*N\s*D\s*E\s*C\s*O\s*M\s*P\s*R\s*A", text_all, re.I):
        m_no = re.search(r"No\.?\s*[:\-]?\s*([0-9]{3,})", text_all, re.I)
        if m_no:
            nro_oc = m_no.group(1).strip()
    if not nro_oc:
        m2 = re.search(r"(?:OC|Orden\s+de\s+Compra)\s*(?:N[°º:]|\:)?\s*([0-9]{3,})", text_all, re.I)
        if m2:
            nro_oc = m2.group(1).strip()

    fecha_iso = None
    m_fnum = re.search(r"\b(0?\d[\/\-\.]0?\d[\/\-\.]\d{2,4})\b", text_all)
    if m_fnum:
        fecha_iso = _to_iso(m_fnum.group(1))
    if not fecha_iso:
        m_fesp = re.search(r"\b(\d{1,2}\s+de\s+[A-Za-zÁÉÍÓÚáéíóú]+\s+de\s+\d{4})\b", text_all, re.I)
        if m_fesp:
            fecha_iso = _to_iso(m_fesp.group(1))

    suc_enc = None
    m_senc = re.search(r"\bSUCURSAL\s+([A-ZÁÉÍÓÚÑ ]+)\b", text_all, re.I)
    if m_senc:
        suc_enc = m_senc.group(1).strip()
    suc_cuadro = None
    m_s1 = re.search(r"\bSucursal\s*[:]*\s*([A-Za-zÁÉÍÓÚÑ ]+)", text_all, re.I)
    if m_s1:
        suc_cuadro = m_s1.group(1).strip()
    sucursal = suc_enc or suc_cuadro

    monto_total = _find_monto_total(pages_text)

    return {
        "nro_oc": nro_oc,
        "fecha_pedido": fecha_iso,
        "sucursal": sucursal,
        "raw_text": text_all,
        "monto_total": monto_total,
    }

# ==========================
# Ayudas de layout
# ==========================

def _is_code_token(t: str) -> bool:
    return bool(re.fullmatch(r"\d{5,7}", t or ""))  # 206000, 228894, etc.

def _locate_table_y_bounds(page):
    words = page.extract_words(use_text_flow=True, keep_blank_chars=False) or []
    y_top = y_bottom = None
    for w in words:
        t = (w.get("text") or "").strip().lower()
        if t in ("codigo", "código", "descripcion", "descripción", "cantidad"):
            y_top = min(y_top, w["top"]) if y_top is not None else w["top"]
    for w in words:
        t = (w.get("text") or "").strip().lower()
        if ("sub" in t and "total" in t) or ("total" in t and "pagar" in t):
            y_bottom = max(y_bottom, w["bottom"]) if y_bottom is not None else w["bottom"]
    if y_top is not None: y_top -= 4
    if y_bottom is not None: y_bottom += 4
    return (y_top, y_bottom)

def _column_x_ranges_from_headers(page):
    words = page.extract_words(use_text_flow=True, keep_blank_chars=False) or []
    W = page.width
    x_desc0 = x_desc1 = x_qty0 = x_qty1 = None
    for w in words:
        t = (w.get("text") or "").strip().lower()
        if t in ("descripcion","descripción") and x_desc0 is None:
            x_desc0 = max(0, w["x0"] - 10); x_desc1 = w["x1"] + 10
        if t == "cantidad" and x_qty0 is None:
            # arranca un pelo a la derecha del header
            x_qty0 = min(W, w["x0"] + 8)
            # y deja margen generoso para capturar todo el número (1,000.0)
            x_qty1 = min(W, w["x1"] + 60)
    if x_desc0 is not None and x_qty0 is not None:
        # deja espacio claro entre columnas
        x_desc1 = min(x_desc1 or (0.80*W), x_qty0 - 12)
    # heurística si faltan headers
    if x_desc0 is None: x_desc0 = 0.16*W
    if x_desc1 is None: x_desc1 = 0.76*W
    if x_qty0  is None: x_qty0  = 0.78*W
    if x_qty1  is None: x_qty1  = 0.92*W
    if x_desc1 >= x_qty0:
        mid = (x_desc0 + x_qty1)/2.0
        x_desc1 = mid - 12; x_qty0 = mid + 12
    return {"desc": (x_desc0, x_desc1), "qty": (x_qty0, x_qty1)}

def _parse_qty_from_tokens(tokens: List[str]) -> Optional[float]:
    if not tokens: return None
    clean = []
    for t in tokens:
        tt = (t or "").strip()
        if not tt or re.search(r"[A-Za-zÁÉÍÓÚÑ]", tt):  # fuera 1KG, G10%
            continue
        if re.fullmatch(r"[()\-*]|de|pag(?:ina)?|\d+/\d+", tt, flags=re.I):
            continue
        clean.append(tt)
    if not clean: return None
    s = re.sub(r"[^0-9\.,]", "", "".join(clean)).replace(",", "")
    if len(re.sub(r"\D", "", s)) < 3:
        return None
    try:
        v = float(s)
    except ValueError:
        parts = s.split(".")
        if len(parts) >= 2:
            s = "".join(parts[:-1]) + "." + parts[-1]
            try: v = float(s)
            except ValueError: return None
        else:
            return None
    if abs(v - round(v)) < 0.01:
        v = float(int(round(v)))
    return v if 0 < v < 1_000_000 else None

# ==========================
# Extracción de ítems
# ==========================

def _extract_items_catalog_driven(pdf_path: Path) -> List[Dict[str, Any]]:
    """
    Usa el catálogo: detecta filas por 'Código' conocido y toma Cantidad.
    Si la cantidad está en la fila siguiente, la captura.
    """
    results: Dict[str, Dict[str, Any]] = {}
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            y_top, y_bottom = _locate_table_y_bounds(page)
            p = page.within_bbox((0, y_top, page.width, y_bottom)) if (y_top and y_bottom) else page
            xr = _column_x_ranges_from_headers(p)
            x_desc0, x_desc1 = xr["desc"]; x_qty0, x_qty1 = xr["qty"]
            x_code_max = x_desc0 - 10

            words = p.extract_words(use_text_flow=True, keep_blank_chars=False) or []

            # cluster por fila
            rows = []
            for w in sorted(words, key=lambda k: (((k["top"]+k["bottom"])/2.0), k["x0"])):
                cy = (w["top"]+w["bottom"])/2.0
                if not rows: rows.append([w]); continue
                last = rows[-1]
                lcy = sum((x["top"]+x["bottom"])/2.0 for x in last)/len(last)
                if abs(cy - lcy) <= 4.0: last.append(w)
                else: rows.append([w])
            for r in rows: r.sort(key=lambda k: k["x0"])

            bins = []
            for row in rows:
                left, desc, qty, all_tokens = [], [], [], []   # <-- añadimos all_tokens
                for w in row:
                    t = (w.get("text") or "").strip(); x0, x1 = w["x0"], w["x1"]
                    if not t: continue
                    all_tokens.append(t)                        # <-- guardamos todos
                    if x1 <= x_code_max:
                        left.append(t)
                    elif x0 >= x_desc0 and x1 <= x_desc1:
                        if not re.fullmatch(r"(?i)(codigo|c[oó]digo|descripcion|descripción|cantidad)", t):
                            desc.append(t)
                    elif x0 >= x_qty0 and x1 <= x_qty1:
                        qty.append(t)
                bins.append((left, desc, qty, all_tokens))


            i = 0
            while i < len(bins):
                left, desc_t, qty_t, all_tokens = bins[i]
                code = next((tok for tok in all_tokens if _is_code_token(tok)), None)
                if not code:
                        desc_join = " ".join(desc_t).lower()
                        for rx, c in ALIASES.items():
                            if re.search(rx, desc_join):
                                code = c; break
                if code and code in CATALOG:
                    name = CATALOG[code]["name"]
                    qty = _parse_qty_from_tokens(qty_t)
                    # solo mirar la siguiente si la actual NO tuvo qty
                    # y la siguiente NO tiene código ni descripción, pero SÍ tiene tokens en la banda de cantidad
                    if qty is None and i + 1 < len(bins):
                        left_n, desc_n, qty_n = bins[i+1]
                        next_has_code = any(_is_code_token(tok) for tok in left_n)
                        if (not next_has_code) and (len(desc_n) == 0) and (len(qty_n) > 0):
                            qty2 = _parse_qty_from_tokens(qty_n)
                            if qty2 is not None:
                                qty = qty2
                                i += 1  # consumimos la fila siguiente solo si realmente aportó la cantidad

                    results[code] = {"descripcion": name, "cantidad": qty, "unidad": None}
                i += 1

    # Solo los que aparecen; si quieres TODOS con 0, cambia aquí
    return [results[c] for c in CATALOG.keys() if c in results]

# Heurísticas de rescate (por si el catálogo falla)
def _extract_tables_with_pdfplumber(pdf_path: Path) -> List[List[List[str]]]:
    tables: List[List[List[str]]] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            try:
                t = page.extract_table()
                if t: tables.append(t)
            except: pass
            try:
                ts = page.extract_tables()
                if ts: tables.extend([x for x in ts if x])
            except: pass
            try:
                ts = page.extract_tables({"vertical_strategy": "lines","horizontal_strategy": "lines"}) or []
                tables.extend([x for x in ts if x])
            except: pass
            try:
                ts = page.extract_tables({"vertical_strategy": "lines","horizontal_strategy": "lines",
                                          "intersection_y_tolerance": 5,"intersection_x_tolerance": 5}) or []
                tables.extend([x for x in ts if x])
            except: pass
            try:
                ts = page.extract_tables({"vertical_strategy": "text","horizontal_strategy": "text"}) or []
                tables.extend([x for x in ts if x])
            except: pass
    return tables

def _guess_header_row(row: List[str]) -> bool:
    joined = " | ".join([(c or "").strip() for c in row]) if row else ""
    return any(all(h.lower() in joined.lower() for h in hdr) for hdr in CANDIDATE_HEADERS)

def _extract_items_from_tables(tables: List[List[List[str]]]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for t in tables:
        header_seen = False; desc_idx = None; qty_idx = None
        for row in t:
            cols = [(c or "").strip() for c in row]
            if not header_seen and _guess_header_row(cols):
                header_seen = True
                for idx, name in enumerate([c.lower() for c in cols]):
                    if "descrip" in name: desc_idx = idx
                    if "cant" in name:    qty_idx  = idx
                continue
            if header_seen and any(cols):
                desc = cols[desc_idx] if (desc_idx is not None and desc_idx < len(cols)) else \
                       (cols[1] if len(cols) > 1 else cols[0] if cols else None)
                qty = None
                if qty_idx is not None and qty_idx < len(cols):
                    qty = _to_number(cols[qty_idx])
                else:
                    for c in reversed(cols):
                        v = _to_number(c)
                        if v is not None: qty = v; break
                if desc and not re.search(r"(subtotal|total|iva|descuento)", desc, re.I):
                    items.append({"descripcion": desc, "cantidad": qty, "unidad": None})
    return items

def _fallback_items_from_text(pages_text: List[str]) -> List[Dict[str, Any]]:
    full = "\n".join(pages_text)
    start = re.search(r"\bCodigo\b.*\bDescripcion\b.*\bCantidad\b", full, re.I | re.S)
    end   = re.search(r"(Sub-?Totales?|Total\s+a\s+Pagar)", full, re.I)
    segment = full[start.start(): end.start()] if (start and end) else full
    ban = re.compile(r"(iva|descuento|%|precio|unitario|dscto|imp\.?|importe|g5%|g10%)", re.I)
    items: List[Dict[str, Any]] = []
    for line in segment.splitlines():
        s = line.strip()
        if len(s) < 4 or ban.search(s): continue
        m = re.match(r"(.+?)\s+(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?)$", s)
        if not m: continue
        desc = m.group(1).strip()
        qty  = _to_number(m.group(2))
        if re.search(r"[A-Za-zÁÉÍÓÚÑ]", desc) and not re.search(r"(codigo|descripcion|cantidad)$", desc, re.I):
            items.append({"descripcion": desc, "cantidad": qty, "unidad": None})
    return items

# ==========================
# Punto de entrada
# ==========================

def parse_pdf(pdf_path: Path) -> Dict[str, Any]:
    pages_text = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            pages_text.append(page.extract_text() or "")
    meta = _find_meta_fields(pages_text)

    # 0) Catálogo primero
    items = _extract_items_catalog_driven(pdf_path)

    # 1) Rescates si el catálogo fallara
    if not items:
        try:
            tables = _extract_tables_with_pdfplumber(pdf_path)
        except Exception:
            tables = []
        items = _extract_items_from_tables(tables) or _fallback_items_from_text(pages_text)

    return {"meta": meta, "items": items}

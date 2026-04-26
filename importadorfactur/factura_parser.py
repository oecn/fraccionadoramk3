# -*- coding: utf-8 -*-
"""
Extractor simple de facturas (kg) usando pdfplumber.
- Meta: numero, fecha_emision, sucursal, cliente, ruc_cliente, condicion_venta, totales.
- Items: descripcion, kg, precio_unitario, total_linea.

Pensado para PDFs similares al ejemplo "GRANOS 16893RAINV100.PDF".
"""
import re
import datetime as dt
from pathlib import Path
from typing import List, Dict, Any, Optional

SPANISH_MONTHS = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04", "mayo": "05", "junio": "06",
    "julio": "07", "agosto": "08", "septiembre": "09", "setiembre": "09", "octubre": "10",
    "noviembre": "11", "diciembre": "12"
}

def _to_iso(d: str) -> Optional[str]:
    if not d:
        return None
    s1 = d.strip().replace(".", "/").replace("-", "/")
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d"):
        try:
            return dt.datetime.strptime(s1, fmt).date().isoformat()
        except ValueError:
            pass
    m = re.search(r"(\d{1,2})\s+de\s+([A-Za-z]+)\s+de\s+(\d{4})", d, flags=re.I)
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
    s = s.strip().replace(" ", "")
    if not s:
        return None

    # Normaliza miles/decimales comunes en es/py
    if s.count(",") > 1 and "." not in s:
        s_clean = s.replace(",", "")
    elif s.count(".") > 1 and "," not in s:
        s_clean = s.replace(".", "")
    elif s.count(",") == 1 and "." not in s:
        left, right = s.split(",")
        s_clean = left + (right if len(right) >= 3 else "." + right)
    elif s.count(".") == 1 and "," not in s:
        left, right = s.split(".")
        s_clean = left + ("." + right if len(right) <= 3 else right)
    else:
        # tiene ambos separadores; detecta el ultimo como decimal
        if s.rfind(",") > s.rfind("."):
            s_clean = s.replace(".", "")
            s_clean = s_clean.replace(",", ".")
        else:
            s_clean = s.replace(",", "")
    try:
        return float(s_clean)
    except ValueError:
        return None


def _extract_meta(pages_text: List[str]) -> Dict[str, Any]:
    text_all = "\n".join(pages_text)

    proveedor = None
    # Busca una línea tipo "LUQUE, 02 de DICIEMBRE de 2025" o "ITAUGUA, 25 de NOVIEMBRE de 2025"
    for line in text_all.splitlines():
        ln = line.strip()
        if not ln:
            continue
        m = re.match(r"^([A-Za-zÁÉÍÓÚÜÑ .-]{3,}?)[,:]\s*\d{1,2}\s+de\s+[A-Za-zÁÉÍÓÚÜÑ]+", ln, flags=re.I)
        if m:
            proveedor = m.group(1).strip()
            break
    # Fallback: busca tokens de ciudades comunes si no se encontró arriba
    if not proveedor:
        m_city = re.search(r"\b(ITAUGUA|LUQUE|AREGUA)\b", text_all, flags=re.I)
        if m_city:
            proveedor = m_city.group(1).upper()

    numero = None
    m_num = re.search(r"\b(\d{3}-\d{3}-\d{7})\b", text_all)
    if m_num:
        numero = m_num.group(1)
    else:
        m_fact = re.search(r"FACTURA\s*No[:\-\s]*([0-9\-]{5,})", text_all, re.I)
        if m_fact:
            numero = m_fact.group(1).strip()

    fecha_emision = None
    m_fnum = re.search(r"\b(0?\d[\/\-\.]0?\d[\/\-\.]\d{2,4})\b", text_all)
    if m_fnum:
        fecha_emision = _to_iso(m_fnum.group(1))
    if not fecha_emision:
        m_fesp = re.search(r"(\d{1,2}\s+de\s+[A-Za-z]+\s+de\s+\d{4})", text_all, re.I)
        if m_fesp:
            fecha_emision = _to_iso(m_fesp.group(1))

    cliente = None; ruc_cliente = None
    for line in text_all.splitlines():
        if re.search(r"SE.?OR", line, re.I):
            m = re.search(r"SE.*?:\s*(.+?)\s+RUC/CI\s*:\s*([0-9\-\.]+)", line, re.I)
            if m:
                cliente = m.group(1).strip()
                ruc_cliente = m.group(2).strip()
                break
    if not ruc_cliente:
        m = re.search(r"RUC/CI\s*:\s*([0-9\-\.]+)", text_all, re.I)
        if m:
            ruc_cliente = m.group(1).strip()

    # Sucursal no confiable en el PDF de ejemplo; se deja sin parsear por ahora.
    sucursal = None

    condicion = None
    m_cond = re.search(r"COND\.\s*DE\s*VENTA\s*:\s*([A-Z]+)", text_all, re.I)
    if m_cond:
        condicion = m_cond.group(1).strip().upper()
    elif "CREDITO" in text_all.upper():
        condicion = "CREDITO"

    return {
        "numero": numero,
        "fecha_emision": fecha_emision,
        "sucursal": sucursal,
        "cliente": cliente,
        "ruc_cliente": ruc_cliente,
        "condicion_venta": condicion,
        "proveedor": proveedor,
    }


def _extract_items(pages_text: List[str]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    lines = "\n".join(pages_text).splitlines()
    for line in lines:
        s = line.strip()
        if len(s) < 10:
            continue
        m = re.match(r"^(?P<codigo>\d{6,})\s+(?P<qty>[\d\.,]+)\s+(?P<desc>.+?)\s+(?P<unit>[\d\.,]+)\s+(?P<total>[\d\.,]+)$", s)
        if m:
            qty = _to_number(m.group("qty"))
            unit = _to_number(m.group("unit"))
            total = _to_number(m.group("total"))
            desc = m.group("desc").strip()
            items.append({
                "codigo": m.group("codigo"),
                "descripcion": desc,
                "kg": qty,
                "precio_unitario": unit,
                "total_linea": total,
            })
    return items


def _extract_totals(pages_text: List[str]) -> Dict[str, Any]:
    totals = {"total": None, "total_exentas": None, "total_iva5": None, "total_iva10": None}
    for line in pages_text[-1].split("\n") + "\n".join(pages_text).splitlines():
        l = line.strip()
        if "TOTAL A PAGAR" in l.upper():
            nums = re.findall(r"([\d\.,]+)", l)
            if nums:
                totals["total"] = _to_number(nums[-1])
        if "VALOR PARCIAL" in l.upper():
            nums = re.findall(r"([\d\.,]+)", l)
            if len(nums) == 2:
                totals["total_exentas"] = _to_number(nums[0])
                totals["total_iva10"] = _to_number(nums[1])
        if "LIQUIDACION" in l.upper():
            nums = re.findall(r"([\d\.,]+)", l)
            if len(nums) >= 4:
                totals["total_iva5"] = _to_number(nums[1])
                totals["total_iva10"] = _to_number(nums[3])
            elif len(nums) >= 2:
                totals["total_iva5"] = _to_number(nums[0])
                totals["total_iva10"] = _to_number(nums[1])
    return totals


def parse_invoice(pdf_path: Path) -> Dict[str, Any]:
    try:
        import pdfplumber
    except Exception as e:
        raise RuntimeError("Falta pdfplumber. Instala con `pip install pdfplumber`." ) from e

    pages_text: List[str] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            pages_text.append(page.extract_text() or "")
    meta = _extract_meta(pages_text)
    items = _extract_items(pages_text)
    totals = _extract_totals(pages_text)
    meta.update(totals)
    meta["raw_text"] = "\n".join(pages_text)
    return {"meta": meta, "items": items}

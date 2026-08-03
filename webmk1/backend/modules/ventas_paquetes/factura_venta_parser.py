from __future__ import annotations

import datetime as dt
import re
from pathlib import Path
from typing import Any


def _to_number(value: str | None) -> float:
    text = str(value or "").strip().replace(" ", "")
    if not text:
        return 0.0
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif text.count(".") > 1:
        text = text.replace(".", "")
    elif text.count(",") == 1:
        left, right = text.split(",", 1)
        text = left + ("." + right if len(right) <= 2 else right)
    elif text.count(".") == 1:
        left, right = text.split(".", 1)
        text = left + ("." + right if len(right) <= 2 else right)
    return float(text)


def _to_iso(value: str) -> str:
    try:
        return dt.datetime.strptime(value.strip(), "%d/%m/%Y").date().isoformat()
    except ValueError:
        return ""


def _line_value(pattern: str, text: str) -> str:
    match = re.search(pattern, text, re.I)
    return match.group(1).strip() if match else ""


def _clean_item_description(desc_base: str, desc_extra: str) -> str:
    base = re.sub(r"^\d{6,}\s+", "", desc_base).strip()
    extra = re.sub(r"^\d{5,}\s+", "", desc_extra).strip()
    return f"{base} {extra}".strip()


def _norm_text(value: str) -> str:
    replacements = {
        "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U", "Ü": "U", "Ñ": "N",
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ü": "u", "ñ": "n",
    }
    text = "".join(replacements.get(ch, ch) for ch in str(value or ""))
    return re.sub(r"\s+", " ", text).upper().strip()


def _extract_direccion_block(lines: list[str]) -> str:
    for idx, line in enumerate(lines):
        if "DIRECCION" not in _norm_text(line):
            continue
        parts = []
        if idx > 0:
            prev_norm = _norm_text(lines[idx - 1])
            if not any(token in prev_norm for token in ("NOMBRE", "RAZON", "RUC", "CONDICION", "FECHA")):
                parts.append(lines[idx - 1])
        parts.append(re.sub(r"^Direcci[oó]n:\s*", "", line, flags=re.I).strip())
        for next_line in lines[idx + 1:]:
            norm = _norm_text(next_line)
            if norm.startswith(("TELEFONO", "CORREO", "PRECIO", "COD.", "SUBTOTAL")):
                break
            parts.append(next_line)
        return " ".join(part for part in parts if part).strip()
    return ""


def _cliente_from_direccion(lines: list[str]) -> str:
    direccion = _norm_text(_extract_direccion_block(lines))
    if "CAACUPEMI" in direccion or "AREGUA" in direccion:
        return "AREGUA"
    if "ITAUGUA" in direccion:
        return "ITAUGUA"
    if "LUQUE" in direccion:
        return "LUQUE"
    return ""


def parse_factura_venta_pdf(pdf_path: str | Path) -> dict[str, Any]:
    try:
        import pdfplumber
    except Exception as exc:
        raise RuntimeError("Falta pdfplumber para parsear PDFs.") from exc

    with pdfplumber.open(str(pdf_path)) as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    if "FACTURA ELECTR" not in text.upper():
        raise ValueError("El PDF no parece ser una factura electronica de venta.")

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    items: list[dict[str, Any]] = []

    for idx, line in enumerate(lines):
        match = re.match(
            r"^(?P<cantidad>\d+(?:[\.,]\d+)?)\s+"
            r"(?P<precio>[\d\.,]+)\s+"
            r"(?P<descuento>[\d\.,]+)\s+"
            r"(?P<exenta>[\d\.,]+)\s+"
            r"(?P<grav5>[\d\.,]+)\s+"
            r"(?P<grav10>[\d\.,]+)$",
            line,
        )
        if not match or idx == 0:
            continue

        desc_base = lines[idx - 1]
        if not re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", desc_base):
            continue

        desc_extra = ""
        if idx + 1 < len(lines) and re.search(r"\b(FARDO|PAQ|KG|GR)\b", lines[idx + 1], re.I):
            desc_extra = lines[idx + 1]

        exenta = _to_number(match.group("exenta"))
        grav5 = _to_number(match.group("grav5"))
        grav10 = _to_number(match.group("grav10"))
        items.append(
            {
                "descripcion": _clean_item_description(desc_base, desc_extra),
                "cantidad": _to_number(match.group("cantidad")),
                "precio_unitario_gs": _to_number(match.group("precio")),
                "iva": 5 if grav5 else (10 if grav10 else 0),
                "total_linea_gs": exenta + grav5 + grav10,
                "gravada5_gs": grav5,
                "gravada10_gs": grav10,
                "exenta_gs": exenta,
            }
        )

    subtotal = re.search(r"SUBTOTAL:\s*([\d\.,]+)\s+([\d\.,]+)\s+([\d\.,]+)\s+([\d\.,]+)", text, re.I)
    iva = re.search(
        r"LIQUIDACI[ÓO]N\s+IVA:\s*\(5%\)\s*([\d\.,]+)\s*\(10%\)\s*([\d\.,]+)\s*TOTAL\s+IVA:\s*([\d\.,]+)",
        text,
        re.I,
    )

    cliente = _cliente_from_direccion(lines) or _line_value(r"Nombre\s+o\s+Raz[óo]n\s+Social:\s*(.+)", text)

    return {
        "numero": _line_value(r"FACTURA\s+ELECTR[ÓO]NICA[\s\S]{0,100}?(\d{3}-\d{3}-\d{7})", text),
        "fecha_emision": _to_iso(_line_value(r"Fecha\s+de\s+emisi[óo]n:\s*(\d{1,2}/\d{1,2}/\d{4})", text)),
        "cliente": cliente,
        "ruc_cliente": _line_value(r"RUC/Documento\s+de\s+Identidad\s+N[°º]?:\s*([0-9\-\.]+)", text),
        "condicion_venta": "CREDITO" if re.search(r"Condici[óo]n\s+de\s+venta:.*Cr[eé]dito", text, re.I) else "CONTADO",
        "gravada5_gs": _to_number(subtotal.group(3)) if subtotal else sum(item["gravada5_gs"] for item in items),
        "gravada10_gs": _to_number(subtotal.group(4)) if subtotal else sum(item["gravada10_gs"] for item in items),
        "iva5_gs": _to_number(iva.group(1)) if iva else 0,
        "iva10_gs": _to_number(iva.group(2)) if iva else 0,
        "total_iva_gs": _to_number(iva.group(3)) if iva else 0,
        "total_gs": _to_number(_line_value(r"TOTAL\s+DE\s+LA\s+OPERACI[ÓO]N:\s*([\d\.,]+)", text)),
        "items": items,
    }

# -*- coding: utf-8 -*-
from __future__ import annotations

import datetime as dt
import re
from pathlib import Path
from typing import Any


def _to_number(value: str | None) -> float | None:
    if not value:
        return None
    clean = value.strip().replace(" ", "")
    if not clean:
        return None
    if "," in clean and "." in clean:
        clean = clean.replace(".", "").replace(",", ".")
    elif "," in clean:
        clean = clean.replace(",", "")
    elif clean.count(".") > 1:
        clean = clean.replace(".", "")
    try:
        return float(clean)
    except ValueError:
        return None


def _to_iso(value: str | None) -> str | None:
    if not value:
        return None
    raw = value.strip().replace("-", "/").replace(".", "/")
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d"):
        try:
            return dt.datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def _extract_text(pdf_path: Path) -> str:
    try:
        import pdfplumber
        with pdfplumber.open(str(pdf_path)) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception:
        pass

    try:
        from pypdf import PdfReader
    except Exception:
        try:
            from PyPDF2 import PdfReader  # type: ignore
        except Exception as exc:
            raise RuntimeError("Falta pypdf. Instala con `pip install pypdf`.") from exc

    reader = PdfReader(str(pdf_path))
    return "\n".join(page.extract_text() or "" for page in reader.pages)


def _extract_facturas(text: str) -> list[dict[str, Any]]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    invoice_nos: list[str] = []
    amounts: list[float] = []

    for line in lines:
        for invoice_no in re.findall(r"\b\d{3}-\d{3}-\d{7}\b", line):
            if invoice_no not in invoice_nos:
                invoice_nos.append(invoice_no)

        if re.fullmatch(r"[\d,\.]+", line):
            amount = _to_number(line)
            if amount and amount > 0:
                amounts.append(amount)

    recibo_no = _extract_recibo_no(text)
    invoice_nos = [nro for nro in invoice_nos if nro != recibo_no]

    if len(amounts) > len(invoice_nos):
        amounts = amounts[: len(invoice_nos)]

    items: list[dict[str, Any]] = []
    for idx, invoice_no in enumerate(invoice_nos, start=1):
        monto = amounts[idx - 1] if idx - 1 < len(amounts) else None
        items.append({"linea": idx, "factura": invoice_no, "monto": monto})
    return items


def _extract_recibo_no(text: str) -> str | None:
    matches = re.findall(r"\b\d{3}-\d{3}-\d{7}\b", text)
    if not matches:
        return None
    if "RECIBO DE DINERO" in text.upper():
        return matches[-1]
    return matches[0]


def parse_receipt(pdf_path: Path) -> dict[str, Any]:
    text = _extract_text(pdf_path)
    one_line = re.sub(r"\s+", " ", text)

    cliente = None
    ruc_cliente = None
    m_cliente = re.search(r"Recib[ií]\s+de\s+(.+?)\s+RUC:\s*([0-9\-]+)", one_line, flags=re.I)
    if m_cliente:
        cliente = m_cliente.group(1).strip()
        ruc_cliente = m_cliente.group(2).strip()

    empresa_ruc = None
    m_ruc = re.search(r"\bRUC\s+([0-9\-]+)", one_line, flags=re.I)
    if m_ruc:
        empresa_ruc = m_ruc.group(1).strip()

    fecha = None
    m_fecha = re.search(r"Fecha\s+(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})", one_line, flags=re.I)
    if m_fecha:
        fecha = _to_iso(m_fecha.group(1))

    cheque_no = ""
    banco = ""
    m_cheque = re.search(r"cheque\s+GS\.\s+No\.\s*([0-9]+)\s+Banco\s+(.+?)(?:\s+en concepto|\s+Fecha|$)", one_line, flags=re.I)
    if m_cheque:
        cheque_no = m_cheque.group(1).strip()
        banco = m_cheque.group(2).strip()

    total = None
    m_total = re.search(r"Firma\s+([\d,\.]+)", one_line, flags=re.I)
    if m_total:
        total = _to_number(m_total.group(1))
    if total is None:
        nums = [_to_number(x) for x in re.findall(r"[\d,\.]+", text)]
        nums = [x for x in nums if x and x > 100000]
        if nums:
            total = max(nums)

    return {
        "meta": {
            "numero": _extract_recibo_no(text),
            "ruc_emisor": empresa_ruc,
            "cliente": cliente,
            "ruc_cliente": ruc_cliente,
            "fecha": fecha,
            "cheque_no": cheque_no,
            "banco": banco,
            "total": total,
            "raw_text": text,
        },
        "items": _extract_facturas(text),
    }

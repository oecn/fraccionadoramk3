from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import pdfplumber
except Exception as exc:  # pragma: no cover
    raise RuntimeError("Falta pdfplumber. Instala la dependencia para parsear PDFs.") from exc


MONTHS = {
    "ENERO": "01",
    "FEBRERO": "02",
    "MARZO": "03",
    "ABRIL": "04",
    "MAYO": "05",
    "JUNIO": "06",
    "JULIO": "07",
    "AGOSTO": "08",
    "SEPTIEMBRE": "09",
    "SETIEMBRE": "09",
    "OCTUBRE": "10",
    "NOVIEMBRE": "11",
    "DICIEMBRE": "12",
}


def _extract_pages_text(pdf_path: Path) -> List[str]:
    with pdfplumber.open(pdf_path) as pdf:
        return [(page.extract_text() or "").strip() for page in pdf.pages]


def _clean_number_token(value: str) -> str:
    return re.sub(r"[^\d,.\-]", "", (value or "").strip())


def _to_number(value: str) -> Optional[float]:
    token = _clean_number_token(value)
    if not token:
        return None

    if re.fullmatch(r"-?\d{1,3}(?:[.,]\d{3})+", token):
        return float(re.sub(r"[.,]", "", token))

    if token.count(",") + token.count(".") > 1:
        compact = re.sub(r"[.,]", "", token)
        if compact and compact not in {"-", "."}:
            return float(compact)

    token = token.replace(",", ".")
    try:
        return float(token)
    except ValueError:
        return None


def _to_int_amount(value: str) -> Optional[int]:
    number = _to_number(value)
    if number is None:
        return None
    return int(round(number))


def _find_first(pattern: str, text: str, flags: int = 0, group: int = 1) -> Optional[str]:
    match = re.search(pattern, text, flags)
    return match.group(group).strip() if match else None


def _find_period(text: str) -> Optional[str]:
    period_raw = _find_first(r"\b([A-ZÁÉÍÓÚ]+/\d{4})\b", text, flags=re.I)
    if not period_raw or "/" not in period_raw:
        return None

    month_name, year = period_raw.upper().split("/", 1)
    month = MONTHS.get(month_name)
    if not month:
        return None
    return f"{year}-{month}"


def _find_consult_date(text: str) -> Optional[str]:
    return _find_first(r"\b(\d{2}/\d{2}/\d{4})\b", text)


def _find_lines_with_numbers(text: str) -> List[str]:
    return [line.strip() for line in text.splitlines() if line.strip() and re.search(r"\d", line)]


def _find_amount_from_line(pattern: str, lines: List[str]) -> Optional[int]:
    for line in lines:
        if not re.search(pattern, line, flags=re.I):
            continue
        match = re.search(r"(-?\d[\d.,]*)\s*$", line)
        if match:
            amount = _to_int_amount(match.group(1))
            if amount is not None:
                return amount
    return None


def _find_amounts(lines: List[str]) -> Dict[str, Optional[int]]:
    total_pagar = _find_amount_from_line(r"total\s+a\s+pagar", lines)
    total_aporte = _find_amount_from_line(r"total\s+aporte", lines)
    total_mora = _find_amount_from_line(r"total\s+mora", lines)
    total_multa = _find_amount_from_line(r"total\s+multa", lines)

    if total_pagar is None:
        parts = [value for value in (total_aporte, total_mora, total_multa) if value is not None]
        if parts:
            total_pagar = sum(parts)

    return {
        "total_a_pagar": total_pagar,
        "total_aporte": total_aporte,
        "total_mora": total_mora,
        "total_multa": total_multa,
    }


def parse_ips_extract(pdf_path: str | Path) -> Dict[str, Any]:
    path = Path(pdf_path)
    pages_text = _extract_pages_text(path)
    text_all = "\n".join(pages_text)
    lines = _find_lines_with_numbers(text_all)
    amounts = _find_amounts(lines)

    employer_line = _find_first(
        r"\b\d{4}-\d{2}-\d{5}\s+(.+?)\s+Consultas:\s+Vencimiento\b",
        text_all,
        flags=re.I,
    )

    return {
        "pdf_path": str(path),
        "page_count": len(pages_text),
        "extracto_nro": _find_first(r"\bNro\.:\s*([0-9]+)\b", text_all, flags=re.I),
        "token": _find_first(r"\bToken:\s*([0-9]+)\b", text_all, flags=re.I),
        "patronal": employer_line,
        "ruc": _find_first(r"\bRUC:\s*([0-9]+)\b", text_all, flags=re.I),
        "periodo": _find_period(text_all),
        "fecha_consulta": _find_consult_date(text_all),
        "raw_text": text_all,
        **amounts,
    }


def main() -> None:
    default_pdf = Path(__file__).with_name("EXTRACTO  IPS  MARZO 2026 GRANOS CENTRAL.pdf")
    result = parse_ips_extract(default_pdf)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

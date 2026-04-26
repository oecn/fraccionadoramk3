from __future__ import annotations

import calendar
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

try:
    import pdfplumber
except Exception as exc:  # pragma: no cover
    raise RuntimeError("Falta pdfplumber. Instala la dependencia para parsear PDFs.") from exc


MONTHS = {
    "ENERO": 1,
    "FEBRERO": 2,
    "MARZO": 3,
    "ABRIL": 4,
    "MAYO": 5,
    "JUNIO": 6,
    "JULIO": 7,
    "AGOSTO": 8,
    "SEPTIEMBRE": 9,
    "SETIEMBRE": 9,
    "OCTUBRE": 10,
    "NOVIEMBRE": 11,
    "DICIEMBRE": 12,
}


@dataclass
class IPSExtractData:
    pdf_path: str
    page_count: int
    extracto_nro: Optional[str]
    token: Optional[str]
    patronal: Optional[str]
    ruc: Optional[str]
    periodo: Optional[str]
    fecha_consulta: Optional[str]
    fecha_impresion: Optional[str]
    total_aporte: Optional[int]
    total_mora: Optional[int]
    total_multa: Optional[int]
    total_a_pagar: Optional[int]
    raw_text: str

    @property
    def fecha_gasto(self) -> Optional[str]:
        if not self.fecha_impresion:
            return None
        dd, mm, yyyy = self.fecha_impresion.split("/")
        return f"{int(yyyy):04d}-{int(mm):02d}-{int(dd):02d}"

    @property
    def descripcion_gasto(self) -> str:
        period_label = self.periodo_display or (self.periodo or "SIN PERIODO")
        patronal = self.patronal or "SIN PATRONAL"
        parts = [f"IPS {period_label}", patronal]
        if self.ruc:
            parts.append(f"RUC {self.ruc}")
        if self.fecha_consulta:
            parts.append(f"Consulta {self.fecha_consulta}")
        if self.fecha_impresion:
            parts.append(f"Impresión {self.fecha_impresion}")
        return " | ".join(parts)

    @property
    def periodo_display(self) -> Optional[str]:
        if not self.periodo:
            return None
        year_s, month_s = self.periodo.split("-", 1)
        month_num = int(month_s)
        month_name = next(
            (name for name, number in MONTHS.items() if number == month_num and name != "SETIEMBRE"),
            None,
        )
        return f"{month_name}/{year_s}" if month_name else self.periodo

    @property
    def periodo_yyyymm(self) -> Optional[str]:
        if not self.periodo:
            return None
        year_s, month_s = self.periodo.split("-", 1)
        return f"{int(year_s):04d}-{int(month_s):02d}"

    @property
    def fecha_fin_periodo(self) -> Optional[str]:
        if not self.periodo:
            return None
        year_s, month_s = self.periodo.split("-", 1)
        last_day = calendar.monthrange(int(year_s), int(month_s))[1]
        return f"{int(year_s):04d}-{int(month_s):02d}-{last_day:02d}"


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
    month_num = MONTHS.get(month_name)
    if not month_num:
        return None
    return f"{int(year):04d}-{month_num:02d}"


def _find_lines_with_numbers(text: str) -> List[str]:
    return [line.strip() for line in text.splitlines() if line.strip() and re.search(r"\d", line)]


def _find_fecha_impresion(text: str) -> Optional[str]:
    match = re.search(r"\b(\d{2}/\d{2}/\d{4})\s+\d{2}:\d{2}:\d{2}\b", text)
    if match:
        return match.group(1)
    return None


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


def parse_ips_extract(pdf_path: str | Path) -> IPSExtractData:
    path = Path(pdf_path)
    pages_text = _extract_pages_text(path)
    text_all = "\n".join(pages_text)
    lines = _find_lines_with_numbers(text_all)

    total_a_pagar = _find_amount_from_line(r"total\s+a\s+pagar", lines)
    total_aporte = _find_amount_from_line(r"total\s+aporte", lines)
    total_mora = _find_amount_from_line(r"total\s+mora", lines)
    total_multa = _find_amount_from_line(r"total\s+multa", lines)

    if total_a_pagar is None:
        parts = [value for value in (total_aporte, total_mora, total_multa) if value is not None]
        if parts:
            total_a_pagar = sum(parts)

    employer_line = _find_first(
        r"\b\d{4}-\d{2}-\d{5}\s+(.+?)\s+Consultas:\s+Vencimiento\b",
        text_all,
        flags=re.I,
    )

    return IPSExtractData(
        pdf_path=str(path),
        page_count=len(pages_text),
        extracto_nro=_find_first(r"\bNro\.:\s*([0-9]+)\b", text_all, flags=re.I),
        token=_find_first(r"\bToken:\s*([0-9]+)\b", text_all, flags=re.I),
        patronal=employer_line,
        ruc=_find_first(r"\bRUC:\s*([0-9]+)\b", text_all, flags=re.I),
        periodo=_find_period(text_all),
        fecha_consulta=_find_first(r"\b(\d{2}/\d{2}/\d{4})\b", text_all),
        fecha_impresion=_find_fecha_impresion(text_all),
        total_aporte=total_aporte,
        total_mora=total_mora,
        total_multa=total_multa,
        total_a_pagar=total_a_pagar,
        raw_text=text_all,
    )

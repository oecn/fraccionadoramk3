from __future__ import annotations

import csv
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import List, Optional

ROOT_DIR = Path(__file__).resolve().parents[2]
GCMK8_DIR = ROOT_DIR / "GCMK8"
if str(GCMK8_DIR) not in sys.path:
    sys.path.insert(0, str(GCMK8_DIR))

from rrhh.rrhh_repo import RRHHRepo


@dataclass
class SalaryAdvanceRow:
    employee_id: Optional[int]
    documento: str
    funcionario: Optional[str]
    cuenta_destino: str
    concepto: str
    monto: float
    confirmado: bool


@dataclass
class SalaryAdvanceFile:
    file_path: str
    suggested_date: Optional[str]
    row_count: int
    total_amount: float
    conceptos: List[str]
    rows: List[SalaryAdvanceRow]


def _to_float(value: str) -> float:
    txt = (value or "").strip()
    if not txt:
        return 0.0
    if "," in txt and "." in txt:
        txt = txt.replace(".", "").replace(",", ".")
    elif "," in txt:
        txt = txt.replace(",", ".")
    return float(txt)


def _to_bool_no_si(value: str) -> bool:
    txt = (value or "").strip().upper()
    if txt in {"SI", "S", "YES", "Y", "TRUE", "1"}:
        return True
    if txt in {"NO", "N", "FALSE", "0"}:
        return False
    return False


def _suggest_date_from_filename(path: Path) -> Optional[str]:
    stem = path.stem.strip()
    if len(stem) == 6 and stem.isdigit():
        dd = int(stem[:2])
        mm = int(stem[2:4])
        yy = int(stem[4:6])
        yyyy = 2000 + yy
        if 1 <= dd <= 31 and 1 <= mm <= 12:
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}"
    return None


def parse_salary_advance_txt(file_path: str | Path) -> SalaryAdvanceFile:
    path = Path(file_path)
    rows: List[SalaryAdvanceRow] = []
    repo = RRHHRepo()

    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.reader(fh)
            for raw in reader:
                if not raw or not any(str(col).strip() for col in raw):
                    continue
                if len(raw) < 5:
                    raise ValueError(f"Línea inválida en {path.name}: se esperaban 5 columnas y llegaron {len(raw)}")

                documento, cuenta_destino, concepto, monto_txt, confirmado_txt = [str(col).strip() for col in raw[:5]]
                emp = repo.get_employee_by_ci(documento)
                rows.append(
                    SalaryAdvanceRow(
                        employee_id=(int(emp[0]) if emp else None),
                        documento=documento,
                        funcionario=(emp[1] if emp else None),
                        cuenta_destino=cuenta_destino,
                        concepto=concepto,
                        monto=_to_float(monto_txt),
                        confirmado=_to_bool_no_si(confirmado_txt),
                    )
                )
    finally:
        repo.close()

    conceptos = sorted({row.concepto for row in rows})
    total_amount = sum(row.monto for row in rows)
    return SalaryAdvanceFile(
        file_path=str(path),
        suggested_date=_suggest_date_from_filename(path),
        row_count=len(rows),
        total_amount=total_amount,
        conceptos=conceptos,
        rows=rows,
    )


def to_json_dict(parsed: SalaryAdvanceFile) -> dict:
    return {
        "file_path": parsed.file_path,
        "suggested_date": parsed.suggested_date,
        "row_count": parsed.row_count,
        "total_amount": parsed.total_amount,
        "conceptos": parsed.conceptos,
        "rows": [asdict(row) for row in parsed.rows],
    }


def main() -> None:
    default_path = Path(__file__).resolve().parent.parent / "200125.txt"
    parsed = parse_salary_advance_txt(default_path)
    print(json.dumps(to_json_dict(parsed), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

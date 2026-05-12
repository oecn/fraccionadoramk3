from __future__ import annotations

import re
import sys
from datetime import datetime
from pathlib import Path

from core.database import connection
from modules.gastos_egresos.schemas import (
    CheckStatus,
    ExpenseCreate,
    ExpenseRow,
    ExpenseSummary,
    ImportResult,
    IpsParseResult,
    RrhhImportRow,
    RrhhParseResult,
)


ROOT_DIR = Path(__file__).resolve().parents[4]
GCMK8_DIR = ROOT_DIR / "GCMK8"
SALARY_PARSER_DIR = ROOT_DIR / "parseo IPS" / "backend"
for _path in (GCMK8_DIR, SALARY_PARSER_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from ips_pdf_parser import parse_ips_extract  # type: ignore  # noqa: E402
from rrhh.rrhh_repo import RRHHRepo  # type: ignore  # noqa: E402
from salary_txt_parser import parse_salary_advance_txt, to_json_dict  # type: ignore  # noqa: E402


TIPOS = ["Caja chica", "CAJA CHICA", "IPS", "Pago a personal", "Adelanto de salario", "Pago a profesionales", "Pagos varios"]
FORMAS_PAGO = ["Efectivo", "Transferencia", "Cheque", "Homebanking"]


class GastosEgresosRepository:
    def _ensure_schema(self, cn) -> None:
        cn.execute(
            """
            CREATE TABLE IF NOT EXISTS expenses(
                id BIGSERIAL PRIMARY KEY,
                ts TIMESTAMP NOT NULL,
                tipo TEXT NOT NULL,
                descripcion TEXT,
                monto_gs REAL NOT NULL,
                nro_factura TEXT,
                forma_pago TEXT,
                referencia_pago TEXT
            )
            """
        )
        cn.execute(
            """
            CREATE TABLE IF NOT EXISTS dashboard_used_checks(
                id BIGSERIAL PRIMARY KEY,
                chequera_id TEXT NOT NULL,
                cheque_no TEXT NOT NULL,
                serie TEXT NOT NULL DEFAULT '',
                referencia TEXT,
                payment_group_id TEXT,
                used_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_dash_used_checks_unique
            ON dashboard_used_checks(chequera_id, cheque_no, serie)
            """
        )

    def summary(self, desde: str = "", hasta: str = "") -> ExpenseSummary:
        where: list[str] = []
        params: list[str] = []
        if desde:
            where.append("ts::date >= %s")
            params.append(desde)
        if hasta:
            where.append("ts::date <= %s")
            params.append(hasta)
        sql = """
            SELECT id, CAST(ts AS TEXT) AS fecha, tipo, COALESCE(descripcion, '') AS descripcion,
                   COALESCE(monto_gs, 0) AS monto_gs, COALESCE(nro_factura, '') AS nro_factura,
                   COALESCE(forma_pago, 'Efectivo') AS forma_pago,
                   COALESCE(referencia_pago, '') AS referencia_pago
            FROM expenses
        """
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY ts DESC, id DESC LIMIT 500"
        with connection("fraccionadora") as cn:
            self._ensure_schema(cn)
            rows = cn.execute(sql, params).fetchall()
        items = [
            ExpenseRow(
                id=int(r["id"]),
                fecha=str(r["fecha"] or "")[:10],
                tipo=r["tipo"] or "",
                descripcion=r["descripcion"] or "",
                monto_gs=float(r["monto_gs"] or 0),
                nro_factura=r["nro_factura"] or "",
                forma_pago=r["forma_pago"] or "Efectivo",
                referencia_pago=r["referencia_pago"] or "",
            )
            for r in rows
        ]
        return ExpenseSummary(rows=items, total_gs=sum(r.monto_gs for r in items), tipos=TIPOS, formas_pago=FORMAS_PAGO)

    def create(self, payload: ExpenseCreate) -> ExpenseRow:
        fecha = (payload.fecha or "").strip()
        try:
            datetime.strptime(fecha, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("Formato de fecha invalido. Use YYYY-MM-DD.") from exc
        forma = (payload.forma_pago or "Efectivo").strip() or "Efectivo"
        referencia = (payload.referencia_pago or "").strip()
        if forma.lower() in {"cheque", "transferencia", "homebanking"} and not referencia:
            raise ValueError("Debe cargar referencia/comprobante para esa forma de pago.")
        if forma.lower() == "efectivo":
            referencia = ""
        check = None
        if forma.lower() == "cheque":
            check = self.check_status_from_reference(referencia)
            if not check.available:
                raise ValueError(check.message)
            referencia = check.referencia

        with connection("fraccionadora") as cn:
            self._ensure_schema(cn)
            row = cn.execute(
                """
                INSERT INTO expenses(ts, tipo, descripcion, monto_gs, nro_factura, forma_pago, referencia_pago)
                VALUES(%s, %s, %s, %s, %s, %s, %s)
                RETURNING id, CAST(ts AS TEXT) AS fecha
                """,
                (
                    fecha,
                    payload.tipo.strip(),
                    payload.descripcion.strip(),
                    float(payload.monto_gs),
                    payload.nro_factura.strip(),
                    forma,
                    referencia,
                ),
            ).fetchone()
            expense_id = int(row["id"])
            if check and check.available:
                cn.execute(
                    """
                    INSERT INTO dashboard_used_checks(chequera_id, cheque_no, serie, referencia, payment_group_id, used_ts)
                    VALUES(%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT(chequera_id, cheque_no, serie) DO UPDATE SET
                        referencia=excluded.referencia,
                        payment_group_id=excluded.payment_group_id,
                        used_ts=CURRENT_TIMESTAMP
                    """,
                    (check.chequera_id, check.cheque_no, check.serie, referencia, f"expense:{expense_id}"),
                )
        return ExpenseRow(
            id=expense_id,
            fecha=fecha,
            tipo=payload.tipo.strip(),
            descripcion=payload.descripcion.strip(),
            monto_gs=float(payload.monto_gs),
            nro_factura=payload.nro_factura.strip(),
            forma_pago=forma,
            referencia_pago=referencia,
        )

    def expense_exists_by_ref(self, tipo: str, nro_factura: str) -> bool:
        if not (nro_factura or "").strip():
            return False
        with connection("fraccionadora") as cn:
            self._ensure_schema(cn)
            row = cn.execute(
                """
                SELECT 1
                FROM expenses
                WHERE tipo = %s AND COALESCE(nro_factura, '') = %s
                LIMIT 1
                """,
                ((tipo or "").strip(), (nro_factura or "").strip()),
            ).fetchone()
        return row is not None

    def parse_ips_pdf(self, pdf_path: str | Path) -> IpsParseResult:
        data = parse_ips_extract(pdf_path)
        if not data.total_a_pagar:
            raise ValueError("No se encontro el monto 'Total a pagar' en el PDF IPS.")
        if not data.periodo:
            raise ValueError("No se pudo identificar el periodo del extracto IPS.")
        fecha = data.fecha_gasto or data.fecha_fin_periodo
        if not fecha:
            raise ValueError("No se pudo identificar la fecha del gasto IPS.")
        nro_ref = data.extracto_nro or data.token or data.periodo_yyyymm or ""
        referencia = data.token or nro_ref or data.periodo_yyyymm or "IPS"
        return IpsParseResult(
            fecha=fecha,
            tipo="IPS",
            descripcion=data.descripcion_gasto,
            monto_gs=float(data.total_a_pagar),
            nro_factura=nro_ref,
            forma_pago="Homebanking",
            referencia_pago=referencia,
            periodo=data.periodo or "",
            periodo_display=data.periodo_display or "",
            duplicate=self.expense_exists_by_ref("IPS", nro_ref),
        )

    def import_ips(self, payload: IpsParseResult) -> ImportResult:
        if self.expense_exists_by_ref("IPS", payload.nro_factura):
            return ImportResult(message=f"El extracto IPS {payload.nro_factura} ya esta cargado.", inserted=0, skipped=1)
        self.create(
            ExpenseCreate(
                fecha=payload.fecha,
                tipo="IPS",
                descripcion=payload.descripcion,
                monto_gs=payload.monto_gs,
                nro_factura=payload.nro_factura,
                forma_pago=payload.forma_pago or "Homebanking",
                referencia_pago=payload.referencia_pago or payload.nro_factura,
            )
        )
        return ImportResult(message="Gasto IPS importado.", inserted=1, skipped=0)

    def parse_rrhh_txt(self, txt_path: str | Path, file_name: str) -> RrhhParseResult:
        parsed = to_json_dict(parse_salary_advance_txt(txt_path))
        rows = [
            RrhhImportRow(
                employee_id=row.get("employee_id"),
                documento=str(row.get("documento") or ""),
                funcionario=str(row.get("funcionario") or ""),
                cuenta_destino=str(row.get("cuenta_destino") or ""),
                concepto=str(row.get("concepto") or ""),
                monto=float(row.get("monto") or 0),
                confirmado=bool(row.get("confirmado")),
            )
            for row in (parsed.get("rows") or [])
        ]
        return RrhhParseResult(
            file_name=file_name,
            fecha=self._suggest_date_from_filename(file_name) or parsed.get("suggested_date") or datetime.today().strftime("%Y-%m-%d"),
            row_count=int(parsed.get("row_count") or len(rows)),
            total_amount=float(parsed.get("total_amount") or 0),
            conceptos=[str(item) for item in (parsed.get("conceptos") or [])],
            unresolved_count=sum(1 for row in rows if not row.employee_id),
            rows=rows,
        )

    def import_rrhh(self, payload: RrhhParseResult) -> ImportResult:
        if payload.unresolved_count:
            raise ValueError("Hay filas con CI no vinculado a funcionarios.")
        try:
            datetime.strptime(payload.fecha, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("Formato de fecha invalido. Use YYYY-MM-DD.") from exc

        source_name = (payload.file_name or "sin_archivo").strip() or "sin_archivo"
        rrhh_repo = RRHHRepo()
        inserted = 0
        skipped = 0
        try:
            for idx, row in enumerate(payload.rows, start=1):
                if not row.employee_id or row.monto <= 0:
                    skipped += 1
                    continue
                concepto = (row.concepto or "Pago a personal").strip() or "Pago a personal"
                if not rrhh_repo.movement_exists(row.employee_id, payload.fecha, concepto, row.monto, source_name):
                    rrhh_repo.insert_movement(
                        employee_id=row.employee_id,
                        fecha=payload.fecha,
                        concepto=concepto,
                        monto_gs=row.monto,
                        confirmado=row.confirmado,
                        cuenta_destino=row.cuenta_destino,
                        documento_ci=row.documento,
                        source_file=source_name,
                    )
                expense_type = self._expense_type_from_rrhh_concept(concepto)
                expense_ref = f"{source_name}:{row.documento}:{idx}"
                if self.expense_exists_by_ref(expense_type, expense_ref):
                    skipped += 1
                    continue
                self.create(
                    ExpenseCreate(
                        fecha=payload.fecha,
                        tipo=expense_type,
                        descripcion=f"{concepto} - {row.funcionario or row.documento}",
                        monto_gs=row.monto,
                        nro_factura=expense_ref,
                        forma_pago="Homebanking",
                        referencia_pago=row.cuenta_destino or source_name,
                    )
                )
                inserted += 1
        finally:
            rrhh_repo.close()
        return ImportResult(message="Importacion RRHH finalizada.", inserted=inserted, skipped=skipped)

    def check_status_from_reference(self, value: str) -> CheckStatus:
        parsed = self._parse_check_reference(value)
        if not parsed:
            return CheckStatus(available=False, found=False, used=False, message="Ingrese cheque como SERIE-NUMERO. Ej: CS-742127.")
        serie, cheque_no, referencia = parsed
        return self.check_status(serie, cheque_no, referencia)

    def check_status(self, serie: str, cheque_no: str, referencia: str | None = None) -> CheckStatus:
        serie_txt = re.sub(r"[^A-Za-z0-9]+", "", str(serie or "")).upper()
        cheque_txt = re.sub(r"\D+", "", str(cheque_no or ""))
        if not serie_txt or not cheque_txt:
            return CheckStatus(available=False, found=False, used=False, message="Ingrese serie y numero de cheque.")
        with connection("fraccionadora") as cn:
            self._ensure_schema(cn)
            row = cn.execute(
                """
                SELECT c.chequera_id
                FROM bank_checkbooks c
                WHERE UPPER(TRIM(COALESCE(c.serie, ''))) = %s
                  AND %s BETWEEN c.nro_inicio AND c.nro_fin
                LIMIT 1
                """,
                (serie_txt, int(cheque_txt)),
            ).fetchone()
            if not row:
                return CheckStatus(available=False, found=False, used=False, message="Cheque no cargado en chequeras.")
            used = cn.execute(
                """
                SELECT 1 FROM dashboard_used_checks
                WHERE chequera_id = %s AND cheque_no = %s AND UPPER(TRIM(COALESCE(serie, ''))) = %s
                LIMIT 1
                """,
                (row["chequera_id"], cheque_txt, serie_txt),
            ).fetchone()
        ref = referencia or f"Cheque: {cheque_txt} | Serie: {serie_txt}"
        if used:
            return CheckStatus(
                available=False,
                found=True,
                used=True,
                message="Cheque cargado, pero ya usado.",
                chequera_id=row["chequera_id"],
                cheque_no=cheque_txt,
                serie=serie_txt,
                referencia=ref,
            )
        return CheckStatus(
            available=True,
            found=True,
            used=False,
            message="Cheque cargado y disponible.",
            chequera_id=row["chequera_id"],
            cheque_no=cheque_txt,
            serie=serie_txt,
            referencia=ref,
        )

    def _parse_check_reference(self, value: str) -> tuple[str, str, str] | None:
        txt = str(value or "").strip()
        m_full = re.search(r"cheque\s*:\s*(\d+)\s*\|\s*serie\s*:\s*([A-Za-z0-9]+)", txt, flags=re.I)
        if m_full:
            cheque_no = re.sub(r"\D+", "", m_full.group(1))
            serie = re.sub(r"[^A-Za-z0-9]+", "", m_full.group(2)).upper()
            return serie, cheque_no, f"Cheque: {cheque_no} | Serie: {serie}"
        m = re.match(r"^\s*([A-Za-z0-9]+)\s*[-/ :]\s*(\d+)\s*$", txt)
        if not m:
            return None
        serie = re.sub(r"[^A-Za-z0-9]+", "", m.group(1)).upper()
        cheque_no = re.sub(r"\D+", "", m.group(2))
        return serie, cheque_no, f"Cheque: {cheque_no} | Serie: {serie}"

    def _expense_type_from_rrhh_concept(self, concepto: str) -> str:
        txt = (concepto or "").strip().lower()
        if "adelanto" in txt:
            return "Adelanto de salario"
        return "Pago a personal"

    def _suggest_date_from_filename(self, file_name: str) -> str | None:
        stem = Path(file_name or "").stem.strip()
        if len(stem) != 6 or not stem.isdigit():
            return None
        dd = int(stem[:2])
        mm = int(stem[2:4])
        yy = int(stem[4:6])
        if 1 <= dd <= 31 and 1 <= mm <= 12:
            return f"{2000 + yy:04d}-{mm:02d}-{dd:02d}"
        return None

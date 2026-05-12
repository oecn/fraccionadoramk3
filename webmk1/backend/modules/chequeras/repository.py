from __future__ import annotations

import re
import unicodedata
from datetime import datetime

from core.database import connection
from modules.chequeras.schemas import BankCreate, BankRow, CheckbookCreate, CheckbookRow, ChequerasSummary, UsedCheckRow


def _norm_token(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


def _bank_id(banco_nombre: str, nro_cuenta: str) -> str:
    return f"banco-{_norm_token(banco_nombre) or 'sin-banco'}-{_norm_token(nro_cuenta) or 'sin-cuenta'}"


def _chequera_id(bank_id: str, formato: str, tipo: str, serie: str, inicio: int, fin: int) -> str:
    return (
        f"{bank_id}-chequera-{_norm_token(formato) or 'sin-formato'}-"
        f"{_norm_token(tipo) or 'sin-tipo'}-{_norm_token(serie) or 'sin-serie'}-{int(inicio)}-{int(fin)}"
    )


def _bank_summary(banco_nombre: str, nro_cuenta: str) -> str:
    return " | ".join(part for part in [banco_nombre.strip().upper(), nro_cuenta.strip()] if part)


def _checkbook_summary(formato: str, tipo: str, serie: str, inicio: int, fin: int) -> str:
    rango = f"{int(inicio)}-{int(fin)}" if int(inicio or 0) or int(fin or 0) else ""
    return " ".join(
        part
        for part in [formato.strip().upper(), tipo.strip().upper(), serie.strip().upper(), rango]
        if part
    )


class ChequerasRepository:
    def _table_columns(self, cn, table_name: str) -> set[str]:
        rows = cn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = %s
            """,
            (table_name,),
        ).fetchall()
        return {str(r["column_name"]) for r in rows}

    def _ensure_schema(self, cn) -> None:
        cn.execute(
            """
            CREATE TABLE IF NOT EXISTS banks(
                id BIGSERIAL PRIMARY KEY,
                bank_id TEXT NOT NULL UNIQUE,
                banco_nombre TEXT NOT NULL,
                nro_cuenta TEXT NOT NULL,
                resumen TEXT,
                ts_registro TEXT NOT NULL
            )
            """
        )
        cn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_banks_nombre_cuenta
            ON banks(banco_nombre, nro_cuenta)
            """
        )
        self._migrate_legacy_checkbooks(cn)
        cn.execute(
            """
            CREATE TABLE IF NOT EXISTS bank_checkbooks(
                id BIGSERIAL PRIMARY KEY,
                chequera_id TEXT NOT NULL UNIQUE,
                bank_id TEXT NOT NULL,
                formato_chequera TEXT NOT NULL DEFAULT 'Formulario',
                tipo_cheque TEXT NOT NULL DEFAULT 'Vista',
                serie TEXT NOT NULL DEFAULT '',
                fecha_recibimiento TEXT NOT NULL DEFAULT '',
                nro_inicio INTEGER NOT NULL,
                nro_fin INTEGER NOT NULL,
                recibido_por TEXT NOT NULL,
                resumen TEXT,
                ts_registro TEXT NOT NULL
            )
            """
        )
        cols = self._table_columns(cn, "bank_checkbooks")
        if cols and "fecha_recibimiento" not in cols:
            cn.execute("ALTER TABLE bank_checkbooks ADD COLUMN fecha_recibimiento TEXT NOT NULL DEFAULT ''")
        if cols and "serie" not in cols:
            cn.execute("ALTER TABLE bank_checkbooks ADD COLUMN serie TEXT NOT NULL DEFAULT ''")
        if cols and "formato_chequera" not in cols and "bank_id" in cols:
            cn.execute("ALTER TABLE bank_checkbooks ADD COLUMN formato_chequera TEXT NOT NULL DEFAULT 'Formulario'")
        if cols and "tipo_cheque" not in cols and "bank_id" in cols:
            cn.execute("ALTER TABLE bank_checkbooks ADD COLUMN tipo_cheque TEXT NOT NULL DEFAULT 'Vista'")
        cn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_bank_checkbooks_unique
            ON bank_checkbooks(bank_id, formato_chequera, tipo_cheque, serie, nro_inicio, nro_fin)
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

    def _migrate_legacy_checkbooks(self, cn) -> None:
        cols = self._table_columns(cn, "bank_checkbooks")
        if not cols or "bank_id" in cols:
            return

        legacy_required = {
            "chequera_id",
            "banco_nombre",
            "nro_cuenta",
            "formulario_tipo",
            "nro_inicio",
            "nro_fin",
            "recibido_por",
            "resumen",
            "ts_registro",
        }
        if not legacy_required.issubset(cols):
            return

        legacy_rows = cn.execute(
            """
            SELECT chequera_id, banco_nombre, nro_cuenta, formulario_tipo,
                   nro_inicio, nro_fin, recibido_por, resumen, ts_registro
            FROM bank_checkbooks
            """
        ).fetchall()
        cn.execute("ALTER TABLE bank_checkbooks RENAME TO bank_checkbooks_legacy")
        cn.execute(
            """
            CREATE TABLE bank_checkbooks(
                id BIGSERIAL PRIMARY KEY,
                chequera_id TEXT NOT NULL UNIQUE,
                bank_id TEXT NOT NULL,
                formato_chequera TEXT NOT NULL DEFAULT 'Formulario',
                tipo_cheque TEXT NOT NULL DEFAULT 'Vista',
                serie TEXT NOT NULL DEFAULT '',
                fecha_recibimiento TEXT NOT NULL DEFAULT '',
                nro_inicio INTEGER NOT NULL,
                nro_fin INTEGER NOT NULL,
                recibido_por TEXT NOT NULL,
                resumen TEXT,
                ts_registro TEXT NOT NULL
            )
            """
        )

        for row in legacy_rows:
            banco_nombre = str(row["banco_nombre"] or "").strip()
            nro_cuenta = str(row["nro_cuenta"] or "").strip()
            bank_id = _bank_id(banco_nombre, nro_cuenta)
            ts_registro = str(row["ts_registro"] or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            legacy_form = str(row["formulario_tipo"] or "").strip().lower()
            formato = "Talonario" if legacy_form == "talonario" else "Formulario"
            tipo = "Diferido" if legacy_form == "diferido" else "Vista"
            nro_inicio = int(row["nro_inicio"] or 0)
            nro_fin = int(row["nro_fin"] or 0)

            cn.execute(
                """
                INSERT INTO banks(bank_id, banco_nombre, nro_cuenta, resumen, ts_registro)
                VALUES(%s, %s, %s, %s, %s)
                ON CONFLICT(bank_id) DO NOTHING
                """,
                (bank_id, banco_nombre, nro_cuenta, "", ts_registro),
            )
            cn.execute(
                """
                INSERT INTO bank_checkbooks(
                    chequera_id, bank_id, formato_chequera, tipo_cheque, serie,
                    fecha_recibimiento, nro_inicio, nro_fin, recibido_por, resumen, ts_registro
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(row["chequera_id"] or "").strip()
                    or _chequera_id(bank_id, formato, tipo, "", nro_inicio, nro_fin),
                    bank_id,
                    formato,
                    tipo,
                    "",
                    "",
                    nro_inicio,
                    nro_fin,
                    str(row["recibido_por"] or "").strip(),
                    str(row["resumen"] or "").strip(),
                    ts_registro,
                ),
            )
        cn.execute("DROP TABLE bank_checkbooks_legacy")

    def summary(self) -> ChequerasSummary:
        with connection("fraccionadora") as cn:
            self._ensure_schema(cn)
            banks = cn.execute(
                """
                SELECT bank_id, banco_nombre, nro_cuenta, COALESCE(resumen, '') AS resumen,
                       COALESCE(ts_registro, '') AS ts_registro
                FROM banks
                ORDER BY banco_nombre, nro_cuenta, id
                """
            ).fetchall()
            checkbooks = cn.execute(
                """
                SELECT c.chequera_id, c.bank_id, b.banco_nombre, b.nro_cuenta,
                       c.formato_chequera, c.tipo_cheque, COALESCE(c.serie, '') AS serie,
                       COALESCE(c.fecha_recibimiento, '') AS fecha_recibimiento,
                       c.nro_inicio, c.nro_fin, c.recibido_por,
                       COALESCE(c.resumen, '') AS resumen, COALESCE(c.ts_registro, '') AS ts_registro
                FROM bank_checkbooks c
                JOIN banks b ON b.bank_id = c.bank_id
                ORDER BY c.ts_registro DESC, c.id DESC
                """
            ).fetchall()
            used = cn.execute(
                """
                SELECT u.id, u.chequera_id, u.cheque_no, COALESCE(u.serie, '') AS serie,
                       COALESCE(u.referencia, '') AS referencia,
                       COALESCE(u.payment_group_id, '') AS payment_group_id,
                       COALESCE(u.used_ts, '') AS used_ts,
                       COALESCE(b.banco_nombre, '') AS banco_nombre,
                       COALESCE(b.nro_cuenta, '') AS nro_cuenta,
                       COALESCE(c.formato_chequera, '') AS formato_chequera,
                       COALESCE(c.tipo_cheque, '') AS tipo_cheque
                FROM dashboard_used_checks u
                LEFT JOIN bank_checkbooks c ON c.chequera_id = u.chequera_id
                LEFT JOIN banks b ON b.bank_id = c.bank_id
                ORDER BY u.used_ts DESC, u.id DESC
                LIMIT 500
                """
            ).fetchall()

        return ChequerasSummary(
            banks=[BankRow(**dict(r)) for r in banks],
            checkbooks=[CheckbookRow(**dict(r)) for r in checkbooks],
            used_checks=[UsedCheckRow(**dict(r)) for r in used],
        )

    def create_bank(self, payload: BankCreate) -> BankRow:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row = {
            "bank_id": _bank_id(payload.banco_nombre, payload.nro_cuenta),
            "banco_nombre": payload.banco_nombre.strip(),
            "nro_cuenta": payload.nro_cuenta.strip(),
            "resumen": payload.resumen.strip() or _bank_summary(payload.banco_nombre, payload.nro_cuenta),
            "ts_registro": now,
        }
        with connection("fraccionadora") as cn:
            self._ensure_schema(cn)
            cn.execute(
                """
                INSERT INTO banks(bank_id, banco_nombre, nro_cuenta, resumen, ts_registro)
                VALUES(%s, %s, %s, %s, %s)
                """,
                (row["bank_id"], row["banco_nombre"], row["nro_cuenta"], row["resumen"], row["ts_registro"]),
            )
        return BankRow(**row)

    def create_checkbook(self, payload: CheckbookCreate) -> CheckbookRow:
        if payload.nro_fin < payload.nro_inicio:
            raise ValueError("El numero final no puede ser menor al inicial.")
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        chequera_id = _chequera_id(
            payload.bank_id, payload.formato_chequera, payload.tipo_cheque, payload.serie, payload.nro_inicio, payload.nro_fin
        )
        resumen = payload.resumen.strip() or _checkbook_summary(
            payload.formato_chequera, payload.tipo_cheque, payload.serie, payload.nro_inicio, payload.nro_fin
        )
        with connection("fraccionadora") as cn:
            self._ensure_schema(cn)
            bank = cn.execute("SELECT banco_nombre, nro_cuenta FROM banks WHERE bank_id = %s", (payload.bank_id,)).fetchone()
            if not bank:
                raise ValueError("Banco no encontrado.")
            overlap = cn.execute(
                """
                SELECT 1 FROM bank_checkbooks
                WHERE bank_id = %s AND formato_chequera = %s AND tipo_cheque = %s AND serie = %s
                  AND NOT (nro_fin < %s OR nro_inicio > %s)
                LIMIT 1
                """,
                (
                    payload.bank_id.strip(),
                    payload.formato_chequera.strip(),
                    payload.tipo_cheque.strip(),
                    payload.serie.strip(),
                    int(payload.nro_inicio),
                    int(payload.nro_fin),
                ),
            ).fetchone()
            if overlap:
                raise ValueError("La chequera se superpone con otra ya cargada.")
            cn.execute(
                """
                INSERT INTO bank_checkbooks(
                    chequera_id, bank_id, formato_chequera, tipo_cheque, serie, fecha_recibimiento,
                    nro_inicio, nro_fin, recibido_por, resumen, ts_registro
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    chequera_id,
                    payload.bank_id.strip(),
                    payload.formato_chequera.strip(),
                    payload.tipo_cheque.strip(),
                    payload.serie.strip(),
                    payload.fecha_recibimiento.strip(),
                    int(payload.nro_inicio),
                    int(payload.nro_fin),
                    payload.recibido_por.strip(),
                    resumen,
                    now,
                ),
            )
        return CheckbookRow(
            chequera_id=chequera_id,
            bank_id=payload.bank_id.strip(),
            banco_nombre=bank["banco_nombre"],
            nro_cuenta=bank["nro_cuenta"],
            formato_chequera=payload.formato_chequera.strip(),
            tipo_cheque=payload.tipo_cheque.strip(),
            serie=payload.serie.strip(),
            fecha_recibimiento=payload.fecha_recibimiento.strip(),
            nro_inicio=int(payload.nro_inicio),
            nro_fin=int(payload.nro_fin),
            recibido_por=payload.recibido_por.strip(),
            resumen=resumen,
            ts_registro=now,
        )

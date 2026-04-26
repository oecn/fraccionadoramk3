from __future__ import annotations

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db


DEFAULT_EMPLOYEES = [
    {"nombre": "ADOLFINO", "ci": "4563502", "activo": 1, "observacion": "Seed inicial RRHH"},
    {"nombre": "LUIS", "ci": "2319454", "activo": 1, "observacion": "Seed inicial RRHH"},
]


def _default_db_path() -> Path:
    root = Path(__file__).resolve().parents[1]
    return root / "fraccionadora.db"


class RRHHRepo:
    def __init__(self, db_path: str | Path | None = None):
        self.db_path = Path(db_path) if db_path else _default_db_path()
        self.cn = db.connect("fraccionadora")
        self._init_schema()
        self._seed_defaults()

    def close(self) -> None:
        try:
            self.cn.close()
        except Exception:
            pass

    def _init_schema(self) -> None:
        cur = self.cn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rrhh_employees(
                id BIGSERIAL PRIMARY KEY,
                nombre TEXT NOT NULL,
                ci TEXT NOT NULL UNIQUE,
                activo INTEGER NOT NULL DEFAULT 1,
                observacion TEXT,
                creado_en TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rrhh_movements(
                id BIGSERIAL PRIMARY KEY,
                employee_id INTEGER NOT NULL,
                fecha TEXT NOT NULL,
                concepto TEXT NOT NULL,
                monto_gs REAL NOT NULL,
                confirmado INTEGER NOT NULL DEFAULT 0,
                cuenta_destino TEXT,
                documento_ci TEXT,
                source_file TEXT,
                creado_en TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(employee_id) REFERENCES rrhh_employees(id)
            );
            """
        )
        self.cn.commit()

    def _seed_defaults(self) -> None:
        cur = self.cn.cursor()
        for row in DEFAULT_EMPLOYEES:
            cur.execute(
                """
                INSERT INTO rrhh_employees(nombre, ci, activo, observacion)
                VALUES(%s, %s, %s, %s)
                ON CONFLICT(ci) DO UPDATE SET
                    nombre=excluded.nombre,
                    activo=excluded.activo,
                    observacion=excluded.observacion;
                """,
                (row["nombre"], row["ci"], row["activo"], row["observacion"]),
            )
        self.cn.commit()

    def list_employees(self):
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT id, nombre, ci, activo, COALESCE(observacion, '')
            FROM rrhh_employees
            ORDER BY UPPER(nombre), ci;
            """
        )
        return cur.fetchall()

    def get_employee_by_ci(self, ci: str):
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT id, nombre, ci, activo, COALESCE(observacion, '')
            FROM rrhh_employees
            WHERE ci = %s
            LIMIT 1;
            """,
            (str(ci).strip(),),
        )
        return cur.fetchone()

    def movement_exists(self, employee_id: int, fecha: str, concepto: str, monto_gs: float, source_file: str) -> bool:
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT id
            FROM rrhh_movements
            WHERE employee_id = %s
              AND fecha = %s
              AND concepto = %s
              AND ABS(monto_gs - %s) < 0.0001
              AND COALESCE(source_file, '') = %s
            LIMIT 1;
            """,
            (int(employee_id), str(fecha), str(concepto), float(monto_gs), str(source_file or "")),
        )
        return cur.fetchone() is not None

    def insert_movement(
        self,
        employee_id: int,
        fecha: str,
        concepto: str,
        monto_gs: float,
        confirmado: bool = False,
        cuenta_destino: str = "",
        documento_ci: str = "",
        source_file: str = "",
    ) -> None:
        cur = self.cn.cursor()
        cur.execute(
            """
            INSERT INTO rrhh_movements(
                employee_id, fecha, concepto, monto_gs, confirmado, cuenta_destino, documento_ci, source_file
            )
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                int(employee_id),
                str(fecha),
                str(concepto),
                float(monto_gs),
                1 if bool(confirmado) else 0,
                str(cuenta_destino or ""),
                str(documento_ci or ""),
                str(source_file or ""),
            ),
        )
        self.cn.commit()

    def list_movements(self):
        cur = self.cn.cursor()
        cur.execute(
            """
            SELECT
                m.id,
                m.fecha,
                e.nombre,
                e.ci,
                m.concepto,
                m.monto_gs,
                m.confirmado,
                COALESCE(m.cuenta_destino, ''),
                COALESCE(m.source_file, '')
            FROM rrhh_movements m
            JOIN rrhh_employees e ON e.id = m.employee_id
            ORDER BY m.fecha DESC, m.id DESC;
            """
        )
        return cur.fetchall()

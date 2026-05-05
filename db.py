"""
Módulo central de conexión a PostgreSQL.
Reemplaza pg_sqlite_compat.py — conexión directa, sin capa de traducción.
"""
from __future__ import annotations

from contextlib import contextmanager
import os
import re
from typing import Any, Iterator, Sequence

import psycopg

DATABASE_URL: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@192.168.10.13:5432/GRANOS",
)

# Mapeo de nombre de archivo .db → schema de PostgreSQL
SCHEMA_MAP: dict[str, str] = {
    "fraccionadora.db": "fraccionadora",
    "facturas.db": "facturas",
    "pedidos.db": "pedidos",
}


# ---------------------------------------------------------------------------
# Row — acceso por índice entero O por nombre de columna
# ---------------------------------------------------------------------------

class Row:
    """Fila que soporta row[0] y row["col_name"] como sqlite3.Row."""

    __slots__ = ("_values", "_index")

    def __init__(self, values: tuple[Any, ...], columns: list[str]) -> None:
        self._values = tuple(values)
        self._index = {name: i for i, name in enumerate(columns)}

    def __getitem__(self, key: int | str) -> Any:
        if isinstance(key, int):
            return self._values[key]
        return self._values[self._index[key]]

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def keys(self) -> list[str]:
        return list(self._index)

    def get(self, key: str, default: Any = None) -> Any:
        return self[key] if key in self._index else default

    def __repr__(self) -> str:  # pragma: no cover
        pairs = ", ".join(f"{k}={self._values[i]!r}" for k, i in self._index.items())
        return f"Row({pairs})"


def _flex_row_factory(cursor: psycopg.Cursor) -> Any:
    """Row factory que devuelve Row con acceso por índice o por nombre."""
    cols = [d.name for d in cursor.description] if cursor.description else []

    def make(values: Sequence[Any]) -> Row:
        return Row(tuple(values), cols)

    return make


# ---------------------------------------------------------------------------
# Conexión
# ---------------------------------------------------------------------------

def connect(schema: str, autocommit: bool = False) -> psycopg.Connection:
    """
    Abre una conexión psycopg al schema indicado.

    Uso:
        cn = db.connect("fraccionadora")
        cur = cn.cursor()
        cur.execute("SELECT ...")
        cn.commit()
        cn.close()
    """
    conn = psycopg.connect(DATABASE_URL, autocommit=autocommit, row_factory=_flex_row_factory)
    conn.execute(f'SET search_path TO "{schema}", public;')
    return conn


@contextmanager
def connection(schema: str) -> Iterator[psycopg.Connection]:
    """
    Context manager transaccional para conexiones cortas.

    Hace commit si el bloque termina bien, rollback si falla y siempre cierra
    la conexion. Mantiene connect() disponible para codigo legacy.
    """
    cn = connect(schema)
    try:
        yield cn
        cn.commit()
    except Exception:
        cn.rollback()
        raise
    finally:
        cn.close()


def fetchone_required(cur: psycopg.Cursor, msg: str = "") -> Row:
    """Devuelve cur.fetchone() o falla con contexto si no hubo fila."""
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(msg or "Query no retorno la fila esperada.")
    return row


# ---------------------------------------------------------------------------
# Helpers para DDL
# ---------------------------------------------------------------------------

_DDL_SUBS: list[tuple[re.Pattern, str]] = [
    # AUTOINCREMENT → BIGSERIAL
    (re.compile(r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b", re.IGNORECASE), "BIGSERIAL PRIMARY KEY"),
    # datetime('now') → CURRENT_TIMESTAMP
    (re.compile(r"datetime\(\s*'now'\s*\)", re.IGNORECASE), "CURRENT_TIMESTAMP"),
    # Eliminar PRAGMA journal_mode
    (re.compile(r"PRAGMA\s+journal_mode\s*=\s*WAL\s*;?", re.IGNORECASE), ""),
]


def _split_statements(script: str) -> list[str]:
    """Divide un script SQL en sentencias individuales (respeta strings)."""
    parts: list[str] = []
    buf: list[str] = []
    in_single = False
    for ch in script:
        if ch == "'" and (not buf or buf[-1] != "\\"):
            in_single = not in_single
        if ch == ";" and not in_single:
            stmt = "".join(buf).strip()
            if stmt:
                parts.append(stmt)
            buf = []
            continue
        buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def run_ddl(conn: psycopg.Connection, script: str) -> None:
    """
    Ejecuta un script DDL multi-sentencia contra PostgreSQL.
    - Reemplaza AUTOINCREMENT → BIGSERIAL
    - Omite sentencias PRAGMA
    - Omite líneas vacías
    No llama a commit() — el llamante decide cuándo confirmar.
    """
    for sub_pattern, replacement in _DDL_SUBS:
        script = sub_pattern.sub(replacement, script)

    for stmt in _split_statements(script):
        normalized = stmt.strip().lstrip("﻿")
        if not normalized:
            continue
        if normalized.upper().startswith("PRAGMA"):
            continue
        conn.execute(normalized)


def table_columns(conn: psycopg.Connection, table_name: str) -> set[str]:
    """
    Devuelve el conjunto de nombres de columnas de una tabla en el schema actual.
    Reemplaza: PRAGMA table_info(<table>)
    """
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name   = %s
        """,
        (table_name,),
    ).fetchall()
    return {r[0] for r in rows}


def table_exists(conn: psycopg.Connection, table_name: str) -> bool:
    """
    Comprueba si una tabla existe en el schema actual.
    Reemplaza: SELECT name FROM sqlite_master WHERE type='table' AND name=?
    """
    row = conn.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name   = %s
        LIMIT 1
        """,
        (table_name,),
    ).fetchone()
    return row is not None

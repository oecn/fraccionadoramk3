from __future__ import annotations

import datetime as dt
import decimal
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import psycopg


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@192.168.10.13:5432/GRANOS"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)

SCHEMA_TO_SQLITE_PATH = {
    "fraccionadora": ROOT_DIR / "GCMK8" / "fraccionadora.db",
    "facturas": ROOT_DIR / "importadorfactur" / "facturas.db",
    "pedidos": ROOT_DIR / "PDFMK10" / "db" / "pedidos.db",
}


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    udt_name: str
    is_nullable: bool
    default: str | None
    ordinal_position: int


@dataclass
class ForeignKeyInfo:
    columns: list[str]
    ref_table: str
    ref_columns: list[str]
    on_update: str
    on_delete: str


@dataclass
class TableInfo:
    name: str
    columns: list[ColumnInfo]
    pk_columns: list[str]
    unique_constraints: list[list[str]]
    foreign_keys: list[ForeignKeyInfo]


def qident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _sqlite_type(col: ColumnInfo, is_single_int_pk: bool) -> str:
    if is_single_int_pk:
        return "INTEGER"

    data_type = (col.data_type or "").lower()
    udt_name = (col.udt_name or "").lower()

    if data_type in {"smallint", "integer", "bigint"}:
        return "INTEGER"
    if data_type in {"real", "double precision"}:
        return "REAL"
    if data_type in {"numeric", "decimal"}:
        return "NUMERIC"
    if data_type == "boolean":
        return "INTEGER"
    if data_type in {"date", "timestamp without time zone", "timestamp with time zone", "time without time zone", "time with time zone"}:
        return "TEXT"
    if data_type == "bytea":
        return "BLOB"
    if udt_name in {"varchar", "text", "bpchar", "uuid"}:
        return "TEXT"
    return "TEXT"


def _translate_default(default: str | None) -> str | None:
    if not default:
        return None
    txt = default.strip()
    upper = txt.upper()

    if "CURRENT_TIMESTAMP" in upper:
        return "CURRENT_TIMESTAMP"
    if "CURRENT_DATE" in upper:
        return "CURRENT_DATE"
    if "CURRENT_TIME" in upper:
        return "CURRENT_TIME"

    if "::" in txt:
        txt = txt.split("::", 1)[0].strip()

    if txt.startswith("'") and txt.endswith("'"):
        return txt

    try:
        float(txt)
        return txt
    except ValueError:
        return None


def _normalize_value(value):
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat(sep=" ") if isinstance(value, dt.datetime) else value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value


def _fetch_tables(pg_conn, schema: str) -> list[str]:
    sql = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = %s
      AND table_type = 'BASE TABLE'
    ORDER BY table_name;
    """
    with pg_conn.cursor() as cur:
        cur.execute(sql, (schema,))
        return [row[0] for row in cur.fetchall()]


def _fetch_columns(pg_conn, schema: str, table: str) -> list[ColumnInfo]:
    sql = """
    SELECT column_name, data_type, udt_name, is_nullable, column_default, ordinal_position
    FROM information_schema.columns
    WHERE table_schema = %s
      AND table_name = %s
    ORDER BY ordinal_position;
    """
    with pg_conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return [
            ColumnInfo(
                name=row[0],
                data_type=row[1],
                udt_name=row[2],
                is_nullable=(row[3] == "YES"),
                default=row[4],
                ordinal_position=row[5],
            )
            for row in cur.fetchall()
        ]


def _fetch_pk_columns(pg_conn, schema: str, table: str) -> list[str]:
    sql = """
    SELECT ku.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage ku
      ON tc.constraint_name = ku.constraint_name
     AND tc.table_schema = ku.table_schema
     AND tc.table_name = ku.table_name
    WHERE tc.table_schema = %s
      AND tc.table_name = %s
      AND tc.constraint_type = 'PRIMARY KEY'
    ORDER BY ku.ordinal_position;
    """
    with pg_conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return [row[0] for row in cur.fetchall()]


def _fetch_unique_constraints(pg_conn, schema: str, table: str) -> list[list[str]]:
    sql = """
    SELECT tc.constraint_name, ku.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage ku
      ON tc.constraint_name = ku.constraint_name
     AND tc.table_schema = ku.table_schema
     AND tc.table_name = ku.table_name
    WHERE tc.table_schema = %s
      AND tc.table_name = %s
      AND tc.constraint_type = 'UNIQUE'
    ORDER BY tc.constraint_name, ku.ordinal_position;
    """
    buckets: dict[str, list[str]] = {}
    with pg_conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        for constraint_name, column_name in cur.fetchall():
            buckets.setdefault(constraint_name, []).append(column_name)
    return list(buckets.values())


def _fetch_foreign_keys(pg_conn, schema: str, table: str) -> list[ForeignKeyInfo]:
    sql = """
    SELECT
        tc.constraint_name,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name,
        rc.update_rule,
        rc.delete_rule,
        kcu.ordinal_position
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema = kcu.table_schema
     AND tc.table_name = kcu.table_name
    JOIN information_schema.referential_constraints rc
      ON tc.constraint_name = rc.constraint_name
     AND tc.table_schema = rc.constraint_schema
    JOIN information_schema.constraint_column_usage ccu
      ON rc.unique_constraint_name = ccu.constraint_name
     AND rc.unique_constraint_schema = ccu.constraint_schema
    WHERE tc.table_schema = %s
      AND tc.table_name = %s
      AND tc.constraint_type = 'FOREIGN KEY'
    ORDER BY tc.constraint_name, kcu.ordinal_position;
    """
    buckets: dict[str, dict] = {}
    with pg_conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        for constraint_name, column_name, ref_table, ref_column, on_update, on_delete, _ordinal in cur.fetchall():
            bucket = buckets.setdefault(
                constraint_name,
                {
                    "columns": [],
                    "ref_table": ref_table,
                    "ref_columns": [],
                    "on_update": on_update,
                    "on_delete": on_delete,
                },
            )
            bucket["columns"].append(column_name)
            bucket["ref_columns"].append(ref_column)
    return [
        ForeignKeyInfo(
            columns=data["columns"],
            ref_table=data["ref_table"],
            ref_columns=data["ref_columns"],
            on_update=data["on_update"],
            on_delete=data["on_delete"],
        )
        for data in buckets.values()
    ]


def _load_table_info(pg_conn, schema: str, table: str) -> TableInfo:
    return TableInfo(
        name=table,
        columns=_fetch_columns(pg_conn, schema, table),
        pk_columns=_fetch_pk_columns(pg_conn, schema, table),
        unique_constraints=_fetch_unique_constraints(pg_conn, schema, table),
        foreign_keys=_fetch_foreign_keys(pg_conn, schema, table),
    )


def _create_sqlite_table(sqlite_conn: sqlite3.Connection, table: TableInfo) -> None:
    single_int_pk = False
    if len(table.pk_columns) == 1:
        pk_name = table.pk_columns[0]
        pk_col = next((col for col in table.columns if col.name == pk_name), None)
        single_int_pk = pk_col is not None and _sqlite_type(pk_col, False) == "INTEGER"

    column_defs: list[str] = []
    for col in table.columns:
        is_single_int_pk_col = single_int_pk and col.name == table.pk_columns[0]
        parts = [qident(col.name)]
        if is_single_int_pk_col and col.name == "id":
            parts.append("INTEGER PRIMARY KEY AUTOINCREMENT")
        else:
            parts.append(_sqlite_type(col, is_single_int_pk_col))
            default_sql = _translate_default(col.default)
            if default_sql is not None:
                parts.append(f"DEFAULT {default_sql}")
            if not col.is_nullable or col.name in table.pk_columns:
                parts.append("NOT NULL")
        column_defs.append(" ".join(parts))

    constraints: list[str] = []
    if table.pk_columns and not single_int_pk:
        constraints.append(f"PRIMARY KEY ({', '.join(qident(col) for col in table.pk_columns)})")
    for unique_cols in table.unique_constraints:
        constraints.append(f"UNIQUE ({', '.join(qident(col) for col in unique_cols)})")
    for fk in table.foreign_keys:
        constraints.append(
            "FOREIGN KEY ({cols}) REFERENCES {ref_table} ({ref_cols}) ON UPDATE {on_update} ON DELETE {on_delete}".format(
                cols=", ".join(qident(col) for col in fk.columns),
                ref_table=qident(fk.ref_table),
                ref_cols=", ".join(qident(col) for col in fk.ref_columns),
                on_update=fk.on_update,
                on_delete=fk.on_delete,
            )
        )

    sql = f"CREATE TABLE {qident(table.name)} (\n    " + ",\n    ".join(column_defs + constraints) + "\n);"
    sqlite_conn.execute(sql)


def _copy_table_rows(pg_conn, sqlite_conn: sqlite3.Connection, schema: str, table: TableInfo) -> int:
    column_names = [col.name for col in table.columns]
    select_sql = f"SELECT {', '.join(qident(col) for col in column_names)} FROM {qident(schema)}.{qident(table.name)}"
    if table.pk_columns:
        select_sql += " ORDER BY " + ", ".join(qident(col) for col in table.pk_columns)
    insert_sql = (
        f"INSERT INTO {qident(table.name)} ({', '.join(qident(col) for col in column_names)}) "
        f"VALUES ({', '.join(['?'] * len(column_names))})"
    )

    copied = 0
    with pg_conn.cursor() as cur:
        cur.execute(select_sql)
        while True:
            rows = cur.fetchmany(1000)
            if not rows:
                break
            payload = [tuple(_normalize_value(value) for value in row) for row in rows]
            sqlite_conn.executemany(insert_sql, payload)
            copied += len(payload)
    return copied


def backup_schema_to_sqlite(pg_conn, schema: str, sqlite_path: Path) -> dict[str, int]:
    tables = [_load_table_info(pg_conn, schema, table_name) for table_name in _fetch_tables(pg_conn, schema)]
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = sqlite_path.with_suffix(sqlite_path.suffix + ".tmp")
    if temp_path.exists():
        temp_path.unlink()

    counts: dict[str, int] = {}
    sqlite_conn = sqlite3.connect(temp_path)
    try:
        sqlite_conn.execute("PRAGMA foreign_keys = OFF;")
        sqlite_conn.execute("PRAGMA journal_mode = DELETE;")
        for table in tables:
            _create_sqlite_table(sqlite_conn, table)
        for table in tables:
            counts[table.name] = _copy_table_rows(pg_conn, sqlite_conn, schema, table)
        sqlite_conn.commit()
    except Exception:
        sqlite_conn.rollback()
        raise
    finally:
        sqlite_conn.close()

    temp_path.replace(sqlite_path)
    return counts


def backup_project_sqlite_from_postgres(dsn: str | None = None) -> dict[str, dict[str, int]]:
    target_dsn = dsn or DATABASE_URL
    results: dict[str, dict[str, int]] = {}
    with psycopg.connect(target_dsn, autocommit=False) as pg_conn:
        for schema, sqlite_path in SCHEMA_TO_SQLITE_PATH.items():
            results[schema] = backup_schema_to_sqlite(pg_conn, schema, sqlite_path)
        pg_conn.rollback()
    return results


if __name__ == "__main__":
    results = backup_project_sqlite_from_postgres()
    for schema, table_counts in results.items():
        total_rows = sum(table_counts.values())
        print(f"[OK] {schema}: {total_rows} filas exportadas a {SCHEMA_TO_SQLITE_PATH[schema]}")

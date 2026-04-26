#!/usr/bin/env python
"""
Migra bases SQLite a PostgreSQL preservando tablas, datos, claves foraneas e indices.

Uso rapido:
    python tools/sqlite_to_postgres_migrator.py --dsn "postgresql://user:pass@localhost:5432/app" --all-project-dbs

Tambien puedes pasar archivos concretos:
    python tools/sqlite_to_postgres_migrator.py --dsn "..." --db GCMK8/fraccionadora.db --db importadorfactur/facturas.db

Por defecto crea un esquema PostgreSQL por archivo SQLite para evitar colisiones entre tablas.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit
from typing import Iterable

try:
    import psycopg
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Falta la dependencia 'psycopg'. Instala con: pip install psycopg[binary]"
    ) from exc


ROOT = pathlib.Path(__file__).resolve().parents[1]
PROJECT_DB_SCHEMA_MAP = {
    ROOT / "GCMK8" / "fraccionadora.db": "fraccionadora",
    ROOT / "importadorfactur" / "facturas.db": "facturas",
    ROOT / "PDFMK10" / "db" / "pedidos.db": "pedidos",
}


@dataclass
class ColumnInfo:
    name: str
    declared_type: str
    notnull: bool
    default: str | None
    pk_pos: int


@dataclass
class ForeignKeyInfo:
    name: str
    columns: list[str]
    ref_table: str
    ref_columns: list[str]
    on_update: str
    on_delete: str


@dataclass
class IndexInfo:
    name: str
    columns: list[str]
    unique: bool


@dataclass
class TableInfo:
    name: str
    columns: list[ColumnInfo]
    pk_columns: list[str]
    foreign_keys: list[ForeignKeyInfo]
    indexes: list[IndexInfo]
    rows: list[sqlite3.Row]


def qident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def qname(*parts: str) -> str:
    return ".".join(qident(part) for part in parts)


def sanitize_name(name: str) -> str:
    txt = re.sub(r"[^a-zA-Z0-9_]+", "_", name.strip().lower()).strip("_")
    return txt or "sqlite_db"


def discover_project_dbs(root: pathlib.Path) -> list[pathlib.Path]:
    return sorted(root.rglob("*.db"))


def project_default_dbs() -> list[pathlib.Path]:
    return [path.resolve() for path in PROJECT_DB_SCHEMA_MAP]


def parse_db_name_from_dsn(dsn: str) -> str:
    parts = urlsplit(dsn)
    db_name = parts.path.lstrip("/")
    if not db_name:
        raise SystemExit("El DSN debe incluir el nombre de la base de datos destino.")
    return db_name


def replace_dsn_database(dsn: str, db_name: str) -> str:
    parts = urlsplit(dsn)
    return urlunsplit((parts.scheme, parts.netloc, f"/{db_name}", parts.query, parts.fragment))


def ensure_database_exists(dsn: str, admin_db: str = "postgres") -> None:
    target_db = parse_db_name_from_dsn(dsn)
    admin_dsn = replace_dsn_database(dsn, admin_db)
    with psycopg.connect(admin_dsn, autocommit=True) as admin_conn:
        with admin_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (target_db,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(f"CREATE DATABASE {qident(target_db)};")
                print(f"[INFO] Base de datos creada: {target_db}")


def sqlite_tables(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name;
        """
    )
    return [str(row[0]) for row in cur.fetchall()]


def map_sqlite_type_to_pg(declared_type: str, has_int_pk: bool = False) -> str:
    dtype = (declared_type or "").strip().upper()
    if has_int_pk:
        return "BIGINT"
    if "INT" in dtype:
        return "BIGINT"
    if any(token in dtype for token in ("CHAR", "CLOB", "TEXT")):
        return "TEXT"
    if "BLOB" in dtype:
        return "BYTEA"
    if any(token in dtype for token in ("REAL", "FLOA", "DOUB")):
        return "DOUBLE PRECISION"
    if "BOOL" in dtype:
        return "BOOLEAN"
    if "DATE" in dtype and "TIME" not in dtype:
        return "DATE"
    if any(token in dtype for token in ("TIMESTAMP", "DATETIME")):
        return "TIMESTAMP"
    if any(token in dtype for token in ("NUMERIC", "DECIMAL")):
        return "NUMERIC"
    return "TEXT"


def translate_default(value: str | None) -> str | None:
    if value is None:
        return None
    txt = value.strip()
    upper = txt.upper()
    if upper in {"CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"}:
        return upper
    if re.fullmatch(r"-?\d+(\.\d+)?", txt):
        return txt
    if txt.startswith("(") and txt.endswith(")"):
        inner = txt[1:-1].strip()
        if inner.upper() in {"CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"}:
            return inner.upper()
    if txt.startswith("'") and txt.endswith("'"):
        return txt
    return None


def load_table_info(conn: sqlite3.Connection, table_name: str) -> TableInfo:
    col_rows = conn.execute(f"PRAGMA table_info({qident(table_name)})").fetchall()
    columns = [
        ColumnInfo(
            name=str(row[1]),
            declared_type=str(row[2] or ""),
            notnull=bool(row[3]),
            default=row[4],
            pk_pos=int(row[5] or 0),
        )
        for row in col_rows
    ]
    pk_columns = [col.name for col in sorted(columns, key=lambda c: c.pk_pos) if col.pk_pos > 0]

    fk_buckets: dict[int, list[sqlite3.Row]] = defaultdict(list)
    for row in conn.execute(f"PRAGMA foreign_key_list({qident(table_name)})").fetchall():
        fk_buckets[int(row[0])].append(row)

    foreign_keys: list[ForeignKeyInfo] = []
    for fk_id, rows in fk_buckets.items():
        ordered = sorted(rows, key=lambda item: int(item[1]))
        foreign_keys.append(
            ForeignKeyInfo(
                name=f"{table_name}_fk_{fk_id}",
                columns=[str(r[3]) for r in ordered],
                ref_table=str(ordered[0][2]),
                ref_columns=[str(r[4]) for r in ordered],
                on_update=str(ordered[0][5] or "NO ACTION"),
                on_delete=str(ordered[0][6] or "NO ACTION"),
            )
        )

    indexes: list[IndexInfo] = []
    for row in conn.execute(f"PRAGMA index_list({qident(table_name)})").fetchall():
        idx_name = str(row[1])
        unique = bool(row[2])
        origin = str(row[3] or "")
        if idx_name.startswith("sqlite_autoindex") or origin == "pk":
            continue
        idx_cols = [
            str(idx_row[2])
            for idx_row in conn.execute(f"PRAGMA index_info({qident(idx_name)})").fetchall()
            if idx_row[2] is not None
        ]
        if idx_cols:
            indexes.append(IndexInfo(name=idx_name, columns=idx_cols, unique=unique))

    rows = conn.execute(f"SELECT * FROM {qident(table_name)}").fetchall()
    return TableInfo(
        name=table_name,
        columns=columns,
        pk_columns=pk_columns,
        foreign_keys=foreign_keys,
        indexes=indexes,
        rows=rows,
    )


def create_schema(pg_conn, schema_name: str, drop_existing: bool) -> None:
    with pg_conn.cursor() as cur:
        if drop_existing:
            cur.execute(f"DROP SCHEMA IF EXISTS {qident(schema_name)} CASCADE;")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {qident(schema_name)};")


def create_table(pg_conn, schema_name: str, table: TableInfo) -> None:
    column_defs: list[str] = []
    single_int_pk = (
        len(table.pk_columns) == 1
        and next((c for c in table.columns if c.name == table.pk_columns[0]), None) is not None
        and "INT" in next(c for c in table.columns if c.name == table.pk_columns[0]).declared_type.upper()
    )
    for col in table.columns:
        is_int_pk = single_int_pk and col.name == table.pk_columns[0]
        pg_type = map_sqlite_type_to_pg(col.declared_type, has_int_pk=is_int_pk)
        parts = [qident(col.name), pg_type]
        default_sql = translate_default(col.default)
        if default_sql is not None:
            parts.append(f"DEFAULT {default_sql}")
        if col.notnull or (len(table.pk_columns) == 1 and col.name in table.pk_columns):
            parts.append("NOT NULL")
        column_defs.append(" ".join(parts))

    constraints: list[str] = []
    if table.pk_columns:
        constraints.append(
            f"CONSTRAINT {qident(table.name + '_pk')} PRIMARY KEY ({', '.join(qident(c) for c in table.pk_columns)})"
        )

    sql = (
        f"CREATE TABLE {qname(schema_name, table.name)} (\n    "
        + ",\n    ".join(column_defs + constraints)
        + "\n);"
    )
    with pg_conn.cursor() as cur:
        cur.execute(sql)


def insert_rows(pg_conn, schema_name: str, table: TableInfo, batch_size: int) -> None:
    if not table.rows:
        return
    column_names = [col.name for col in table.columns]
    placeholders = ", ".join(["%s"] * len(column_names))
    sql = (
        f"INSERT INTO {qname(schema_name, table.name)} "
        f"({', '.join(qident(c) for c in column_names)}) VALUES ({placeholders})"
    )
    values = []
    for row in table.rows:
        values.append(tuple(row[col] for col in column_names))
    with pg_conn.cursor() as cur:
        for start in range(0, len(values), batch_size):
            cur.executemany(sql, values[start:start + batch_size])


def add_foreign_keys(pg_conn, schema_name: str, table: TableInfo) -> None:
    with pg_conn.cursor() as cur:
        for fk in table.foreign_keys:
            cur.execute(
                f"""
                ALTER TABLE {qname(schema_name, table.name)}
                ADD CONSTRAINT {qident(fk.name)}
                FOREIGN KEY ({', '.join(qident(c) for c in fk.columns)})
                REFERENCES {qname(schema_name, fk.ref_table)} ({', '.join(qident(c) for c in fk.ref_columns)})
                ON UPDATE {fk.on_update}
                ON DELETE {fk.on_delete}
                NOT VALID;
                """
            )


def create_indexes(pg_conn, schema_name: str, table: TableInfo) -> None:
    with pg_conn.cursor() as cur:
        for idx in table.indexes:
            unique = "UNIQUE " if idx.unique else ""
            cur.execute(
                f"""
                CREATE {unique}INDEX {qident(idx.name)}
                ON {qname(schema_name, table.name)} ({', '.join(qident(c) for c in idx.columns)});
                """
            )


def sync_identity_sequences(pg_conn, schema_name: str, table: TableInfo) -> None:
    if len(table.pk_columns) != 1:
        return
    pk_col = next((c for c in table.columns if c.name == table.pk_columns[0]), None)
    if pk_col is None or "INT" not in pk_col.declared_type.upper():
        return

    seq_name = f"{table.name}_{pk_col.name}_seq"
    full_seq = qname(schema_name, seq_name)
    full_table = qname(schema_name, table.name)
    with pg_conn.cursor() as cur:
        cur.execute(f"CREATE SEQUENCE IF NOT EXISTS {full_seq};")
        cur.execute(
            f"""
            ALTER TABLE {full_table}
            ALTER COLUMN {qident(pk_col.name)}
            SET DEFAULT nextval('{schema_name}.{seq_name}');
            """
        )
        cur.execute(
            f"""
            SELECT setval(
                '{schema_name}.{seq_name}',
                COALESCE((SELECT MAX({qident(pk_col.name)}) FROM {full_table}), 0) + 1,
                false
            );
            """
        )


def migrate_sqlite_db(pg_conn, sqlite_path: pathlib.Path, schema_name: str, batch_size: int, drop_existing: bool) -> None:
    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = sqlite3.Row
    try:
        tables = [load_table_info(sqlite_conn, table_name) for table_name in sqlite_tables(sqlite_conn)]
    finally:
        sqlite_conn.close()

    create_schema(pg_conn, schema_name, drop_existing)
    for table in tables:
        create_table(pg_conn, schema_name, table)
    for table in tables:
        insert_rows(pg_conn, schema_name, table, batch_size=batch_size)
    for table in tables:
        add_foreign_keys(pg_conn, schema_name, table)
    for table in tables:
        create_indexes(pg_conn, schema_name, table)
        sync_identity_sequences(pg_conn, schema_name, table)


def build_schema_name(db_path: pathlib.Path, used: set[str]) -> str:
    preferred = PROJECT_DB_SCHEMA_MAP.get(db_path.resolve())
    if preferred:
        if preferred in used:
            raise SystemExit(f"El esquema fijo {preferred!r} ya fue asignado a otro archivo SQLite.")
        used.add(preferred)
        return preferred
    base = sanitize_name(db_path.stem)
    if base not in used:
        used.add(base)
        return base
    composed = sanitize_name(f"{db_path.parent.name}_{db_path.stem}")
    if composed not in used:
        used.add(composed)
        return composed
    idx = 2
    while True:
        candidate = f"{composed}_{idx}"
        if candidate not in used:
            used.add(candidate)
            return candidate
        idx += 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migra SQLite .db a PostgreSQL.")
    parser.add_argument("--dsn", required=True, help="DSN de PostgreSQL. Ej: postgresql://user:pass@host:5432/db")
    parser.add_argument(
        "--ensure-database",
        action="store_true",
        help="Crea la base de datos del DSN si no existe antes de migrar.",
    )
    parser.add_argument(
        "--admin-db",
        default="postgres",
        help="Base administrativa usada para crear la base destino si falta.",
    )
    parser.add_argument("--db", action="append", default=[], help="Archivo SQLite a migrar. Se puede repetir.")
    parser.add_argument("--all-project-dbs", action="store_true", help="Descubre todos los .db dentro del proyecto.")
    parser.add_argument(
        "--project-default-dbs",
        action="store_true",
        help="Migra las tres bases canónicas del proyecto: GCMK8/fraccionadora.db, importadorfactur/facturas.db y PDFMK10/db/pedidos.db.",
    )
    parser.add_argument("--drop-existing", action="store_true", help="Elimina el esquema destino si ya existe.")
    parser.add_argument("--batch-size", type=int, default=1000, help="Tamano de lote para inserts.")
    parser.add_argument(
        "--schema-prefix",
        default="",
        help="Prefijo opcional para los esquemas destino. Ej: legacy_",
    )
    return parser.parse_args()


def resolve_db_paths(args: argparse.Namespace) -> list[pathlib.Path]:
    dbs = [pathlib.Path(p).resolve() for p in args.db]
    if args.project_default_dbs:
        dbs.extend(project_default_dbs())
    if args.all_project_dbs:
        dbs.extend(discover_project_dbs(ROOT))
    unique_paths = sorted({p for p in dbs if p.exists()})
    if not unique_paths:
        raise SystemExit("No se encontraron archivos .db para migrar.")
    return unique_paths


def main() -> None:
    args = parse_args()
    db_paths = resolve_db_paths(args)
    used_schema_names: set[str] = set()

    if args.ensure_database:
        ensure_database_exists(args.dsn, admin_db=args.admin_db)

    with psycopg.connect(args.dsn, autocommit=False) as pg_conn:
        for db_path in db_paths:
            schema_name = args.schema_prefix + build_schema_name(db_path, used_schema_names)
            print(f"[INFO] Migrando {db_path} -> esquema {schema_name}")
            try:
                migrate_sqlite_db(
                    pg_conn=pg_conn,
                    sqlite_path=db_path,
                    schema_name=schema_name,
                    batch_size=args.batch_size,
                    drop_existing=args.drop_existing,
                )
                pg_conn.commit()
                print(f"[OK] {db_path.name} migrado en esquema {schema_name}")
            except Exception:
                pg_conn.rollback()
                print(f"[ERROR] Fallo migrando {db_path}")
                raise


if __name__ == "__main__":
    main()

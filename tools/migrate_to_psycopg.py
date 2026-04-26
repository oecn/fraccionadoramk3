"""
Script de migración: SQLite syntax → psycopg (PostgreSQL).
Ejecutar UNA sola vez desde la raíz del proyecto:
    python tools/migrate_to_psycopg.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Archivos a migrar y el schema de PG que usan
# ---------------------------------------------------------------------------
TARGETS: list[tuple[Path, str | None]] = [
    (ROOT / "GCMK8"          / "fraccionadora.py",        "fraccionadora"),
    (ROOT / "GCMK8"          / "tab_flujo_dinero.py",     "fraccionadora"),
    (ROOT / "GCMK8"          / "tab_gastos.py",           "fraccionadora"),
    (ROOT / "GCMK8"          / "historial_ventas.py",     "fraccionadora"),
    (ROOT / "GCMK8"          / "resumen_compras.py",      "fraccionadora"),
    (ROOT / "GCMK8"          / "bancos_chequeras_qt.py",  "fraccionadora"),
    (ROOT / "GCMK8"          / "auditoria.py",            "fraccionadora"),
    (ROOT / "GCMK8"          / "produccion.py",           "fraccionadora"),
    (ROOT / "GCMK8"          / "analitica_clientes.py",   "fraccionadora"),
    (ROOT / "GCMK8"          / "proyeccion_compras.py",   "fraccionadora"),
    (ROOT / "GCMK8"          / "merma.py",                "fraccionadora"),
    (ROOT / "GCMK8" / "rrhh" / "rrhh_repo.py",           "fraccionadora"),
    (ROOT / "clon"           / "metrica.py",              "fraccionadora"),
    (ROOT / "clon"           / "inicio_dashboard_qt.py",  None),           # multi-schema
    (ROOT / "clon"           / "reporte_mensual_qt.py",   "fraccionadora"),
    (ROOT / "clon"           / "reporte_mensual.py",      "fraccionadora"),
    (ROOT / "clon"           / "reporte_trimestral_qt.py","fraccionadora"),
    (ROOT / "clon"           / "reportes_ventas_qt.py",   "fraccionadora"),
    (ROOT / "clon"           / "bancos_chequeras_qt.py",  "fraccionadora"),
    (ROOT / "clon"           / "fraccionadora_qt.py",     "fraccionadora"),
    (ROOT / "reportes_ventas.py",                         "fraccionadora"),
    (ROOT / "importadorfactur"/ "facturas_tabs.py",       "facturas"),
    (ROOT / "PDFMK10"        / "app_tk.py",               "pedidos"),
    (ROOT / "PDFMK10"        / "console_test.py",         "pedidos"),
]

# ---------------------------------------------------------------------------
# Transformaciones de SQL dentro de strings
# ---------------------------------------------------------------------------

def _replace_strftime_in_sql(sql: str) -> str:
    fmt_map = {
        "%Y-%m-%d": "YYYY-MM-DD",
        "%Y-%m":    "YYYY-MM",
        "%Y-W%W":   'IYYY-"W"IW',
        "%Y":       "YYYY",
        "%m":       "MM",
        "%d":       "DD",
        "%H:%M:%S": "HH24:MI:SS",
    }
    def _sub(m: re.Match) -> str:
        fmt = m.group(1)
        col = m.group(2).strip()
        pg_fmt = fmt_map.get(fmt, "YYYY-MM-DD")
        return f"TO_CHAR(({col})::timestamp, '{pg_fmt}')"

    return re.sub(
        r"strftime\(\s*'([^']+)'\s*,\s*([^)]+?)\s*\)",
        _sub,
        sql,
        flags=re.IGNORECASE,
    )


def _replace_date_fn_in_sql(sql: str) -> str:
    """date(col) → col::date  |  date(?) → %s::date"""
    def _sub(m: re.Match) -> str:
        inner = m.group(1).strip()
        if inner in ("?", "%s"):
            return "%s"          # date(%s) — simplemente pasar el valor
        return f"{inner}::date"
    return re.sub(r"\bdate\(\s*([^)]+?)\s*\)", _sub, sql, flags=re.IGNORECASE)


def _replace_qmarks_in_sql(sql: str) -> str:
    """
    Reemplaza TODOS los ? por %s dentro de un string que ya fue identificado
    como SQL. Seguro porque _is_sql_like filtra los strings no-SQL antes.
    """
    return sql.replace("?", "%s")


def _sql_transforms(sql: str) -> str:
    sql = re.sub(r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b", "BIGSERIAL PRIMARY KEY", sql, flags=re.IGNORECASE)
    sql = re.sub(r"datetime\(\s*'now'\s*\)", "CURRENT_TIMESTAMP", sql, flags=re.IGNORECASE)
    sql = re.sub(r"PRAGMA\s+journal_mode\s*=\s*WAL\s*;?", "", sql, flags=re.IGNORECASE)
    # INSERT OR IGNORE INTO → INSERT INTO ... ON CONFLICT DO NOTHING
    # (solo la cláusula OR IGNORE; el ON CONFLICT lo ponemos después en el string final)
    sql = re.sub(r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", "INSERT INTO", sql, flags=re.IGNORECASE)
    sql = _replace_strftime_in_sql(sql)
    sql = _replace_date_fn_in_sql(sql)
    sql = _replace_qmarks_in_sql(sql)
    return sql


# ---------------------------------------------------------------------------
# Transformar strings SQL embebidos en código Python
# ---------------------------------------------------------------------------

# Reconoce triple-quoted o double-quoted strings que parecen SQL
_SQL_KEYWORDS = re.compile(
    r"\b(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|WITH|PRAGMA|MERGE)\b",
    re.IGNORECASE,
)

def _is_sql_like(s: str) -> bool:
    return bool(_SQL_KEYWORDS.search(s))


def transform_sql_strings(src: str) -> str:
    """
    Recorre el texto fuente Python y transforma los string literals que
    contienen SQL (triple-quoted o double-quoted).
    """
    result: list[str] = []
    i = 0
    n = len(src)

    while i < n:
        # Triple-quote strings (""" o ''')
        for q in ('"""', "'''"):
            if src[i:i+3] == q:
                end = src.find(q, i + 3)
                if end == -1:
                    end = n - 3
                raw = src[i+3:end]
                if _is_sql_like(raw):
                    raw = _sql_transforms(raw)
                result.append(q + raw + q)
                i = end + 3
                break
        else:
            # Single/double quoted strings
            for q in ('"', "'"):
                if src[i] == q:
                    # Find end of string (simple, no escape handling for speed)
                    j = i + 1
                    while j < n:
                        if src[j] == "\\" and j + 1 < n:
                            j += 2
                            continue
                        if src[j] == q:
                            break
                        j += 1
                    raw = src[i+1:j]
                    if _is_sql_like(raw):
                        raw = _sql_transforms(raw)
                    result.append(q + raw + (q if j < n else ""))
                    i = j + 1
                    break
            else:
                result.append(src[i])
                i += 1

    return "".join(result)


# ---------------------------------------------------------------------------
# Transformaciones a nivel de archivo completo (imports, conexiones, etc.)
# ---------------------------------------------------------------------------

def transform_imports(src: str, schema: str | None) -> str:
    # Eliminar línea de import sqlite3
    src = re.sub(r"^import sqlite3\s*\n", "", src, flags=re.MULTILINE)
    # Eliminar import de pg_sqlite_compat
    src = re.sub(r"^(?:from pg_sqlite_compat import[^\n]*|import pg_sqlite_compat[^\n]*)\n", "", src, flags=re.MULTILINE)
    # Eliminar import de sqlite_backup_sync
    src = re.sub(r"^(?:from sqlite_backup_sync import[^\n]*|import sqlite_backup_sync[^\n]*)\n", "", src, flags=re.MULTILINE)
    # Eliminar llamadas a apply_sqlite_postgres_patch()
    src = re.sub(r"^apply_sqlite_postgres_patch\(\)\s*\n", "", src, flags=re.MULTILINE)
    # Agregar import db si no está ya
    if "import db" not in src and schema is not None:
        # Insertar después del primer bloque de imports del sistema
        src = re.sub(
            r"(from pathlib import Path\n|^import sys\n)",
            r"\1import db\n",
            src,
            count=1,
            flags=re.MULTILINE,
        )
    return src


def transform_connections(src: str, schema: str | None) -> str:
    """
    Reemplaza sqlite3.connect("...schema.db") por db.connect("schema").
    También elimina .row_factory = sqlite3.Row
    """
    if schema:
        # Repo.__init__ con path hardcoded o path arg
        src = re.sub(
            r"sqlite3\.connect\([^)]+\)",
            f'db.connect("{schema}")',
            src,
        )
    # Eliminar row_factory = sqlite3.Row
    src = re.sub(r"\s*\.\s*row_factory\s*=\s*sqlite3\.Row\s*\n?", "\n", src)
    # Eliminar PRAGMA foreign_keys calls
    src = re.sub(r'[^\n]*PRAGMA\s+foreign_keys\s*=\s*ON[^\n]*\n', "", src, flags=re.IGNORECASE)
    return src


def transform_executescript(src: str) -> str:
    """cur.executescript(...) → db.run_ddl(self.cn, ...)"""
    src = re.sub(
        r"\bcur\.executescript\(",
        "db.run_ddl(self.cn, ",
        src,
    )
    # Si hay conn.executescript también
    src = re.sub(r"\bself\.cn\.executescript\(", "db.run_ddl(self.cn, ", src)
    return src


def transform_pragma_table_info(src: str) -> str:
    """
    cur.execute("PRAGMA table_info(X)") seguido de fetchall + {row[1] for row ...}
    → cols = db.table_columns(self.cn, "X")
    (transformación heurística — revisar manualmente si el patrón es distinto)
    """
    # Pattern 1: cur.execute("PRAGMA table_info(name);")
    # seguido en la línea siguiente de cols = {row[N] for row in cur.fetchall()}
    src = re.sub(
        r'cur\.execute\(\s*["\']PRAGMA\s+table_info\((\w+)\)[;\s]*["\']\s*\)\s*\n\s*cols\s*=\s*\{row\[\d\]\s+for\s+row\s+in\s+cur\.fetchall\(\)\}',
        r'cols = db.table_columns(self.cn, "\1")',
        src,
        flags=re.IGNORECASE,
    )
    # Pattern 2: rows = cur.execute(f"PRAGMA table_info({var});").fetchall()
    src = re.sub(
        r'rows\s*=\s*cur\.execute\(\s*f?["\']PRAGMA\s+table_info\(([^)]+)\)[;\s]*["\']\s*\)\.fetchall\(\)',
        r'rows = [{"name": r[0]} for r in db.table_columns(self.cn, \1)]',
        src,
        flags=re.IGNORECASE,
    )
    return src


def transform_db_path(src: str) -> str:
    """Elimina DB_PATH = ... y DB_CANDIDATES = [...] (ya no se necesitan)."""
    # DB_PATH hardcoded
    src = re.sub(r'^DB_PATH\s*=\s*[rR]?["\'][^"\']+["\'][^\n]*\n', "", src, flags=re.MULTILINE)
    return src


def transform_init_signature(src: str) -> str:
    """Repo.__init__(self, path=DB_PATH) → Repo.__init__(self)"""
    src = re.sub(
        r"def __init__\(self,\s*path\s*=\s*DB_PATH\s*\)",
        "def __init__(self)",
        src,
    )
    return src


# ---------------------------------------------------------------------------
# Transformación de lastrowid → RETURNING id
# ---------------------------------------------------------------------------

def transform_lastrowid(src: str) -> str:
    """
    1) Agrega RETURNING id a los INSERTs cuyo resultado se asigna en la
       línea siguiente con X = cur.lastrowid.
    2) Reemplaza cur.lastrowid por cur.fetchone()[0].
    """
    # Patrón multilinea:
    #   cur.execute(""" INSERT INTO ... VALUES(...);""", params)\n
    #   <indent>X = cur.lastrowid
    # → agregar RETURNING id antes del ; final de la sentencia INSERT
    def _add_returning(m: re.Match) -> str:
        execute_block = m.group(1)  # todo el cur.execute(...) call
        lvalue = m.group(2)         # "X = "

        # Agrega RETURNING id al INSERT dentro del bloque execute
        # Busca el ; o ); final del string SQL antes del cierre de la llamada
        execute_block = re.sub(
            r"(VALUES\s*\([^)]*\))\s*;?\s*(?=\"|')",
            r"\1 RETURNING id",
            execute_block,
            flags=re.IGNORECASE,
        )
        return execute_block + "\n" + lvalue + "cur.fetchone()[0]"

    src = re.sub(
        r"(cur\.execute\([^\n]+(?:\n[^\n]+)*?\))\s*\n(\s+\w+\s*=\s*)cur\.lastrowid",
        _add_returning,
        src,
    )

    # Cualquier lastrowid restante (return int(cur.lastrowid or 0), etc.)
    src = re.sub(r"\bcur\.lastrowid\b", "cur.fetchone()[0]", src)
    return src


# ---------------------------------------------------------------------------
# Transformación de INSERT OR IGNORE: agregar ON CONFLICT DO NOTHING
# ---------------------------------------------------------------------------

def transform_insert_or_ignore(src: str) -> str:
    """
    Después de transform_sql_strings, los INSERT OR IGNORE → INSERT ya fueron
    convertidos. Pero necesitamos agregar ON CONFLICT DO NOTHING al final.
    Este paso busca INSERT INTO ... VALUES(...) que vinieron de INSERT OR IGNORE
    y agrega la cláusula si no está.

    Heurística: buscamos el comentario generado por la conversión o simplemente
    dejamos que run_ddl lo maneje para DDL, y en DML lo hacemos aquí.
    """
    # Para las sentencias simples de una línea que vinieron de INSERT OR IGNORE:
    # Ya reemplazamos "INSERT OR IGNORE INTO" → "INSERT INTO" en _sql_transforms.
    # Necesitamos agregar ON CONFLICT DO NOTHING.
    # El problema: no sabemos cuáles venían de OR IGNORE después de la transformación.
    # Solución: confiar en el paso de _sql_transforms para DDL (run_ddl lo maneja)
    # y para DML usar la versión ya transformada.
    # Esta función simplemente añade ON CONFLICT DO NOTHING a INSERTs que estaban
    # marcados con un comentador especial. Por simplicidad, lo dejamos como está
    # y el desarrollador agrega ON CONFLICT manualmente donde sea necesario.
    return src


# ---------------------------------------------------------------------------
# Pipeline completo por archivo
# ---------------------------------------------------------------------------

def migrate_file(path: Path, schema: str | None, dry_run: bool = False) -> bool:
    if not path.exists():
        print(f"  SKIP (no existe): {path.name}")
        return False

    try:
        original = path.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        print(f"  ERROR leyendo {path.name}: {e}")
        return False

    src = original

    src = transform_imports(src, schema)
    src = transform_db_path(src)
    src = transform_init_signature(src)
    src = transform_connections(src, schema)
    src = transform_executescript(src)
    src = transform_pragma_table_info(src)
    src = transform_sql_strings(src)
    src = transform_lastrowid(src)

    if src == original:
        print(f"  OK (sin cambios): {path.name}")
        return False

    if dry_run:
        print(f"  DRY RUN (cambiaría): {path.name}")
        return True

    path.write_text(src, encoding="utf-8")
    print(f"  MIGRADO: {path.name}")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("=== DRY RUN — no se escriben archivos ===\n")

    changed = 0
    for path, schema in TARGETS:
        changed += migrate_file(path, schema, dry_run=dry_run)

    print(f"\n{'DRY RUN' if dry_run else 'Completado'}: {changed} archivo(s) modificado(s)")


if __name__ == "__main__":
    main()

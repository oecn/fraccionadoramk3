from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PDF_PATH = BASE_DIR / "data" / "GRANOS0209.PDF"
DB_PATH  = BASE_DIR / "db" / "pedidos.db"
SCHEMA   = BASE_DIR / "db" / "schema.sql"

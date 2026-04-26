PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS factura (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  numero         TEXT,
  sucursal       TEXT,
  fecha_emision  TEXT,
  cliente        TEXT,
  ruc_cliente    TEXT,
  condicion_venta TEXT,
  proveedor      TEXT,
  total          REAL,
  total_exentas  REAL,
  total_iva5     REAL,
  total_iva10    REAL,
  pdf_path       TEXT,
  raw_text       TEXT,
  created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_factura_numero ON factura (numero);

CREATE TABLE IF NOT EXISTS factura_item (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  factura_id    INTEGER NOT NULL REFERENCES factura(id) ON DELETE CASCADE,
  linea         INTEGER,
  descripcion   TEXT NOT NULL,
  kg            REAL,
  precio_unitario REAL,
  total_linea   REAL,
  enviado       INTEGER NOT NULL DEFAULT 0
);

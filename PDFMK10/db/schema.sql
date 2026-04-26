PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS orden_compra (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  nro_oc        TEXT NOT NULL,
  sucursal      TEXT,
  fecha_pedido  TEXT,
  monto_total   REAL,
  raw_text      TEXT,
  created_at    TEXT DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_oc_unica ON orden_compra (nro_oc);

CREATE TABLE IF NOT EXISTS orden_item (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  oc_id     INTEGER NOT NULL REFERENCES orden_compra(id) ON DELETE CASCADE,
  linea     INTEGER,
  descripcion TEXT NOT NULL,
  cantidad  REAL,
  unidad    TEXT,
  enviado   INTEGER NOT NULL DEFAULT 0
);

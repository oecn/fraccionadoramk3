# check_fraccionadora.py
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db

DB_PATH = Path(r"C:\Users\osval\Desktop\dev\PDFREADER\GCPDFMK1\GCMK8\fraccionadora.db")

def main():
    if not DB_PATH.exists():
        print(f"❌ No se encontró la base de datos en: {DB_PATH}")
        return

    conn = db.connect("pedidos")
    cur = conn.cursor()

    print(f"📂 Usando base de datos: {DB_PATH}\n")

    # Verificar tabla products
    try:
        cur.execute("SELECT COUNT(*) FROM products;")
        count = cur.fetchone()[0]
        print(f"✅ Tabla 'products' encontrada con {count} filas.")
        if count > 0:
            cur.execute("SELECT id, name FROM products LIMIT 10;")
            print("Ejemplo de productos:")
            for row in cur.fetchall():
                print("   ", row)
    except Exception as e:
        print(f"❌ Problema con tabla 'products': {e}")

    print()

    # Verificar tabla package_stock
    try:
        cur.execute("SELECT COUNT(*) FROM package_stock;")
        count = cur.fetchone()[0]
        print(f"✅ Tabla 'package_stock' encontrada con {count} filas.")
        if count > 0:
            cur.execute("SELECT product_id, gramaje, paquetes FROM package_stock LIMIT 10;")
            print("Ejemplo de stock:")
            for row in cur.fetchall():
                print("   ", row)
    except Exception as e:
        print(f"❌ Problema con tabla 'package_stock': {e}")

    conn.close()

if __name__ == "__main__":
    main()

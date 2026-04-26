# Importador OC desde PDF

## 1. Instalar dependencias
```bash
python -m venv .venv
# Linux/Mac
. .venv/bin/activate
# Windows (PowerShell)
# .venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

## 2. Colocar el PDF
Deja `GRANOS0209.PDF` en `data/`. Ya incluí una copia si el archivo estaba disponible en el entorno.

## 3. Probar por consola
```bash
python console_test.py
```
Verás metadatos, primeros items y se guardará en `db/pedidos.db`.

## 4. Abrir la app Tkinter
```bash
python app_tk.py
```
Selecciona el PDF, pulsa “Parsear y Guardar” y revisa.

## Notas
- Parser robustece: busca OC, fecha, sucursal con regex flexibles.
- Items: intenta extraer tablas con pdfplumber; si falla, recurre a texto con heurísticas.
- Cantidades se normalizan (miles/puntos/comas).

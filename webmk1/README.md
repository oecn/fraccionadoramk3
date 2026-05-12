# WebMK1

Version web inicial del sistema.

## Estructura

```text
webmk1/
  backend/     FastAPI
  frontend/    Angular
```

## Backend

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn main:app --reload
```

Si ya hay otro backend usando `8000`, usar el puerto aislado de WebMK1:

```powershell
uvicorn main:app --reload --host 127.0.0.1 --port 8010
```

## Frontend

```powershell
cd frontend
npm install
npm start
```

Backend esperado: `http://localhost:8000`.
Frontend esperado: `http://localhost:4200`.

Configuracion actual del frontend WebMK1:

```text
API: http://127.0.0.1:8010
Frontend alternativo: http://127.0.0.1:4300
```

Si el puerto `4200` ya esta ocupado:

```powershell
cd frontend
npm run start:4300
```

Frontend alternativo: `http://127.0.0.1:4300`.

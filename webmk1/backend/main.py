from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import health
from modules.chequeras import router as chequeras_router
from modules.compra_materia_prima import router as compra_materia_prima_router
from modules.dashboard import router as dashboard_router
from modules.flujo_dinero import router as flujo_dinero_router
from modules.fraccionamiento import router as fraccionamiento_router
from modules.gastos_egresos import router as gastos_egresos_router
from modules.historial_compras import router as historial_compras_router
from modules.historial_ventas import router as historial_ventas_router
from modules.inventario import router as inventario_router
from modules.ordenes_compra import router as ordenes_compra_router
from modules.productos import router as productos_router
from modules.reportes_ventas import router as reportes_ventas_router
from modules.resumenes import router as resumenes_router
from modules.ventas_paquetes import router as ventas_paquetes_router


app = FastAPI(title="WebMK1 API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",
        "http://127.0.0.1:4200",
        "http://localhost:4300",
        "http://127.0.0.1:4300",
        "http://192.168.10.12:4300",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(chequeras_router.router)
app.include_router(compra_materia_prima_router.router)
app.include_router(dashboard_router.router)
app.include_router(flujo_dinero_router.router)
app.include_router(fraccionamiento_router.router)
app.include_router(gastos_egresos_router.router)
app.include_router(historial_compras_router.router)
app.include_router(historial_ventas_router.router)
app.include_router(inventario_router.router)
app.include_router(ordenes_compra_router.router)
app.include_router(productos_router.router)
app.include_router(reportes_ventas_router.router)
app.include_router(resumenes_router.router)
app.include_router(ventas_paquetes_router.router)

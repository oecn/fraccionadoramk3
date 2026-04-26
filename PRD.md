# PRD — Sistema de Gestión Granos Central

**Versión:** 1.0  
**Fecha:** 2026-04-24  
**Estado:** Activo

---

## 1. Resumen Ejecutivo

**Granos Central** es una empresa que fracciona (empaqueta) granos y alimentos a granel en paquetes de consumo final. El sistema cubre la operación completa: desde la compra de materia prima hasta la venta de los productos fraccionados, pasando por producción, inventario, finanzas, RRHH y reportes.

El stack es **Python desktop** (Tkinter → PySide6), con **SQLite** en desarrollo y **PostgreSQL** en producción. El sistema corre en red local en la planta.

---

## 2. Objetivos del Producto

1. Eliminar el control manual en planillas (papel / Excel) para inventario y producción.
2. Garantizar trazabilidad completa lote → fraccionamiento → venta.
3. Automatizar la importación de facturas de proveedores y liquidaciones de IPS desde PDF.
4. Mantener sincronía de ventas con Google Sheets para reportes gerenciales.
5. Proveer base de datos centralizada en PostgreSQL accesible desde múltiples puestos de trabajo.

---

## 3. Usuarios y Roles

| Rol | Descripción |
|-----|-------------|
| **Operador de Producción** | Registra fracciones, mermas, lotes. |
| **Vendedor / Cajero** | Emite facturas, registra cobros, gestiona crédito. |
| **Administrativo** | Gestiona compras, gastos, RRHH, cuentas bancarias. |
| **Gerencia** | Consulta reportes, analytics, KPIs. |
| **IT / Soporte** | Migración BD, configuración, backups. |

---

## 4. Módulos y Requerimientos Funcionales

### 4.1 Gestión de Materia Prima

| ID | Requerimiento |
|----|--------------|
| MP-01 | Registrar compra de materia prima con número de lote, proveedor, KG, costo unitario. |
| MP-02 | Seguimiento de KG disponibles por producto en tiempo real. |
| MP-03 | Alertas de stock mínimo configurables por producto. |
| MP-04 | Ajustes manuales de inventario con registro de auditoría (motivo, usuario, fecha). |
| MP-05 | Historial de compras por proveedor y producto con totales. |

**Productos gestionados:** Azúcar, Arroz, Pororó, Poroto Rojo, Galleta Molida, Locro, Locrillo, Lenteja.

---

### 4.2 Producción y Fraccionamiento

| ID | Requerimiento |
|----|--------------|
| PR-01 | Registrar fraccionamiento: producto, gramaje, cantidad de unidades, lote de origen. |
| PR-02 | Calcular automáticamente KG consumidos según gramaje y cantidad. |
| PR-03 | Registrar merma por lote (diferencia entre KG teóricos y reales). |
| PR-04 | Cerrar lotes cuando se agota la materia prima. |
| PR-05 | Planificador de producción: estimar cuántas unidades se pueden producir con el stock disponible. |
| PR-06 | Gramajes permitidos por producto: <br>• Arroz, Azúcar → 250g, 500g, 1000g <br>• Lenteja → 250g, 500g <br>• Resto → 200g, 400g, 800g |
| PR-07 | Unidades por bolsa: ≤250g → 20 unidades; >250g → 10 unidades. |

---

### 4.3 Inventario de Producto Terminado

| ID | Requerimiento |
|----|--------------|
| INV-01 | Stock en tiempo real de unidades empaquetadas por producto y gramaje. |
| INV-02 | Descuento automático de stock al emitir factura de venta. |
| INV-03 | Registro de ventas de bolsas sin fraccionar (25kg, 30kg, 50kg). |
| INV-04 | Auditoría visual de inventario (PySide6) con comparativa teórico vs. real. |

---

### 4.4 Ventas y Facturación

| ID | Requerimiento |
|----|--------------|
| VT-01 | Crear facturas con múltiples ítems: producto, gramaje, cantidad, precio, IVA. |
| VT-02 | IVA diferenciado: 5% (alimentos básicos) y 10% (otros). |
| VT-03 | Gestión de precios por producto+gramaje con historial de cambios. |
| VT-04 | Notas de crédito (devoluciones) vinculadas a factura origen. |
| VT-05 | Historial de ventas filtrable por fecha, cliente, producto. |
| VT-06 | Sincronización automática con Google Sheets (hoja 'VENTA') al emitir factura. |
| VT-07 | Ventas de bolsas a granel (no fraccionado) como módulo separado. |
| VT-08 | Edición de ítems de factura con registro de auditoría (antes/después). |

---

### 4.5 Finanzas y Flujo de Dinero

| ID | Requerimiento |
|----|--------------|
| FIN-01 | Registro de gastos categorizados: IPS, salarios, caja chica, gastos generales. |
| FIN-02 | Visualización de flujo de caja (ingresos vs. egresos) con gráficos Matplotlib. |
| FIN-03 | Gestión de cuentas bancarias y chequeras. |
| FIN-04 | Método de pago por venta (efectivo, transferencia, cheque). |
| FIN-05 | Saldo inicial configurable para arranque del sistema. |

---

### 4.6 RRHH y Nómina

| ID | Requerimiento |
|----|--------------|
| RH-01 | Legajo de empleados: nombre, CI, concepto salarial, monto. |
| RH-02 | Registro de anticipos salariales. |
| RH-03 | Integración con módulo de gastos (liquidaciones vinculadas a empleado). |
| RH-04 | Parser de PDF de IPS: extrae datos de liquidaciones de seguridad social automáticamente. |

---

### 4.7 Importación de PDFs

| ID | Requerimiento |
|----|--------------|
| PDF-01 | Parsear órdenes de compra de proveedores desde PDF → guardar en `pedidos.db`. |
| PDF-02 | Parsear facturas de proveedores desde PDF → guardar en `facturas.db`. |
| PDF-03 | Parsear liquidaciones de IPS desde PDF → volcar a módulo de RRHH/gastos. |
| PDF-04 | Catálogo de productos con aliases y regex para matching difuso en PDFs. |
| PDF-05 | Interfaz GUI (Tkinter) y CLI para procesar PDFs en lote o individuales. |

---

### 4.8 Reportes y Analytics

| ID | Requerimiento |
|----|--------------|
| REP-01 | Reporte mensual: ventas, compras, gastos, margen. |
| REP-02 | Reporte trimestral con comparativa entre meses. |
| REP-03 | Métricas de producción: unidades producidas, merma acumulada por producto. |
| REP-04 | Analytics de clientes: frecuencia, ticket promedio, productos preferidos. |
| REP-05 | Vistas SQL compatibles con Power BI (`create_powerbi_views.sql`). |
| REP-06 | Registro de temperatura y humedad (Open-Meteo API) vinculado a lotes de producción. |

---

### 4.9 Infraestructura y Base de Datos

| ID | Requerimiento |
|----|--------------|
| DB-01 | SQLite como base de datos local para desarrollo y operación offline. |
| DB-02 | PostgreSQL como base de datos central en producción (192.168.10.13:5432, DB: GRANOS). |
| DB-03 | Capa de compatibilidad en runtime para traducir queries SQLite → PostgreSQL. |
| DB-04 | Herramienta de migración SQLite → PostgreSQL para los tres esquemas: `fraccionadora`, `facturas`, `pedidos`. |
| DB-05 | Script de sincronización bidireccional SQLite ↔ PostgreSQL para backup y failover. |
| DB-06 | Variable de entorno `DATABASE_URL` para configurar la conexión sin modificar código. |

---

## 5. Requerimientos No Funcionales

| Categoría | Requerimiento |
|-----------|--------------|
| **Plataforma** | Windows 10/11, red local (LAN). |
| **Performance** | Consultas de reportes < 3 segundos sobre 12 meses de datos. |
| **Seguridad** | Credenciales de BD nunca hardcoded en código de producción; usar `DATABASE_URL`. La clave de servicio de Google (`.json`) no debe commitearse a repositorios públicos. |
| **Disponibilidad** | Operación offline posible mediante SQLite local; sincronización con PostgreSQL cuando haya red. |
| **Escalabilidad** | El sistema debe soportar múltiples puestos conectados simultáneamente a PostgreSQL. |
| **Mantenibilidad** | Migración de UI progresiva Tkinter → PySide6 sin romper funcionalidad existente. |
| **Auditabilidad** | Toda modificación de precio, stock o factura queda registrada con timestamp. |

---

## 6. Integraciones Externas

| Sistema | Propósito | Credencial |
|---------|-----------|-----------|
| Google Sheets API | Sync de ventas a hoja 'VENTA' en tiempo real | Service account JSON |
| Open-Meteo API | Temperatura y humedad para registro de producción | Sin clave (API pública) |
| PostgreSQL (LAN) | Base de datos central multi-puesto | `DATABASE_URL` env var |

---

## 7. Modelo de Datos — Tablas Clave

```
fraccionadora schema:
  products, raw_stock, package_stock
  raw_lots, lot_mermas, fractionations, lot_fractionations
  sales, sales_invoices, sales_invoice_items, sales_invoice_item_edits
  credit_notes, bag_sales, purchases
  package_prices, package_price_history, raw_alerts
  stock_adjustments, expenses, weather_readings

facturas schema:
  factura, factura_item

pedidos schema:
  orden_compra, orden_item
```

---

## 8. Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────┐
│                     Puestos de Trabajo (Windows LAN)    │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ fraccionadora│  │ clon/ (Qt)   │  │ PDFMK10/     │  │
│  │ .py (Tkinter)│  │ dashboard,   │  │ parser PDF   │  │
│  │              │  │ reportes     │  │ OC/facturas  │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │
│         │                 │                  │          │
│  ┌──────▼─────────────────▼──────────────────▼───────┐  │
│  │           pg_sqlite_compat.py                     │  │
│  │     (Capa de compatibilidad SQLite ↔ PostgreSQL)  │  │
│  └──────┬────────────────────────────────────────────┘  │
│         │                                               │
└─────────┼───────────────────────────────────────────────┘
          │
   ┌──────▼──────────────────┐     ┌──────────────────────┐
   │  PostgreSQL             │     │  Google Sheets       │
   │  192.168.10.13:5432     │     │  (sheets/sheet.py)   │
   │  DB: GRANOS             │     │  Hoja: VENTA         │
   │  schemas: fraccionadora,│     └──────────────────────┘
   │  facturas, pedidos      │
   └─────────────────────────┘
```

---

## 9. Entregables y Estado Actual

| Módulo | Estado |
|--------|--------|
| Fraccionamiento y producción (Tkinter) | Producción |
| Ventas y facturación | Producción |
| Inventario raw y empaquetado | Producción |
| Flujo de dinero y gastos | Producción |
| Sync Google Sheets | Producción |
| Parser PDF órdenes de compra | Producción |
| Parser PDF facturas proveedores | Producción |
| Parser PDF IPS/nómina | Producción |
| Dashboard PySide6 (clon/) | En desarrollo |
| Reportes Qt (mensual, trimestral) | En desarrollo |
| Migración completa a PostgreSQL | En progreso |
| Módulo RRHH PySide6 | En desarrollo |

---

## 10. Riesgos y Mitigaciones

| Riesgo | Impacto | Mitigación |
|--------|---------|-----------|
| Pérdida de conexión a PostgreSQL | Alto | SQLite local como fallback; sync al reconectar. |
| Cambio de formato en PDF de proveedores | Medio | Catálogo con aliases/regex; actualizar `catalog.py` al detectar cambios. |
| Clave de servicio Google expuesta | Alto | No commitear `.json`; usar variable de entorno o secret manager. |
| Migración Tkinter → PySide6 incompleta | Medio | Mantener ambas versiones operativas en paralelo hasta validar la nueva. |
| Divergencia SQLite ↔ PostgreSQL en esquema | Alto | `pg_sqlite_compat.py` + tests de integración contra ambas BD antes de deploy. |

---

## 11. Glosario

| Término | Definición |
|---------|-----------|
| **Fraccionamiento** | Proceso de empaquetar granel en unidades de consumo (200g–1000g). |
| **Gramaje** | Peso del paquete individual (200g, 250g, 400g, 500g, 800g, 1000g). |
| **Lote / OC** | Número de lote asignado a una compra de materia prima. |
| **Merma** | Diferencia entre KG teóricos y reales al fraccionar (pérdida en proceso). |
| **IPS** | Instituto de Previsión Social (seguridad social paraguaya). |
| **IVA** | Impuesto al Valor Agregado: 5% alimentos básicos, 10% otros. |
| **Guaraní (Gs)** | Moneda local de Paraguay utilizada en todos los valores. |

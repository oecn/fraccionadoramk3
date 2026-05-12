import { Routes } from '@angular/router';

import { ChequerasPageComponent } from './modules/chequeras/pages/chequeras-page.component';
import { CompraMateriaPrimaPageComponent } from './modules/compra-materia-prima/pages/compra-materia-prima-page.component';
import { DashboardPageComponent } from './modules/dashboard/pages/dashboard-page.component';
import { FlujoDineroPageComponent } from './modules/flujo-dinero/pages/flujo-dinero-page.component';
import { FraccionamientoPageComponent } from './modules/fraccionamiento/pages/fraccionamiento-page.component';
import { GastosEgresosPageComponent } from './modules/gastos-egresos/pages/gastos-egresos-page.component';
import { HistorialComprasPageComponent } from './modules/historial-compras/pages/historial-compras-page.component';
import { HistorialVentasPageComponent } from './modules/historial-ventas/pages/historial-ventas-page.component';
import { InventarioPageComponent } from './modules/inventario/pages/inventario-page.component';
import { OrdenesCompraPageComponent } from './modules/ordenes-compra/pages/ordenes-compra-page.component';
import { ProductosPageComponent } from './modules/productos/pages/productos-page.component';
import { ReportesVentasPageComponent } from './modules/reportes-ventas/pages/reportes-ventas-page.component';
import { ResumenesPageComponent } from './modules/resumenes/pages/resumenes-page.component';
import { VentasPaquetesPageComponent } from './modules/ventas-paquetes/pages/ventas-paquetes-page.component';

export const routes: Routes = [
  {
    path: '',
    component: DashboardPageComponent,
    title: 'Inicio Operativo',
  },
  {
    path: 'flujo-dinero',
    component: FlujoDineroPageComponent,
    title: 'Flujo de Dinero',
  },
  {
    path: 'chequeras',
    component: ChequerasPageComponent,
    title: 'Chequeras',
  },
  {
    path: 'gastos-egresos',
    component: GastosEgresosPageComponent,
    title: 'Gastos y Egresos',
  },
  {
    path: 'fraccionamiento',
    component: FraccionamientoPageComponent,
    title: 'Fraccionamiento',
  },
  {
    path: 'resumenes',
    component: ResumenesPageComponent,
    title: 'Resumenes',
  },
  {
    path: 'compra-materia-prima',
    component: CompraMateriaPrimaPageComponent,
    title: 'Compra Materia Prima',
  },
  {
    path: 'ventas-paquetes',
    component: VentasPaquetesPageComponent,
    title: 'Ventas de Paquetes',
  },
  {
    path: 'ordenes-compra',
    component: OrdenesCompraPageComponent,
    title: 'Importar OC',
  },
  {
    path: 'inventario',
    component: InventarioPageComponent,
    title: 'Inventario',
  },
  {
    path: 'productos',
    component: ProductosPageComponent,
    title: 'Productos',
  },
  {
    path: 'historial-compras',
    component: HistorialComprasPageComponent,
    title: 'Historial de Compras',
  },
  {
    path: 'historial-ventas',
    component: HistorialVentasPageComponent,
    title: 'Historial de Ventas',
  },
  {
    path: 'reportes-ventas',
    component: ReportesVentasPageComponent,
    title: 'Reportes de Ventas',
  },
  {
    path: '**',
    redirectTo: '',
  },
];

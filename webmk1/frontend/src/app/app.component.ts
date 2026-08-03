import { CommonModule } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, HostListener, OnInit, computed, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { NavigationEnd, Router, RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';
import { filter } from 'rxjs';

import { dateOffset } from './shared/utils';
import { CobrosFacturasService } from './modules/cobros-facturas/cobros-facturas.service';
import { GastosEgresosService } from './modules/gastos-egresos/gastos-egresos.service';
import { InventarioService } from './modules/inventario/inventario.service';
import { OrdenesCompraService } from './modules/ordenes-compra/ordenes-compra.service';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, RouterLink, RouterLinkActive, RouterOutlet],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class AppComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly cobrosFacturas = inject(CobrosFacturasService);
  private readonly gastosEgresos = inject(GastosEgresosService);
  private readonly inventario = inject(InventarioService);
  private readonly ordenesCompra = inject(OrdenesCompraService);
  private readonly router = inject(Router);
  readonly sidebarOpen = signal<boolean>(false);
  readonly sidebarExpanded = signal<boolean>(true);
  readonly rawAlertsCount = signal<number>(0);
  readonly ocAlertsCount = signal<number>(0);
  readonly cobrosAlertsCount = signal<number>(0);
  readonly gastosFijosPendingCount = signal<number>(0);
  readonly comprasOpen = signal<boolean>(false);
  readonly facturacionOpen = signal<boolean>(false);
  readonly stockOpen = signal<boolean>(false);
  readonly searchQuery = signal<string>('');
  readonly searchItems = [
    { label: 'Dashboard', route: '/' },
    { label: 'Ventas paquetes', route: '/ventas-paquetes' },
    { label: 'Fraccionamiento', route: '/fraccionamiento' },
    { label: 'Rendimiento máquina', route: '/produccion' },
    { label: 'Inventario', route: '/inventario' },
    { label: 'Productos', route: '/productos' },
    { label: 'Compra materia prima', route: '/compra-materia-prima' },
    { label: 'Órdenes de compra', route: '/ordenes-compra' },
    { label: 'Historial compras', route: '/historial-compras' },
    { label: 'Cobros facturas', route: '/cobros-facturas' },
    { label: 'Flujo de dinero', route: '/flujo-dinero' },
    { label: 'Chequeras', route: '/chequeras' },
    { label: 'Gastos y egresos', route: '/gastos-egresos' },
    { label: 'Historial ventas', route: '/historial-ventas' },
    { label: 'Reportes ventas', route: '/reportes-ventas' },
    { label: 'Estadísticas', route: '/estadisticas' },
    { label: 'Análisis', route: '/analisis' },
    { label: 'Resúmenes', route: '/resumenes' },
  ];
  readonly searchResults = computed(() => {
    const query = this.searchQuery().trim().toLowerCase();
    if (!query) {
      return [];
    }
    return this.searchItems.filter((item) => item.label.toLowerCase().includes(query)).slice(0, 6);
  });

  ngOnInit(): void {
    this.loadAlertCounts();
    this.router.events
      .pipe(
        filter((event): event is NavigationEnd => event instanceof NavigationEnd),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe(() => this.loadAlertCounts());
  }

  toggleSidebar(): void {
    if (window.innerWidth <= 760) {
      this.sidebarOpen.update((value) => !value);
      return;
    }
    this.sidebarExpanded.update((value) => !value);
  }

  closeSidebar(): void {
    this.sidebarOpen.set(false);
  }

  updateSearch(event: Event): void {
    const input = event.target as HTMLInputElement;
    this.searchQuery.set(input.value);
  }

  clearSearch(): void {
    this.searchQuery.set('');
  }

  setComprasOpen(event: Event): void {
    const details = event.target as HTMLDetailsElement;
    this.comprasOpen.set(details.open);
  }

  setStockOpen(event: Event): void {
    const details = event.target as HTMLDetailsElement;
    this.stockOpen.set(details.open);
  }

  setFacturacionOpen(event: Event): void {
    const details = event.target as HTMLDetailsElement;
    this.facturacionOpen.set(details.open);
  }

  goToFirstSearchResult(): void {
    const firstResult = this.searchResults()[0];
    if (!firstResult) {
      return;
    }
    this.router.navigateByUrl(firstResult.route);
    this.clearSearch();
    this.closeSidebar();
  }

  toggleExpanded(): void {
    this.sidebarExpanded.update((value) => !value);
  }

  loadRawAlertsCount(): void {
    this.inventario.getSummary().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (summary) => this.rawAlertsCount.set(summary.raw_alerts_count || 0),
      error: () => this.rawAlertsCount.set(0),
    });
  }

  loadOcAlertsCount(): void {
    this.ordenesCompra.pendientesAcumulados().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (summary) => this.ocAlertsCount.set(summary.total_ocs || 0),
      error: () => this.ocAlertsCount.set(0),
    });
  }

  loadCobrosAlertsCount(): void {
    this.cobrosFacturas.summary({ from_date: dateOffset(-30), to_date: dateOffset(30) }).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (summary) => this.cobrosAlertsCount.set(summary.pendientes.length || 0),
      error: () => this.cobrosAlertsCount.set(0),
    });
  }

  loadGastosFijosPendingCount(): void {
    const now = new Date();
    const first = new Date(now.getFullYear(), now.getMonth(), 1);
    const last = new Date(now.getFullYear(), now.getMonth() + 1, 0);
    const fromDate = this.toIsoDate(first);
    const toDate = this.toIsoDate(last);
    this.gastosEgresos.getSummary(fromDate, toDate).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (summary) => this.gastosFijosPendingCount.set((summary.fixed_tasks || []).filter((task) => !task.paid).length),
      error: () => this.gastosFijosPendingCount.set(0),
    });
  }

  loadAlertCounts(): void {
    this.loadRawAlertsCount();
    this.loadOcAlertsCount();
    this.loadCobrosAlertsCount();
    this.loadGastosFijosPendingCount();
  }

  private toIsoDate(date: Date): string {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  @HostListener('document:click', ['$event'])
  onDocumentClick(event: MouseEvent): void {
    const target = event.target as HTMLElement | null;
    if (!target || target.closest('.sidebar') || target.closest('.burger-button')) {
      return;
    }
    if (this.sidebarOpen()) {
      this.closeSidebar();
    }
  }

  @HostListener('window:raw-stock-alerts-changed')
  onRawStockAlertsChanged(): void {
    this.loadRawAlertsCount();
  }

  @HostListener('window:ordenes-compra-alerts-changed')
  onOrdenesCompraAlertsChanged(): void {
    this.loadOcAlertsCount();
  }

  @HostListener('window:cobros-facturas-alerts-changed')
  onCobrosFacturasAlertsChanged(): void {
    this.loadCobrosAlertsCount();
  }

  @HostListener('window:gastos-egresos-alerts-changed')
  onGastosEgresosAlertsChanged(): void {
    this.loadGastosFijosPendingCount();
  }
}

import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

import { FormsModule } from '@angular/forms';

import {
  FacturaItemsResponse,
  ReporteVentaRow,
  ReportesVentasFilters,
  ReportesVentasOptions,
  ReportesVentasResumen,
} from '../models/reportes-ventas.models';
import { ReportesVentasService } from '../reportes-ventas.service';
import { fmtGs } from '../../../shared/formatters';

@Component({
  selector: 'app-reportes-ventas-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './reportes-ventas-page.component.html',
  styleUrl: './reportes-ventas-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ReportesVentasPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(ReportesVentasService);
  readonly fmtGs = fmtGs;

  readonly options = signal<ReportesVentasOptions>({ productos: [], meses: [], gramajes: [] });
  readonly resumen = signal<ReportesVentasResumen | null>(null);
  readonly detalle = signal<FacturaItemsResponse | null>(null);
  readonly loading = signal<boolean>(false);
  readonly detalleLoading = signal<boolean>(false);
  readonly error = signal<string>('');
  readonly activeTab = signal<'mes' | 'semana' | 'detalle'>('mes');
  readonly expandedPeriods = signal<Set<string>>(new Set<string>());

  filters: ReportesVentasFilters = {
    periodo: 'mes',
    product_id: null,
    gramaje: null,
    ym: '',
    desde: '',
    hasta: '',
    ranking_scope: 'month',
    ranking_ym: new Date().toISOString().slice(0, 7),
  };

  ngOnInit(): void {
    this.service.getOptions().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (opts) => {
        this.options.set(opts);
        const firstMonth = opts.meses[0];
        if (firstMonth && !opts.meses.includes(this.filters.ranking_ym)) {
          this.filters.ranking_ym = firstMonth;
        }
        this.refresh();
      },
      error: (err) => {
        this.error.set(err?.error?.detail || 'No se pudieron cargar las opciones');
        this.refresh();
      },
    });
  }

  setTab(tab: 'mes' | 'semana' | 'detalle'): void {
    this.activeTab.set(tab);
    this.filters.periodo = tab === 'semana' ? 'semana' : 'mes';
    if (tab === 'detalle') {
      this.cargarDetalle();
    } else {
      this.refresh();
    }
  }

  refresh(): void {
    this.loading.set(true);
    this.error.set('');
    this.service.getResumen(this.filters).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (data) => {
        this.resumen.set(data);
        this.syncExpandedPeriods(data);
        this.loading.set(false);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(err?.error?.detail || 'No se pudo cargar el reporte');
      },
    });
  }

  cargarDetalle(): void {
    this.detalleLoading.set(true);
    this.error.set('');
    this.service.getDetalleFacturas(this.filters).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (data) => {
        this.detalle.set(data);
        this.detalleLoading.set(false);
      },
      error: (err) => {
        this.detalleLoading.set(false);
        this.error.set(err?.error?.detail || 'No se pudo cargar el detalle');
      },
    });
  }

  limpiarFiltros(): void {
    const month = this.options().meses[0] || new Date().toISOString().slice(0, 7);
    this.filters.product_id = null;
    this.filters.gramaje = null;
    this.filters.ym = '';
    this.filters.desde = '';
    this.filters.hasta = '';
    this.filters.ranking_scope = 'month';
    this.filters.ranking_ym = month;
    this.activeTab() === 'detalle' ? this.cargarDetalle() : this.refresh();
  }

  exportarCsv(): void {
    const data = this.activeTab() === 'detalle' ? this.detalle()?.rows : this.resumen()?.rows;
    if (!data || data.length === 0) return;
    const headers = Object.keys(data[0]);
    const lines = [
      headers.join(';'),
      ...data.map((row) => headers.map((key) => this.csvValue((row as Record<string, unknown>)[key])).join(';')),
    ];
    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `reportes_ventas_${this.activeTab()}_${this.filters.ym || 'rango'}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  totalRows(): number {
    return this.resumen()?.rows.length || 0;
  }

  groupedRows(): Array<{
    periodo: string;
    rows: ReporteVentaRow[];
    paquetes: number;
    importe_gs: number;
    base_gs: number;
    iva_gs: number;
    luque: number;
    aregua: number;
    itaugua: number;
    share_pct: number;
  }> {
    const rows = this.resumen()?.rows || [];
    const totalImporte = rows.reduce((acc, row) => acc + row.importe_gs, 0);
    const groups = new Map<string, ReporteVentaRow[]>();
    rows.forEach((row) => {
      const bucket = groups.get(row.periodo) || [];
      bucket.push(row);
      groups.set(row.periodo, bucket);
    });

    return Array.from(groups.entries()).map(([periodo, groupRows]) => {
      const importe = groupRows.reduce((acc, row) => acc + row.importe_gs, 0);
      return {
        periodo,
        rows: groupRows.map((row) => ({
          ...row,
          share_pct: importe ? (row.importe_gs / importe) * 100 : 0,
        })),
        paquetes: groupRows.reduce((acc, row) => acc + row.paquetes, 0),
        importe_gs: importe,
        base_gs: groupRows.reduce((acc, row) => acc + row.base_gs, 0),
        iva_gs: groupRows.reduce((acc, row) => acc + row.iva_gs, 0),
        luque: groupRows.reduce((acc, row) => acc + row.paq_luque, 0),
        aregua: groupRows.reduce((acc, row) => acc + row.paq_aregua, 0),
        itaugua: groupRows.reduce((acc, row) => acc + row.paq_itaugua, 0),
        share_pct: totalImporte ? (importe / totalImporte) * 100 : 0,
      };
    });
  }

  isExpanded(periodo: string): boolean {
    return this.expandedPeriods().has(periodo);
  }

  togglePeriod(periodo: string): void {
    const next = new Set(this.expandedPeriods());
    next.has(periodo) ? next.delete(periodo) : next.add(periodo);
    this.expandedPeriods.set(next);
  }

  expandAll(): void {
    this.expandedPeriods.set(new Set(this.groupedRows().map((group) => group.periodo)));
  }

  collapseAll(): void {
    this.expandedPeriods.set(new Set<string>());
  }

  fmtPct(value: number | null | undefined): string {
    if (value === null || value === undefined) return 's/d';
    const sign = value > 0 ? '+' : '';
    return `${sign}${value.toFixed(0)}%`;
  }

  trendClass(value: number | null | undefined): string {
    if (value === null || value === undefined || value === 0) return 'flat';
    return value > 0 ? 'up' : 'down';
  }

  kpiValue(key: string, value: number): string {
    if (key === 'importe' || key === 'ticket_promedio') return `${this.fmtGs(value)} Gs`;
    if (key === 'facturas') return this.fmtGs(value);
    return this.fmtGs(value);
  }

  private csvValue(value: unknown): string {
    const raw = value === null || value === undefined ? '' : String(value);
    return `"${raw.replace(/"/g, '""')}"`;
  }

  private syncExpandedPeriods(data: ReportesVentasResumen): void {
    if (this.activeTab() !== 'mes') return;
    const periods = Array.from(new Set(data.rows.map((row) => row.periodo)));
    const current = this.expandedPeriods();
    const kept = periods.filter((period) => current.has(period));
    this.expandedPeriods.set(new Set(kept.length > 0 ? kept : periods.slice(0, 1)));
  }
}

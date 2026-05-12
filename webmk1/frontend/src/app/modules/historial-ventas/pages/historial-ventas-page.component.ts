import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

import { FormsModule } from '@angular/forms';

import {
  HistorialVentasSummary,
  ProductoItem,
  ReporteMensualData,
  VentasDetalleResponse,
} from '../models/historial-ventas.models';
import { HistorialVentasService } from '../historial-ventas.service';
import { fmtGs } from '../../../shared/formatters';
import { dateOffset } from '../../../shared/utils';

@Component({
  selector: 'app-historial-ventas-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './historial-ventas-page.component.html',
  styleUrl: './historial-ventas-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class HistorialVentasPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(HistorialVentasService);
  readonly fmtGs = fmtGs;

  // ── Historial simple ──────────────────────────────────────────
  readonly data = signal<HistorialVentasSummary | null>(null);
  readonly loading = signal<boolean>(false);
  readonly error = signal<string>('');

  filters = {
    search: '',
    from_date: dateOffset(-90),
    to_date: dateOffset(0),
    retencion_mode: false,
  };

  // ── Detalle de ventas ─────────────────────────────────────────
  readonly productos = signal<ProductoItem[]>([]);
  readonly meses = signal<string[]>([]);
  readonly detalle = signal<VentasDetalleResponse | null>(null);
  readonly detalleLoading = signal<boolean>(false);
  readonly detalleError = signal<string>('');

  detalleFilters = {
    periodo: 'mes',
    product_id: null as number | null,
    gramaje: null as number | null,
    ym: new Date().toISOString().slice(0, 7),
    desde: '',
    hasta: '',
  };

  // ── Reporte mensual ───────────────────────────────────────────
  readonly reporte = signal<ReporteMensualData | null>(null);
  readonly reporteLoading = signal<boolean>(false);
  readonly reporteError = signal<string>('');

  reporteFiltros = {
    ym: new Date().toISOString().slice(0, 7),
    empresa: 'Fraccionadora',
  };

  ngOnInit(): void {
    this.refresh();
    this.service.getProductos().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({ next: (p) => this.productos.set(p), error: () => {} });
    this.service.getMeses().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({ next: (m) => this.meses.set(m), error: () => {} });
  }

  refresh(): void {
    this.loading.set(true);
    this.error.set('');
    this.service.getSummary(this.filters).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (d) => { this.data.set(d); this.loading.set(false); },
      error: (err) => { this.loading.set(false); this.error.set(err?.error?.detail || 'No se pudo cargar el historial'); },
    });
  }

  cargarDetalle(): void {
    this.detalleLoading.set(true);
    this.detalleError.set('');
    this.service.getDetalle(this.detalleFilters).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (d) => { this.detalle.set(d); this.detalleLoading.set(false); },
      error: (err) => { this.detalleLoading.set(false); this.detalleError.set(err?.error?.detail || 'No se pudo cargar el detalle'); },
    });
  }

  generarReporte(): void {
    if (!this.reporteFiltros.ym) return;
    this.reporteLoading.set(true);
    this.reporteError.set('');
    this.reporte.set(null);
    this.service.getReporteMensual(this.reporteFiltros.ym, this.reporteFiltros.empresa).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (r) => { this.reporte.set(r); this.reporteLoading.set(false); },
      error: (err) => { this.reporteLoading.set(false); this.reporteError.set(err?.error?.detail || 'No se pudo generar el reporte'); },
    });
  }

  copiarReporte(): void {
    const r = this.reporte();
    if (r) navigator.clipboard.writeText(r.reporte_txt);
  }

  descargarReporte(): void {
    const r = this.reporte();
    if (!r) return;
    const blob = new Blob([r.reporte_txt], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `reporte_mensual_${r.ym}.txt`;
    a.click();
    URL.revokeObjectURL(url);
  }

  fmtPct(value: number | null | undefined): string {
    return Number(value || 0).toFixed(1) + '%';
  }
}

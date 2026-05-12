import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

import { FormsModule } from '@angular/forms';

import { FlujoSummary } from '../models/flujo-dinero.models';
import { FlujoDineroService } from '../flujo-dinero.service';
import { fmtGs } from '../../../shared/formatters';
import { todayIso } from '../../../shared/utils';

type ChartBar = {
  label: string;
  value: number;
  x: number;
  width: number;
  className: string;
};

@Component({
  selector: 'app-flujo-dinero-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './flujo-dinero-page.component.html',
  styleUrl: './flujo-dinero-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class FlujoDineroPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(FlujoDineroService);
  readonly fmtGs = fmtGs;
  readonly chart = {
    width: 980,
    height: 390,
    top: 34,
    right: 34,
    bottom: 54,
    left: 54,
  };

  readonly years = signal<number[]>([]);
  readonly data = signal<FlujoSummary | null>(null);
  readonly loading = signal<boolean>(false);
  readonly error = signal<string>('');

  filters = {
    year: new Date().getFullYear(),
    quarter: 'Todos',
    retencion_mode: false,
    from_date: `${new Date().getFullYear()}-01-01`,
    to_date: todayIso(),
  };

  ngOnInit(): void {
    this.service.getYears().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (years) => {
        this.years.set(years);
        if (!years.includes(this.filters.year) && years.length > 0) {
          this.filters.year = years[years.length - 1];
        }
      },
      error: () => this.years.set([this.filters.year]),
    });
    this.refresh();
  }

  refresh(): void {
    this.loading.set(true);
    this.error.set('');
    this.service.getSummary(this.filters).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (data) => {
        this.data.set(data);
        this.loading.set(false);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(err?.error?.detail || 'No se pudo cargar flujo de dinero');
      },
    });
  }

  flujoClass(value: number): string {
    if (value > 0) return 'ok';
    if (value < 0) return 'danger';
    return '';
  }

  maxBarValue(rows: FlujoSummary['rows']): number {
    return Math.max(1, ...rows.flatMap((row) => [row.ventas, row.compras, row.gastos, row.notas_credito].map(Math.abs)));
  }

  barWidth(value: number, max: number): string {
    return `${Math.max(2, Math.round((Math.abs(value || 0) / max) * 100))}%`;
  }

  chartBars(row: FlujoSummary['rows'][number], index: number, rows: FlujoSummary['rows']): ChartBar[] {
    const center = this.chartX(index, rows.length);
    const width = Math.max(8, Math.min(18, this.groupWidth(rows.length) / 7));
    const gap = width * 0.35;
    const start = center - (width * 4 + gap * 3) / 2;
    return [
      { label: 'Ventas', value: row.ventas, x: start, width, className: 'ventas' },
      { label: 'Compras', value: row.compras, x: start + width + gap, width, className: 'compras' },
      { label: 'Gastos', value: row.gastos, x: start + (width + gap) * 2, width, className: 'gastos' },
      { label: 'NC', value: row.notas_credito, x: start + (width + gap) * 3, width, className: 'notas' },
    ];
  }

  chartX(index: number, total: number): number {
    const plotWidth = this.chart.width - this.chart.left - this.chart.right;
    if (total <= 1) return this.chart.left + plotWidth / 2;
    return this.chart.left + (plotWidth / (total - 1)) * index;
  }

  chartY(value: number, rows: FlujoSummary['rows'], mode: 'main' | 'acumulado' = 'main'): number {
    const range = this.chartRange(rows, mode);
    const plotHeight = this.chart.height - this.chart.top - this.chart.bottom;
    const ratio = (range.max - value) / (range.max - range.min || 1);
    return this.chart.top + ratio * plotHeight;
  }

  chartBarY(value: number, rows: FlujoSummary['rows']): number {
    return Math.min(this.chartY(value, rows, 'main'), this.chartY(0, rows, 'main'));
  }

  chartBarHeight(value: number, rows: FlujoSummary['rows']): number {
    return Math.max(1, Math.abs(this.chartY(value, rows, 'main') - this.chartY(0, rows, 'main')));
  }

  linePoints(rows: FlujoSummary['rows'], key: 'flujo' | 'acumulado'): string {
    return rows
      .map((row, index) => `${this.chartX(index, rows.length)},${this.chartY(row[key], rows, key === 'acumulado' ? 'acumulado' : 'main')}`)
      .join(' ');
  }

  fmtM(value: number): string {
    const amount = Number(value || 0) / 1_000_000;
    if (Math.abs(amount) >= 10) return `${amount.toFixed(0)}M`;
    return `${amount.toFixed(1)}M`;
  }

  showBarLabel(value: number): boolean {
    return Math.abs(value || 0) >= 1_000_000;
  }

  private chartRange(rows: FlujoSummary['rows'], mode: 'main' | 'acumulado'): { min: number; max: number } {
    const values =
      mode === 'acumulado'
        ? rows.flatMap((row) => [row.acumulado, 0])
        : rows.flatMap((row) => [row.ventas, row.compras, row.gastos, row.notas_credito, row.flujo, 0]);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const pad = Math.max((max - min) * 0.12, 1_000_000);
    return { min: min - pad, max: max + pad };
  }

  private groupWidth(total: number): number {
    const plotWidth = this.chart.width - this.chart.left - this.chart.right;
    return total <= 1 ? plotWidth : plotWidth / total;
  }
}

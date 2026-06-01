import { CommonModule } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormsModule } from '@angular/forms';

import { fmtKg, fmtNumber } from '../../../shared/formatters';
import { httpErrorMessage } from '../../../shared/http-error';
import { ProduccionMesRow, ProduccionOptions, ProduccionResumen } from '../models/produccion.models';
import { ProduccionService } from '../produccion.service';

@Component({
  selector: 'app-produccion-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './produccion-page.component.html',
  styleUrl: './produccion-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ProduccionPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(ProduccionService);
  readonly fmtKg = fmtKg;
  readonly fmtNumber = fmtNumber;

  readonly options = signal<ProduccionOptions>({ years: [], current_year: new Date().getFullYear(), current_month: new Date().getMonth() + 1 });
  readonly data = signal<ProduccionResumen | null>(null);
  readonly loading = signal(false);
  readonly error = signal('');

  filters = {
    year: new Date().getFullYear(),
    month: new Date().getMonth() + 1,
    rangeMonths: 12,
  };

  readonly months = [
    { value: 1, label: 'Enero' },
    { value: 2, label: 'Febrero' },
    { value: 3, label: 'Marzo' },
    { value: 4, label: 'Abril' },
    { value: 5, label: 'Mayo' },
    { value: 6, label: 'Junio' },
    { value: 7, label: 'Julio' },
    { value: 8, label: 'Agosto' },
    { value: 9, label: 'Septiembre' },
    { value: 10, label: 'Octubre' },
    { value: 11, label: 'Noviembre' },
    { value: 12, label: 'Diciembre' },
  ];

  ngOnInit(): void {
    this.loadOptions();
  }

  loadOptions(): void {
    this.service.options().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (options) => {
        this.options.set(options);
        this.filters.year = options.current_year;
        this.filters.month = options.current_month;
        this.load();
      },
      error: (err) => this.error.set(httpErrorMessage(err, 'No se pudieron cargar opciones de produccion')),
    });
  }

  load(): void {
    this.loading.set(true);
    this.error.set('');
    this.service.resumen(this.filters.year, this.filters.month, this.filters.rangeMonths).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (data) => {
        this.data.set(data);
        this.loading.set(false);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo cargar rendimiento de maquina'));
      },
    });
  }

  monthLabel(month: number): string {
    return this.months.find((item) => item.value === month)?.label || String(month);
  }

  barWidth(row: ProduccionMesRow): number {
    const max = Math.max(1, ...(this.data()?.trend || []).map((item) => item.total_unidades));
    return Math.round((row.total_unidades / max) * 100);
  }

  trendNewestFirst(data: ProduccionResumen): ProduccionMesRow[] {
    return [...data.trend].reverse();
  }
}

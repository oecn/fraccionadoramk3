import { CommonModule } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormsModule } from '@angular/forms';

import { fmtGs, fmtKg, fmtNumber } from '../../../shared/formatters';
import { dateOffset, todayIso } from '../../../shared/utils';
import { EstadisticasService } from '../estadisticas.service';
import { EstadisticaAlerta, EstadisticasResumen } from '../models/estadisticas.models';

@Component({
  selector: 'app-estadisticas-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './estadisticas-page.component.html',
  styleUrl: './estadisticas-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class EstadisticasPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(EstadisticasService);
  readonly fmtGs = fmtGs;
  readonly fmtKg = fmtKg;
  readonly fmtNumber = fmtNumber;

  readonly data = signal<EstadisticasResumen | null>(null);
  readonly loading = signal<boolean>(false);
  readonly error = signal<string>('');
  readonly selectedAlert = signal<EstadisticaAlerta | null>(null);

  filters = {
    from_date: dateOffset(-30),
    to_date: todayIso(),
    top_n: 12,
  };

  ngOnInit(): void {
    this.refresh();
  }

  refresh(): void {
    this.loading.set(true);
    this.error.set('');
    this.service.resumen(this.filters).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (data) => {
        this.data.set(data);
        this.loading.set(false);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(err?.error?.detail || 'No se pudieron cargar las estadisticas');
      },
    });
  }

  coverageLabel(value: number | null): string {
    if (value === null || value === undefined) return 'Sin consumo';
    return `${this.fmtNumber(value, 1)} dias`;
  }

  stateClass(value: string): string {
    const txt = (value || '').toLowerCase();
    if (txt.includes('critic')) return 'bad';
    if (txt.includes('atencion')) return 'warn';
    if (txt.includes('normal')) return 'good';
    return 'flat';
  }

  alertClass(value: string): string {
    const txt = (value || '').toLowerCase();
    if (txt === 'alta') return 'bad';
    if (txt === 'media') return 'warn';
    return 'good';
  }

  openAlert(alert: EstadisticaAlerta): void {
    this.selectedAlert.set(alert);
  }

  closeAlert(): void {
    this.selectedAlert.set(null);
  }
}

import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

import { FormsModule } from '@angular/forms';

import { HistorialComprasSummary } from '../models/historial-compras.models';
import { HistorialComprasService } from '../historial-compras.service';
import { fmtGs } from '../../../shared/formatters';
import { dateOffset } from '../../../shared/utils';

@Component({
  selector: 'app-historial-compras-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './historial-compras-page.component.html',
  styleUrl: './historial-compras-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class HistorialComprasPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(HistorialComprasService);
  readonly fmtGs = fmtGs;

  readonly data = signal<HistorialComprasSummary | null>(null);
  readonly loading = signal<boolean>(false);
  readonly error = signal<string>('');

  filters = {
    search: '',
    from_date: dateOffset(-90),
    to_date: dateOffset(0),
  };

  ngOnInit(): void {
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
        this.error.set(err?.error?.detail || 'No se pudo cargar el historial de compras');
      },
    });
  }
}

import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';


import { httpErrorMessage } from '../../../shared/http-error';
import { fmtGs, fmtNumber } from '../../../shared/formatters';
import { productPillClass } from '../../../shared/utils';
import { InventarioService } from '../inventario.service';
import { InventorySummary } from '../models/inventario.models';

@Component({
  selector: 'app-inventario-page',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './inventario-page.component.html',
  styleUrl: './inventario-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class InventarioPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(InventarioService);
  readonly fmtGs = fmtGs;
  readonly fmtNumber = fmtNumber;
  readonly productPillClass = productPillClass;

  readonly loading = signal<boolean>(false);
  readonly error = signal<string>('');
  readonly summary = signal<InventorySummary>({
    raw_stock: [],
    package_stock: [],
    lotes_abiertos: [],
    total_raw_kg: 0,
    total_raw_valor_gs: 0,
    total_paquetes: 0,
    total_unidades: 0,
    total_venta_gs: 0,
  });

  ngOnInit(): void {
    this.load();
  }

  load(): void {
    this.loading.set(true);
    this.error.set('');
    this.service.getSummary().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (summary) => {
        this.summary.set(summary);
        this.loading.set(false);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo cargar inventario'));
      },
    });
  }
}

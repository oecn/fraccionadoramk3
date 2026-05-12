import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

import { FormsModule } from '@angular/forms';

import { fmtGs } from '../../../shared/formatters';
import { productPillClass } from '../../../shared/utils';
import { httpErrorMessage } from '../../../shared/http-error';
import { PrecioHistoryResponse, PrecioRow } from '../models/productos.models';
import { ProductosService } from '../productos.service';

@Component({
  selector: 'app-productos-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './productos-page.component.html',
  styleUrl: './productos-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ProductosPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(ProductosService);
  readonly fmtGs = fmtGs;
  readonly productPillClass = productPillClass;

  readonly loading = signal(false);
  readonly savingKey = signal('');
  readonly error = signal('');
  readonly message = signal('');
  readonly rows = signal<PrecioRow[]>([]);
  readonly historyLoading = signal(false);
  readonly history = signal<PrecioHistoryResponse | null>(null);

  ngOnInit(): void {
    this.load();
  }

  load(): void {
    this.loading.set(true);
    this.error.set('');
    this.service.precios().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (data) => {
        this.rows.set(data.rows);
        this.loading.set(false);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudieron cargar productos'));
      },
    });
  }

  save(row: PrecioRow): void {
    const key = this.rowKey(row);
    this.savingKey.set(key);
    this.error.set('');
    this.message.set('');
    this.service.updatePrecio({
      product_id: row.product_id,
      gramaje: row.gramaje,
      price_gs: Number(row.price_gs || 0),
      iva: Number(row.iva || 10),
    }).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (updated) => {
        this.rows.update((rows) => rows.map((r) => this.rowKey(r) === key ? updated : r));
        this.savingKey.set('');
        this.message.set(`Precio actualizado: ${updated.producto} ${updated.gramaje} g`);
      },
      error: (err) => {
        this.savingKey.set('');
        this.error.set(httpErrorMessage(err, 'No se pudo guardar precio'));
      },
    });
  }

  openHistory(row: PrecioRow): void {
    this.historyLoading.set(true);
    this.error.set('');
    this.history.set({
      product_id: row.product_id,
      producto: row.producto,
      gramaje: row.gramaje,
      rows: [],
    });
    this.service.history(row.product_id, row.gramaje).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (history) => {
        this.history.set(history);
        this.historyLoading.set(false);
      },
      error: (err) => {
        this.historyLoading.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo cargar historico'));
      },
    });
  }

  closeHistory(): void {
    this.history.set(null);
  }

  chartPoints(history: PrecioHistoryResponse): string {
    const rows = history.rows;
    if (!rows.length) return '';
    const values = rows.map((r) => Number(r.price_gs || 0));
    const min = Math.min(...values);
    const max = Math.max(...values);
    const span = max - min || 1;
    return rows.map((row, index) => {
      const x = rows.length === 1 ? 50 : (index / (rows.length - 1)) * 100;
      const y = 90 - ((Number(row.price_gs || 0) - min) / span) * 75;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    }).join(' ');
  }

  chartDots(history: PrecioHistoryResponse): Array<{ x: number; y: number; row: { fecha: string; price_gs: number; iva: number } }> {
    const rows = history.rows;
    if (!rows.length) return [];
    const values = rows.map((r) => Number(r.price_gs || 0));
    const min = Math.min(...values);
    const max = Math.max(...values);
    const span = max - min || 1;
    return rows.map((row, index) => ({
      x: rows.length === 1 ? 50 : (index / (rows.length - 1)) * 100,
      y: 90 - ((Number(row.price_gs || 0) - min) / span) * 75,
      row,
    }));
  }

  firstDate(history: PrecioHistoryResponse): string {
    return history.rows[0]?.fecha?.slice(0, 10) || '-';
  }

  lastDate(history: PrecioHistoryResponse): string {
    return history.rows[history.rows.length - 1]?.fecha?.slice(0, 10) || '-';
  }

  rowKey(row: PrecioRow): string {
    return `${row.product_id}-${row.gramaje}`;
  }

  totalValorStock(): number {
    return this.rows().reduce((acc, row) => acc + Number(row.price_gs || 0) * Number(row.paquetes_stock || 0), 0);
  }
}

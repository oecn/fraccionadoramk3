import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

import { FormsModule } from '@angular/forms';

import { fmtGs, fmtKg } from '../../../shared/formatters';
import { productPillClass } from '../../../shared/utils';
import { httpErrorMessage } from '../../../shared/http-error';
import { LoteDetalle, LoteResumenRow, ResumenesOptions } from '../models/resumenes.models';
import { ResumenesService } from '../resumenes.service';

@Component({
  selector: 'app-resumenes-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './resumenes-page.component.html',
  styleUrl: './resumenes-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ResumenesPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(ResumenesService);
  readonly fmtGs = fmtGs;
  readonly productPillClass = productPillClass;
  readonly fmtKg = fmtKg;

  readonly loading = signal(false);
  readonly detailLoading = signal(false);
  readonly saving = signal(false);
  readonly error = signal('');
  readonly message = signal('');
  readonly options = signal<ResumenesOptions>({ productos: [] });
  readonly lotes = signal<LoteResumenRow[]>([]);
  readonly selected = signal<LoteDetalle | null>(null);

  productId: number | null = null;
  soloAbiertos = false;
  selectedLotId: number | null = null;

  ngOnInit(): void {
    this.service.options().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (options) => this.options.set(options),
      error: (err) => this.error.set(httpErrorMessage(err, 'No se pudieron cargar productos')),
    });
    this.loadLotes();
  }

  loadLotes(): void {
    this.loading.set(true);
    this.error.set('');
    this.service.lotes(this.productId, this.soloAbiertos).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (rows) => {
        this.lotes.set(rows);
        this.loading.set(false);
        if (this.selectedLotId && rows.some((r) => r.id === this.selectedLotId)) {
          this.loadDetalle(this.selectedLotId);
        } else {
          this.selectedLotId = null;
          this.selected.set(null);
        }
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudieron cargar lotes'));
      },
    });
  }

  selectLot(row: LoteResumenRow): void {
    this.selectedLotId = row.id;
    this.loadDetalle(row.id);
  }

  loadDetalle(lotId: number): void {
    this.detailLoading.set(true);
    this.error.set('');
    this.service.detalle(lotId).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (detail) => {
        this.selected.set(detail);
        this.detailLoading.set(false);
      },
      error: (err) => {
        this.detailLoading.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo cargar detalle del lote'));
      },
    });
  }

  toggleCerrado(): void {
    const detail = this.selected();
    if (!detail) return;
    this.saving.set(true);
    this.message.set('');
    const request = detail.cerrado ? this.service.abrir(detail.id) : this.service.cerrar(detail.id);
    request.pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (updated) => {
        this.selected.set(updated);
        this.saving.set(false);
        this.message.set(updated.cerrado ? 'Lote cerrado.' : 'Lote abierto.');
        this.loadLotes();
      },
      error: (err) => {
        this.saving.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo actualizar el lote'));
      },
    });
  }

  totalDisponible(): number {
    return this.lotes().reduce((acc, row) => acc + Number(row.kg_disponible || 0), 0);
  }

  consumoWidth(detail: LoteDetalle): number {
    return Math.max(0, Math.min(100, detail.consumo_pct));
  }

  mermaWidth(detail: LoteDetalle): number {
    const total = Math.max(0, Math.min(100, detail.consumo_pct + detail.merma_pct));
    return Math.max(0, total - this.consumoWidth(detail));
  }
}


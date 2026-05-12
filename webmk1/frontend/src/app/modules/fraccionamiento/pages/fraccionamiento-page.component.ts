import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

import { FormsModule } from '@angular/forms';

import {
  ConsumoPreview,
  FraccionamientoCreate,
  FraccionamientoOptions,
  FraccionamientoSummary,
  PackageStockRow,
  ProductoItem,
} from '../models/fraccionamiento.models';
import { FraccionamientoService } from '../fraccionamiento.service';
import { httpErrorMessage } from '../../../shared/http-error';
import { fmtGs, fmtKg } from '../../../shared/formatters';
import { productPillClass, todayIso } from '../../../shared/utils';

@Component({
  selector: 'app-fraccionamiento-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './fraccionamiento-page.component.html',
  styleUrl: './fraccionamiento-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class FraccionamientoPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(FraccionamientoService);
  readonly fmtGs = fmtGs;
  readonly productPillClass = productPillClass;
  readonly fmtKg = fmtKg;

  readonly options = signal<FraccionamientoOptions>({ productos: [], lotes: [], hoy: '' });
  readonly summary = signal<FraccionamientoSummary | null>(null);
  readonly preview = signal<ConsumoPreview | null>(null);
  readonly loading = signal<boolean>(false);
  readonly saving = signal<boolean>(false);
  readonly error = signal<string>('');
  readonly message = signal<string>('');
  showOnlyPackageStock = true;

  form: FraccionamientoCreate = {
    product_id: null,
    gramaje: null,
    paquetes: 0,
    fecha: todayIso(),
    lot_id: null,
  };

  filters = {
    product_id: null as number | null,
    desde: '',
    hasta: '',
  };

  ngOnInit(): void {
    this.loadOptions();
    this.refresh();
  }

  loadOptions(): void {
    this.service.getOptions().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (opts) => {
        this.options.set(opts);
        this.form.fecha = opts.hoy || this.form.fecha;
        if (!this.form.product_id && opts.productos.length > 0) {
          this.form.product_id = opts.productos[0].id;
          this.form.gramaje = opts.productos[0].gramajes[0] || null;
          this.updatePreview();
        }
      },
      error: (err) => this.error.set(httpErrorMessage(err, 'No se pudieron cargar las opciones')),
    });
  }

  onProductChange(): void {
    const product = this.selectedProduct();
    this.form.gramaje = product?.gramajes[0] || null;
    this.form.lot_id = null;
    this.updatePreview();
  }

  updatePreview(): void {
    if (!this.form.product_id || !this.form.gramaje || !this.form.paquetes) {
      this.preview.set(null);
      return;
    }
    this.service.getPreview(this.form.product_id, this.form.gramaje, this.form.paquetes).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (data) => this.preview.set(data),
      error: () => this.preview.set(null),
    });
  }

  refresh(): void {
    this.loading.set(true);
    this.error.set('');
    this.service.getSummary(this.filters).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (data) => {
        this.summary.set(data);
        this.loading.set(false);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo cargar fraccionamiento'));
      },
    });
  }

  registrar(): void {
    if (!this.form.product_id || !this.form.gramaje || this.form.paquetes <= 0) {
      this.error.set('Seleccione producto, gramaje y paquetes validos.');
      return;
    }
    this.saving.set(true);
    this.error.set('');
    this.message.set('');
    this.service.registrar(this.form).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (row) => {
        this.saving.set(false);
        this.message.set(`Fraccionado: ${row.paquetes} paquetes de ${row.gramaje} g en ${row.producto}.`);
        this.form.paquetes = 0;
        this.form.lot_id = null;
        this.preview.set(null);
        this.loadOptions();
        this.refresh();
      },
      error: (err) => {
        this.saving.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo registrar el fraccionamiento'));
      },
    });
  }

  selectedProduct(): ProductoItem | null {
    const pid = this.form.product_id;
    return this.options().productos.find((p) => p.id === pid) || null;
  }

  lotesDisponibles() {
    const pid = this.form.product_id;
    return this.options().lotes.filter((l) => l.product_id === pid);
  }

  packageStockRows(summary: FraccionamientoSummary): PackageStockRow[] {
    if (this.showOnlyPackageStock) {
      return summary.package_stock.filter((row) => row.paquetes > 0);
    }

    const current = new Map<string, PackageStockRow>();
    for (const row of summary.package_stock) {
      current.set(`${row.product_id}:${row.gramaje}`, row);
    }

    return this.options()
      .productos.filter((producto) => !this.filters.product_id || producto.id === this.filters.product_id)
      .flatMap((producto) =>
        producto.gramajes.map((gramaje) => {
          const row = current.get(`${producto.id}:${gramaje}`);
          return row || { product_id: producto.id, producto: producto.name, gramaje, paquetes: 0 };
        }),
      );
  }
}

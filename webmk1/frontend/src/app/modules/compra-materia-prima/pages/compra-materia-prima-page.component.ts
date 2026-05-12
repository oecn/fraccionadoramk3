import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

import { FormsModule } from '@angular/forms';

import {
  CompraMateriaPrimaCreate,
  CompraMateriaPrimaOptions,
  CompraMateriaPrimaSummary,
  FacturaCompraImportRequest,
  FacturaCompraPreview,
} from '../models/compra-materia-prima.models';
import { CompraMateriaPrimaService } from '../compra-materia-prima.service';
import { httpErrorMessage } from '../../../shared/http-error';
import { fmtGs, fmtKg } from '../../../shared/formatters';
import { productPillClass } from '../../../shared/utils';

@Component({
  selector: 'app-compra-materia-prima-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './compra-materia-prima-page.component.html',
  styleUrl: './compra-materia-prima-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class CompraMateriaPrimaPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(CompraMateriaPrimaService);
  readonly fmtGs = fmtGs;
  readonly productPillClass = productPillClass;
  readonly fmtKg = fmtKg;

  readonly options = signal<CompraMateriaPrimaOptions>({ productos: [], bolsa_kg_presets: [25, 30, 50] });
  readonly summary = signal<CompraMateriaPrimaSummary | null>(null);
  readonly loading = signal<boolean>(false);
  readonly saving = signal<boolean>(false);
  readonly parsingFactura = signal<boolean>(false);
  readonly importingFactura = signal<boolean>(false);
  readonly error = signal<string>('');
  readonly message = signal<string>('');
  readonly facturaPreview = signal<FacturaCompraPreview | null>(null);
  facturaFileName = '';
  facturaFile: File | null = null;

  filters = {
    product_id: null as number | null,
  };

  form: CompraMateriaPrimaCreate = {
    product_id: null,
    lote: '',
    proveedor: '',
    factura: '',
    bolsa_kg: 50,
    bolsas: 0,
    costo_total_gs: 0,
  };

  useCustomBagKg = false;

  ngOnInit(): void {
    this.loadOptions();
    this.refresh();
  }

  loadOptions(): void {
    this.service.getOptions().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (options) => {
        this.options.set(options);
        if (!this.form.product_id && options.productos.length > 0) {
          this.form.product_id = options.productos[0].id;
          this.filters.product_id = options.productos[0].id;
          this.refresh();
        }
      },
      error: (err) => this.error.set(httpErrorMessage(err, 'No se pudieron cargar las opciones')),
    });
  }

  refresh(): void {
    this.loading.set(true);
    this.error.set('');
    this.service.getSummary(this.filters.product_id).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (summary) => {
        this.summary.set(summary);
        this.loading.set(false);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo cargar compra de materia prima'));
      },
    });
  }

  onFilterProductChange(): void {
    this.form.product_id = this.filters.product_id || this.form.product_id;
    this.refresh();
  }

  registrar(): void {
    if (!this.form.product_id || !this.form.bolsa_kg || this.form.bolsas <= 0) {
      this.error.set('Seleccione producto, kg por bolsa y cantidad de bolsas validos.');
      return;
    }

    this.saving.set(true);
    this.error.set('');
    this.message.set('');
    this.service.registrar(this.form).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (row) => {
        this.saving.set(false);
        this.message.set(`Lote registrado: ${row.producto} ${row.lote || 's/lote'} (${this.fmtKg(row.kg_inicial)} kg).`);
        this.form.lote = '';
        this.form.proveedor = '';
        this.form.factura = '';
        this.form.bolsas = 0;
        this.form.costo_total_gs = 0;
        if (this.useCustomBagKg) {
          this.form.bolsa_kg = null;
        }
        this.loadOptions();
        this.refresh();
      },
      error: (err) => {
        this.saving.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo registrar la compra'));
      },
    });
  }

  onFacturaFileChange(event: Event): void {
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0] || null;
    this.facturaFile = file;
    this.facturaFileName = file?.name || '';
    this.facturaPreview.set(null);
    this.error.set('');
    this.message.set('');
  }

  parseFactura(): void {
    if (!this.facturaFile) {
      this.error.set('Seleccione un PDF de factura.');
      return;
    }
    this.parsingFactura.set(true);
    this.error.set('');
    this.message.set('');
    this.service.parseFactura(this.facturaFile).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (preview) => {
        this.parsingFactura.set(false);
        this.facturaPreview.set(preview);
      },
      error: (err) => {
        this.parsingFactura.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo parsear la factura'));
      },
    });
  }

  importarFactura(): void {
    const preview = this.facturaPreview();
    if (!preview) {
      return;
    }
    const items = preview.items.filter((item) => item.importable && item.product_id);
    if (items.length === 0) {
      this.error.set('No hay items importables en la factura.');
      return;
    }
    const payload: FacturaCompraImportRequest = {
      numero: preview.numero,
      proveedor: preview.proveedor,
      fecha_emision: preview.fecha_emision,
      items: items.map((item) => ({
        product_id: item.product_id as number,
        descripcion: item.descripcion,
        kg: item.kg,
        total_linea: item.total_linea,
        bolsa_kg: item.bolsa_kg,
        bolsas: item.bolsas,
      })),
    };
    this.importingFactura.set(true);
    this.error.set('');
    this.message.set('');
    this.service.importarFactura(payload).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (res) => {
        this.importingFactura.set(false);
        this.message.set(res.message);
        this.facturaPreview.set(null);
        this.facturaFile = null;
        this.facturaFileName = '';
        this.loadOptions();
        this.refresh();
      },
      error: (err) => {
        this.importingFactura.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo importar la factura'));
      },
    });
  }

  importableFacturaItems(): number {
    return this.facturaPreview()?.items.filter((item) => item.importable).length || 0;
  }

  onBagModeChange(): void {
    this.form.bolsa_kg = this.useCustomBagKg ? null : 50;
  }

  kgTotal(): number {
    return Number(this.form.bolsa_kg || 0) * Number(this.form.bolsas || 0);
  }

  costoKg(): number {
    const kg = this.kgTotal();
    return kg > 0 ? Number(this.form.costo_total_gs || 0) / kg : 0;
  }
}

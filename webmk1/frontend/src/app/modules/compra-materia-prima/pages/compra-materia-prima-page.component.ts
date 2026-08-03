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
  LoteAbiertoRow,
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
  readonly deletingLote = signal<boolean>(false);
  readonly reviewingPrice = signal<number | null>(null);
  readonly deleteDialogOpen = signal<boolean>(false);
  readonly loteToDelete = signal<LoteAbiertoRow | null>(null);
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
    fecha: this.today(),
    lote: '',
    proveedor: '',
    factura: '',
    bolsa_kg: 50,
    bolsas: 0,
    costo_total_gs: 0,
  };

  useCustomBagKg = false;
  deleteForm = {
    motivo: '',
  };
  priceReviewForm = {
    revisado_por: 'Admin',
    estado: 'Revisado y OK',
  };

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
        gravada5_gs: item.gravada5_gs,
        iva5_gs: item.iva5_gs,
        gravada10_gs: item.gravada10_gs,
        iva10_gs: item.iva10_gs,
        exenta_gs: item.exenta_gs,
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

  private today(): string {
    return new Date().toISOString().slice(0, 10);
  }

  kgTotal(): number {
    return Number(this.form.bolsa_kg || 0) * Number(this.form.bolsas || 0);
  }

  costoKg(): number {
    const kg = this.kgTotal();
    return kg > 0 ? Number(this.form.costo_total_gs || 0) / kg : 0;
  }

  selectedProductLastCost(): number | null {
    const productId = this.form.product_id;
    if (!productId) return null;
    return this.options().productos.find((p) => p.id === productId)?.ultimo_costo_kg_gs ?? null;
  }

  manualPriceVariationPct(): number | null {
    return this.priceVariationPct(this.costoKg(), this.selectedProductLastCost());
  }

  priceVariationPct(current: number, previous: number | null | undefined): number | null {
    if (!previous || previous <= 0 || !current) return null;
    return ((Number(current) - Number(previous)) / Number(previous)) * 100;
  }

  priceToneClass(value: number | null | undefined): string {
    if (value === null || value === undefined) return 'neutral';
    if (value > 0.01) return 'up';
    if (value < -0.01) return 'down';
    return 'same';
  }

  priceToneLabel(value: number | null | undefined): string {
    if (value === null || value === undefined) return 'Sin compra anterior';
    if (value > 0.01) return `Subió ${Math.abs(value).toFixed(1)}%`;
    if (value < -0.01) return `Bajó ${Math.abs(value).toFixed(1)}%`;
    return 'Sin variación';
  }

  pendingPriceAlerts(): LoteAbiertoRow[] {
    return this.summary()?.lotes_abiertos.filter((row) => row.precio_cambio_detectado) || [];
  }

  priceChangeMessage(row: LoteAbiertoRow): string {
    return `Se ha detectado un cambio de precios. Favor revisar ${row.producto} y realizar cambio de precio de ser necesario.`;
  }

  importFacturaPriceAlerts(): number {
    return this.facturaPreview()?.items.filter((item) => item.importable && item.variacion_costo_pct !== null && Math.abs(item.variacion_costo_pct || 0) > 0.01).length || 0;
  }

  priceImpactLabel(row: LoteAbiertoRow): string {
    return `Diferencia ${this.fmtGs(row.diferencia_costo_kg_gs)} / kg; impacto lote ${this.fmtGs(row.diferencia_costo_total_gs)} Gs`;
  }

  pctLabel(value: number | null | undefined): string {
    return value === null || value === undefined ? '-' : `${Number(value).toFixed(1)}%`;
  }

  markPriceReviewed(row: LoteAbiertoRow, estado = this.priceReviewForm.estado): void {
    this.reviewingPrice.set(row.id);
    this.error.set('');
    this.message.set('');
    this.service.marcarPrecioRevisado(row.id, {
      estado,
      revisado_por: this.priceReviewForm.revisado_por,
    }).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (res) => {
        this.reviewingPrice.set(null);
        this.message.set(res.message);
        this.refresh();
      },
      error: (err) => {
        this.reviewingPrice.set(null);
        this.error.set(httpErrorMessage(err, 'No se pudo marcar el precio como revisado'));
      },
    });
  }

  openDeleteDialog(row: LoteAbiertoRow): void {
    this.loteToDelete.set(row);
    this.deleteForm.motivo = '';
    this.error.set('');
    this.deleteDialogOpen.set(true);
  }

  closeDeleteDialog(): void {
    if (this.deletingLote()) return;
    this.deleteDialogOpen.set(false);
    this.loteToDelete.set(null);
    this.deleteForm.motivo = '';
  }

  confirmDeleteLote(): void {
    const row = this.loteToDelete();
    const motivo = this.deleteForm.motivo.trim();
    if (!row) return;
    if (motivo.length < 3) {
      this.error.set('Ingrese el motivo de eliminacion del lote.');
      return;
    }
    this.deletingLote.set(true);
    this.error.set('');
    this.message.set('');
    this.service.eliminarLote(row.id, { motivo }).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (res) => {
        this.deletingLote.set(false);
        this.message.set(res.message);
        this.closeDeleteDialog();
        this.loadOptions();
        this.refresh();
      },
      error: (err) => {
        this.deletingLote.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo eliminar el lote'));
      },
    });
  }
}

import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

import { FormsModule } from '@angular/forms';

import { VentaOptions, VentaStockItem } from '../models/ventas-paquetes.models';
import { VentasPaquetesService } from '../ventas-paquetes.service';
import { httpErrorMessage } from '../../../shared/http-error';
import { fmtGs } from '../../../shared/formatters';
import { productPillClass, todayIso } from '../../../shared/utils';

@Component({
  selector: 'app-ventas-paquetes-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './ventas-paquetes-page.component.html',
  styleUrl: './ventas-paquetes-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class VentasPaquetesPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(VentasPaquetesService);
  readonly fmtGs = fmtGs;
  readonly productPillClass = productPillClass;

  readonly options = signal<VentaOptions>({ stock: [], hoy: '' });
  readonly loading = signal<boolean>(false);
  readonly saving = signal<boolean>(false);
  readonly error = signal<string>('');
  readonly message = signal<string>('');

  invoice = {
    invoice_no: '',
    customer: '',
    fecha: todayIso(),
    send_to_sheet: true,
  };

  line = {
    key: '',
    cantidad: 1,
  };

  sellQuantities: Record<string, number | null> = {};

  ngOnInit(): void {
    this.loadOptions();
  }

  loadOptions(): void {
    this.loading.set(true);
    this.error.set('');
    this.service.getOptions().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (options) => {
        this.options.set(options);
        this.invoice.fecha = options.hoy || this.invoice.fecha;
        this.loading.set(false);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudieron cargar las opciones de venta'));
      },
    });
  }

  registrar(): void {
    const items = this.invoiceItems();
    if (items.length === 0) {
      this.error.set('Cargue al menos una cantidad para vender.');
      return;
    }
    const invalid = this.options().stock.find((item) => this.isOverStock(item));
    if (invalid) {
      this.error.set(`Stock insuficiente para ${invalid.producto} ${invalid.gramaje} g. Disponible: ${invalid.paquetes}.`);
      return;
    }

    this.saving.set(true);
    this.error.set('');
    this.message.set('');
    this.service
      .registrar({
          invoice_no: this.invoice.invoice_no,
          customer: this.invoice.customer,
          fecha: this.invoice.fecha,
          send_to_sheet: this.invoice.send_to_sheet,
          items: items.map((item) => ({
          product_id: item.product_id,
          gramaje: item.gramaje,
          cantidad: item.cantidad,
        })),
      })
      .pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
        next: (res) => {
          this.saving.set(false);
          const sheetMsg = res.sheet_sent
            ? ' Enviada a Google Sheets.'
            : !this.invoice.send_to_sheet
              ? ' No se envio a Google Sheets por opcion desmarcada.'
            : ` No se envio a Google Sheets: ${res.sheet_error || 'sin detalle'}.`;
          this.message.set(`Factura #${res.invoice_id} registrada por ${this.fmtGs(res.total_gs)} Gs.${sheetMsg}`);
          this.sellQuantities = {};
          this.invoice.invoice_no = '';
          this.invoice.customer = '';
          this.loadOptions();
        },
        error: (err) => {
          this.saving.set(false);
          this.error.set(httpErrorMessage(err, 'No se pudo registrar la venta'));
        },
      });
  }

  selectedStock(): VentaStockItem | null {
    return this.options().stock.find((item) => this.stockKey(item) === this.line.key) || null;
  }

  stockKey(item: VentaStockItem): string {
    return `${item.product_id}:${item.gramaje}`;
  }

  invoiceItems() {
    return this.options().stock
      .map((item) => ({ ...item, cantidad: Number(this.sellQuantities[this.stockKey(item)] || 0) }))
      .filter((item) => item.cantidad > 0);
  }

  lineTotal(item: VentaStockItem & { cantidad: number }): number {
    return Number(item.price_gs || 0) * item.cantidad;
  }

  pendingLineTotal(item: VentaStockItem): number {
    return Number(item.price_gs || 0) * Number(this.sellQuantities[this.stockKey(item)] || 0);
  }

  isOverStock(item: VentaStockItem): boolean {
    return Number(this.sellQuantities[this.stockKey(item)] || 0) > item.paquetes;
  }

  total(): number {
    return this.invoiceItems().reduce((sum, item) => sum + this.lineTotal(item), 0);
  }

  taxSummary() {
    return this.invoiceItems().reduce(
      (acc, item) => {
        const lineTotal = this.lineTotal(item);
        const iva = Number(item.iva || 0);
        const base = iva > 0 ? lineTotal / (1 + iva / 100) : lineTotal;
        const ivaMonto = lineTotal - base;

        if (iva === 5) {
          acc.gravada5 += base;
          acc.iva5 += ivaMonto;
        } else if (iva === 10) {
          acc.gravada10 += base;
          acc.iva10 += ivaMonto;
        }
        return acc;
      },
      { gravada5: 0, iva5: 0, gravada10: 0, iva10: 0 },
    );
  }

  totalIva(): number {
    const tax = this.taxSummary();
    return tax.iva5 + tax.iva10;
  }

  sheetPreview() {
    const date = this.parseInvoiceDate();
    const ivaTotal = this.totalIva();
    const total = this.total();
    return {
      mes: date
        ? [
            'ENERO',
            'FEBRERO',
            'MARZO',
            'ABRIL',
            'MAYO',
            'JUNIO',
            'JULIO',
            'AGOSTO',
            'SEPTIEMBRE',
            'OCTUBRE',
            'NOVIEMBRE',
            'DICIEMBRE',
          ][date.getMonth()]
        : '',
      cliente: this.sheetCustomer(),
      factura: this.invoice.invoice_no || '',
      fecha: date ? date.toLocaleDateString('es-PY') : this.invoice.fecha,
      remision: 'Listo',
      estado: 'Entregado',
      cobranza: 'Sin OP',
      recibo: '',
      total,
      iva_total: ivaTotal,
      extra2: total - 0.3 * ivaTotal,
    };
  }

  private sheetCustomer(): string {
    const customer = (this.invoice.customer || '').trim().toUpperCase();
    return ['LUQUE', 'AREGUA', 'ITAUGUA'].includes(customer) ? customer : 'LUQUE';
  }

  private parseInvoiceDate(): Date | null {
    if (!this.invoice.fecha) return null;
    const date = new Date(`${this.invoice.fecha}T00:00:00`);
    return Number.isNaN(date.getTime()) ? null : date;
  }
}

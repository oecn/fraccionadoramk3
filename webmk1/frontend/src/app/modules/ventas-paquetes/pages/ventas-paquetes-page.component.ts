import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

import { FormsModule } from '@angular/forms';

import { FacturaVentaParseItem, FacturaVentaParsePreview, VentaOptions, VentaStockItem } from '../models/ventas-paquetes.models';
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
  readonly parsingPdf = signal<boolean>(false);
  readonly parsedFactura = signal<FacturaVentaParsePreview | null>(null);
  readonly unmatchedPdfItems = signal<FacturaVentaParseItem[]>([]);
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
  pdfOverrides: Record<string, { price_gs: number; iva: number }> = {};

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
    this.invoice.invoice_no = this.formatInvoiceNo(this.invoice.invoice_no);
    if (this.invoice.invoice_no && !this.isInvoiceNoComplete(this.invoice.invoice_no)) {
      this.error.set('El nro. de factura debe tener formato 000-000-0000000.');
      return;
    }
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
          price_gs: this.effectivePrice(item),
          iva: this.effectiveIva(item),
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
          this.pdfOverrides = {};
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

  onFacturaPdfSelected(event: Event): void {
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0];
    input.value = '';
    if (!file) return;

    this.parsingPdf.set(true);
    this.error.set('');
    this.message.set('');
    this.service.parseFacturaPdf(file).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (preview) => {
        this.parsingPdf.set(false);
        this.parsedFactura.set(preview);
        this.unmatchedPdfItems.set([]);
        this.message.set(`PDF parseado: ${preview.items.length} items, total ${this.fmtGs(preview.total_gs)} Gs.`);
      },
      error: (err) => {
        this.parsingPdf.set(false);
        this.parsedFactura.set(null);
        this.unmatchedPdfItems.set([]);
        this.error.set(httpErrorMessage(err, 'No se pudo parsear la factura PDF'));
      },
    });
  }

  aplicarFacturaParseada(): void {
    const parsed = this.parsedFactura();
    if (!parsed) return;

    this.invoice.invoice_no = parsed.numero || this.invoice.invoice_no;
    this.invoice.customer = parsed.cliente || this.invoice.customer;
    this.invoice.fecha = parsed.fecha_emision || this.invoice.fecha;

    const nextQuantities: Record<string, number | null> = { ...this.sellQuantities };
    const nextOverrides: Record<string, { price_gs: number; iva: number }> = { ...this.pdfOverrides };
    const unmatched: FacturaVentaParseItem[] = [];
    for (const item of parsed.items) {
      const stock = this.matchParsedItem(item);
      if (!stock) {
        unmatched.push(item);
        continue;
      }
      const key = this.stockKey(stock);
      nextQuantities[key] = item.cantidad;
      nextOverrides[key] = { price_gs: item.precio_unitario_gs, iva: item.iva };
    }

    this.sellQuantities = nextQuantities;
    this.pdfOverrides = nextOverrides;
    this.unmatchedPdfItems.set(unmatched);
    this.message.set(
      unmatched.length
        ? `Factura aplicada con ${unmatched.length} item(s) sin match. Revise cantidades antes de registrar.`
        : 'Factura aplicada al formulario. Revise stock y registre la venta.',
    );
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
    return this.effectivePrice(item) * item.cantidad;
  }

  pendingLineTotal(item: VentaStockItem): number {
    return this.effectivePrice(item) * Number(this.sellQuantities[this.stockKey(item)] || 0);
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
        const iva = this.effectiveIva(item);
        const ivaMonto = this.ivaIncludedAmount(lineTotal, iva);

        if (iva === 5) {
          acc.gravada5 += lineTotal;
          acc.iva5 += ivaMonto;
        } else if (iva === 10) {
          acc.gravada10 += lineTotal;
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

  parsedTotalQty(): number {
    return (this.parsedFactura()?.items || []).reduce((sum, item) => sum + Number(item.cantidad || 0), 0);
  }

  onInvoiceNoInput(event: Event): void {
    const input = event.target as HTMLInputElement;
    this.invoice.invoice_no = this.formatInvoiceNo(input.value);
    input.value = this.invoice.invoice_no;
  }

  private formatInvoiceNo(value: string): string {
    const digits = String(value || '').replace(/\D+/g, '').slice(0, 13);
    const first = digits.slice(0, 3);
    const second = digits.slice(3, 6);
    const third = digits.slice(6, 13);
    return [first, second, third].filter(Boolean).join('-');
  }

  private isInvoiceNoComplete(value: string): boolean {
    return /^\d{3}-\d{3}-\d{7}$/.test(value || '');
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

  private ivaIncludedAmount(lineTotal: number, iva: number): number {
    if (iva === 5) return lineTotal / 21;
    if (iva === 10) return lineTotal / 11;
    return 0;
  }

  effectivePrice(item: VentaStockItem): number {
    return Number(this.pdfOverrides[this.stockKey(item)]?.price_gs ?? item.price_gs ?? 0);
  }

  effectiveIva(item: VentaStockItem): number {
    return Number(this.pdfOverrides[this.stockKey(item)]?.iva ?? item.iva ?? 0);
  }

  private matchParsedItem(item: FacturaVentaParseItem): VentaStockItem | null {
    const desc = this.normalizeText(item.descripcion);
    const grams = this.parsedGramaje(desc);
    const productKey = this.parsedProductKey(desc);
    if (!productKey || !grams) return null;
    return this.options().stock.find((stock) => (
      this.normalizeText(stock.producto).includes(productKey) &&
      Number(stock.gramaje) === grams
    )) || null;
  }

  private parsedProductKey(desc: string): string {
    if (desc.includes('azucar')) return 'azucar';
    if (desc.includes('arroz')) return 'arroz';
    if (desc.includes('locrillo')) return 'locrillo';
    if (desc.includes('locro')) return 'locro';
    if (desc.includes('pororo')) return 'pororo';
    if (desc.includes('poroto')) return 'poroto';
    if (desc.includes('gallet')) return 'galleta';
    return '';
  }

  private parsedGramaje(desc: string): number | null {
    const kg = desc.match(/\b1\s*kg\b/);
    if (kg) return 1000;
    const gr = desc.match(/\b(\d{2,4})\s*gr?\b/);
    return gr ? Number(gr[1]) : null;
  }

  private normalizeText(value: string): string {
    return String(value || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase();
  }
}

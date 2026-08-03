import { CommonModule } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormsModule } from '@angular/forms';

import { fmtGs } from '../../../shared/formatters';
import { httpErrorMessage } from '../../../shared/http-error';
import { dateOffset } from '../../../shared/utils';
import { CobrosFacturasService } from '../cobros-facturas.service';
import { CobroFacturaCreate, CobroFacturaRow, CobrosSummary, FacturaPendienteRow } from '../models/cobros-facturas.models';

type CobroFormItem = {
  invoice_id: number;
  invoice_source: string;
  invoice_no: string;
  customer: string;
  monto_gs: number;
};

@Component({
  selector: 'app-cobros-facturas-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './cobros-facturas-page.component.html',
  styleUrl: './cobros-facturas-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class CobrosFacturasPageComponent implements OnInit {
  private readonly defaultWhatsappKey = 'cobrosFacturasWhatsappDefault';
  private readonly sharedSucursalNames = ['LUQUE', 'AREGUA', 'ITAUGUA'];
  private readonly whatsappByClienteKey = 'cobrosFacturasWhatsappByCliente';
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(CobrosFacturasService);
  readonly fmtGs = fmtGs;

  readonly loading = signal(false);
  readonly saving = signal(false);
  readonly error = signal('');
  readonly message = signal('');
  readonly data = signal<CobrosSummary>({ pendientes: [], cobros: [] });

  editingId: number | null = null;
  filters = {
    from_date: dateOffset(-30),
    to_date: dateOffset(30),
  };
  form = this.emptyForm();
  confirmWhatsappPhone = '';
  selectedItems: CobroFormItem[] = [];
  defaultWhatsappPhone = '';
  whatsappNumbers: Record<string, string> = {};

  ngOnInit(): void {
    this.defaultWhatsappPhone = localStorage.getItem(this.defaultWhatsappKey) || '';
    this.whatsappNumbers = this.readWhatsappMap();
    this.load();
  }

  load(): void {
    this.loading.set(true);
    this.error.set('');
    this.service.summary(this.filters).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (data) => {
        this.data.set(data);
        this.loading.set(false);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudieron cargar cobros'));
      },
    });
  }

  addFactura(row: FacturaPendienteRow): void {
    const source = row.invoice_source || 'sales';
    if (this.selectedItems.some((item) => item.invoice_id === row.invoice_id && item.invoice_source === source)) return;
    this.selectedItems = [
      ...this.selectedItems,
      {
        invoice_id: row.invoice_id,
        invoice_source: source,
        invoice_no: row.invoice_no || `ID ${row.invoice_id}`,
        customer: row.customer || '-',
        monto_gs: Number(row.total_gs || 0),
      },
    ];
  }

  removeFactura(invoiceId: number): void {
    this.selectedItems = this.selectedItems.filter((item) => item.invoice_id !== invoiceId);
  }

  editCobro(row: CobroFacturaRow): void {
    this.editingId = row.id;
    this.form = {
      fecha_cobro: row.fecha_cobro || new Date().toISOString().slice(0, 10),
      cheque_no: row.cheque_no,
      boleta_deposito: row.boleta_deposito,
      banco: row.banco,
      observacion: row.observacion,
    };
    this.selectedItems = row.items.map((item) => ({
      invoice_id: item.invoice_id,
      invoice_source: item.invoice_source || 'sales',
      invoice_no: item.invoice_no || `ID ${item.invoice_id}`,
      customer: item.customer || '-',
      monto_gs: Number(item.monto_gs || 0),
    }));
    this.message.set('');
    this.error.set('');
  }

  save(): void {
    const payload = this.payload();
    if (!payload) return;
    const whatsappUrl = this.whatsappDepositoUrlFromForm();
    this.saving.set(true);
    this.error.set('');
    this.message.set('');
    const request = this.editingId ? this.service.update(this.editingId, payload) : this.service.create(payload);
    request.pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: () => {
        this.saving.set(false);
        this.message.set(this.editingId ? 'Cobro actualizado.' : 'Cobro registrado.');
        if (whatsappUrl) {
          window.open(whatsappUrl, '_blank', 'noopener');
        }
        this.resetForm();
        this.load();
        window.dispatchEvent(new Event('cobros-facturas-alerts-changed'));
      },
      error: (err) => {
        this.saving.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo guardar el cobro'));
      },
    });
  }

  resetForm(): void {
    this.editingId = null;
    this.form = this.emptyForm();
    this.confirmWhatsappPhone = '';
    this.selectedItems = [];
  }

  totalSeleccionado(): number {
    return this.selectedItems.reduce((acc, item) => acc + Number(item.monto_gs || 0), 0);
  }

  whatsappNumberForCliente(cliente: string): string {
    if (this.isSharedSucursal(cliente)) {
      return this.defaultWhatsappPhone;
    }
    return this.whatsappNumbers[this.whatsappKey(cliente)] || '';
  }

  setWhatsappNumberForCliente(cliente: string, value: string): void {
    if (this.isSharedSucursal(cliente)) {
      this.setDefaultWhatsappPhone(value);
      return;
    }
    this.whatsappNumbers = { ...this.whatsappNumbers, [this.whatsappKey(cliente)]: value };
    localStorage.setItem(this.whatsappByClienteKey, JSON.stringify(this.whatsappNumbers));
  }

  setDefaultWhatsappPhone(value: string): void {
    this.defaultWhatsappPhone = value;
    localStorage.setItem(this.defaultWhatsappKey, value);
  }

  whatsappUrl(row: FacturaPendienteRow): string {
    const phone = this.cleanPhone(this.whatsappNumberForCliente(row.customer));
    if (!phone) {
      return '';
    }
    const pendientes = this.data().pendientes.filter((item) => this.whatsappKey(item.customer) === this.whatsappKey(row.customer));
    const total = pendientes.reduce((acc, item) => acc + Number(item.total_gs || 0), 0);
    const facturas = pendientes
      .map((item) => `- Factura ${item.invoice_no || `ID ${item.invoice_id}`} | Sucursal: ${item.customer || '-'} | Monto: ${this.fmtGs(item.total_gs)} Gs`)
      .join('\n');
    const saludo = new Date().getHours() < 12 ? 'buen dia' : 'buenas tardes';
    const text = [
      'Aviso automatico - Granos Central.',
      `Hola, ${saludo}. Espero que se encuentren bien.`,
      '- Consultamos por favor el estado de pago de las siguientes facturas pendientes:',
      facturas,
      `- Total pendiente: ${this.fmtGs(total)} Gs.`,
      'Agradeceriamos su confirmacion para actualizar nuestro registro de cobranzas.',
      'Muchas gracias.',
    ].join('\n');
    return `https://wa.me/${phone}?text=${encodeURIComponent(text)}`;
  }

  whatsappAllUrl(): string {
    const phone = this.cleanPhone(this.defaultWhatsappPhone);
    if (!phone) {
      return '';
    }
    const pendientes = this.data().pendientes.filter((row) => this.isSharedSucursal(row.customer));
    if (pendientes.length === 0) {
      return '';
    }
    const total = pendientes.reduce((acc, item) => acc + Number(item.total_gs || 0), 0);
    const facturas = pendientes
      .map((item) => `* Factura ${item.invoice_no || `ID ${item.invoice_id}`} | Sucursal: ${item.customer || '-'} | Monto: ${this.fmtGs(item.total_gs)} Gs`)
      .join('\n');
    const saludo = new Date().getHours() < 12 ? 'buen dia' : 'buenas tardes';
    const text = [
      'Aviso automatico - Granos Central.',
      `Hola, ${saludo}. Espero que se encuentren bien.`,
      '* Consultamos por favor el estado de pago de las siguientes facturas pendientes:',
      facturas,
      `* Total pendiente: ${this.fmtGs(total)} Gs.`,
      'Agradeceriamos su confirmacion para actualizar nuestro registro de cobranzas.',
      'Muchas gracias.',
    ].join('\n');
    return `https://wa.me/${phone}?text=${encodeURIComponent(text)}`;
  }

  whatsappDepositoUrl(row: CobroFacturaRow): string {
    const phone = this.cleanPhone(this.whatsappNumberForCobro(row));
    if (!phone) {
      return '';
    }
    return this.buildDepositoWhatsappUrl(row, phone);
  }

  whatsappDepositoUrlFromForm(): string {
    const phone = this.cleanPhone(this.confirmWhatsappPhone || this.whatsappNumberForSelectedItems());
    if (!phone || this.selectedItems.length === 0) {
      return '';
    }
    const row: CobroFacturaRow = {
      id: this.editingId || 0,
      fecha_cobro: this.form.fecha_cobro,
      cheque_no: this.form.cheque_no,
      boleta_deposito: this.form.boleta_deposito,
      banco: this.form.banco,
      observacion: this.form.observacion,
      total_gs: this.totalSeleccionado(),
      created_at: '',
      updated_at: '',
      items: this.selectedItems.map((item) => ({
        id: 0,
        invoice_id: item.invoice_id,
        invoice_source: item.invoice_source || 'sales',
        invoice_no: item.invoice_no,
        customer: item.customer,
        factura_total_gs: item.monto_gs,
        monto_gs: item.monto_gs,
      })),
    };
    return this.buildDepositoWhatsappUrl(row, phone);
  }

  updateConfirmWhatsappPhone(value: string): void {
    this.confirmWhatsappPhone = value;
    const first = this.selectedItems[0]?.customer || '';
    if (first) {
      this.setWhatsappNumberForCliente(first, value);
    } else {
      this.setDefaultWhatsappPhone(value);
    }
  }

  openWhatsappAll(): void {
    const url = this.whatsappAllUrl();
    if (!url) {
      this.error.set('Cargue un numero de WhatsApp para este modulo.');
      return;
    }
    window.open(url, '_blank', 'noopener');
  }

  private whatsappNumberForCobro(row: CobroFacturaRow): string {
    const firstCustomer = row.items[0]?.customer || '';
    return this.whatsappNumberForCliente(firstCustomer) || this.defaultWhatsappPhone;
  }

  whatsappNumberForSelectedItems(): string {
    const firstCustomer = this.selectedItems[0]?.customer || '';
    return this.whatsappNumberForCliente(firstCustomer) || this.defaultWhatsappPhone;
  }

  private buildDepositoWhatsappUrl(row: CobroFacturaRow, phone: string): string {
    const facturas = row.items
      .map((item) => `- Factura ${item.invoice_no || `ID ${item.invoice_id}`} | Sucursal: ${item.customer || '-'} | Monto: ${this.fmtGs(item.monto_gs)} Gs`)
      .join('\n');
    const sucursales = [...new Set(row.items.map((item) => item.customer || '-'))].join(', ');
    const saludo = new Date().getHours() < 12 ? 'buen dia' : 'buenas tardes';
    const text = [
      'Aviso automatico - Granos Central.',
      `Hola, ${saludo}. Espero que se encuentren bien.`,
      `Informamos cordialmente que ha sido depositado el cheque Nro. ${row.cheque_no || '-'}, con boleta de deposito Nro. ${row.boleta_deposito || '-'}.`,
      `El deposito corresponde a la sucursal ${sucursales || '-'} y hace referencia a:`,
      facturas,
      `Total depositado: ${this.fmtGs(row.total_gs)} Gs.`,
      'Quedamos atentos a cualquier confirmacion o consulta.',
      'Muchas gracias.',
    ].join('\n');
    return `https://wa.me/${phone}?text=${encodeURIComponent(text)}`;
  }

  private payload(): CobroFacturaCreate | null {
    if (!this.form.cheque_no.trim()) {
      this.error.set('Ingrese numero de cheque.');
      return null;
    }
    if (!this.form.boleta_deposito.trim()) {
      this.error.set('Ingrese boleta de deposito.');
      return null;
    }
    if (this.selectedItems.length === 0) {
      this.error.set('Seleccione al menos una factura.');
      return null;
    }
    return {
      ...this.form,
      items: this.selectedItems.map((item) => ({
        invoice_id: item.invoice_id,
        invoice_source: item.invoice_source || 'sales',
        monto_gs: Number(item.monto_gs || 0),
      })),
    };
  }

  private emptyForm() {
    return {
      fecha_cobro: new Date().toISOString().slice(0, 10),
      cheque_no: '',
      boleta_deposito: '',
      banco: '',
      observacion: '',
    };
  }

  private clienteKey(value: string): string {
    return (value || 'SIN_CLIENTE').trim().toUpperCase();
  }

  private whatsappKey(value: string): string {
    const key = this.clienteKey(value);
    return this.sharedSucursalNames.includes(key) ? `SUCURSAL:${key}` : `CLIENTE:${key}`;
  }

  private isSharedSucursal(value: string): boolean {
    return this.sharedSucursalNames.includes(this.clienteKey(value));
  }

  private readWhatsappMap(): Record<string, string> {
    try {
      return JSON.parse(localStorage.getItem(this.whatsappByClienteKey) || '{}') as Record<string, string>;
    } catch {
      return {};
    }
  }

  private cleanPhone(value: string): string {
    const digits = String(value || '').replace(/\D+/g, '');
    return digits.startsWith('0') ? `595${digits.slice(1)}` : digits;
  }
}

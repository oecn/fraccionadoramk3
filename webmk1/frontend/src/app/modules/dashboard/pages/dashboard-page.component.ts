import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

import { FormsModule } from '@angular/forms';

import {
  DashboardSummary,
  OrderRow,
  PaymentCheckOption,
  PaymentCheckStatus,
  PaymentDetailRow,
  PaymentInvoiceDetail,
  PaymentRow,
} from '../models/dashboard.models';
import { DashboardService } from '../dashboard.service';
import { fmtGs } from '../../../shared/formatters';
import { dateOffset, todayIso } from '../../../shared/utils';
import { CobrosFacturasService } from '../../cobros-facturas/cobros-facturas.service';
import { CobroFacturaCreate, CobroFacturaRow } from '../../cobros-facturas/models/cobros-facturas.models';

@Component({
  selector: 'app-dashboard-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './dashboard-page.component.html',
  styleUrl: './dashboard-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class DashboardPageComponent implements OnInit {
  private readonly whatsappBySucursalKey = 'ordenesCompraWhatsappBySucursal';
  private readonly collectionWhatsappGroupUrl = 'https://chat.whatsapp.com/FNm13JeV6rY8FPMInrbs8N';
  private readonly destroyRef = inject(DestroyRef);
  private readonly dashboard = inject(DashboardService);
  private readonly cobrosFacturas = inject(CobrosFacturasService);
  readonly fmtGs = fmtGs;

  readonly sucursales = signal<string[]>([]);
  readonly data = signal<DashboardSummary | null>(null);
  readonly loading = signal<boolean>(false);
  readonly savingOrder = signal<number | null>(null);
  readonly savingPayment = signal<boolean>(false);
  readonly error = signal<string>('');
  readonly message = signal<string>('');
  readonly paymentChecks = signal<PaymentCheckOption[]>([]);
  readonly checkStatus = signal<PaymentCheckStatus | null>(null);
  readonly selectedPayments = signal<Record<number, boolean>>({});
  readonly paymentInvoiceDetail = signal<PaymentInvoiceDetail | null>(null);
  readonly loadingPaymentInvoice = signal<boolean>(false);
  readonly paymentDialogOpen = signal<boolean>(false);
  readonly reviewPaymentsOpen = signal<boolean>(false);
  readonly paymentDetails = signal<PaymentDetailRow[]>([]);
  readonly selectedPaymentDetails = signal<Record<number, boolean>>({});
  readonly invoiceCollections = signal<CobroFacturaRow[]>([]);
  readonly invoiceCollectionsOpen = signal<boolean>(false);
  readonly editingInvoiceCollection = signal<CobroFacturaRow | null>(null);
  readonly selectedCollections = signal<Record<number, boolean>>({});
  readonly collectionDialogOpen = signal<boolean>(false);
  readonly savingCollection = signal<boolean>(false);
  readonly savingCollectionEdit = signal<boolean>(false);

  filters = {
    sucursal: '',
    search: '',
    from_date: dateOffset(-30),
    to_date: dateOffset(30),
  };

  paymentForm = {
    fecha_pago: todayIso(),
    medio: 'Cheque',
    referencia: '',
    nro_deposito: '',
    nro_recibo_dinero: '',
    observacion: '',
    check_key: '',
    cheque_no: '',
    serie: '',
  };

  receiptForm = {
    nro_recibo_dinero: '',
  };

  collectionForm = {
    fecha_cobro: todayIso(),
    cheque_no: '',
    boleta_deposito: '',
    banco: '',
    observacion: '',
  };

  collectionEditForm = {
    fecha_cobro: todayIso(),
    cheque_no: '',
    boleta_deposito: '',
  };

  ngOnInit(): void {
    this.dashboard.getSucursales().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (items) => this.sucursales.set(items),
      error: () => this.sucursales.set([]),
    });
    this.refresh();
    this.loadPaymentChecks();
    this.loadInvoiceCollections();
  }

  refresh(): void {
    this.loading.set(true);
    this.error.set('');
    this.dashboard.getSummary(this.filters).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (data) => {
        this.data.set(data);
        this.selectedPayments.set({});
        this.selectedCollections.set({});
        this.loading.set(false);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(err?.error?.detail || 'No se pudo cargar el dashboard');
      },
    });
    this.loadInvoiceCollections();
  }

  loadInvoiceCollections(): void {
    this.cobrosFacturas.summary().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (summary) => this.invoiceCollections.set(summary.cobros),
      error: () => this.invoiceCollections.set([]),
    });
  }

  openInvoiceCollections(): void {
    this.invoiceCollectionsOpen.set(true);
    this.loadInvoiceCollections();
  }

  closeInvoiceCollections(): void {
    this.invoiceCollectionsOpen.set(false);
    this.editingInvoiceCollection.set(null);
  }

  openPaymentInvoice(row: PaymentRow): void {
    this.loadingPaymentInvoice.set(true);
    this.paymentInvoiceDetail.set(null);
    this.error.set('');
    this.dashboard.getPaymentInvoiceDetail(row.factura_id).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (detail) => {
        this.paymentInvoiceDetail.set(detail);
        this.loadingPaymentInvoice.set(false);
      },
      error: (err) => {
        this.loadingPaymentInvoice.set(false);
        this.error.set(err?.error?.detail || 'No se pudo cargar la factura');
      },
    });
  }

  closePaymentInvoice(): void {
    this.paymentInvoiceDetail.set(null);
    this.loadingPaymentInvoice.set(false);
  }

  editInvoiceCollection(row: CobroFacturaRow): void {
    this.editingInvoiceCollection.set(row);
    this.collectionEditForm = {
      fecha_cobro: row.fecha_cobro || todayIso(),
      cheque_no: row.cheque_no || '',
      boleta_deposito: row.boleta_deposito || '',
    };
  }

  cancelInvoiceCollectionEdit(): void {
    this.editingInvoiceCollection.set(null);
  }

  saveInvoiceCollectionEdit(): void {
    const row = this.editingInvoiceCollection();
    if (!row) return;
    if (!this.collectionEditForm.cheque_no.trim()) {
      this.error.set('Ingrese numero de cheque.');
      return;
    }
    if (!this.collectionEditForm.boleta_deposito.trim()) {
      this.error.set('Ingrese boleta de deposito.');
      return;
    }
    this.savingCollectionEdit.set(true);
    this.error.set('');
    this.cobrosFacturas.update(row.id, {
      fecha_cobro: this.collectionEditForm.fecha_cobro,
      cheque_no: this.collectionEditForm.cheque_no,
      boleta_deposito: this.collectionEditForm.boleta_deposito,
      banco: row.banco || '',
      observacion: row.observacion || '',
      items: row.items.map((item) => ({
        invoice_id: item.invoice_id,
        monto_gs: Number(item.monto_gs || 0),
      })),
    }).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: () => {
        this.savingCollectionEdit.set(false);
        this.editingInvoiceCollection.set(null);
        this.message.set('Cobro actualizado.');
        this.loadInvoiceCollections();
        this.refresh();
      },
      error: (err) => {
        this.savingCollectionEdit.set(false);
        this.error.set(err?.error?.detail || 'No se pudo actualizar el cobro');
      },
    });
  }

  toggleCollection(row: { invoice_id: number }, checked: boolean): void {
    this.selectedCollections.update((current) => ({ ...current, [row.invoice_id]: checked }));
  }

  selectedCollectionRows() {
    const selected = this.selectedCollections();
    return (this.data()?.collections || []).filter((row) => selected[row.invoice_id]);
  }

  selectedCollectionTotal(): number {
    return this.selectedCollectionRows().reduce((sum, row) => sum + Number(row.total_con_retencion || row.total_gs || 0), 0);
  }

  openCollectionDialog(): void {
    if (this.selectedCollectionRows().length === 0) {
      this.error.set('Seleccione una o mas facturas emitidas.');
      return;
    }
    this.error.set('');
    this.collectionDialogOpen.set(true);
  }

  closeCollectionDialog(): void {
    if (!this.savingCollection()) {
      this.collectionDialogOpen.set(false);
    }
  }

  registerCollection(): void {
    const rows = this.selectedCollectionRows();
    if (rows.length === 0) {
      this.error.set('Seleccione una o mas facturas emitidas.');
      return;
    }
    if (!this.collectionForm.cheque_no.trim()) {
      this.error.set('Ingrese numero de cheque.');
      return;
    }
    if (!this.collectionForm.boleta_deposito.trim()) {
      this.error.set('Ingrese boleta de deposito.');
      return;
    }
    const whatsappMessage = this.collectionWhatsappMessage(rows);
    const payload: CobroFacturaCreate = {
      ...this.collectionForm,
      banco: this.collectionBankName(),
      items: rows.map((row) => ({
        invoice_id: row.invoice_id,
        monto_gs: Number(row.total_con_retencion || row.total_gs || 0),
      })),
    };
    this.savingCollection.set(true);
    this.error.set('');
    this.message.set('');
    this.cobrosFacturas.create(payload).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: () => {
        this.savingCollection.set(false);
        this.collectionDialogOpen.set(false);
        this.copyTextToClipboard(whatsappMessage);
        this.message.set(`Cobro registrado. Mensaje copiado para WhatsApp.`);
        window.open(this.collectionWhatsappGroupUrl, '_blank', 'noopener');
        this.collectionForm = {
          fecha_cobro: todayIso(),
          cheque_no: '',
          boleta_deposito: '',
          banco: '',
          observacion: '',
        };
        this.selectedCollections.set({});
        this.refresh();
      },
      error: (err) => {
        this.savingCollection.set(false);
        this.error.set(err?.error?.detail || 'No se pudo registrar el cobro');
      },
    });
  }

  loadPaymentChecks(): void {
    this.dashboard.getPaymentChecks().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (checks) => this.paymentChecks.set(checks),
      error: () => this.paymentChecks.set([]),
    });
  }

  confirmDeliveryFromRow(row: OrderRow, event?: Event): void {
    const phone = this.cleanPhone(this.getWhatsappBySucursal(row.sucursal));
    if (!phone) {
      event?.preventDefault();
      this.error.set('Cargue el numero de WhatsApp de esta sucursal en Importar OC > Numeros para entrega.');
      return;
    }
    if (this.savingOrder() === row.oc_id) {
      event?.preventDefault();
      return;
    }
    this.markOrderDelivered(row.oc_id);
  }

  deliveryWhatsappHref(row: OrderRow): string {
    const phone = this.cleanPhone(this.getWhatsappBySucursal(row.sucursal));
    return phone ? this.deliveryWhatsappUrl(row, phone) : '';
  }

  markOrderDelivered(ocId: number): void {
    this.savingOrder.set(ocId);
    this.error.set('');
    this.message.set('');
    this.dashboard.markOrderDelivered(ocId).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (res) => {
        this.savingOrder.set(null);
        this.message.set(res.message);
        this.refresh();
      },
      error: (err) => {
        this.savingOrder.set(null);
        this.error.set(err?.error?.detail || 'No se pudo marcar el pedido como entregado');
      },
    });
  }

  private cleanPhone(value: string): string {
    const digits = String(value || '').replace(/\D+/g, '');
    return digits.startsWith('0') ? `595${digits.slice(1)}` : digits;
  }

  private sucursalKey(value: string): string {
    return (value || 'SIN_SUCURSAL').trim().toUpperCase();
  }

  private readWhatsappMap(): Record<string, string> {
    try {
      const raw = JSON.parse(localStorage.getItem(this.whatsappBySucursalKey) || '{}') as Record<string, string | { principal?: string }>;
      return Object.fromEntries(
        Object.entries(raw).map(([key, value]) => [key, typeof value === 'string' ? value : value?.principal || '']),
      );
    } catch {
      return {};
    }
  }

  private getWhatsappBySucursal(sucursal: string): string {
    return this.readWhatsappMap()[this.sucursalKey(sucursal)] || '';
  }

  private collectionWhatsappMessage(rows: ReturnType<DashboardPageComponent['selectedCollectionRows']>): string {
    const saludo = new Date().getHours() < 12 ? 'Buen dia' : 'Buenas tardes';
    const banco = this.collectionBankName();
    const sucursales = Array.from(new Set(rows.map((row) => row.customer).filter(Boolean)));
    const detalle = rows
      .map((row) => {
        const factura = row.invoice_no || `ID ${row.invoice_id}`;
        const sucursal = row.customer || '-';
        const monto = this.fmtGs(Number(row.total_con_retencion || row.total_gs || 0));
        return `- Factura ${factura} / Sucursal ${sucursal} / Monto ${monto} Gs`;
      })
      .join('\n');
    const text = [
      `Mensaje automatico. ${saludo}. Avisamos que fue depositado el cheque Nro. ${this.collectionForm.cheque_no.trim()} (${banco}), boleta Nro. ${this.collectionForm.boleta_deposito.trim()}.`,
      `Referencia: ${sucursales.length === 1 ? sucursales[0] : 'sucursales seleccionadas'}.`,
      detalle,
      `Total: ${this.fmtGs(this.selectedCollectionTotal())} Gs. Saludos.`,
    ].join('\n');
    return text;
  }

  private collectionBankName(): string {
    return this.collectionForm.cheque_no.trim().length >= 8 ? 'Banco ATLAS' : 'Banco CONTINENTAL';
  }

  private copyTextToClipboard(text: string): void {
    if (navigator.clipboard?.writeText) {
      navigator.clipboard.writeText(text).catch(() => this.fallbackCopyText(text));
      return;
    }
    this.fallbackCopyText(text);
  }

  private fallbackCopyText(text: string): void {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.style.position = 'fixed';
    textarea.style.opacity = '0';
    document.body.appendChild(textarea);
    textarea.select();
    document.execCommand('copy');
    document.body.removeChild(textarea);
  }

  private deliveryWhatsappUrl(row: OrderRow, phone: string): string {
    const oc = row.numero || String(row.oc_id);
    const saludo = new Date().getHours() < 12 ? 'Buen dia' : 'Buenas tardes';
    const destino = row.sucursal ? ` a ${row.sucursal}` : '';
    const text = `${saludo}. El pedido correspondiente a la OC ${oc} ya fue despachado y se encuentra en camino${destino}.`;
    return `https://wa.me/${phone}?text=${encodeURIComponent(text)}`;
  }

  togglePayment(row: PaymentRow, checked: boolean): void {
    this.selectedPayments.update((current) => ({ ...current, [row.factura_id]: checked }));
  }

  selectedPaymentRows(): PaymentRow[] {
    const selected = this.selectedPayments();
    return (this.data()?.payments || []).filter((row) => selected[row.factura_id]);
  }

  selectedPaymentTotal(): number {
    return this.selectedPaymentRows().reduce((sum, row) => sum + Number(row.monto || 0), 0);
  }

  selectedCheck(): PaymentCheckOption | null {
    return this.paymentChecks().find((item) => this.checkKey(item) === this.paymentForm.check_key) || null;
  }

  checkKey(item: PaymentCheckOption): string {
    return `${item.chequera_id}:${item.serie}:${item.cheque_no}`;
  }

  onCheckChange(): void {
    const check = this.selectedCheck();
    if (!check) return;
    this.paymentForm.referencia = check.reference_value;
  }

  openPaymentDialog(): void {
    if (this.selectedPaymentRows().length === 0) {
      this.error.set('Seleccione una o mas facturas pendientes.');
      return;
    }
    this.error.set('');
    this.checkStatus.set(null);
    this.paymentDialogOpen.set(true);
  }

  closePaymentDialog(): void {
    if (!this.savingPayment()) {
      this.paymentDialogOpen.set(false);
    }
  }

  openReviewPayments(): void {
    this.reviewPaymentsOpen.set(true);
    this.selectedPaymentDetails.set({});
    this.loadPaymentDetails();
  }

  closeReviewPayments(): void {
    this.reviewPaymentsOpen.set(false);
  }

  loadPaymentDetails(): void {
    this.dashboard.getPaymentDetails().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (rows) => this.paymentDetails.set(rows),
      error: (err) => this.error.set(err?.error?.detail || 'No se pudieron cargar pagos realizados'),
    });
  }

  togglePaymentDetail(row: PaymentDetailRow, checked: boolean): void {
    this.selectedPaymentDetails.update((current) => ({ ...current, [row.id]: checked }));
  }

  selectedPaymentDetailRows(): PaymentDetailRow[] {
    const selected = this.selectedPaymentDetails();
    return this.paymentDetails().filter((row) => selected[row.id]);
  }

  applyReceiptToSelected(): void {
    const rows = this.selectedPaymentDetailRows();
    if (rows.length === 0) {
      this.error.set('Seleccione uno o mas pagos realizados.');
      return;
    }
    this.dashboard
      .updatePaymentReceipt({
        detail_ids: rows.map((row) => row.id),
        nro_recibo_dinero: this.receiptForm.nro_recibo_dinero,
      })
      .pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
        next: (res) => {
          this.message.set(`Recibo ${res.nro_recibo_dinero} aplicado a ${res.updated} pago(s).`);
          this.receiptForm.nro_recibo_dinero = '';
          this.selectedPaymentDetails.set({});
          this.loadPaymentDetails();
        },
        error: (err) => this.error.set(err?.error?.detail || 'No se pudo aplicar el recibo'),
      });
  }

  registerPayment(): void {
    const rows = this.selectedPaymentRows();
    if (rows.length === 0) {
      this.error.set('Seleccione una o mas facturas pendientes.');
      return;
    }
    const check = this.checkStatus()?.check || this.selectedCheck();
    if (this.paymentForm.medio === 'Cheque' && (!check || !this.checkStatus()?.available)) {
      this.error.set('Ingrese un cheque cargado y disponible.');
      return;
    }
    this.savingPayment.set(true);
    this.error.set('');
    this.message.set('');
    this.dashboard
      .registerPayment({
        lot_ids: rows.map((row) => row.factura_id),
        fecha_pago: this.paymentForm.fecha_pago,
        medio: this.paymentForm.medio,
        referencia: this.paymentForm.medio === 'Cheque' ? check?.reference_value || '' : this.paymentForm.referencia,
        nro_deposito: '',
        nro_recibo_dinero: this.paymentForm.nro_recibo_dinero,
        observacion: this.paymentForm.observacion,
        chequera_id: check?.chequera_id || '',
        cheque_no: check?.cheque_no || '',
        serie: check?.serie || '',
      })
      .pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
        next: (res) => {
          this.savingPayment.set(false);
          this.paymentDialogOpen.set(false);
          this.message.set(`Pago registrado: ${res.facturas} factura(s), ${this.fmtGs(res.total_gs)} Gs.`);
          this.paymentForm.referencia = '';
          this.paymentForm.nro_deposito = '';
          this.paymentForm.nro_recibo_dinero = '';
          this.paymentForm.observacion = '';
          this.paymentForm.check_key = '';
          this.paymentForm.cheque_no = '';
          this.paymentForm.serie = '';
          this.checkStatus.set(null);
          this.loadPaymentChecks();
          this.refresh();
        },
        error: (err) => {
          this.savingPayment.set(false);
          this.error.set(err?.error?.detail || 'No se pudo registrar el pago');
        },
      });
  }

  validateCheck(): void {
    if (!this.paymentForm.serie || !this.paymentForm.cheque_no) {
      this.checkStatus.set({ available: false, found: false, used: false, message: 'Ingrese serie y numero de cheque.', check: null });
      return;
    }
    this.dashboard.getPaymentCheckStatus(this.paymentForm.serie, this.paymentForm.cheque_no).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (status) => {
        this.checkStatus.set(status);
        if (status.check) {
          this.paymentForm.referencia = status.check.reference_value;
          this.paymentForm.check_key = this.checkKey(status.check);
        }
      },
      error: (err) => {
        this.checkStatus.set({
          available: false,
          found: false,
          used: false,
          message: err?.error?.detail || 'No se pudo validar el cheque.',
          check: null,
        });
      },
    });
  }

  urgencyClass(value: number | null): string {
    if (value === null || value === undefined) return '';
    if (value > 0) return 'danger';
    if (value >= -1) return 'warning';
    return 'ok';
  }

  readyClass(value: number): string {
    if (value >= 100) return 'ok';
    if (value >= 60) return 'warning';
    return 'danger';
  }

  dueClass(value: number | null): string {
    if (value === null || value === undefined) return '';
    if (value < 0) return 'danger';
    if (value <= 3) return 'warning';
    return 'ok';
  }
}

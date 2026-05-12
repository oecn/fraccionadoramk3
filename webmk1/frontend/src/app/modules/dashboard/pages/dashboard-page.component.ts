import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

import { FormsModule } from '@angular/forms';

import {
  DashboardSummary,
  PaymentCheckOption,
  PaymentCheckStatus,
  PaymentDetailRow,
  PaymentRow,
} from '../models/dashboard.models';
import { DashboardService } from '../dashboard.service';
import { fmtGs } from '../../../shared/formatters';
import { dateOffset, todayIso } from '../../../shared/utils';

@Component({
  selector: 'app-dashboard-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './dashboard-page.component.html',
  styleUrl: './dashboard-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class DashboardPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly dashboard = inject(DashboardService);
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
  readonly paymentDialogOpen = signal<boolean>(false);
  readonly reviewPaymentsOpen = signal<boolean>(false);
  readonly paymentDetails = signal<PaymentDetailRow[]>([]);
  readonly selectedPaymentDetails = signal<Record<number, boolean>>({});

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

  ngOnInit(): void {
    this.dashboard.getSucursales().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (items) => this.sucursales.set(items),
      error: () => this.sucursales.set([]),
    });
    this.refresh();
    this.loadPaymentChecks();
  }

  refresh(): void {
    this.loading.set(true);
    this.error.set('');
    this.dashboard.getSummary(this.filters).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (data) => {
        this.data.set(data);
        this.selectedPayments.set({});
        this.loading.set(false);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(err?.error?.detail || 'No se pudo cargar el dashboard');
      },
    });
  }

  loadPaymentChecks(): void {
    this.dashboard.getPaymentChecks().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (checks) => this.paymentChecks.set(checks),
      error: () => this.paymentChecks.set([]),
    });
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

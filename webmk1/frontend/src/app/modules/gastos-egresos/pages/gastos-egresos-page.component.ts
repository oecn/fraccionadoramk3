import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

import { FormsModule } from '@angular/forms';

import { httpErrorMessage } from '../../../shared/http-error';
import { fmtGs } from '../../../shared/formatters';
import { dateOffset, todayIso } from '../../../shared/utils';
import { GastosEgresosService } from '../gastos-egresos.service';
import { CheckStatus, ExpenseCreate, ExpenseSummary, IpsParseResult, RrhhParseResult } from '../models/gastos-egresos.models';

@Component({
  selector: 'app-gastos-egresos-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './gastos-egresos-page.component.html',
  styleUrl: './gastos-egresos-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class GastosEgresosPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(GastosEgresosService);
  readonly fmtGs = fmtGs;

  readonly data = signal<ExpenseSummary>({ rows: [], total_gs: 0, tipos: [], formas_pago: [] });
  readonly loading = signal<boolean>(false);
  readonly saving = signal<boolean>(false);
  readonly parsingIps = signal<boolean>(false);
  readonly importingIps = signal<boolean>(false);
  readonly parsingRrhh = signal<boolean>(false);
  readonly importingRrhh = signal<boolean>(false);
  readonly error = signal<string>('');
  readonly message = signal<string>('');
  readonly checkStatus = signal<CheckStatus | null>(null);
  readonly ipsPreview = signal<IpsParseResult | null>(null);
  readonly rrhhPreview = signal<RrhhParseResult | null>(null);

  filters = {
    desde: dateOffset(-30),
    hasta: todayIso(),
  };

  form: ExpenseCreate = {
    fecha: todayIso(),
    tipo: 'Caja chica',
    descripcion: '',
    monto_gs: 0,
    nro_factura: '',
    forma_pago: 'Efectivo',
    referencia_pago: '',
  };

  cheque = {
    serie: '',
    numero: '',
  };

  ngOnInit(): void {
    this.refresh();
  }

  refresh(): void {
    this.loading.set(true);
    this.error.set('');
    this.service.getSummary(this.filters.desde, this.filters.hasta).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (data) => {
        this.data.set(data);
        if (!this.form.tipo && data.tipos.length) this.form.tipo = data.tipos[0];
        this.loading.set(false);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudieron cargar gastos'));
      },
    });
  }

  onTipoChange(): void {
    if (this.form.tipo.toUpperCase() === 'IPS') {
      this.form.forma_pago = 'Homebanking';
    } else if (this.form.forma_pago === 'Homebanking') {
      this.form.forma_pago = 'Efectivo';
    }
    this.onFormaPagoChange();
  }

  onFormaPagoChange(): void {
    this.checkStatus.set(null);
    if (this.form.forma_pago === 'Efectivo') {
      this.form.referencia_pago = '';
    }
  }

  validateCheck(): void {
    if (!this.cheque.serie || !this.cheque.numero) {
      this.checkStatus.set({ available: false, found: false, used: false, message: 'Ingrese serie y numero.', chequera_id: '', cheque_no: '', serie: '', referencia: '' });
      return;
    }
    this.service.getCheckStatus(this.cheque.serie, this.cheque.numero).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (status) => {
        this.checkStatus.set(status);
        if (status.available) this.form.referencia_pago = status.referencia;
      },
      error: (err) => {
        this.checkStatus.set({ available: false, found: false, used: false, message: httpErrorMessage(err, 'No se pudo validar cheque'), chequera_id: '', cheque_no: '', serie: '', referencia: '' });
      },
    });
  }

  registrar(): void {
    if (this.form.forma_pago === 'Cheque') {
      const status = this.checkStatus();
      if (!status?.available) {
        this.error.set('Valide un cheque disponible antes de registrar.');
        return;
      }
      this.form.referencia_pago = status.referencia;
    }
    this.saving.set(true);
    this.error.set('');
    this.message.set('');
    this.service.create(this.form).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (row) => {
        this.saving.set(false);
        this.message.set(`Gasto registrado: ${row.tipo} ${this.fmtGs(row.monto_gs)} Gs.`);
        this.form.descripcion = '';
        this.form.monto_gs = 0;
        this.form.nro_factura = '';
        this.form.referencia_pago = '';
        this.cheque = { serie: '', numero: '' };
        this.checkStatus.set(null);
        this.refresh();
      },
      error: (err) => {
        this.saving.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo registrar el gasto'));
      },
    });
  }

  referenceLabel(): string {
    if (this.form.forma_pago === 'Cheque') return 'Serie y cheque';
    if (this.form.forma_pago === 'Transferencia') return 'Nro transferencia';
    if (this.form.forma_pago === 'Homebanking') return 'Referencia';
    return 'Referencia';
  }

  parseIps(event: Event): void {
    const file = this.inputFile(event);
    if (!file) return;
    this.parsingIps.set(true);
    this.error.set('');
    this.message.set('');
    this.ipsPreview.set(null);
    this.service.parseIps(file).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (preview) => {
        this.parsingIps.set(false);
        this.ipsPreview.set(preview);
        if (preview.duplicate) this.message.set(`El extracto IPS ${preview.nro_factura} ya esta cargado.`);
      },
      error: (err) => {
        this.parsingIps.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo parsear el PDF IPS'));
      },
    });
  }

  importIps(): void {
    const preview = this.ipsPreview();
    if (!preview) return;
    this.importingIps.set(true);
    this.error.set('');
    this.message.set('');
    this.service.importIps(preview).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (res) => {
        this.importingIps.set(false);
        this.message.set(`${res.message} Insertados: ${res.inserted}, omitidos: ${res.skipped}.`);
        this.ipsPreview.set(null);
        this.refresh();
      },
      error: (err) => {
        this.importingIps.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo importar IPS'));
      },
    });
  }

  parseRrhh(event: Event): void {
    const file = this.inputFile(event);
    if (!file) return;
    this.parsingRrhh.set(true);
    this.error.set('');
    this.message.set('');
    this.rrhhPreview.set(null);
    this.service.parseRrhh(file).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (preview) => {
        this.parsingRrhh.set(false);
        this.rrhhPreview.set(preview);
        if (preview.unresolved_count) this.message.set(`Hay ${preview.unresolved_count} CI sin funcionario vinculado.`);
      },
      error: (err) => {
        this.parsingRrhh.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo parsear el TXT RRHH'));
      },
    });
  }

  importRrhh(): void {
    const preview = this.rrhhPreview();
    if (!preview) return;
    this.importingRrhh.set(true);
    this.error.set('');
    this.message.set('');
    this.service.importRrhh(preview).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (res) => {
        this.importingRrhh.set(false);
        this.message.set(`${res.message} Insertados: ${res.inserted}, omitidos: ${res.skipped}.`);
        this.rrhhPreview.set(null);
        this.refresh();
      },
      error: (err) => {
        this.importingRrhh.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo importar RRHH'));
      },
    });
  }

  private inputFile(event: Event): File | null {
    const input = event.target as HTMLInputElement | null;
    return input?.files?.item(0) || null;
  }
}

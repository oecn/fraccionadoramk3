import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

import { FormsModule } from '@angular/forms';

import { httpErrorMessage } from '../../../shared/http-error';
import { todayIso } from '../../../shared/utils';
import { ChequerasSummary } from '../models/chequeras.models';
import { ChequerasService } from '../chequeras.service';

@Component({
  selector: 'app-chequeras-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './chequeras-page.component.html',
  styleUrl: './chequeras-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ChequerasPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(ChequerasService);

  readonly data = signal<ChequerasSummary | null>(null);
  readonly loading = signal<boolean>(false);
  readonly saving = signal<boolean>(false);
  readonly error = signal<string>('');
  readonly message = signal<string>('');

  bankForm = {
    banco_nombre: '',
    nro_cuenta: '',
    resumen: '',
  };

  checkbookForm = {
    bank_id: '',
    formato_chequera: 'Formulario',
    tipo_cheque: 'Vista',
    serie: '',
    fecha_recibimiento: todayIso(),
    nro_inicio: 0,
    nro_fin: 0,
    recibido_por: '',
    resumen: '',
  };

  ngOnInit(): void {
    this.refresh();
  }

  refresh(): void {
    this.loading.set(true);
    this.error.set('');
    this.service.getSummary().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (data) => {
        this.data.set(data);
        this.loading.set(false);
        if (!this.checkbookForm.bank_id && data.banks.length > 0) {
          this.checkbookForm.bank_id = data.banks[0].bank_id;
        }
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo cargar chequeras'));
      },
    });
  }

  saveBank(): void {
    this.saving.set(true);
    this.error.set('');
    this.message.set('');
    this.service.createBank({ ...this.bankForm, resumen: this.bankSummary() }).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (bank) => {
        this.saving.set(false);
        this.message.set(`Banco registrado: ${bank.banco_nombre}`);
        this.bankForm = { banco_nombre: '', nro_cuenta: '', resumen: '' };
        this.checkbookForm.bank_id = bank.bank_id;
        this.refresh();
      },
      error: (err) => {
        this.saving.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo registrar banco'));
      },
    });
  }

  saveCheckbook(): void {
    this.saving.set(true);
    this.error.set('');
    this.message.set('');
    this.service.createCheckbook({ ...this.checkbookForm, resumen: this.checkbookSummary() }).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (checkbook) => {
        this.saving.set(false);
        this.message.set(`Chequera registrada: ${checkbook.serie || '-'} ${checkbook.nro_inicio}-${checkbook.nro_fin}`);
        this.checkbookForm = {
          ...this.checkbookForm,
          serie: '',
          nro_inicio: 0,
          nro_fin: 0,
          recibido_por: '',
          resumen: '',
        };
        this.refresh();
      },
      error: (err) => {
        this.saving.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo registrar chequera'));
      },
    });
  }

  fmtRange(inicio: number, fin: number): string {
    return `${Number(inicio || 0).toLocaleString('es-PY')} - ${Number(fin || 0).toLocaleString('es-PY')}`;
  }

  bankSummary(): string {
    return [this.bankForm.banco_nombre, this.bankForm.nro_cuenta]
      .map((value) => String(value || '').trim().toUpperCase())
      .filter(Boolean)
      .join(' | ');
  }

  checkbookSummary(): string {
    return [
      this.checkbookForm.formato_chequera,
      this.checkbookForm.tipo_cheque,
      this.checkbookForm.serie,
      this.checkbookRangeSummary(),
    ]
      .map((value) => String(value || '').trim().toUpperCase())
      .filter(Boolean)
      .join(' ');
  }

  private checkbookRangeSummary(): string {
    const inicio = Number(this.checkbookForm.nro_inicio || 0);
    const fin = Number(this.checkbookForm.nro_fin || 0);
    if (!inicio && !fin) return '';
    return `${inicio}-${fin}`;
  }
}

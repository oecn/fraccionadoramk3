import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';


import { fmtGs } from '../../../shared/formatters';
import { httpErrorMessage } from '../../../shared/http-error';
import {
  BolsasPendientesSummary,
  OrdenCompraDetail,
  OrdenCompraImportResponse,
  OrdenCompraRow,
  PendientesAcumuladosSummary,
} from '../models/ordenes-compra.models';
import { OrdenesCompraService } from '../ordenes-compra.service';

@Component({
  selector: 'app-ordenes-compra-page',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './ordenes-compra-page.component.html',
  styleUrl: './ordenes-compra-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class OrdenesCompraPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(OrdenesCompraService);
  readonly fmtGs = fmtGs;

  readonly importing = signal<boolean>(false);
  readonly loadingList = signal<boolean>(false);
  readonly loadingDetail = signal<boolean>(false);
  readonly loadingPendientes = signal<boolean>(false);
  readonly loadingBolsas = signal<boolean>(false);
  readonly deleting = signal<number | null>(null);
  readonly savingStatus = signal<number | null>(null);
  readonly error = signal<string>('');
  readonly message = signal<string>('');
  readonly result = signal<OrdenCompraImportResponse | null>(null);
  readonly importadas = signal<OrdenCompraRow[]>([]);
  readonly selected = signal<OrdenCompraDetail | null>(null);
  readonly pendientes = signal<PendientesAcumuladosSummary | null>(null);
  readonly bolsas = signal<BolsasPendientesSummary | null>(null);
  selectedFile: File | null = null;
  fileName = '';

  ngOnInit(): void {
    this.loadImportadas();
    this.loadPendientes();
    this.loadBolsas();
  }

  loadImportadas(): void {
    this.loadingList.set(true);
    this.service.listarImportadas().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (rows) => {
        this.importadas.set(rows);
        this.loadingList.set(false);
        const currentId = this.selected()?.oc_id;
        const stillExists = currentId ? rows.some((row) => row.oc_id === currentId) : false;
        if (!stillExists && rows.length > 0) {
          this.selectOc(rows[0]);
        }
        if (rows.length === 0) {
          this.selected.set(null);
        }
      },
      error: (err) => {
        this.loadingList.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudieron cargar las OC importadas.'));
      },
    });
  }

  loadPendientes(): void {
    this.loadingPendientes.set(true);
    this.service.pendientesAcumulados().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (summary) => {
        this.pendientes.set(summary);
        this.loadingPendientes.set(false);
      },
      error: (err) => {
        this.loadingPendientes.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudieron cargar los pendientes acumulados.'));
      },
    });
  }

  loadBolsas(): void {
    this.loadingBolsas.set(true);
    this.service.bolsasPendientes().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (summary) => {
        this.bolsas.set(summary);
        this.loadingBolsas.set(false);
      },
      error: (err) => {
        this.loadingBolsas.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo cargar el resumen de bolsas.'));
      },
    });
  }

  onFileChange(event: Event): void {
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0] || null;
    this.selectedFile = file;
    this.fileName = file?.name || '';
    this.error.set('');
    this.message.set('');
  }

  importar(): void {
    if (!this.selectedFile) {
      this.error.set('Seleccione un PDF de orden de compra.');
      return;
    }

    this.importing.set(true);
    this.error.set('');
    this.message.set('');
    this.result.set(null);
    this.service.importarPdf(this.selectedFile).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (result) => {
        this.importing.set(false);
        this.result.set(result);
        this.message.set(result.message);
        this.loadImportadas();
        this.loadPendientes();
        this.loadBolsas();
        this.selectOc({
          oc_id: result.oc_id,
          nro_oc: result.nro_oc,
          sucursal: result.sucursal,
          fecha_pedido: result.fecha_pedido,
          monto_total: result.monto_total || 0,
          items_count: result.items.length,
          completada: false,
          created_at: '',
          pct_listo_envio: 0,
        });
      },
      error: (err) => {
        this.importing.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo importar la OC.'));
      },
    });
  }

  selectOc(row: OrdenCompraRow): void {
    this.loadingDetail.set(true);
    this.error.set('');
    this.service.detalle(row.oc_id).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (detail) => {
        this.loadingDetail.set(false);
        this.selected.set(detail);
      },
      error: (err) => {
        this.loadingDetail.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo cargar el detalle de la OC.'));
      },
    });
  }

  ocTone(row: OrdenCompraRow): string {
    if (row.completada) {
      return 'done';
    }
    const date = row.fecha_pedido ? new Date(`${row.fecha_pedido}T00:00:00`) : null;
    if (!date || Number.isNaN(date.getTime())) {
      return 'pending';
    }
    const now = new Date();
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const ageDays = Math.floor((today.getTime() - date.getTime()) / 86_400_000);
    return ageDays > 3 ? 'late' : 'pending';
  }

  stockLabel(value: number | null): string {
    return value == null ? 'Sin match' : String(value);
  }

  percentTone(percent: number): string {
    if (percent >= 100) {
      return 'ok';
    }
    return percent < 75 ? 'bad' : 'warn';
  }

  eliminar(row: OrdenCompraRow): void {
    const label = row.nro_oc || `ID ${row.oc_id}`;
    if (!window.confirm(`Eliminar la OC ${label}? Esta accion tambien borra sus items.`)) {
      return;
    }

    this.deleting.set(row.oc_id);
    this.error.set('');
    this.message.set('');
    this.service.eliminar(row.oc_id).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (res) => {
        this.deleting.set(null);
        this.message.set(res.message);
        if (this.selected()?.oc_id === row.oc_id) {
          this.selected.set(null);
        }
        this.loadImportadas();
        this.loadPendientes();
        this.loadBolsas();
      },
      error: (err) => {
        this.deleting.set(null);
        this.error.set(httpErrorMessage(err, 'No se pudo eliminar la OC.'));
      },
    });
  }

  toggleEstado(row: OrdenCompraRow): void {
    const next = !row.completada;
    this.savingStatus.set(row.oc_id);
    this.error.set('');
    this.message.set('');
    this.service.cambiarEstado(row.oc_id, next).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (res) => {
        this.savingStatus.set(null);
        this.message.set(res.message);
        this.loadImportadas();
        this.loadPendientes();
        this.loadBolsas();
        this.selectOc({ ...row, completada: res.completada });
      },
      error: (err) => {
        this.savingStatus.set(null);
        this.error.set(httpErrorMessage(err, 'No se pudo cambiar el estado de la OC.'));
      },
    });
  }
}

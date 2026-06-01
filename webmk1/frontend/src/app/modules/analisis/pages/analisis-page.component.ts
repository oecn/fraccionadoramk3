import { CommonModule } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormsModule } from '@angular/forms';

import { fmtGs, fmtKg } from '../../../shared/formatters';
import { httpErrorMessage } from '../../../shared/http-error';
import { AnalisisService } from '../analisis.service';
import { AnaliticaClientesResponse, ProyeccionComprasResponse } from '../models/analisis.models';

@Component({
  selector: 'app-analisis-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './analisis-page.component.html',
  styleUrl: './analisis-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class AnalisisPageComponent implements OnInit {
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(AnalisisService);
  readonly fmtGs = fmtGs;
  readonly fmtKg = fmtKg;

  readonly loadingClientes = signal(false);
  readonly loadingProyeccion = signal(false);
  readonly error = signal('');
  readonly clientes = signal<AnaliticaClientesResponse | null>(null);
  readonly proyeccion = signal<ProyeccionComprasResponse | null>(null);

  topClientes = 15;
  ventanaDias = 30;
  topProductos: number | null = null;

  ngOnInit(): void {
    this.loadClientes();
    this.loadProyeccion();
  }

  loadClientes(): void {
    this.loadingClientes.set(true);
    this.error.set('');
    this.service.clientes(this.topClientes).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (res) => {
        this.clientes.set(res);
        this.loadingClientes.set(false);
      },
      error: (err) => {
        this.loadingClientes.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo cargar analitica de clientes'));
      },
    });
  }

  loadProyeccion(): void {
    this.loadingProyeccion.set(true);
    this.error.set('');
    this.service.proyeccionCompras(this.ventanaDias, this.topProductos).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (res) => {
        this.proyeccion.set(res);
        this.loadingProyeccion.set(false);
      },
      error: (err) => {
        this.loadingProyeccion.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo cargar proyeccion de compras'));
      },
    });
  }

  diasLabel(value: number | null): string {
    return value === null ? '-' : value.toLocaleString('es-PY', { maximumFractionDigits: 1 });
  }

  urgencyClass(value: number | null): string {
    if (value === null) return 'flat';
    if (value <= 7) return 'bad';
    if (value <= 15) return 'warn';
    return 'ok';
  }
}


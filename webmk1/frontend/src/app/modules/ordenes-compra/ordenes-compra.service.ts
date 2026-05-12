import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import {
  BolsasPendientesSummary,
  OrdenCompraDeleteResponse,
  OrdenCompraDetail,
  OrdenCompraImportResponse,
  OrdenCompraRow,
  OrdenCompraStatusResponse,
  PendientesAcumuladosSummary,
} from './models/ordenes-compra.models';

@Injectable({ providedIn: 'root' })
export class OrdenesCompraService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = environment.apiBaseUrl;

  importarPdf(file: File): Observable<OrdenCompraImportResponse> {
    const form = new FormData();
    form.append('file', file);
    return this.http.post<OrdenCompraImportResponse>(`${this.baseUrl}/ordenes-compra/importar-pdf`, form);
  }

  listarImportadas(): Observable<OrdenCompraRow[]> {
    return this.http.get<OrdenCompraRow[]>(`${this.baseUrl}/ordenes-compra`);
  }

  detalle(ocId: number): Observable<OrdenCompraDetail> {
    return this.http.get<OrdenCompraDetail>(`${this.baseUrl}/ordenes-compra/${ocId}`);
  }

  pendientesAcumulados(): Observable<PendientesAcumuladosSummary> {
    return this.http.get<PendientesAcumuladosSummary>(`${this.baseUrl}/ordenes-compra/pendientes/acumulados`);
  }

  bolsasPendientes(): Observable<BolsasPendientesSummary> {
    return this.http.get<BolsasPendientesSummary>(`${this.baseUrl}/ordenes-compra/pendientes/bolsas`);
  }

  eliminar(ocId: number): Observable<OrdenCompraDeleteResponse> {
    return this.http.delete<OrdenCompraDeleteResponse>(`${this.baseUrl}/ordenes-compra/${ocId}`);
  }

  cambiarEstado(ocId: number, completada: boolean): Observable<OrdenCompraStatusResponse> {
    return this.http.post<OrdenCompraStatusResponse>(
      `${this.baseUrl}/ordenes-compra/${ocId}/status?completada=${completada}`,
      {},
    );
  }
}

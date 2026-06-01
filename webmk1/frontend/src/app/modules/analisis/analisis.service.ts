import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import { AnaliticaClientesResponse, ProyeccionComprasResponse } from './models/analisis.models';

@Injectable({ providedIn: 'root' })
export class AnalisisService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = `${environment.apiBaseUrl}/analisis`;

  clientes(topN: number): Observable<AnaliticaClientesResponse> {
    const params = new HttpParams().set('top_n', String(topN));
    return this.http.get<AnaliticaClientesResponse>(`${this.baseUrl}/clientes`, { params });
  }

  proyeccionCompras(ventanaDias: number, topN: number | null): Observable<ProyeccionComprasResponse> {
    let params = new HttpParams().set('ventana_dias', String(ventanaDias));
    if (topN !== null) {
      params = params.set('top_n', String(topN));
    }
    return this.http.get<ProyeccionComprasResponse>(`${this.baseUrl}/proyeccion-compras`, { params });
  }
}


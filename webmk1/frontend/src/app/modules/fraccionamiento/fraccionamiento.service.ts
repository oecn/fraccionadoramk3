import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import {
  ConsumoPreview,
  FraccionamientoCreate,
  FraccionamientoHistoryRow,
  FraccionamientoOptions,
  FraccionamientoSummary,
} from './models/fraccionamiento.models';

@Injectable({ providedIn: 'root' })
export class FraccionamientoService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = `${environment.apiBaseUrl}/fraccionamiento`;

  getOptions(): Observable<FraccionamientoOptions> {
    return this.http.get<FraccionamientoOptions>(`${this.baseUrl}/options`);
  }

  getSummary(filters: { product_id?: number | null; desde?: string; hasta?: string }): Observable<FraccionamientoSummary> {
    let params = new HttpParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value !== null && value !== undefined && value !== '') {
        params = params.set(key, String(value));
      }
    });
    return this.http.get<FraccionamientoSummary>(`${this.baseUrl}/summary`, { params });
  }

  getPreview(product_id: number, gramaje: number, paquetes: number): Observable<ConsumoPreview> {
    const params = new HttpParams()
      .set('product_id', product_id)
      .set('gramaje', gramaje)
      .set('paquetes', paquetes);
    return this.http.get<ConsumoPreview>(`${this.baseUrl}/preview`, { params });
  }

  registrar(payload: FraccionamientoCreate): Observable<FraccionamientoHistoryRow> {
    return this.http.post<FraccionamientoHistoryRow>(`${this.baseUrl}/registrar`, payload);
  }
}

import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import { HistorialComprasFilters, HistorialComprasSummary } from './models/historial-compras.models';

@Injectable({ providedIn: 'root' })
export class HistorialComprasService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = environment.apiBaseUrl;

  getSummary(filters: HistorialComprasFilters): Observable<HistorialComprasSummary> {
    let params = new HttpParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value) {
        params = params.set(key, String(value));
      }
    });
    return this.http.get<HistorialComprasSummary>(`${this.baseUrl}/historial-compras/summary`, { params });
  }
}

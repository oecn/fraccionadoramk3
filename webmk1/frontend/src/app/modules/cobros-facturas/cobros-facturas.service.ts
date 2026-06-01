import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import { CobroFacturaCreate, CobroFacturaRow, CobrosSummary } from './models/cobros-facturas.models';

@Injectable({ providedIn: 'root' })
export class CobrosFacturasService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = `${environment.apiBaseUrl}/cobros-facturas`;

  summary(filters: { from_date?: string; to_date?: string } = {}): Observable<CobrosSummary> {
    let params = new HttpParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value) {
        params = params.set(key, value);
      }
    });
    return this.http.get<CobrosSummary>(`${this.baseUrl}/summary`, { params });
  }

  create(payload: CobroFacturaCreate): Observable<CobroFacturaRow> {
    return this.http.post<CobroFacturaRow>(this.baseUrl, payload);
  }

  update(id: number, payload: CobroFacturaCreate): Observable<CobroFacturaRow> {
    return this.http.put<CobroFacturaRow>(`${this.baseUrl}/${id}`, payload);
  }
}

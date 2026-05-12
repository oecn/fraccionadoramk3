import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import { FlujoFilters, FlujoSummary } from './models/flujo-dinero.models';

@Injectable({ providedIn: 'root' })
export class FlujoDineroService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = environment.apiBaseUrl;

  getYears(): Observable<number[]> {
    return this.http.get<number[]>(`${this.baseUrl}/flujo-dinero/years`);
  }

  getSummary(filters: FlujoFilters): Observable<FlujoSummary> {
    let params = new HttpParams()
      .set('year', filters.year)
      .set('quarter', filters.quarter)
      .set('retencion_mode', filters.retencion_mode);
    if (filters.from_date) params = params.set('from_date', filters.from_date);
    if (filters.to_date) params = params.set('to_date', filters.to_date);
    return this.http.get<FlujoSummary>(`${this.baseUrl}/flujo-dinero/summary`, { params });
  }
}


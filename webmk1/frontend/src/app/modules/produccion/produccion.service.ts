import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import { ProduccionOptions, ProduccionResumen } from './models/produccion.models';

@Injectable({ providedIn: 'root' })
export class ProduccionService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = `${environment.apiBaseUrl}/produccion`;

  options(): Observable<ProduccionOptions> {
    return this.http.get<ProduccionOptions>(`${this.baseUrl}/options`);
  }

  resumen(year: number, month: number, rangeMonths: number): Observable<ProduccionResumen> {
    const params = new HttpParams()
      .set('year', String(year))
      .set('month', String(month))
      .set('range_months', String(rangeMonths));
    return this.http.get<ProduccionResumen>(`${this.baseUrl}/resumen`, { params });
  }
}

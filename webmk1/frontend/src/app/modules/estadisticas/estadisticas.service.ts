import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import { EstadisticasResumen } from './models/estadisticas.models';

@Injectable({ providedIn: 'root' })
export class EstadisticasService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = `${environment.apiBaseUrl}/estadisticas`;

  resumen(filters: { from_date: string; to_date: string; top_n: number }): Observable<EstadisticasResumen> {
    const params = new HttpParams()
      .set('from_date', filters.from_date)
      .set('to_date', filters.to_date)
      .set('top_n', String(filters.top_n));
    return this.http.get<EstadisticasResumen>(`${this.baseUrl}/resumen`, { params });
  }
}

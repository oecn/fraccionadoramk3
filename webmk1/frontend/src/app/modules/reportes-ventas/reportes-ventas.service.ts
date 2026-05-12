import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import {
  FacturaItemsResponse,
  ReportesVentasFilters,
  ReportesVentasOptions,
  ReportesVentasResumen,
} from './models/reportes-ventas.models';

@Injectable({ providedIn: 'root' })
export class ReportesVentasService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = `${environment.apiBaseUrl}/reportes-ventas`;

  getOptions(): Observable<ReportesVentasOptions> {
    return this.http.get<ReportesVentasOptions>(`${this.baseUrl}/options`);
  }

  getResumen(filters: ReportesVentasFilters): Observable<ReportesVentasResumen> {
    return this.http.get<ReportesVentasResumen>(`${this.baseUrl}/resumen`, {
      params: this.toParams(filters),
    });
  }

  getDetalleFacturas(filters: ReportesVentasFilters): Observable<FacturaItemsResponse> {
    const params = this.toParams({
      product_id: filters.product_id,
      gramaje: filters.gramaje,
      ym: filters.ym,
      desde: filters.desde,
      hasta: filters.hasta,
    });
    return this.http.get<FacturaItemsResponse>(`${this.baseUrl}/detalle-facturas`, { params });
  }

  private toParams(values: Record<string, string | number | boolean | null | undefined>): HttpParams {
    let params = new HttpParams();
    Object.entries(values).forEach(([key, value]) => {
      if (value !== null && value !== undefined && value !== '') {
        params = params.set(key, String(value));
      }
    });
    return params;
  }
}

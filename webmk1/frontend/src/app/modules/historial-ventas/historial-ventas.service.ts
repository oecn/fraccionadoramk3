import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import {
  HistorialVentasFilters,
  HistorialVentasSummary,
  ProductoItem,
  ReporteMensualData,
  VentasDetalleResponse,
} from './models/historial-ventas.models';

@Injectable({ providedIn: 'root' })
export class HistorialVentasService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = environment.apiBaseUrl;

  getSummary(filters: HistorialVentasFilters): Observable<HistorialVentasSummary> {
    let params = new HttpParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value !== '' && value !== false && value !== null) {
        params = params.set(key, String(value));
      }
    });
    return this.http.get<HistorialVentasSummary>(`${this.baseUrl}/historial-ventas/summary`, { params });
  }

  getReporteMensual(ym: string, empresa: string = 'Fraccionadora'): Observable<ReporteMensualData> {
    const params = new HttpParams().set('ym', ym).set('empresa', empresa);
    return this.http.get<ReporteMensualData>(`${this.baseUrl}/historial-ventas/reporte-mensual`, { params });
  }

  getProductos(): Observable<ProductoItem[]> {
    return this.http.get<ProductoItem[]>(`${this.baseUrl}/historial-ventas/productos`);
  }

  getMeses(): Observable<string[]> {
    return this.http.get<string[]>(`${this.baseUrl}/historial-ventas/meses`);
  }

  getDetalle(filters: {
    periodo?: string;
    product_id?: number | null;
    gramaje?: number | null;
    ym?: string;
    desde?: string;
    hasta?: string;
  }): Observable<VentasDetalleResponse> {
    let params = new HttpParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value !== null && value !== undefined && value !== '') {
        params = params.set(key, String(value));
      }
    });
    return this.http.get<VentasDetalleResponse>(`${this.baseUrl}/historial-ventas/detalle`, { params });
  }
}

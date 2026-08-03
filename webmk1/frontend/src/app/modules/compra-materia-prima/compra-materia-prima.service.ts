import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import {
  CompraMateriaPrimaCreate,
  CompraMateriaPrimaOptions,
  CompraMateriaPrimaSummary,
  FacturaCompraImportRequest,
  FacturaCompraImportResponse,
  FacturaCompraPreview,
  LoteAbiertoRow,
  LoteDeleteRequest,
  LoteDeleteResponse,
  LotePrecioReviewRequest,
  LotePrecioReviewResponse,
} from './models/compra-materia-prima.models';

@Injectable({ providedIn: 'root' })
export class CompraMateriaPrimaService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = environment.apiBaseUrl;

  getOptions(): Observable<CompraMateriaPrimaOptions> {
    return this.http.get<CompraMateriaPrimaOptions>(`${this.baseUrl}/compra-materia-prima/options`);
  }

  getSummary(productId: number | null): Observable<CompraMateriaPrimaSummary> {
    let params = new HttpParams();
    if (productId) {
      params = params.set('product_id', productId);
    }
    return this.http.get<CompraMateriaPrimaSummary>(`${this.baseUrl}/compra-materia-prima/summary`, { params });
  }

  registrar(payload: CompraMateriaPrimaCreate): Observable<LoteAbiertoRow> {
    return this.http.post<LoteAbiertoRow>(`${this.baseUrl}/compra-materia-prima/compras`, payload);
  }

  parseFactura(file: File): Observable<FacturaCompraPreview> {
    const form = new FormData();
    form.append('file', file);
    return this.http.post<FacturaCompraPreview>(`${this.baseUrl}/compra-materia-prima/facturas/parse`, form);
  }

  importarFactura(payload: FacturaCompraImportRequest): Observable<FacturaCompraImportResponse> {
    return this.http.post<FacturaCompraImportResponse>(`${this.baseUrl}/compra-materia-prima/facturas/import`, payload);
  }

  eliminarLote(lotId: number, payload: LoteDeleteRequest): Observable<LoteDeleteResponse> {
    return this.http.delete<LoteDeleteResponse>(`${this.baseUrl}/compra-materia-prima/compras/${lotId}`, { body: payload });
  }

  marcarPrecioRevisado(lotId: number, payload: LotePrecioReviewRequest): Observable<LotePrecioReviewResponse> {
    return this.http.patch<LotePrecioReviewResponse>(`${this.baseUrl}/compra-materia-prima/compras/${lotId}/precio-revisado`, payload);
  }
}

import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import { PrecioHistoryResponse, PrecioRow, PrecioUpdate, ProductoPrecioSummary } from './models/productos.models';

@Injectable({ providedIn: 'root' })
export class ProductosService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = `${environment.apiBaseUrl}/productos`;

  precios(): Observable<ProductoPrecioSummary> {
    return this.http.get<ProductoPrecioSummary>(`${this.baseUrl}/precios`);
  }

  updatePrecio(payload: PrecioUpdate): Observable<PrecioRow> {
    return this.http.put<PrecioRow>(`${this.baseUrl}/precios`, payload);
  }

  history(productId: number, gramaje: number): Observable<PrecioHistoryResponse> {
    return this.http.get<PrecioHistoryResponse>(`${this.baseUrl}/precios/${productId}/${gramaje}/history`);
  }
}

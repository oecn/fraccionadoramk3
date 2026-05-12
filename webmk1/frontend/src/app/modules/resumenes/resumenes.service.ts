import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import { LoteDetalle, LoteResumenRow, ResumenesOptions } from './models/resumenes.models';

@Injectable({ providedIn: 'root' })
export class ResumenesService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = `${environment.apiBaseUrl}/resumenes`;

  options(): Observable<ResumenesOptions> {
    return this.http.get<ResumenesOptions>(`${this.baseUrl}/options`);
  }

  lotes(productId: number | null, soloAbiertos: boolean): Observable<LoteResumenRow[]> {
    let params = new HttpParams().set('solo_abiertos', String(soloAbiertos));
    if (productId !== null) {
      params = params.set('product_id', String(productId));
    }
    return this.http.get<LoteResumenRow[]>(`${this.baseUrl}/lotes`, { params });
  }

  detalle(lotId: number): Observable<LoteDetalle> {
    return this.http.get<LoteDetalle>(`${this.baseUrl}/lotes/${lotId}`);
  }

  cerrar(lotId: number): Observable<LoteDetalle> {
    return this.http.post<LoteDetalle>(`${this.baseUrl}/lotes/${lotId}/cerrar`, {});
  }

  abrir(lotId: number): Observable<LoteDetalle> {
    return this.http.post<LoteDetalle>(`${this.baseUrl}/lotes/${lotId}/abrir`, {});
  }
}


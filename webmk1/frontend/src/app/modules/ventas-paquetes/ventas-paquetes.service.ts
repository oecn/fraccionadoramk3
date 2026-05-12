import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import { VentaCreate, VentaOptions, VentaResumen } from './models/ventas-paquetes.models';

@Injectable({ providedIn: 'root' })
export class VentasPaquetesService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = environment.apiBaseUrl;

  getOptions(): Observable<VentaOptions> {
    return this.http.get<VentaOptions>(`${this.baseUrl}/ventas-paquetes/options`);
  }

  registrar(payload: VentaCreate): Observable<VentaResumen> {
    return this.http.post<VentaResumen>(`${this.baseUrl}/ventas-paquetes/facturas`, payload);
  }
}

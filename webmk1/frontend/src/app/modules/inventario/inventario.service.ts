import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import { InventorySummary } from './models/inventario.models';

@Injectable({ providedIn: 'root' })
export class InventarioService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = environment.apiBaseUrl;

  getSummary(): Observable<InventorySummary> {
    return this.http.get<InventorySummary>(`${this.baseUrl}/inventario/summary`);
  }
}

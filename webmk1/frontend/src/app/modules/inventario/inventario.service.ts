import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import { CompraSugeridaResponse, InventoryAdjustmentUpdate, InventorySummary, RawStockAlertUpdate } from './models/inventario.models';

@Injectable({ providedIn: 'root' })
export class InventarioService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = environment.apiBaseUrl;

  getSummary(): Observable<InventorySummary> {
    return this.http.get<InventorySummary>(`${this.baseUrl}/inventario/summary`);
  }

  updateRawAlerts(alerts: RawStockAlertUpdate[]): Observable<InventorySummary> {
    return this.http.put<InventorySummary>(`${this.baseUrl}/inventario/raw-alerts`, { alerts });
  }

  adjustInventory(payload: InventoryAdjustmentUpdate): Observable<InventorySummary> {
    return this.http.put<InventorySummary>(`${this.baseUrl}/inventario/adjustments`, payload);
  }

  compraSugerida(ventanaDias: number): Observable<CompraSugeridaResponse> {
    return this.http.get<CompraSugeridaResponse>(`${this.baseUrl}/analisis/proyeccion-compras`, {
      params: { ventana_dias: ventanaDias },
    });
  }
}

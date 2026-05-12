import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import {
  CheckStatus,
  ExpenseCreate,
  ExpenseRow,
  ExpenseSummary,
  ImportResult,
  IpsParseResult,
  RrhhParseResult,
} from './models/gastos-egresos.models';

@Injectable({ providedIn: 'root' })
export class GastosEgresosService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = `${environment.apiBaseUrl}/gastos-egresos`;

  getSummary(desde = '', hasta = ''): Observable<ExpenseSummary> {
    let params = new HttpParams();
    if (desde) params = params.set('desde', desde);
    if (hasta) params = params.set('hasta', hasta);
    return this.http.get<ExpenseSummary>(`${this.baseUrl}/summary`, { params });
  }

  getCheckStatus(serie: string, chequeNo: string): Observable<CheckStatus> {
    const params = new HttpParams().set('serie', serie).set('cheque_no', chequeNo);
    return this.http.get<CheckStatus>(`${this.baseUrl}/check-status`, { params });
  }

  create(payload: ExpenseCreate): Observable<ExpenseRow> {
    return this.http.post<ExpenseRow>(`${this.baseUrl}/gastos`, payload);
  }

  parseIps(file: File): Observable<IpsParseResult> {
    const formData = new FormData();
    formData.append('file', file);
    return this.http.post<IpsParseResult>(`${this.baseUrl}/ips/parse`, formData);
  }

  importIps(payload: IpsParseResult): Observable<ImportResult> {
    return this.http.post<ImportResult>(`${this.baseUrl}/ips/import`, payload);
  }

  parseRrhh(file: File): Observable<RrhhParseResult> {
    const formData = new FormData();
    formData.append('file', file);
    return this.http.post<RrhhParseResult>(`${this.baseUrl}/rrhh/parse`, formData);
  }

  importRrhh(payload: RrhhParseResult): Observable<ImportResult> {
    return this.http.post<ImportResult>(`${this.baseUrl}/rrhh/import`, payload);
  }
}

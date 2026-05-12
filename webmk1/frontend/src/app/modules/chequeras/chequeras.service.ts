import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import { BankCreate, BankRow, CheckbookCreate, CheckbookRow, ChequerasSummary } from './models/chequeras.models';

@Injectable({ providedIn: 'root' })
export class ChequerasService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = environment.apiBaseUrl;

  getSummary(): Observable<ChequerasSummary> {
    return this.http.get<ChequerasSummary>(`${this.baseUrl}/chequeras/summary`);
  }

  createBank(payload: BankCreate): Observable<BankRow> {
    return this.http.post<BankRow>(`${this.baseUrl}/chequeras/banks`, payload);
  }

  createCheckbook(payload: CheckbookCreate): Observable<CheckbookRow> {
    return this.http.post<CheckbookRow>(`${this.baseUrl}/chequeras/checkbooks`, payload);
  }
}

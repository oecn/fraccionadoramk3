import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../../environments/environment';
import {
  DashboardFilters,
  DashboardSummary,
  OrderDeliveryResponse,
  PaymentCheckOption,
  PaymentCheckStatus,
  PaymentDetailRow,
  PaymentReceiptUpdateRequest,
  PaymentReceiptUpdateResponse,
  PaymentRegisterRequest,
  PaymentRegisterResponse,
} from './models/dashboard.models';

@Injectable({ providedIn: 'root' })
export class DashboardService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = environment.apiBaseUrl;

  getSucursales(): Observable<string[]> {
    return this.http.get<string[]>(`${this.baseUrl}/dashboard/sucursales`);
  }

  getSummary(filters: DashboardFilters): Observable<DashboardSummary> {
    let params = new HttpParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value) {
        params = params.set(key, value);
      }
    });
    return this.http.get<DashboardSummary>(`${this.baseUrl}/dashboard/summary`, { params });
  }

  markOrderDelivered(ocId: number): Observable<OrderDeliveryResponse> {
    return this.http.post<OrderDeliveryResponse>(`${this.baseUrl}/dashboard/orders/${ocId}/delivered`, {});
  }

  getPaymentChecks(): Observable<PaymentCheckOption[]> {
    return this.http.get<PaymentCheckOption[]>(`${this.baseUrl}/dashboard/payment-checks`);
  }

  getPaymentCheckStatus(serie: string, chequeNo: string): Observable<PaymentCheckStatus> {
    const params = new HttpParams().set('serie', serie).set('cheque_no', chequeNo);
    return this.http.get<PaymentCheckStatus>(`${this.baseUrl}/dashboard/payment-check-status`, { params });
  }

  registerPayment(payload: PaymentRegisterRequest): Observable<PaymentRegisterResponse> {
    return this.http.post<PaymentRegisterResponse>(`${this.baseUrl}/dashboard/payments/register`, payload);
  }

  getPaymentDetails(): Observable<PaymentDetailRow[]> {
    return this.http.get<PaymentDetailRow[]>(`${this.baseUrl}/dashboard/payments/details`);
  }

  updatePaymentReceipt(payload: PaymentReceiptUpdateRequest): Observable<PaymentReceiptUpdateResponse> {
    return this.http.post<PaymentReceiptUpdateResponse>(`${this.baseUrl}/dashboard/payments/receipt`, payload);
  }
}

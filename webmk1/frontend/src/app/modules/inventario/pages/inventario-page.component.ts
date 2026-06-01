import { CommonModule } from '@angular/common';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

import { FormsModule } from '@angular/forms';

import { httpErrorMessage } from '../../../shared/http-error';
import { fmtGs, fmtNumber } from '../../../shared/formatters';
import { productPillClass } from '../../../shared/utils';
import { InventarioService } from '../inventario.service';
import { CompraSugeridaRow, InventoryPackageRow, InventoryRawRow, InventorySummary } from '../models/inventario.models';

@Component({
  selector: 'app-inventario-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './inventario-page.component.html',
  styleUrl: './inventario-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class InventarioPageComponent implements OnInit {
  private readonly suggestedObjectivesKey = 'inventarioSuggestedBagObjectives';
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(InventarioService);
  readonly fmtGs = fmtGs;
  readonly fmtNumber = fmtNumber;
  readonly productPillClass = productPillClass;

  readonly loading = signal<boolean>(false);
  readonly savingAlerts = signal<boolean>(false);
  readonly savingAdjustments = signal<boolean>(false);
  readonly loadingSuggested = signal<boolean>(false);
  readonly alertsOpen = signal<boolean>(false);
  readonly adjustmentsOpen = signal<boolean>(false);
  readonly suggestedOpen = signal<boolean>(false);
  readonly error = signal<string>('');
  readonly message = signal<string>('');
  readonly summary = signal<InventorySummary>({
    raw_stock: [],
    package_stock: [],
    lotes_abiertos: [],
    raw_alerts_count: 0,
    total_raw_kg: 0,
    total_raw_valor_gs: 0,
    total_paquetes: 0,
    total_unidades: 0,
    total_venta_gs: 0,
  });
  alertForm: Record<number, number | null> = {};
  orderQtyForm: Record<number, number | null> = {};
  whatsappForm: Record<number, string> = {};
  rawAdjustmentForm: Record<number, number | null> = {};
  packageAdjustmentForm: Record<string, number | null> = {};
  adjustmentReason = 'Ajuste semanal';
  suggestedWindowDays = 30;
  suggestedTargetDays = 15;
  suggestedRows: CompraSugeridaRow[] = [];
  suggestedObjectiveForm: Record<string, number | null> = {};

  ngOnInit(): void {
    this.load();
  }

  load(): void {
    this.loading.set(true);
    this.error.set('');
    this.service.getSummary().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (summary) => {
        this.summary.set(summary);
        this.syncAlertForm(summary);
        this.syncAdjustmentForm(summary);
        this.loading.set(false);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo cargar inventario'));
      },
    });
  }

  openAlerts(): void {
    this.syncAlertForm(this.summary());
    this.alertsOpen.set(true);
  }

  closeAlerts(): void {
    if (!this.savingAlerts()) {
      this.alertsOpen.set(false);
    }
  }

  openAdjustments(): void {
    this.syncAdjustmentForm(this.summary());
    this.adjustmentReason = 'Ajuste semanal';
    this.adjustmentsOpen.set(true);
  }

  closeAdjustments(): void {
    if (!this.savingAdjustments()) {
      this.adjustmentsOpen.set(false);
    }
  }

  openSuggested(): void {
    this.suggestedObjectiveForm = this.readSuggestedObjectives();
    this.suggestedOpen.set(true);
    this.loadSuggested();
  }

  closeSuggested(): void {
    if (!this.loadingSuggested()) {
      this.suggestedOpen.set(false);
    }
  }

  loadSuggested(): void {
    this.loadingSuggested.set(true);
    this.error.set('');
    this.service.compraSugerida(this.suggestedWindowDays).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (res) => {
        this.suggestedRows = res.rows;
        this.ensureSuggestedDefaults();
        this.loadingSuggested.set(false);
      },
      error: (err) => {
        this.loadingSuggested.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo cargar sugerido de compra'));
      },
    });
  }

  saveAlerts(): void {
    const alerts = this.summary().raw_stock.map((item) => ({
      product_id: item.product_id,
      min_kg: this.alertBagsValue(item.product_id) ? this.alertBagsValue(item.product_id)! * this.bagKg(item) : null,
      reposicion_bolsas: this.orderBagsValue(item.product_id),
      proveedor_whatsapp: this.whatsappForm[item.product_id] || '',
    }));
    this.savingAlerts.set(true);
    this.error.set('');
    this.message.set('');
    this.service.updateRawAlerts(alerts).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (summary) => {
        this.summary.set(summary);
        this.syncAlertForm(summary);
        this.syncAdjustmentForm(summary);
        this.savingAlerts.set(false);
        this.alertsOpen.set(false);
        this.message.set('Alertas de materia prima actualizadas.');
        this.load();
        window.dispatchEvent(new Event('raw-stock-alerts-changed'));
      },
      error: (err) => {
        this.savingAlerts.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudieron guardar las alertas'));
      },
    });
  }

  saveAdjustments(): void {
    const payload = {
      raw_stock: this.summary().raw_stock.map((item) => ({
        product_id: item.product_id,
        kg: this.positiveNumber(this.rawAdjustmentForm[item.product_id]),
      })),
      package_stock: this.summary().package_stock.map((item) => ({
        product_id: item.product_id,
        gramaje: item.gramaje,
        paquetes: Math.trunc(this.positiveNumber(this.packageAdjustmentForm[this.packageKey(item)])),
      })),
      motivo: this.adjustmentReason || 'Ajuste semanal',
    };
    this.savingAdjustments.set(true);
    this.error.set('');
    this.message.set('');
    this.service.adjustInventory(payload).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (summary) => {
        this.summary.set(summary);
        this.syncAlertForm(summary);
        this.syncAdjustmentForm(summary);
        this.savingAdjustments.set(false);
        this.adjustmentsOpen.set(false);
        this.message.set('Inventario ajustado a la realidad.');
        this.load();
        window.dispatchEvent(new Event('raw-stock-alerts-changed'));
      },
      error: (err) => {
        this.savingAdjustments.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo modificar inventario'));
      },
    });
  }

  alertRowClass(state: string): string {
    if (state === 'bajo') return 'raw-low';
    if (state === 'cerca') return 'raw-near';
    return '';
  }

  alertLabel(state: string): string {
    if (state === 'bajo') return 'Stock bajo';
    if (state === 'cerca') return 'Cerca limite';
    return 'OK';
  }

  whatsappUrl(item: InventoryRawRow): string {
    const phone = this.cleanPhone(item.proveedor_whatsapp || '595981232950');
    const saludo = this.timeGreeting();
    const mensaje = [
      'Aviso automatico - Granos Central.',
      `Hola, ${saludo}. Espero que se encuentren bien.`,
      '',
      'Necesitamos reponer materia prima:',
      '',
      `- ${item.producto}: ${this.fmtNumber(item.reposicion_bolsas || 0)} bolsas x ${this.bagKg(item)} kg`,
      '',
      'Por favor, nos podrian confirmar disponibilidad, precio y tiempo de entrega?',
      'Muchas gracias.',
    ].join('\n');
    return `https://wa.me/${phone}?text=${encodeURIComponent(mensaje)}`;
  }

  whatsappAllLowUrl(): string {
    const lows = this.lowStockItems();
    const phone = this.cleanPhone(lows[0]?.proveedor_whatsapp || '595981232950');
    const saludo = this.timeGreeting();
    const lines = [
      'Aviso automatico - Granos Central.',
      `Hola, ${saludo}. Espero que se encuentren bien.`,
      '',
      'Necesitamos reponer materia prima:',
      '',
    ];
    for (const item of lows) {
      lines.push(`- ${item.producto}: ${this.fmtNumber(item.reposicion_bolsas || 0)} bolsas x ${this.bagKg(item)} kg`);
    }
    lines.push('');
    lines.push('Por favor, nos podrian confirmar disponibilidad, precio y tiempo de entrega?');
    lines.push('Muchas gracias.');
    return `https://wa.me/${phone}?text=${encodeURIComponent(lines.join('\n'))}`;
  }

  lowStockItems(): InventoryRawRow[] {
    return this.summary().raw_stock.filter((item) => item.alerta_estado === 'bajo');
  }

  packageKey(item: Pick<InventoryPackageRow, 'product_id' | 'gramaje'>): string {
    return `${item.product_id}:${item.gramaje}`;
  }

  suggestedKg(row: CompraSugeridaRow): number {
    return this.suggestedBags(row) * this.bagKg(row);
  }

  suggestedBags(row: CompraSugeridaRow): number {
    return Math.max(0, Math.ceil(this.objectiveBags(row) - this.currentBags(row)));
  }

  currentBags(row: CompraSugeridaRow): number {
    const bagKg = this.bagKg(row);
    return bagKg > 0 ? Number(row.stock_kg || 0) / bagKg : 0;
  }

  objectiveBags(row: CompraSugeridaRow): number {
    const value = Number(this.suggestedObjectiveForm[this.productKey(row.producto)] || 0);
    return Number.isFinite(value) && value > 0 ? value : 0;
  }

  updateObjectiveBags(row: CompraSugeridaRow, value: number | string | null): void {
    const parsed = Number(String(value ?? 0).replace(',', '.'));
    this.suggestedObjectiveForm = {
      ...this.suggestedObjectiveForm,
      [this.productKey(row.producto)]: Number.isFinite(parsed) && parsed > 0 ? parsed : null,
    };
    localStorage.setItem(this.suggestedObjectivesKey, JSON.stringify(this.suggestedObjectiveForm));
  }

  suggestedUrgencyClass(row: CompraSugeridaRow): string {
    const objective = this.objectiveBags(row);
    if (objective <= 0) return 'normal';
    const current = this.currentBags(row);
    if (current <= objective * 0.5) return 'bajo';
    if (current < objective) return 'cerca';
    return 'normal';
  }

  daysLabel(value: number | null): string {
    return value == null ? 'sin consumo' : `${this.fmtNumber(value)} dias`;
  }

  private syncAlertForm(summary: InventorySummary): void {
    this.alertForm = Object.fromEntries(
      summary.raw_stock.map((item) => [item.product_id, item.alerta_min_bolsas]),
    );
    this.orderQtyForm = Object.fromEntries(
      summary.raw_stock.map((item) => [item.product_id, item.reposicion_bolsas]),
    );
    this.whatsappForm = Object.fromEntries(
      summary.raw_stock.map((item) => [item.product_id, item.proveedor_whatsapp || '595981232950']),
    );
  }

  private syncAdjustmentForm(summary: InventorySummary): void {
    this.rawAdjustmentForm = Object.fromEntries(
      summary.raw_stock.map((item) => [item.product_id, item.kg]),
    );
    this.packageAdjustmentForm = Object.fromEntries(
      summary.package_stock.map((item) => [this.packageKey(item), item.paquetes]),
    );
  }

  private ensureSuggestedDefaults(): void {
    let changed = false;
    const next = { ...this.suggestedObjectiveForm };
    for (const row of this.suggestedRows) {
      const key = this.productKey(row.producto);
      if (next[key] === undefined) {
        next[key] = Math.ceil(this.currentBags(row));
        changed = true;
      }
    }
    this.suggestedObjectiveForm = next;
    if (changed) {
      localStorage.setItem(this.suggestedObjectivesKey, JSON.stringify(next));
    }
  }

  private readSuggestedObjectives(): Record<string, number | null> {
    try {
      return JSON.parse(localStorage.getItem(this.suggestedObjectivesKey) || '{}') as Record<string, number | null>;
    } catch {
      return {};
    }
  }

  private productKey(value: string): string {
    return String(value || '').trim().toLowerCase();
  }

  private positiveNumber(raw: number | string | null | undefined): number {
    const value = Number(String(raw ?? 0).replace(',', '.'));
    return Number.isFinite(value) && value > 0 ? value : 0;
  }

  private alertBagsValue(productId: number): number | null {
    const raw = this.alertForm[productId];
    if (raw === null || raw === undefined || raw === 0) return null;
    const value = Number(String(raw).replace(',', '.'));
    return Number.isFinite(value) && value > 0 ? value : null;
  }

  private orderBagsValue(productId: number): number | null {
    const raw = this.orderQtyForm[productId];
    if (raw === null || raw === undefined || raw === 0) return null;
    const value = Number(String(raw).replace(',', '.'));
    return Number.isFinite(value) && value > 0 ? value : null;
  }

  private bagKg(item: { alerta_bolsa_kg?: number | null; producto: string }): number {
    if (item.alerta_bolsa_kg) return Number(item.alerta_bolsa_kg);
    return item.producto.toLowerCase().includes('galleta') ? 25 : 50;
  }

  private cleanPhone(value: string): string {
    const digits = String(value || '').replace(/\D+/g, '');
    return digits.startsWith('0') ? `595${digits.slice(1)}` : digits;
  }

  private timeGreeting(): string {
    return new Date().getHours() < 12 ? 'buen dia' : 'buenas tardes';
  }
}

import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { ChangeDetectionStrategy, Component, DestroyRef, OnInit, computed, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';


import { fmtGs } from '../../../shared/formatters';
import { httpErrorMessage } from '../../../shared/http-error';
import {
  BolsasPendientesSummary,
  OrdenCompraDetail,
  OrdenCompraImportResponse,
  OrdenCompraRow,
  PendientesAcumuladosSummary,
} from '../models/ordenes-compra.models';
import { OrdenesCompraService } from '../ordenes-compra.service';

@Component({
  selector: 'app-ordenes-compra-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './ordenes-compra-page.component.html',
  styleUrl: './ordenes-compra-page.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class OrdenesCompraPageComponent implements OnInit {
  private readonly whatsappBySucursalKey = 'ordenesCompraWhatsappBySucursal';
  private readonly productionGroupUrl = 'https://chat.whatsapp.com/FYJkw5TgZU7A4mzJfSfWFw';
  private readonly destroyRef = inject(DestroyRef);
  private readonly service = inject(OrdenesCompraService);
  readonly fmtGs = fmtGs;

  readonly importing = signal<boolean>(false);
  readonly loadingList = signal<boolean>(false);
  readonly loadingDetail = signal<boolean>(false);
  readonly loadingPendientes = signal<boolean>(false);
  readonly loadingBolsas = signal<boolean>(false);
  readonly deleting = signal<number | null>(null);
  readonly savingStatus = signal<number | null>(null);
  readonly error = signal<string>('');
  readonly message = signal<string>('');
  readonly result = signal<OrdenCompraImportResponse | null>(null);
  readonly importadas = signal<OrdenCompraRow[]>([]);
  readonly selected = signal<OrdenCompraDetail | null>(null);
  readonly pendientes = signal<PendientesAcumuladosSummary | null>(null);
  readonly bolsas = signal<BolsasPendientesSummary | null>(null);
  readonly pedidoTxt = signal<string>('');
  readonly whatsappTarget = signal<OrdenCompraRow | null>(null);
  readonly whatsappPhone = signal<string>('');
  readonly notaRemision = signal<string>('');
  readonly showingDeliveryNumbers = signal<boolean>(false);
  readonly whatsappNumbers = signal<Record<string, string>>({});
  readonly sucursalesEntrega = computed(() => {
    const names = this.importadas()
      .map((row) => row.sucursal || 'SIN_SUCURSAL')
      .filter(Boolean);
    return [...new Set(names)].sort((a, b) => a.localeCompare(b));
  });
  selectedFile: File | null = null;
  fileName = '';

  ngOnInit(): void {
    this.whatsappNumbers.set(this.readWhatsappMap());
    this.loadImportadas();
    this.loadPendientes();
    this.loadBolsas();
  }

  loadImportadas(): void {
    this.loadingList.set(true);
    this.service.listarImportadas().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (rows) => {
        this.importadas.set(rows);
        this.loadingList.set(false);
        const currentId = this.selected()?.oc_id;
        const stillExists = currentId ? rows.some((row) => row.oc_id === currentId) : false;
        if (!stillExists && rows.length > 0) {
          this.selectOc(rows[0]);
        }
        if (rows.length === 0) {
          this.selected.set(null);
        }
      },
      error: (err) => {
        this.loadingList.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudieron cargar las OC importadas.'));
      },
    });
  }

  loadPendientes(): void {
    this.loadingPendientes.set(true);
    this.service.pendientesAcumulados().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (summary) => {
        this.pendientes.set(summary);
        this.loadingPendientes.set(false);
      },
      error: (err) => {
        this.loadingPendientes.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudieron cargar los pendientes acumulados.'));
      },
    });
  }

  loadBolsas(): void {
    this.loadingBolsas.set(true);
    this.service.bolsasPendientes().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (summary) => {
        this.bolsas.set(summary);
        this.loadingBolsas.set(false);
      },
      error: (err) => {
        this.loadingBolsas.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo cargar el resumen de bolsas.'));
      },
    });
  }

  onFileChange(event: Event): void {
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0] || null;
    this.selectedFile = file;
    this.fileName = file?.name || '';
    this.error.set('');
    this.message.set('');
  }

  importar(): void {
    if (!this.selectedFile) {
      this.error.set('Seleccione un PDF de orden de compra.');
      return;
    }

    this.importing.set(true);
    this.error.set('');
    this.message.set('');
    this.result.set(null);
    this.service.importarPdf(this.selectedFile).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (result) => {
        this.importing.set(false);
        this.result.set(result);
        this.message.set(result.message);
        this.loadImportadas();
        this.loadPendientes();
        this.loadBolsas();
        this.notifyOcAlertsChanged();
        this.selectOc({
          oc_id: result.oc_id,
          nro_oc: result.nro_oc,
          sucursal: result.sucursal,
          fecha_pedido: result.fecha_pedido,
          monto_total: result.monto_total || 0,
          items_count: result.items.length,
          completada: false,
          nota_remision: '',
          created_at: '',
          pct_listo_envio: 0,
        });
      },
      error: (err) => {
        this.importing.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo importar la OC.'));
      },
    });
  }

  selectOc(row: OrdenCompraRow): void {
    this.loadingDetail.set(true);
    this.error.set('');
    this.service.detalle(row.oc_id).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (detail) => {
        this.loadingDetail.set(false);
        this.selected.set(detail);
      },
      error: (err) => {
        this.loadingDetail.set(false);
        this.error.set(httpErrorMessage(err, 'No se pudo cargar el detalle de la OC.'));
      },
    });
  }

  ocTone(row: OrdenCompraRow): string {
    if (row.completada) {
      return 'done';
    }
    const date = row.fecha_pedido ? new Date(`${row.fecha_pedido}T00:00:00`) : null;
    if (!date || Number.isNaN(date.getTime())) {
      return 'pending';
    }
    const now = new Date();
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const ageDays = Math.floor((today.getTime() - date.getTime()) / 86_400_000);
    return ageDays > 3 ? 'late' : 'pending';
  }

  stockLabel(value: number | null): string {
    return value == null ? 'Sin match' : String(value);
  }

  stockTone(item: { cantidad: number | null; stock_disponible: number | null }): string {
    return this.stockToneByValues(item.cantidad, item.stock_disponible);
  }

  pendingStockTone(row: { necesario: number; stock_disponible: number | null }): string {
    return this.stockToneByValues(row.necesario, row.stock_disponible);
  }

  fmtKg(value: number): string {
    return `${value.toFixed(3).replace(/\.?0+$/, '')} kg`;
  }

  itemPesoKg(item: { descripcion: string; cantidad: number | null }): number {
    const gramosPack = this.gramajeTotalPorPaquete(item.descripcion);
    const cantidad = Number(item.cantidad || 0);
    return gramosPack && cantidad ? (cantidad * gramosPack) / 1000 : 0;
  }

  totalPesoKg(detail: OrdenCompraDetail): number {
    return detail.items.reduce((acc, item) => acc + this.itemPesoKg(item), 0);
  }

  private stockToneByValues(required: number | null, available: number | null): string {
    const necesario = Number(required || 0);
    const disponible = available == null ? -1 : Number(available || 0);
    if (necesario <= 0 || disponible < 0) {
      return 'stock-low';
    }
    const pct = disponible / necesario;
    if (pct >= 1) {
      return 'stock-ok';
    }
    return pct >= 0.6 ? 'stock-mid' : 'stock-low';
  }

  percentTone(percent: number): string {
    if (percent >= 100) {
      return 'ok';
    }
    return percent < 75 ? 'bad' : 'warn';
  }

  eliminar(row: OrdenCompraRow): void {
    const label = row.nro_oc || `ID ${row.oc_id}`;
    if (!window.confirm(`Eliminar la OC ${label}? Esta accion tambien borra sus items.`)) {
      return;
    }

    this.deleting.set(row.oc_id);
    this.error.set('');
    this.message.set('');
    this.service.eliminar(row.oc_id).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (res) => {
        this.deleting.set(null);
        this.message.set(res.message);
        if (this.selected()?.oc_id === row.oc_id) {
          this.selected.set(null);
        }
        this.loadImportadas();
        this.loadPendientes();
        this.loadBolsas();
        this.notifyOcAlertsChanged();
      },
      error: (err) => {
        this.deleting.set(null);
        this.error.set(httpErrorMessage(err, 'No se pudo eliminar la OC.'));
      },
    });
  }

  toggleEstado(row: OrdenCompraRow): void {
    const next = !row.completada;
    if (next) {
      this.openWhatsappPrompt(row);
      return;
    }
    this.savingStatus.set(row.oc_id);
    this.error.set('');
    this.message.set('');
    this.service.cambiarEstado(row.oc_id, next).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (res) => {
        this.savingStatus.set(null);
        this.message.set(res.message);
        this.loadImportadas();
        this.loadPendientes();
        this.loadBolsas();
        this.notifyOcAlertsChanged();
        this.selectOc({ ...row, completada: res.completada });
      },
      error: (err) => {
        this.savingStatus.set(null);
        this.error.set(httpErrorMessage(err, 'No se pudo cambiar el estado de la OC.'));
      },
    });
  }

  openWhatsappPrompt(row: OrdenCompraRow): void {
    this.whatsappTarget.set(row);
    this.whatsappPhone.set(this.getWhatsappBySucursal(row.sucursal));
    this.notaRemision.set(row.nota_remision || '');
    this.error.set('');
    this.message.set('');
  }

  openDeliveryNumbers(): void {
    this.showingDeliveryNumbers.set(true);
  }

  closeDeliveryNumbers(): void {
    this.showingDeliveryNumbers.set(false);
  }

  whatsappNumberForSucursal(sucursal: string): string {
    return this.whatsappNumbers()[this.sucursalKey(sucursal)] || '';
  }

  updateWhatsappSucursal(sucursal: string, value: string): void {
    const map = { ...this.whatsappNumbers(), [this.sucursalKey(sucursal)]: value };
    this.whatsappNumbers.set(map);
    localStorage.setItem(this.whatsappBySucursalKey, JSON.stringify(map));
  }

  closeWhatsappPrompt(): void {
    this.whatsappTarget.set(null);
    this.whatsappPhone.set('');
    this.notaRemision.set('');
  }

  confirmEntregaWhatsapp(event?: Event): void {
    const row = this.whatsappTarget();
    if (!row) {
      event?.preventDefault();
      return;
    }

    const phone = this.cleanPhone(this.whatsappPhone());
    if (!phone) {
      event?.preventDefault();
      this.error.set('Cargue un numero de WhatsApp para esta sucursal.');
      return;
    }
    const nota = this.notaRemision().trim();
    if (!nota) {
      event?.preventDefault();
      this.error.set('Ingrese el numero de nota de remision.');
      return;
    }
    if (this.savingStatus() === row.oc_id) {
      event?.preventDefault();
      return;
    }
    this.saveWhatsappBySucursal(row.sucursal, phone);

    this.savingStatus.set(row.oc_id);
    this.error.set('');
    this.message.set('');
    this.service.cambiarEstado(row.oc_id, true, nota).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (res) => {
        this.savingStatus.set(null);
        this.message.set(res.message);
        this.closeWhatsappPrompt();
        this.loadImportadas();
        this.loadPendientes();
        this.loadBolsas();
        this.notifyOcAlertsChanged();
        this.selectOc({ ...row, completada: res.completada, nota_remision: res.nota_remision });
      },
      error: (err) => {
        this.savingStatus.set(null);
        this.error.set(httpErrorMessage(err, 'No se pudo cambiar el estado de la OC.'));
      },
    });
  }

  whatsappHrefForTarget(row: OrdenCompraRow): string {
    const phone = this.cleanPhone(this.whatsappPhone());
    return phone ? this.whatsappUrl(row, phone, this.notaRemision().trim()) : '';
  }

  openProductionGroup(detail: OrdenCompraDetail): void {
    const text = this.buildPedidoTxt(detail);
    if (navigator.clipboard?.writeText) {
      navigator.clipboard.writeText(text).then(
        () => {
          this.message.set('Pedido TXT copiado. Pegalo en PRODUCCION GRANOS CENTRAL.');
          window.open(this.productionGroupUrl, '_blank', 'noopener');
        },
        () => {
          this.copyPedidoTxtFallback(text);
          window.open(this.productionGroupUrl, '_blank', 'noopener');
        },
      );
      return;
    }
    this.copyPedidoTxtFallback(text);
    window.open(this.productionGroupUrl, '_blank', 'noopener');
  }

  mostrarPedidoTxt(detail: OrdenCompraDetail): void {
    if (!detail.items.length) {
      this.error.set('No hay items para esta OC.');
      return;
    }
    this.pedidoTxt.set(this.buildPedidoTxt(detail));
  }

  private buildPedidoTxt(detail: OrdenCompraDetail): string {
    const header = `Destino: ${detail.sucursal || '-'} | OC: ${detail.nro_oc} | Peso: ${this.fmtKg(this.totalPesoKg(detail))}`;
    const lines = [header, 'Cantidad - Descripcion'];
    for (const item of detail.items) {
      const desc = this.cleanPedidoDescription(item.descripcion);
      const cantidad = Math.round(Number(item.cantidad || 0)).toString();
      const peso = this.itemPesoKg(item);
      const pesoLabel = peso ? ` - (${this.fmtKg(peso)})` : '';
      lines.push(`${cantidad} - ${desc.slice(0, 60)}${pesoLabel}`);
    }
    return lines.join('\n');
  }

  cerrarPedidoTxt(): void {
    this.pedidoTxt.set('');
  }

  copiarPedidoTxt(): void {
    const texto = this.pedidoTxt();
    if (!texto) return;
    if (navigator.clipboard?.writeText) {
      navigator.clipboard.writeText(texto).then(
        () => this.message.set('Pedido TXT copiado al portapapeles.'),
        () => this.copyPedidoTxtFallback(texto),
      );
      return;
    }
    this.copyPedidoTxtFallback(texto);
  }

  private cleanPedidoDescription(value: string): string {
    return (value || '')
      .toLowerCase()
      .replaceAll('el cacique', '')
      .replaceAll('cacique', '')
      .replace(/\(\s*\d+\s*\)/g, ' ')
      .replace(/\bel\b/g, ' ')
      .split(/\s+/)
      .filter(Boolean)
      .join(' ');
  }

  private gramajeTotalPorPaquete(descripcion: string): number | null {
    const text = String(descripcion || '').toLowerCase();
    let gramos: number | null = null;
    let match = text.match(/(\d+)\s*(?:kg|kilo|kilos)\b/);
    if (match) {
      gramos = Number(match[1]) * 1000;
    } else {
      match = text.match(/(\d+)\s*(?:g|gr|gramo|gramos)\b/);
      if (match) {
        gramos = Number(match[1]);
      } else {
        match = text.match(/\*\s*(\d{2,4})\b/);
        gramos = match ? Number(match[1]) : null;
      }
    }
    if (!gramos) return null;
    const unidadesMatch = text.match(/\((\d+)\)/);
    const unidades = unidadesMatch ? Number(unidadesMatch[1]) : 1;
    return gramos * unidades;
  }

  private cleanPhone(value: string): string {
    const digits = String(value || '').replace(/\D+/g, '');
    return digits.startsWith('0') ? `595${digits.slice(1)}` : digits;
  }

  private sucursalKey(value: string): string {
    return (value || 'SIN_SUCURSAL').trim().toUpperCase();
  }

  private readWhatsappMap(): Record<string, string> {
    try {
      const raw = JSON.parse(localStorage.getItem(this.whatsappBySucursalKey) || '{}') as Record<string, string | { principal?: string }>;
      return Object.fromEntries(
        Object.entries(raw).map(([key, value]) => [
          key,
          typeof value === 'string' ? value : value?.principal || '',
        ]),
      );
    } catch {
      return {};
    }
  }

  private getWhatsappBySucursal(sucursal: string): string {
    return this.whatsappNumbers()[this.sucursalKey(sucursal)] || '';
  }

  private saveWhatsappBySucursal(sucursal: string, phone: string): void {
    const map = { ...this.whatsappNumbers() };
    map[this.sucursalKey(sucursal)] = phone;
    this.whatsappNumbers.set(map);
    localStorage.setItem(this.whatsappBySucursalKey, JSON.stringify(map));
  }

  private whatsappUrl(row: OrdenCompraRow, phone: string, notaRemision: string): string {
    const oc = row.nro_oc || String(row.oc_id);
    const saludo = new Date().getHours() < 12 ? 'buen dia' : 'buenas tardes';
    const destino = row.sucursal ? ` a ${row.sucursal}` : '';
    const nota = notaRemision ? `- Nota de remision: ${notaRemision}.` : '';
    const text = [
      'Aviso automatico - Granos Central.',
      `Hola, ${saludo}. Espero que se encuentren bien.`,
      `- Informamos que el pedido correspondiente a la OC ${oc} ya fue despachado y se encuentra en camino${destino}.`,
      nota,
      'Agradecemos la confianza y esperamos atenderles nuevamente pronto.',
      'Muchas gracias.',
    ].filter(Boolean).join('\n');
    return `https://wa.me/${phone}?text=${encodeURIComponent(text)}`;
  }

  private copyPedidoTxtFallback(texto: string): void {
    const area = document.createElement('textarea');
    area.value = texto;
    area.setAttribute('readonly', 'true');
    area.style.position = 'fixed';
    area.style.left = '-9999px';
    area.style.top = '0';
    document.body.appendChild(area);
    area.focus();
    area.select();
    try {
      const copied = document.execCommand('copy');
      if (copied) {
        this.message.set('Pedido TXT copiado al portapapeles.');
      } else {
        this.error.set('No se pudo copiar el texto.');
      }
    } catch {
      this.error.set('No se pudo copiar el texto.');
    } finally {
      document.body.removeChild(area);
    }
  }

  private notifyOcAlertsChanged(): void {
    window.dispatchEvent(new Event('ordenes-compra-alerts-changed'));
  }
}

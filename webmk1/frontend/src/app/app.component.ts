import { CommonModule } from '@angular/common';
import { ChangeDetectionStrategy, Component, HostListener, signal } from '@angular/core';
import { RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, RouterLink, RouterLinkActive, RouterOutlet],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class AppComponent {
  readonly sidebarOpen = signal<boolean>(false);
  readonly sidebarExpanded = signal<boolean>(false);

  toggleSidebar(): void {
    this.sidebarOpen.update((value) => !value);
    this.sidebarExpanded.update((value) => !value);
  }

  closeSidebar(): void {
    this.sidebarOpen.set(false);
    this.sidebarExpanded.set(false);
  }

  toggleExpanded(): void {
    this.sidebarExpanded.update((value) => !value);
  }

  @HostListener('document:click', ['$event'])
  onDocumentClick(event: MouseEvent): void {
    const target = event.target as HTMLElement | null;
    if (!target || target.closest('.sidebar') || target.closest('.burger-button')) {
      return;
    }
    if (this.sidebarOpen() || this.sidebarExpanded()) {
      this.closeSidebar();
    }
  }
}

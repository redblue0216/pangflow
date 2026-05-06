const items = [
  { hash: '#dashboard', label: 'Dashboard', icon: '\uD83D\uDCCA' },
  { hash: '#workflows', label: 'Workflows', icon: '\u2699\uFE0F' },
  { hash: '#executions', label: 'Executions', icon: '\uD83D\uDCDD' },
  { hash: '#models', label: 'Models', icon: '\uD83D\uDCE6' },
  { hash: '#lineage', label: 'Lineage', icon: '\uD83D\uDD17' },
  { hash: '#settings', label: 'Settings', icon: '\u2699\uFE0F' },
];

export function renderSidebar(container: HTMLElement): void {
  const current = window.location.hash || '#dashboard';

  container.innerHTML = `
    <aside class="w-64 bg-slate-900 text-white flex flex-col h-screen fixed left-0 top-0 z-20">
      <div class="p-6 border-b border-slate-700">
        <h1 class="text-xl font-bold tracking-wide">PangFlow</h1>
        <span class="text-xs text-slate-400">v0.2.6</span>
      </div>
      <nav class="flex-1 overflow-y-auto py-4">
        ${items.map(item => `
          <a href="${item.hash}"
             class="flex items-center gap-3 px-6 py-3 text-sm font-medium transition-colors ${current === item.hash ? 'bg-slate-800 text-white' : 'text-slate-300 hover:bg-slate-800 hover:text-white'}">
            <span class="text-lg">${item.icon}</span>
            ${item.label}
          </a>
        `).join('')}
      </nav>
      <div class="p-4 border-t border-slate-700 text-xs text-slate-500">
        &copy; 2025 PangFlow
      </div>
    </aside>
  `;
}

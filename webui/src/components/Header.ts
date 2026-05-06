export function renderHeader(container: HTMLElement, title: string): void {
  container.innerHTML = `
    <header class="bg-white border-b border-slate-200 px-8 py-4 flex items-center justify-between sticky top-0 z-10">
      <h2 class="text-2xl font-semibold text-slate-800">${title}</h2>
      <div class="flex items-center gap-4">
        <span class="text-sm text-slate-500">Admin</span>
        <div class="w-8 h-8 rounded-full bg-slate-200 flex items-center justify-center text-sm font-bold text-slate-600">
          A
        </div>
      </div>
    </header>
  `;
}

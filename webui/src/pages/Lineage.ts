import { api } from '../api/client';

export async function renderLineage(container: HTMLElement): Promise<void> {
  container.innerHTML = `
    <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
      <div class="flex items-center justify-between mb-6">
        <h3 class="text-lg font-semibold text-slate-800">Data Lineage</h3>
        <span class="text-xs text-slate-500">Live data from /api/lineage</span>
      </div>
      <div id="lineage-content" class="font-mono text-sm text-slate-700 space-y-2">
        <p class="text-slate-400">Loading...</p>
      </div>
    </div>
  `;

  try {
    const edges = await api.getLineage();
    const content = document.getElementById('lineage-content');
    if (!content) return;

    if (edges.length === 0) {
      content.innerHTML = `<p class="text-slate-400">No lineage data found.</p>`;
      return;
    }

    content.innerHTML = edges.map((edge, i) => `
      <div class="flex items-center gap-3 p-3 rounded-lg bg-slate-50 border border-slate-100 hover:bg-slate-100 transition">
        <span class="text-slate-500 font-bold w-6">${i + 1}</span>
        <span class="font-medium text-slate-800 bg-white px-3 py-1 rounded border border-slate-200">${edge.source}</span>
        <span class="text-slate-400">&rarr;</span>
        <span class="font-medium text-slate-800 bg-white px-3 py-1 rounded border border-slate-200">${edge.target}</span>
        <span class="ml-auto text-xs px-2 py-1 rounded bg-slate-200 text-slate-600">${edge.type}</span>
      </div>
    `).join('');
  } catch {
    const content = document.getElementById('lineage-content');
    if (content) content.innerHTML = `<p class="text-red-500">Failed to load lineage data.</p>`;
  }
}

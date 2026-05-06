import { api } from '../api/client';

export async function renderModels(container: HTMLElement): Promise<void> {
  container.innerHTML = `
    <div class="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
      <div class="px-6 py-4 border-b border-slate-200 flex items-center justify-between">
        <h3 class="text-lg font-semibold text-slate-800">Model Artifacts</h3>
        <span class="text-xs text-slate-500">Live data from /api/models</span>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-sm text-left">
          <thead class="bg-slate-50 text-slate-600 uppercase text-xs font-semibold">
            <tr>
              <th class="px-6 py-3">Name</th>
              <th class="px-6 py-3">Version</th>
              <th class="px-6 py-3">Stage</th>
              <th class="px-6 py-3">Created</th>
              <th class="px-6 py-3 text-right">Actions</th>
            </tr>
          </thead>
          <tbody id="models-tbody" class="divide-y divide-slate-100">
            <tr><td colspan="5" class="px-6 py-4 text-slate-400">Loading...</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  `;

  try {
    const models = await api.getModels();
    const tbody = document.getElementById('models-tbody');
    if (!tbody) return;

    if (models.length === 0) {
      tbody.innerHTML = `<tr><td colspan="5" class="px-6 py-4 text-slate-400">No models found.</td></tr>`;
      return;
    }

    tbody.innerHTML = models.map(m => `
      <tr class="hover:bg-slate-50 transition">
        <td class="px-6 py-4 font-medium text-slate-800">${m.name}</td>
        <td class="px-6 py-4 text-slate-600">${m.version}</td>
        <td class="px-6 py-4">
          <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${getStageClass(m.stage)}">
            ${m.stage}
          </span>
        </td>
        <td class="px-6 py-4 text-slate-500">${formatDate(m.created_at)}</td>
        <td class="px-6 py-4 text-right">
          <button data-id="${m.id}" data-action="promote" class="px-3 py-1.5 bg-emerald-600 text-white rounded-md text-xs font-medium hover:bg-emerald-700 transition mr-2">Promote</button>
          <button data-id="${m.id}" data-action="rollback" class="px-3 py-1.5 bg-white border border-slate-300 text-slate-700 rounded-md text-xs font-medium hover:bg-slate-50 transition">Rollback</button>
        </td>
      </tr>
    `).join('');

    tbody.addEventListener('click', async (e) => {
      const btn = (e.target as HTMLElement).closest('button');
      if (!btn) return;
      const id = btn.getAttribute('data-id');
      const action = btn.getAttribute('data-action');
      if (!id || !action) return;

      btn.disabled = true;
      btn.textContent = '...';

      try {
        if (action === 'promote') {
          await api.promoteModel(id);
        } else {
          await api.rollbackModel(id);
        }
        await renderModels(container);
      } catch (err) {
        alert(`Failed to ${action} model: ${err instanceof Error ? err.message : 'Unknown error'}`);
        btn.disabled = false;
        btn.textContent = action === 'promote' ? 'Promote' : 'Rollback';
      }
    });
  } catch {
    const tbody = document.getElementById('models-tbody');
    if (tbody) tbody.innerHTML = `<tr><td colspan="5" class="px-6 py-4 text-red-500">Failed to load models.</td></tr>`;
  }
}

function getStageClass(stage: string): string {
  const s = stage.toLowerCase();
  if (s === 'production' || s === 'prod') return 'bg-emerald-100 text-emerald-700';
  if (s === 'staging' || s === 'stage') return 'bg-amber-100 text-amber-700';
  if (s === 'development' || s === 'dev') return 'bg-blue-100 text-blue-700';
  return 'bg-slate-100 text-slate-700';
}

function formatDate(date: string): string {
  try {
    return new Date(date).toLocaleString();
  } catch {
    return date;
  }
}

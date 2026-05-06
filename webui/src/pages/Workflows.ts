import { api } from '../api/client';

export async function renderWorkflows(container: HTMLElement): Promise<void> {
  container.innerHTML = `
    <div class="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
      <div class="px-6 py-4 border-b border-slate-200 flex items-center justify-between">
        <h3 class="text-lg font-semibold text-slate-800">Workflows</h3>
        <span class="text-xs text-slate-500">Live data from /api/workflows</span>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-sm text-left">
          <thead class="bg-slate-50 text-slate-600 uppercase text-xs font-semibold">
            <tr>
              <th class="px-6 py-3">Name</th>
              <th class="px-6 py-3">Version</th>
              <th class="px-6 py-3">Status</th>
              <th class="px-6 py-3">Updated</th>
            </tr>
          </thead>
          <tbody id="workflows-tbody" class="divide-y divide-slate-100">
            <tr><td colspan="4" class="px-6 py-4 text-slate-400">Loading...</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  `;

  try {
    const workflows = await api.getWorkflows();
    const tbody = document.getElementById('workflows-tbody');
    if (!tbody) return;

    if (workflows.length === 0) {
      tbody.innerHTML = `<tr><td colspan="4" class="px-6 py-4 text-slate-400">No workflows found.</td></tr>`;
      return;
    }

    tbody.innerHTML = workflows.map(wf => `
      <tr class="hover:bg-slate-50 transition">
        <td class="px-6 py-4 font-medium text-slate-800">${wf.name}</td>
        <td class="px-6 py-4 text-slate-600">${wf.version}</td>
        <td class="px-6 py-4">
          <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${getStatusClass(wf.status)}">
            ${wf.status}
          </span>
        </td>
        <td class="px-6 py-4 text-slate-500">${formatDate(wf.updated_at)}</td>
      </tr>
    `).join('');
  } catch {
    const tbody = document.getElementById('workflows-tbody');
    if (tbody) tbody.innerHTML = `<tr><td colspan="4" class="px-6 py-4 text-red-500">Failed to load workflows.</td></tr>`;
  }
}

function getStatusClass(status: string): string {
  const s = status.toLowerCase();
  if (s === 'active' || s === 'success') return 'bg-emerald-100 text-emerald-700';
  if (s === 'inactive' || s === 'failed') return 'bg-red-100 text-red-700';
  return 'bg-slate-100 text-slate-700';
}

function formatDate(date: string): string {
  try {
    return new Date(date).toLocaleString();
  } catch {
    return date;
  }
}

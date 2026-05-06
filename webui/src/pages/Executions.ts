import { api } from '../api/client';

export async function renderExecutions(container: HTMLElement): Promise<void> {
  container.innerHTML = `
    <div class="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
      <div class="px-6 py-4 border-b border-slate-200 flex items-center justify-between">
        <h3 class="text-lg font-semibold text-slate-800">Executions</h3>
        <span class="text-xs text-slate-500">Live data from /api/executions</span>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-sm text-left">
          <thead class="bg-slate-50 text-slate-600 uppercase text-xs font-semibold">
            <tr>
              <th class="px-6 py-3">Workflow</th>
              <th class="px-6 py-3">Status</th>
              <th class="px-6 py-3">Started</th>
              <th class="px-6 py-3">Duration</th>
              <th class="px-6 py-3">Actions</th>
            </tr>
          </thead>
          <tbody id="executions-tbody" class="divide-y divide-slate-100">
            <tr><td colspan="5" class="px-6 py-4 text-slate-400">Loading...</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  `;

  try {
    const executions = await api.getExecutions();
    const tbody = document.getElementById('executions-tbody');
    if (!tbody) return;

    if (executions.length === 0) {
      tbody.innerHTML = `<tr><td colspan="5" class="px-6 py-4 text-slate-400">No executions found.</td></tr>`;
      return;
    }

    tbody.innerHTML = executions.map((ex, idx) => `
      <tr class="hover:bg-slate-50 transition cursor-pointer" data-run-id="${ex.run_id || ex.id}" data-idx="${idx}">
        <td class="px-6 py-4 font-medium text-slate-800">${ex.workflow_name}</td>
        <td class="px-6 py-4">
          <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${getStatusClass(ex.status)}">
            ${ex.status}
          </span>
        </td>
        <td class="px-6 py-4 text-slate-500">${formatDate(ex.started_at)}</td>
        <td class="px-6 py-4 text-slate-500">${calculateDuration(ex.started_at, ex.ended_at)}</td>
        <td class="px-6 py-4">
          <button class="text-blue-600 hover:text-blue-800 text-xs font-medium" onclick="toggleNodeDetails(event, '${ex.run_id || ex.id}', ${idx})">
            View DAG
          </button>
        </td>
      </tr>
      <tr id="node-details-${idx}" class="hidden bg-slate-50">
        <td colspan="5" class="px-6 py-4">
          <div id="node-content-${idx}" class="text-sm text-slate-600">
            Loading node status...
          </div>
        </td>
      </tr>
    `).join('');

    // Expose toggle function globally for inline onclick handlers
    (window as any).toggleNodeDetails = toggleNodeDetails;
  } catch {
    const tbody = document.getElementById('executions-tbody');
    if (tbody) tbody.innerHTML = `<tr><td colspan="5" class="px-6 py-4 text-red-500">Failed to load executions.</td></tr>`;
  }
}

async function toggleNodeDetails(event: Event, runId: string, idx: number): Promise<void> {
  event.stopPropagation();
  const row = document.getElementById(`node-details-${idx}`);
  const content = document.getElementById(`node-content-${idx}`);
  if (!row || !content) return;

  const isHidden = row.classList.contains('hidden');
  if (isHidden) {
    row.classList.remove('hidden');
    try {
      const nodes = await api.getExecutionNodes(runId);
      if (nodes.length === 0) {
        content.innerHTML = `<p class="text-slate-400 italic">No node-level logs found for this run. Run the workflow to see DAG node status.</p>`;
        return;
      }
      content.innerHTML = `
        <div class="mb-2 font-semibold text-slate-700">DAG Node Status — Run ${runId.slice(0, 8)}</div>
        <div class="flex flex-wrap gap-3">
          ${nodes.map(n => `
            <div class="flex items-center gap-2 bg-white border border-slate-200 rounded-lg px-3 py-2 shadow-sm">
              <div class="w-3 h-3 rounded-full ${getNodeDotClass(n.status)}"></div>
              <div>
                <div class="font-medium text-slate-800">${n.node_name || 'unknown'}</div>
                <div class="text-xs text-slate-500">
                  ${n.status}${n.duration_ms ? ` · ${(n.duration_ms).toFixed(0)}ms` : ''}
                </div>
              </div>
            </div>
          `).join('')}
        </div>
        ${nodes.some(n => n.exception) ? `
          <div class="mt-3 p-3 bg-red-50 border border-red-200 rounded text-red-700 text-xs">
            ${nodes.filter(n => n.exception).map(n => `<div><strong>${n.node_name}:</strong> ${n.exception}</div>`).join('')}
          </div>
        ` : ''}
      `;
    } catch {
      content.innerHTML = `<p class="text-red-500">Failed to load node details.</p>`;
    }
  } else {
    row.classList.add('hidden');
  }
}

function getStatusClass(status: string): string {
  const s = status.toLowerCase();
  if (s === 'success' || s === 'completed') return 'bg-emerald-100 text-emerald-700';
  if (s === 'failed' || s === 'error') return 'bg-red-100 text-red-700';
  if (s === 'running') return 'bg-blue-100 text-blue-700';
  return 'bg-slate-100 text-slate-700';
}

function getNodeDotClass(status: string): string {
  const s = status.toLowerCase();
  if (s === 'success') return 'bg-emerald-500';
  if (s === 'failed') return 'bg-red-500';
  if (s === 'running') return 'bg-blue-500 animate-pulse';
  return 'bg-slate-400';
}

function formatDate(date: string): string {
  try {
    return new Date(date).toLocaleString();
  } catch {
    return date;
  }
}

function calculateDuration(start: string, end: string | null): string {
  if (!end) return 'In progress';
  try {
    const diff = new Date(end).getTime() - new Date(start).getTime();
    if (diff < 1000) return `${diff}ms`;
    if (diff < 60000) return `${Math.round(diff / 1000)}s`;
    return `${Math.round(diff / 60000)}m`;
  } catch {
    return '-';
  }
}

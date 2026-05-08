import { api } from '../api/client';

const POLL_INTERVAL_MS = 5000;
let pollTimer: ReturnType<typeof setInterval> | null = null;

export async function renderExecutions(container: HTMLElement): Promise<void> {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }

  container.innerHTML = `
    <div class="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
      <div class="px-6 py-4 border-b border-slate-200 flex items-center justify-between">
        <div>
          <h3 class="text-lg font-semibold text-slate-800">Executions</h3>
          <p class="text-xs text-slate-500 mt-1">All workflow runs including scheduled & trigger</p>
        </div>
        <span class="text-xs text-slate-500">Live data from /api/executions</span>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-sm text-left">
          <thead class="bg-slate-50 text-slate-600 uppercase text-xs font-semibold">
            <tr>
              <th class="px-6 py-3">Workflow</th>
              <th class="px-6 py-3">Type</th>
              <th class="px-6 py-3">Status</th>
              <th class="px-6 py-3">Started</th>
              <th class="px-6 py-3">Duration</th>
              <th class="px-6 py-3">Actions</th>
            </tr>
          </thead>
          <tbody id="executions-tbody" class="divide-y divide-slate-100">
            <tr><td colspan="6" class="px-6 py-4 text-slate-400">Loading...</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  `;

  const tbody = document.getElementById('executions-tbody');
  if (!tbody) return;

  const loadExecutions = async () => {
    try {
      return await api.getExecutions();
    } catch {
      return null;
    }
  };

  const renderRows = (executions: any[]) => {
    if (executions.length === 0) {
      tbody.innerHTML = `<tr><td colspan="6" class="px-6 py-4 text-slate-400">No executions found.</td></tr>`;
      return;
    }

    tbody.innerHTML = executions.map((ex, idx) => `
      <tr class="hover:bg-slate-50 transition cursor-pointer" data-run-id="${ex.run_id || ex.id}" data-idx="${idx}" data-status="${ex.status}">
        <td class="px-6 py-4 font-medium text-slate-800">${ex.workflow_name}</td>
        <td class="px-6 py-4">
          <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${getTypeClass(ex.execution_type)}">
            ${ex.execution_type || 'trigger'}
          </span>
        </td>
        <td class="px-6 py-4">
          <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${getStatusClass(ex.status)}" id="status-badge-${idx}">
            ${ex.status}
          </span>
        </td>
        <td class="px-6 py-4 text-slate-500" id="started-at-${idx}">${formatDate(ex.started_at)}</td>
        <td class="px-6 py-4 text-slate-500" id="duration-${idx}">${calculateDuration(ex.started_at, ex.ended_at)}</td>
        <td class="px-6 py-4">
          <button class="text-blue-600 hover:text-blue-800 text-xs font-medium" onclick="toggleNodeDetails(event, '${ex.run_id || ex.id}', ${idx})">
            View DAG
          </button>
        </td>
      </tr>
      <tr id="node-details-${idx}" class="hidden bg-slate-50">
        <td colspan="6" class="px-6 py-4">
          <div id="node-content-${idx}" class="text-sm text-slate-600">
            Loading DAG...
          </div>
        </td>
      </tr>
    `).join('');
  };

  const executions = await loadExecutions();
  if (!executions) {
    tbody.innerHTML = `<tr><td colspan="6" class="px-6 py-4 text-red-500">Failed to load executions.</td></tr>`;
    return;
  }
  renderRows(executions);

  // Expose toggle function globally for inline onclick handlers
  (window as any).toggleNodeDetails = toggleNodeDetails;

  // Start polling for running executions
  startPolling(executions, loadExecutions);
}

function startPolling(
  initialExecutions: any[],
  loadExecutions: () => Promise<any[] | null>,
) {
  if (pollTimer) clearInterval(pollTimer);

  pollTimer = setInterval(async () => {
    const latest = await loadExecutions();
    if (!latest) return;

    let stillRunning = false;
    for (const ex of latest) {
      const idx = initialExecutions.findIndex(e => (e.run_id || e.id) === (ex.run_id || ex.id));
      if (idx < 0) continue;

      const row = document.querySelector(`tr[data-idx="${idx}"]`);
      const badge = document.getElementById(`status-badge-${idx}`);
      const durationCell = document.getElementById(`duration-${idx}`);

      // Update status badge if changed
      if (badge && badge.textContent !== ex.status) {
        badge.textContent = ex.status;
        badge.className = `inline-flex px-2 py-1 rounded-full text-xs font-medium ${getStatusClass(ex.status)}`;
        if (row) row.setAttribute('data-status', ex.status);
      }

      // Update duration if changed
      if (durationCell) {
        const newDuration = calculateDuration(ex.started_at, ex.ended_at);
        if (durationCell.textContent !== newDuration) {
          durationCell.textContent = newDuration;
        }
      }

      if (ex.status === 'running') {
        stillRunning = true;
      }

      // Refresh DAG if visible
      const content = document.getElementById(`node-content-${idx}`);
      const detailsRow = document.getElementById(`node-details-${idx}`);
      if (content && detailsRow && !detailsRow.classList.contains('hidden')) {
        try {
          const dag = await api.getExecutionDag(ex.run_id || ex.id);
          renderDag(content, dag);
        } catch {
          content.innerHTML = `<p class="text-red-500">Failed to load DAG.</p>`;
        }
      }
    }

    if (!stillRunning && pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }, POLL_INTERVAL_MS);
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
      const dag = await api.getExecutionDag(runId);
      if (dag.nodes.length === 0) {
        content.innerHTML = `<p class="text-slate-400 italic">No node-level logs found for this run.</p>`;
        return;
      }
      renderDag(content, dag);
    } catch {
      content.innerHTML = `<p class="text-red-500">Failed to load DAG.</p>`;
    }
  } else {
    row.classList.add('hidden');
  }
}

function renderDag(container: HTMLElement, dag: any): void {
  const nodes = dag.nodes || [];
  const edges = dag.edges || [];

  // Interleave nodes and arrows
  let flowHtml = '';
  for (let i = 0; i < nodes.length; i++) {
    flowHtml += `<div class="flex flex-col items-center gap-1 min-w-[100px]">
      <div class="w-10 h-10 rounded-full flex items-center justify-center text-white font-bold text-sm shadow ${getNodeBgClass(nodes[i].status)}">
        ${(nodes[i].node_name || nodes[i].node_id).slice(0, 2).toUpperCase()}
      </div>
      <div class="text-xs font-medium text-slate-700 text-center leading-tight">${nodes[i].node_name || nodes[i].node_id}</div>
      <div class="text-[10px] text-slate-500">${nodes[i].status}${nodes[i].duration_ms ? ` · ${nodes[i].duration_ms}ms` : ''}</div>
    </div>`;
    if (i < edges.length) {
      flowHtml += `<div class="flex items-center px-1 text-slate-400">
        <svg width="20" height="12" viewBox="0 0 20 12" fill="none" xmlns="http://www.w3.org/2000/svg">
          <path d="M0 6H18M18 6L13 1M18 6L13 11" stroke="currentColor" stroke-width="1.5"/>
        </svg>
      </div>`;
    }
  }

  container.innerHTML = `
    <div class="mb-2 font-semibold text-slate-700">DAG Topology — Run ${dag.run_id?.slice(0, 8)}</div>
    <div class="flex items-center gap-2 overflow-x-auto py-3 px-1">
      ${flowHtml}
    </div>
    ${nodes.some((n: any) => n.exception) ? `
      <div class="mt-3 p-3 bg-red-50 border border-red-200 rounded text-red-700 text-xs">
        ${nodes.filter((n: any) => n.exception).map((n: any) => `<div><strong>${n.node_name}:</strong> ${n.exception}</div>`).join('')}
      </div>
    ` : ''}
  `;
}

function getNodeBgClass(status: string): string {
  const s = status.toLowerCase();
  if (s === 'success') return 'bg-emerald-500';
  if (s === 'failed') return 'bg-red-500';
  if (s === 'running') return 'bg-blue-500 animate-pulse';
  return 'bg-slate-400';
}

function getStatusClass(status: string): string {
  const s = status.toLowerCase();
  if (s === 'success' || s === 'completed') return 'bg-emerald-100 text-emerald-700';
  if (s === 'failed' || s === 'error') return 'bg-red-100 text-red-700';
  if (s === 'running') return 'bg-blue-100 text-blue-700';
  return 'bg-slate-100 text-slate-700';
}

function getTypeClass(type: string | undefined): string {
  const t = (type || '').toLowerCase();
  if (t === 'scheduled') return 'bg-purple-100 text-purple-700';
  if (t === 'trigger') return 'bg-sky-100 text-sky-700';
  return 'bg-slate-100 text-slate-700';
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

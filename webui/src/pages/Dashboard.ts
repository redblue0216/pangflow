import { api } from '../api/client';

export async function renderDashboard(container: HTMLElement): Promise<void> {
  container.innerHTML = `
    <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
      <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
        <div class="text-sm text-slate-500 mb-1">Total Workflows</div>
        <div class="text-3xl font-bold text-slate-800" id="dash-workflows">...</div>
      </div>
      <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
        <div class="text-sm text-slate-500 mb-1">Recent Executions</div>
        <div class="text-3xl font-bold text-slate-800" id="dash-executions">...</div>
      </div>
      <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
        <div class="text-sm text-slate-500 mb-1">System Health</div>
        <div class="text-3xl font-bold text-emerald-600" id="dash-health">Healthy</div>
      </div>
    </div>
    <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
      <h3 class="text-lg font-semibold text-slate-800 mb-4">Quick Actions</h3>
      <div class="flex gap-3">
        <a href="#workflows" class="px-4 py-2 bg-slate-900 text-white rounded-lg text-sm font-medium hover:bg-slate-800 transition">View Workflows</a>
        <a href="#executions" class="px-4 py-2 bg-white border border-slate-300 text-slate-700 rounded-lg text-sm font-medium hover:bg-slate-50 transition">View Executions</a>
      </div>
    </div>
  `;

  try {
    const workflows = await api.getWorkflows();
    const executions = await api.getExecutions();
    const wfEl = document.getElementById('dash-workflows');
    const exEl = document.getElementById('dash-executions');
    if (wfEl) wfEl.textContent = String(workflows.length);
    if (exEl) exEl.textContent = String(executions.length);
  } catch {
    // Fallback to ellipsis if API is unavailable
  }
}

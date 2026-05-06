import { api } from '../api/client';

export async function renderSettings(container: HTMLElement): Promise<void> {
  container.innerHTML = `
    <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
      <div class="flex items-center justify-between mb-6">
        <h3 class="text-lg font-semibold text-slate-800">System Settings</h3>
        <span class="text-xs text-slate-500">Live data from /api/settings</span>
      </div>
      <div id="settings-content" class="space-y-4 max-w-2xl">
        <p class="text-slate-400">Loading...</p>
      </div>
    </div>
  `;

  try {
    const config = await api.getSettings();
    const content = document.getElementById('settings-content');
    if (!content) return;

    content.innerHTML = `
      <div class="grid grid-cols-1 gap-4">
        ${settingRow('Version', config.version)}
        ${settingRow('Environment', config.environment)}
        ${settingRow('Database URL', maskUrl(config.database_url))}
        ${settingRow('Log Level', config.log_level)}
      </div>
    `;
  } catch {
    const content = document.getElementById('settings-content');
    if (content) content.innerHTML = `<p class="text-red-500">Failed to load settings.</p>`;
  }
}

function settingRow(label: string, value: string): string {
  return `
    <div class="flex items-center justify-between p-4 bg-slate-50 rounded-lg border border-slate-100">
      <span class="text-sm font-medium text-slate-600">${label}</span>
      <span class="text-sm font-semibold text-slate-800 font-mono">${value}</span>
    </div>
  `;
}

function maskUrl(url: string): string {
  try {
    const u = new URL(url);
    return `${u.protocol}//${u.hostname}:****${u.pathname}`;
  } catch {
    return '***masked***';
  }
}

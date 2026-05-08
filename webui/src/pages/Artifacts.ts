import { api } from '../api/client';

export async function renderArtifacts(container: HTMLElement): Promise<void> {
  container.innerHTML = `
    <div class="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
      <div class="px-6 py-4 border-b border-slate-200 flex items-center justify-between">
        <div>
          <h3 class="text-lg font-semibold text-slate-800">Artifacts</h3>
          <p class="text-xs text-slate-500 mt-1">Models, datasets, features and other workflow outputs</p>
        </div>
        <div class="flex items-center gap-2">
          <select id="artifact-type-filter" class="text-xs border border-slate-300 rounded-md px-2 py-1 bg-white text-slate-700">
            <option value="">All Types</option>
            <option value="model">Model</option>
            <option value="data">Data</option>
            <option value="feature">Feature</option>
          </select>
          <span class="text-xs text-slate-500">/api/artifacts</span>
        </div>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-sm text-left">
          <thead class="bg-slate-50 text-slate-600 uppercase text-xs font-semibold">
            <tr>
              <th class="px-6 py-3">Name</th>
              <th class="px-6 py-3">Version</th>
              <th class="px-6 py-3">Type</th>
              <th class="px-6 py-3">Stage</th>
              <th class="px-6 py-3">Created</th>
              <th class="px-6 py-3 text-right">Actions</th>
            </tr>
          </thead>
          <tbody id="artifacts-tbody" class="divide-y divide-slate-100">
            <tr><td colspan="6" class="px-6 py-4 text-slate-400">Loading...</td></tr>
          </tbody>
        </table>
      </div>
    </div>
    <div id="versions-modal" class="hidden fixed inset-0 bg-black/50 z-50 flex items-center justify-center">
      <div class="bg-white rounded-xl shadow-xl max-w-3xl w-full mx-4 max-h-[80vh] overflow-hidden flex flex-col">
        <div class="px-6 py-4 border-b border-slate-200 flex items-center justify-between">
          <h3 class="text-lg font-semibold text-slate-800" id="versions-title">Versions</h3>
          <button id="close-versions" class="text-slate-400 hover:text-slate-600">✕</button>
        </div>
        <div class="overflow-y-auto p-6" id="versions-content"></div>
      </div>
    </div>
  `;

  const loadArtifacts = async (typeFilter: string = '') => {
    try {
      const artifacts = await api.getArtifacts(typeFilter || undefined);
      const tbody = document.getElementById('artifacts-tbody');
      if (!tbody) return;

      if (artifacts.length === 0) {
        tbody.innerHTML = `<tr><td colspan="6" class="px-6 py-4 text-slate-400">No artifacts found.</td></tr>`;
        return;
      }

      tbody.innerHTML = artifacts.map(m => `
        <tr class="hover:bg-slate-50 transition">
          <td class="px-6 py-4 font-medium text-slate-800">${m.name}</td>
          <td class="px-6 py-4 text-slate-600">${m.version}</td>
          <td class="px-6 py-4">
            <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${getTypeClass(m.artifact_type)}">
              ${m.artifact_type}
            </span>
          </td>
          <td class="px-6 py-4">
            <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${getStageClass(m.stage)}">
              ${m.stage}
            </span>
          </td>
          <td class="px-6 py-4 text-slate-500">${formatDate(m.created_at)}</td>
          <td class="px-6 py-4 text-right">
            <button data-name="${m.name}" data-action="versions" class="px-3 py-1.5 bg-white border border-slate-300 text-slate-700 rounded-md text-xs font-medium hover:bg-slate-50 transition mr-2">Versions</button>
            <button data-id="${m.id}" data-action="promote" class="px-3 py-1.5 bg-emerald-600 text-white rounded-md text-xs font-medium hover:bg-emerald-700 transition mr-2">Promote</button>
            <button data-id="${m.id}" data-action="rollback" class="px-3 py-1.5 bg-white border border-slate-300 text-slate-700 rounded-md text-xs font-medium hover:bg-slate-50 transition">Rollback</button>
          </td>
        </tr>
      `).join('');
    } catch {
      const tbody = document.getElementById('artifacts-tbody');
      if (tbody) tbody.innerHTML = `<tr><td colspan="6" class="px-6 py-4 text-red-500">Failed to load artifacts.</td></tr>`;
    }
  };

  await loadArtifacts();

  // Type filter
  const filterSelect = document.getElementById('artifact-type-filter') as HTMLSelectElement | null;
  if (filterSelect) {
    filterSelect.addEventListener('change', () => loadArtifacts(filterSelect.value));
  }

  // Actions
  const tbody = document.getElementById('artifacts-tbody');
  if (tbody) {
    tbody.addEventListener('click', async (e) => {
      const btn = (e.target as HTMLElement).closest('button');
      if (!btn) return;
      const id = btn.getAttribute('data-id');
      const name = btn.getAttribute('data-name');
      const action = btn.getAttribute('data-action');
      if (!action) return;

      if (action === 'versions' && name) {
        await showVersions(name);
        return;
      }

      if (!id) return;
      btn.disabled = true;
      btn.textContent = '...';

      try {
        if (action === 'promote') {
          await api.promoteModel(id);
        } else if (action === 'rollback') {
          await api.rollbackModel(id);
        }
        await loadArtifacts(filterSelect?.value || '');
      } catch (err) {
        alert(`Failed to ${action} artifact: ${err instanceof Error ? err.message : 'Unknown error'}`);
        btn.disabled = false;
        btn.textContent = action === 'promote' ? 'Promote' : action === 'rollback' ? 'Rollback' : 'Versions';
      }
    });
  }

  // Close modal
  const closeBtn = document.getElementById('close-versions');
  if (closeBtn) {
    closeBtn.addEventListener('click', () => {
      const modal = document.getElementById('versions-modal');
      if (modal) modal.classList.add('hidden');
    });
  }
}

async function showVersions(name: string): Promise<void> {
  const modal = document.getElementById('versions-modal');
  const title = document.getElementById('versions-title');
  const content = document.getElementById('versions-content');
  if (!modal || !title || !content) return;

  title.textContent = `Versions – ${name}`;
  content.innerHTML = `<p class="text-slate-400">Loading...</p>`;
  modal.classList.remove('hidden');

  try {
    const versions = await api.getArtifactVersions(name);
    if (versions.length === 0) {
      content.innerHTML = `<p class="text-slate-400">No versions found.</p>`;
      return;
    }

    content.innerHTML = `
      <table class="w-full text-sm text-left">
        <thead class="bg-slate-50 text-slate-600 uppercase text-xs font-semibold">
          <tr>
            <th class="px-4 py-2">Version</th>
            <th class="px-4 py-2">Type</th>
            <th class="px-4 py-2">ID</th>
            <th class="px-4 py-2">Stage</th>
            <th class="px-4 py-2">Created</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-slate-100">
          ${versions.map(v => `
            <tr class="hover:bg-slate-50">
              <td class="px-4 py-2 font-medium text-slate-800">${v.version}</td>
              <td class="px-4 py-2">
                <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${getTypeClass(v.artifact_type)}">${v.artifact_type}</span>
              </td>
              <td class="px-4 py-2 text-slate-600 font-mono text-xs">${v.id}</td>
              <td class="px-4 py-2">
                <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${getStageClass(v.stage)}">${v.stage}</span>
              </td>
              <td class="px-4 py-2 text-slate-500">${formatDate(v.created_at)}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    `;
  } catch {
    content.innerHTML = `<p class="text-red-500">Failed to load versions.</p>`;
  }
}

function getTypeClass(type: string): string {
  const t = (type || '').toLowerCase();
  if (t === 'model') return 'bg-purple-100 text-purple-700';
  if (t === 'data') return 'bg-sky-100 text-sky-700';
  if (t === 'feature') return 'bg-amber-100 text-amber-700';
  return 'bg-slate-100 text-slate-700';
}

function getStageClass(stage: string): string {
  const s = (stage || '').toLowerCase();
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

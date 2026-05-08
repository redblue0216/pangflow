(function(){const t=document.createElement("link").relList;if(t&&t.supports&&t.supports("modulepreload"))return;for(const a of document.querySelectorAll('link[rel="modulepreload"]'))o(a);new MutationObserver(a=>{for(const i of a)if(i.type==="childList")for(const n of i.addedNodes)n.tagName==="LINK"&&n.rel==="modulepreload"&&o(n)}).observe(document,{childList:!0,subtree:!0});function s(a){const i={};return a.integrity&&(i.integrity=a.integrity),a.referrerPolicy&&(i.referrerPolicy=a.referrerPolicy),a.crossOrigin==="use-credentials"?i.credentials="include":a.crossOrigin==="anonymous"?i.credentials="omit":i.credentials="same-origin",i}function o(a){if(a.ep)return;a.ep=!0;const i=s(a);fetch(a.href,i)}})();const w="";async function u(e){const t=await fetch(`${w}${e}`);if(!t.ok)throw new Error(`HTTP ${t.status}`);return t.json()}async function b(e,t){const s=await fetch(`${w}${e}`,{method:"POST",headers:{"Content-Type":"application/json"},body:t?JSON.stringify(t):void 0});if(!s.ok)throw new Error(`HTTP ${s.status}`);return s.json()}const d={getWorkflows:()=>u("/api/workflows"),getExecutions:()=>u("/api/executions"),getExecutionNodes:e=>u(`/api/executions/${e}/nodes`),getExecutionDag:e=>u(`/api/executions/${e}/dag`),getModels:()=>u("/api/models"),getArtifacts:e=>u(`/api/artifacts${e?`?artifact_type=${encodeURIComponent(e)}`:""}`),getArtifactVersions:e=>u(`/api/artifacts/${encodeURIComponent(e)}/versions`),getLineage:()=>u("/api/lineage"),getSettings:()=>u("/api/settings"),promoteModel:e=>b(`/api/models/${e}/promote`,{stage:"production"}),rollbackModel:e=>b(`/api/models/${e}/rollback`)},H=[{hash:"#dashboard",label:"Dashboard",icon:"📊"},{hash:"#workflows",label:"Workflows",icon:"⚙️"},{hash:"#executions",label:"Executions",icon:"📝"},{hash:"#artifacts",label:"Artifacts",icon:"📦"},{hash:"#lineage",label:"Lineage",icon:"🔗"},{hash:"#settings",label:"Settings",icon:"⚙️"}];function h(e){const t=window.location.hash||"#dashboard";e.innerHTML=`
    <aside class="w-64 bg-slate-900 text-white flex flex-col h-screen fixed left-0 top-0 z-20">
      <div class="p-6 border-b border-slate-700">
        <h1 class="text-xl font-bold tracking-wide">PangFlow</h1>
        <span id="sidebar-version" class="text-xs text-slate-400">...</span>
      </div>
      <nav class="flex-1 overflow-y-auto py-4">
        ${H.map(s=>`
          <a href="${s.hash}"
             class="flex items-center gap-3 px-6 py-3 text-sm font-medium transition-colors ${t===s.hash?"bg-slate-800 text-white":"text-slate-300 hover:bg-slate-800 hover:text-white"}">
            <span class="text-lg">${s.icon}</span>
            ${s.label}
          </a>
        `).join("")}
      </nav>
      <div class="p-4 border-t border-slate-700 text-xs text-slate-500">
        &copy; 2025 PangFlow
      </div>
    </aside>
  `,d.getSettings().then(s=>{const o=document.getElementById("sidebar-version");o&&s.version&&(o.textContent=`v${s.version}`)}).catch(()=>{const s=document.getElementById("sidebar-version");s&&(s.textContent="")})}function I(e,t){e.innerHTML=`
    <header class="bg-white border-b border-slate-200 px-8 py-4 flex items-center justify-between sticky top-0 z-10">
      <h2 class="text-2xl font-semibold text-slate-800">${t}</h2>
      <div class="flex items-center gap-4">
        <span class="text-sm text-slate-500">Admin</span>
        <div class="w-8 h-8 rounded-full bg-slate-200 flex items-center justify-center text-sm font-bold text-slate-600">
          A
        </div>
      </div>
    </header>
  `}function _(){document.body.innerHTML="",document.body.className="bg-slate-50 text-slate-800 font-sans antialiased";const e=document.createElement("div");e.className="flex min-h-screen";const t=document.createElement("div");h(t);const s=document.createElement("div");s.className="flex-1 ml-64 flex flex-col min-h-screen";const o=document.createElement("div"),a=document.createElement("main");return a.className="flex-1 p-8",s.appendChild(o),s.appendChild(a),e.appendChild(t),e.appendChild(s),document.body.appendChild(e),{contentContainer:a,updateTitle:i=>I(o,i),updateSidebar:()=>h(t)}}async function D(e){e.innerHTML=`
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
  `;try{const t=await d.getWorkflows(),s=await d.getExecutions(),o=document.getElementById("dash-workflows"),a=document.getElementById("dash-executions");o&&(o.textContent=String(t.length)),a&&(a.textContent=String(s.length))}catch{}}async function B(e){e.innerHTML=`
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
  `;try{const t=await d.getWorkflows(),s=document.getElementById("workflows-tbody");if(!s)return;if(t.length===0){s.innerHTML='<tr><td colspan="4" class="px-6 py-4 text-slate-400">No workflows found.</td></tr>';return}s.innerHTML=t.map(o=>`
      <tr class="hover:bg-slate-50 transition">
        <td class="px-6 py-4 font-medium text-slate-800">${o.name}</td>
        <td class="px-6 py-4 text-slate-600">${o.version}</td>
        <td class="px-6 py-4">
          <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${S(o.status)}">
            ${o.status}
          </span>
        </td>
        <td class="px-6 py-4 text-slate-500">${A(o.updated_at)}</td>
      </tr>
    `).join("")}catch{const t=document.getElementById("workflows-tbody");t&&(t.innerHTML='<tr><td colspan="4" class="px-6 py-4 text-red-500">Failed to load workflows.</td></tr>')}}function S(e){const t=e.toLowerCase();return t==="active"||t==="success"?"bg-emerald-100 text-emerald-700":t==="inactive"||t==="failed"?"bg-red-100 text-red-700":"bg-slate-100 text-slate-700"}function A(e){try{return new Date(e).toLocaleString()}catch{return e}}const N=5e3;let p=null;async function j(e){p&&(clearInterval(p),p=null),e.innerHTML=`
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
  `;const t=document.getElementById("executions-tbody");if(!t)return;const s=async()=>{try{return await d.getExecutions()}catch{return null}},o=i=>{if(i.length===0){t.innerHTML='<tr><td colspan="6" class="px-6 py-4 text-slate-400">No executions found.</td></tr>';return}t.innerHTML=i.map((n,l)=>`
      <tr class="hover:bg-slate-50 transition cursor-pointer" data-run-id="${n.run_id||n.id}" data-idx="${l}" data-status="${n.status}">
        <td class="px-6 py-4 font-medium text-slate-800">${n.workflow_name}</td>
        <td class="px-6 py-4">
          <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${F(n.execution_type)}">
            ${n.execution_type||"trigger"}
          </span>
        </td>
        <td class="px-6 py-4">
          <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${L(n.status)}" id="status-badge-${l}">
            ${n.status}
          </span>
        </td>
        <td class="px-6 py-4 text-slate-500" id="started-at-${l}">${W(n.started_at)}</td>
        <td class="px-6 py-4 text-slate-500" id="duration-${l}">${E(n.started_at,n.ended_at)}</td>
        <td class="px-6 py-4">
          <button class="text-blue-600 hover:text-blue-800 text-xs font-medium" onclick="toggleNodeDetails(event, '${n.run_id||n.id}', ${l})">
            View DAG
          </button>
        </td>
      </tr>
      <tr id="node-details-${l}" class="hidden bg-slate-50">
        <td colspan="6" class="px-6 py-4">
          <div id="node-content-${l}" class="text-sm text-slate-600">
            Loading DAG...
          </div>
        </td>
      </tr>
    `).join("")},a=await s();if(!a){t.innerHTML='<tr><td colspan="6" class="px-6 py-4 text-red-500">Failed to load executions.</td></tr>';return}o(a),window.toggleNodeDetails=V,P(a,s)}function P(e,t){p&&clearInterval(p),p=setInterval(async()=>{const s=await t();if(!s)return;let o=!1;for(const a of s){const i=e.findIndex(x=>(x.run_id||x.id)===(a.run_id||a.id));if(i<0)continue;const n=document.querySelector(`tr[data-idx="${i}"]`),l=document.getElementById(`status-badge-${i}`),r=document.getElementById(`duration-${i}`);if(l&&l.textContent!==a.status&&(l.textContent=a.status,l.className=`inline-flex px-2 py-1 rounded-full text-xs font-medium ${L(a.status)}`,n&&n.setAttribute("data-status",a.status)),r){const x=E(a.started_at,a.ended_at);r.textContent!==x&&(r.textContent=x)}a.status==="running"&&(o=!0);const c=document.getElementById(`node-content-${i}`),f=document.getElementById(`node-details-${i}`);if(c&&f&&!f.classList.contains("hidden"))try{const x=await d.getExecutionDag(a.run_id||a.id);$(c,x)}catch{c.innerHTML='<p class="text-red-500">Failed to load DAG.</p>'}}!o&&p&&(clearInterval(p),p=null)},N)}async function V(e,t,s){e.stopPropagation();const o=document.getElementById(`node-details-${s}`),a=document.getElementById(`node-content-${s}`);if(!o||!a)return;if(o.classList.contains("hidden")){o.classList.remove("hidden");try{const n=await d.getExecutionDag(t);if(n.nodes.length===0){a.innerHTML='<p class="text-slate-400 italic">No node-level logs found for this run.</p>';return}$(a,n)}catch{a.innerHTML='<p class="text-red-500">Failed to load DAG.</p>'}}else o.classList.add("hidden")}function $(e,t){var i;const s=t.nodes||[],o=t.edges||[];let a="";for(let n=0;n<s.length;n++)a+=`<div class="flex flex-col items-center gap-1 min-w-[100px]">
      <div class="w-10 h-10 rounded-full flex items-center justify-center text-white font-bold text-sm shadow ${R(s[n].status)}">
        ${(s[n].node_name||s[n].node_id).slice(0,2).toUpperCase()}
      </div>
      <div class="text-xs font-medium text-slate-700 text-center leading-tight">${s[n].node_name||s[n].node_id}</div>
      <div class="text-[10px] text-slate-500">${s[n].status}${s[n].duration_ms?` · ${s[n].duration_ms}ms`:""}</div>
    </div>`,n<o.length&&(a+=`<div class="flex items-center px-1 text-slate-400">
        <svg width="20" height="12" viewBox="0 0 20 12" fill="none" xmlns="http://www.w3.org/2000/svg">
          <path d="M0 6H18M18 6L13 1M18 6L13 11" stroke="currentColor" stroke-width="1.5"/>
        </svg>
      </div>`);e.innerHTML=`
    <div class="mb-2 font-semibold text-slate-700">DAG Topology — Run ${(i=t.run_id)==null?void 0:i.slice(0,8)}</div>
    <div class="flex items-center gap-2 overflow-x-auto py-3 px-1">
      ${a}
    </div>
    ${s.some(n=>n.exception)?`
      <div class="mt-3 p-3 bg-red-50 border border-red-200 rounded text-red-700 text-xs">
        ${s.filter(n=>n.exception).map(n=>`<div><strong>${n.node_name}:</strong> ${n.exception}</div>`).join("")}
      </div>
    `:""}
  `}function R(e){const t=e.toLowerCase();return t==="success"?"bg-emerald-500":t==="failed"?"bg-red-500":t==="running"?"bg-blue-500 animate-pulse":"bg-slate-400"}function L(e){const t=e.toLowerCase();return t==="success"||t==="completed"?"bg-emerald-100 text-emerald-700":t==="failed"||t==="error"?"bg-red-100 text-red-700":t==="running"?"bg-blue-100 text-blue-700":"bg-slate-100 text-slate-700"}function F(e){const t=(e||"").toLowerCase();return t==="scheduled"?"bg-purple-100 text-purple-700":t==="trigger"?"bg-sky-100 text-sky-700":"bg-slate-100 text-slate-700"}function W(e){try{return new Date(e).toLocaleString()}catch{return e}}function E(e,t){if(!t)return"In progress";try{const s=new Date(t).getTime()-new Date(e).getTime();return s<1e3?`${s}ms`:s<6e4?`${Math.round(s/1e3)}s`:`${Math.round(s/6e4)}m`}catch{return"-"}}async function y(e){e.innerHTML=`
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
  `;const t=async(i="")=>{try{const n=await d.getArtifacts(i||void 0),l=document.getElementById("artifacts-tbody");if(!l)return;if(n.length===0){l.innerHTML='<tr><td colspan="6" class="px-6 py-4 text-slate-400">No artifacts found.</td></tr>';return}l.innerHTML=n.map(r=>`
        <tr class="hover:bg-slate-50 transition">
          <td class="px-6 py-4 font-medium text-slate-800">${r.name}</td>
          <td class="px-6 py-4 text-slate-600">${r.version}</td>
          <td class="px-6 py-4">
            <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${T(r.artifact_type)}">
              ${r.artifact_type}
            </span>
          </td>
          <td class="px-6 py-4">
            <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${k(r.stage)}">
              ${r.stage}
            </span>
          </td>
          <td class="px-6 py-4 text-slate-500">${M(r.created_at)}</td>
          <td class="px-6 py-4 text-right">
            <button data-name="${r.name}" data-action="versions" class="px-3 py-1.5 bg-white border border-slate-300 text-slate-700 rounded-md text-xs font-medium hover:bg-slate-50 transition mr-2">Versions</button>
            <button data-id="${r.id}" data-action="promote" class="px-3 py-1.5 bg-emerald-600 text-white rounded-md text-xs font-medium hover:bg-emerald-700 transition mr-2">Promote</button>
            <button data-id="${r.id}" data-action="rollback" class="px-3 py-1.5 bg-white border border-slate-300 text-slate-700 rounded-md text-xs font-medium hover:bg-slate-50 transition">Rollback</button>
          </td>
        </tr>
      `).join("")}catch{const n=document.getElementById("artifacts-tbody");n&&(n.innerHTML='<tr><td colspan="6" class="px-6 py-4 text-red-500">Failed to load artifacts.</td></tr>')}};await t();const s=document.getElementById("artifact-type-filter");s&&s.addEventListener("change",()=>t(s.value));const o=document.getElementById("artifacts-tbody");o&&o.addEventListener("click",async i=>{const n=i.target.closest("button");if(!n)return;const l=n.getAttribute("data-id"),r=n.getAttribute("data-name"),c=n.getAttribute("data-action");if(c){if(c==="versions"&&r){await O(r);return}if(l){n.disabled=!0,n.textContent="...";try{c==="promote"?await d.promoteModel(l):c==="rollback"&&await d.rollbackModel(l),await t((s==null?void 0:s.value)||"")}catch(f){alert(`Failed to ${c} artifact: ${f instanceof Error?f.message:"Unknown error"}`),n.disabled=!1,n.textContent=c==="promote"?"Promote":c==="rollback"?"Rollback":"Versions"}}}});const a=document.getElementById("close-versions");a&&a.addEventListener("click",()=>{const i=document.getElementById("versions-modal");i&&i.classList.add("hidden")})}async function O(e){const t=document.getElementById("versions-modal"),s=document.getElementById("versions-title"),o=document.getElementById("versions-content");if(!(!t||!s||!o)){s.textContent=`Versions – ${e}`,o.innerHTML='<p class="text-slate-400">Loading...</p>',t.classList.remove("hidden");try{const a=await d.getArtifactVersions(e);if(a.length===0){o.innerHTML='<p class="text-slate-400">No versions found.</p>';return}o.innerHTML=`
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
          ${a.map(i=>`
            <tr class="hover:bg-slate-50">
              <td class="px-4 py-2 font-medium text-slate-800">${i.version}</td>
              <td class="px-4 py-2">
                <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${T(i.artifact_type)}">${i.artifact_type}</span>
              </td>
              <td class="px-4 py-2 text-slate-600 font-mono text-xs">${i.id}</td>
              <td class="px-4 py-2">
                <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${k(i.stage)}">${i.stage}</span>
              </td>
              <td class="px-4 py-2 text-slate-500">${M(i.created_at)}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    `}catch{o.innerHTML='<p class="text-red-500">Failed to load versions.</p>'}}}function T(e){const t=(e||"").toLowerCase();return t==="model"?"bg-purple-100 text-purple-700":t==="data"?"bg-sky-100 text-sky-700":t==="feature"?"bg-amber-100 text-amber-700":"bg-slate-100 text-slate-700"}function k(e){const t=(e||"").toLowerCase();return t==="production"||t==="prod"?"bg-emerald-100 text-emerald-700":t==="staging"||t==="stage"?"bg-amber-100 text-amber-700":t==="development"||t==="dev"?"bg-blue-100 text-blue-700":"bg-slate-100 text-slate-700"}function M(e){try{return new Date(e).toLocaleString()}catch{return e}}async function U(e){e.innerHTML=`
    <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
      <div class="flex items-center justify-between mb-6">
        <h3 class="text-lg font-semibold text-slate-800">Data Lineage</h3>
        <span class="text-xs text-slate-500">Live data from /api/lineage</span>
      </div>
      <div id="lineage-content" class="font-mono text-sm text-slate-700 space-y-2">
        <p class="text-slate-400">Loading...</p>
      </div>
    </div>
  `;try{const t=await d.getLineage(),s=document.getElementById("lineage-content");if(!s)return;if(t.length===0){s.innerHTML='<p class="text-slate-400">No lineage data found.</p>';return}s.innerHTML=t.map((o,a)=>`
      <div class="flex items-center gap-3 p-3 rounded-lg bg-slate-50 border border-slate-100 hover:bg-slate-100 transition">
        <span class="text-slate-500 font-bold w-6">${a+1}</span>
        <span class="font-medium text-slate-800 bg-white px-3 py-1 rounded border border-slate-200">${o.source}</span>
        <span class="text-slate-400">&rarr;</span>
        <span class="font-medium text-slate-800 bg-white px-3 py-1 rounded border border-slate-200">${o.target}</span>
        <span class="ml-auto text-xs px-2 py-1 rounded bg-slate-200 text-slate-600">${o.type}</span>
      </div>
    `).join("")}catch{const t=document.getElementById("lineage-content");t&&(t.innerHTML='<p class="text-red-500">Failed to load lineage data.</p>')}}async function G(e){e.innerHTML=`
    <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
      <div class="flex items-center justify-between mb-6">
        <h3 class="text-lg font-semibold text-slate-800">System Settings</h3>
        <span class="text-xs text-slate-500">Live data from /api/settings</span>
      </div>
      <div id="settings-content" class="space-y-4 max-w-2xl">
        <p class="text-slate-400">Loading...</p>
      </div>
    </div>
  `;try{const t=await d.getSettings(),s=document.getElementById("settings-content");if(!s)return;s.innerHTML=`
      <div class="grid grid-cols-1 gap-4">
        ${g("Version",t.version)}
        ${g("Environment",t.environment)}
        ${g("Database URL",q(t.database_url))}
        ${g("Log Level",t.log_level)}
      </div>
    `}catch{const t=document.getElementById("settings-content");t&&(t.innerHTML='<p class="text-red-500">Failed to load settings.</p>')}}function g(e,t){return`
    <div class="flex items-center justify-between p-4 bg-slate-50 rounded-lg border border-slate-100">
      <span class="text-sm font-medium text-slate-600">${e}</span>
      <span class="text-sm font-semibold text-slate-800 font-mono">${t}</span>
    </div>
  `}function q(e){try{const t=new URL(e);return`${t.protocol}//${t.hostname}:****${t.pathname}`}catch{return"***masked***"}}const v={"#dashboard":{title:"Dashboard",render:D},"#workflows":{title:"Workflows",render:B},"#executions":{title:"Executions",render:j},"#artifacts":{title:"Artifacts",render:y},"#models":{title:"Models",render:y},"#lineage":{title:"Lineage",render:U},"#settings":{title:"Settings",render:G}};let m=null;async function C(){const e=window.location.hash||"#dashboard",t=v[e]||v["#dashboard"];m||(m=_()),m.updateTitle(t.title),m.updateSidebar(),await t.render(m.contentContainer)}window.addEventListener("hashchange",C);document.addEventListener("DOMContentLoaded",C);

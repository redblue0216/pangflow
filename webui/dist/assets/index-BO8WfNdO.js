(function(){const t=document.createElement("link").relList;if(t&&t.supports&&t.supports("modulepreload"))return;for(const n of document.querySelectorAll('link[rel="modulepreload"]'))a(n);new MutationObserver(n=>{for(const o of n)if(o.type==="childList")for(const r of o.addedNodes)r.tagName==="LINK"&&r.rel==="modulepreload"&&a(r)}).observe(document,{childList:!0,subtree:!0});function s(n){const o={};return n.integrity&&(o.integrity=n.integrity),n.referrerPolicy&&(o.referrerPolicy=n.referrerPolicy),n.crossOrigin==="use-credentials"?o.credentials="include":n.crossOrigin==="anonymous"?o.credentials="omit":o.credentials="same-origin",o}function a(n){if(n.ep)return;n.ep=!0;const o=s(n);fetch(n.href,o)}})();const b=[{hash:"#dashboard",label:"Dashboard",icon:"📊"},{hash:"#workflows",label:"Workflows",icon:"⚙️"},{hash:"#executions",label:"Executions",icon:"📝"},{hash:"#models",label:"Models",icon:"📦"},{hash:"#lineage",label:"Lineage",icon:"🔗"},{hash:"#settings",label:"Settings",icon:"⚙️"}];function p(e){const t=window.location.hash||"#dashboard";e.innerHTML=`
    <aside class="w-64 bg-slate-900 text-white flex flex-col h-screen fixed left-0 top-0 z-20">
      <div class="p-6 border-b border-slate-700">
        <h1 class="text-xl font-bold tracking-wide">PangFlow</h1>
        <span class="text-xs text-slate-400">v0.2.1</span>
      </div>
      <nav class="flex-1 overflow-y-auto py-4">
        ${b.map(s=>`
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
  `}function h(e,t){e.innerHTML=`
    <header class="bg-white border-b border-slate-200 px-8 py-4 flex items-center justify-between sticky top-0 z-10">
      <h2 class="text-2xl font-semibold text-slate-800">${t}</h2>
      <div class="flex items-center gap-4">
        <span class="text-sm text-slate-500">Admin</span>
        <div class="w-8 h-8 rounded-full bg-slate-200 flex items-center justify-center text-sm font-bold text-slate-600">
          A
        </div>
      </div>
    </header>
  `}function g(){document.body.innerHTML="",document.body.className="bg-slate-50 text-slate-800 font-sans antialiased";const e=document.createElement("div");e.className="flex min-h-screen";const t=document.createElement("div");p(t);const s=document.createElement("div");s.className="flex-1 ml-64 flex flex-col min-h-screen";const a=document.createElement("div"),n=document.createElement("main");return n.className="flex-1 p-8",s.appendChild(a),s.appendChild(n),e.appendChild(t),e.appendChild(s),document.body.appendChild(e),{contentContainer:n,updateTitle:o=>h(a,o),updateSidebar:()=>p(t)}}const y="";async function d(e){const t=await fetch(`${y}${e}`);if(!t.ok)throw new Error(`HTTP ${t.status}`);return t.json()}const l={getWorkflows:()=>d("/api/workflows"),getExecutions:()=>d("/api/executions"),getModels:()=>d("/api/models"),getLineage:()=>d("/api/lineage"),getSettings:()=>d("/api/settings"),promoteModel:e=>d(`/api/models/${e}/promote`),rollbackModel:e=>d(`/api/models/${e}/rollback`)};async function w(e){e.innerHTML=`
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
  `;try{const t=await l.getWorkflows(),s=await l.getExecutions(),a=document.getElementById("dash-workflows"),n=document.getElementById("dash-executions");a&&(a.textContent=String(t.length)),n&&(n.textContent=String(s.length))}catch{}}async function v(e){e.innerHTML=`
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
  `;try{const t=await l.getWorkflows(),s=document.getElementById("workflows-tbody");if(!s)return;if(t.length===0){s.innerHTML='<tr><td colspan="4" class="px-6 py-4 text-slate-400">No workflows found.</td></tr>';return}s.innerHTML=t.map(a=>`
      <tr class="hover:bg-slate-50 transition">
        <td class="px-6 py-4 font-medium text-slate-800">${a.name}</td>
        <td class="px-6 py-4 text-slate-600">${a.version}</td>
        <td class="px-6 py-4">
          <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${L(a.status)}">
            ${a.status}
          </span>
        </td>
        <td class="px-6 py-4 text-slate-500">${$(a.updated_at)}</td>
      </tr>
    `).join("")}catch{const t=document.getElementById("workflows-tbody");t&&(t.innerHTML='<tr><td colspan="4" class="px-6 py-4 text-red-500">Failed to load workflows.</td></tr>')}}function L(e){const t=e.toLowerCase();return t==="active"||t==="success"?"bg-emerald-100 text-emerald-700":t==="inactive"||t==="failed"?"bg-red-100 text-red-700":"bg-slate-100 text-slate-700"}function $(e){try{return new Date(e).toLocaleString()}catch{return e}}async function M(e){e.innerHTML=`
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
            </tr>
          </thead>
          <tbody id="executions-tbody" class="divide-y divide-slate-100">
            <tr><td colspan="4" class="px-6 py-4 text-slate-400">Loading...</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  `;try{const t=await l.getExecutions(),s=document.getElementById("executions-tbody");if(!s)return;if(t.length===0){s.innerHTML='<tr><td colspan="4" class="px-6 py-4 text-slate-400">No executions found.</td></tr>';return}s.innerHTML=t.map(a=>`
      <tr class="hover:bg-slate-50 transition">
        <td class="px-6 py-4 font-medium text-slate-800">${a.workflow_name}</td>
        <td class="px-6 py-4">
          <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${E(a.status)}">
            ${a.status}
          </span>
        </td>
        <td class="px-6 py-4 text-slate-500">${k(a.started_at)}</td>
        <td class="px-6 py-4 text-slate-500">${T(a.started_at,a.ended_at)}</td>
      </tr>
    `).join("")}catch{const t=document.getElementById("executions-tbody");t&&(t.innerHTML='<tr><td colspan="4" class="px-6 py-4 text-red-500">Failed to load executions.</td></tr>')}}function E(e){const t=e.toLowerCase();return t==="success"||t==="completed"?"bg-emerald-100 text-emerald-700":t==="failed"||t==="error"?"bg-red-100 text-red-700":t==="running"?"bg-blue-100 text-blue-700":"bg-slate-100 text-slate-700"}function k(e){try{return new Date(e).toLocaleString()}catch{return e}}function T(e,t){if(!t)return"In progress";try{const s=new Date(t).getTime()-new Date(e).getTime();return s<1e3?`${s}ms`:s<6e4?`${Math.round(s/1e3)}s`:`${Math.round(s/6e4)}m`}catch{return"-"}}async function f(e){e.innerHTML=`
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
  `;try{const t=await l.getModels(),s=document.getElementById("models-tbody");if(!s)return;if(t.length===0){s.innerHTML='<tr><td colspan="5" class="px-6 py-4 text-slate-400">No models found.</td></tr>';return}s.innerHTML=t.map(a=>`
      <tr class="hover:bg-slate-50 transition">
        <td class="px-6 py-4 font-medium text-slate-800">${a.name}</td>
        <td class="px-6 py-4 text-slate-600">${a.version}</td>
        <td class="px-6 py-4">
          <span class="inline-flex px-2 py-1 rounded-full text-xs font-medium ${H(a.stage)}">
            ${a.stage}
          </span>
        </td>
        <td class="px-6 py-4 text-slate-500">${S(a.created_at)}</td>
        <td class="px-6 py-4 text-right">
          <button data-id="${a.id}" data-action="promote" class="px-3 py-1.5 bg-emerald-600 text-white rounded-md text-xs font-medium hover:bg-emerald-700 transition mr-2">Promote</button>
          <button data-id="${a.id}" data-action="rollback" class="px-3 py-1.5 bg-white border border-slate-300 text-slate-700 rounded-md text-xs font-medium hover:bg-slate-50 transition">Rollback</button>
        </td>
      </tr>
    `).join(""),s.addEventListener("click",async a=>{const n=a.target.closest("button");if(!n)return;const o=n.getAttribute("data-id"),r=n.getAttribute("data-action");if(!(!o||!r)){n.disabled=!0,n.textContent="...";try{r==="promote"?await l.promoteModel(o):await l.rollbackModel(o),await f(e)}catch(x){alert(`Failed to ${r} model: ${x instanceof Error?x.message:"Unknown error"}`),n.disabled=!1,n.textContent=r==="promote"?"Promote":"Rollback"}}})}catch{const t=document.getElementById("models-tbody");t&&(t.innerHTML='<tr><td colspan="5" class="px-6 py-4 text-red-500">Failed to load models.</td></tr>')}}function H(e){const t=e.toLowerCase();return t==="production"||t==="prod"?"bg-emerald-100 text-emerald-700":t==="staging"||t==="stage"?"bg-amber-100 text-amber-700":t==="development"||t==="dev"?"bg-blue-100 text-blue-700":"bg-slate-100 text-slate-700"}function S(e){try{return new Date(e).toLocaleString()}catch{return e}}async function C(e){e.innerHTML=`
    <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
      <div class="flex items-center justify-between mb-6">
        <h3 class="text-lg font-semibold text-slate-800">Data Lineage</h3>
        <span class="text-xs text-slate-500">Live data from /api/lineage</span>
      </div>
      <div id="lineage-content" class="font-mono text-sm text-slate-700 space-y-2">
        <p class="text-slate-400">Loading...</p>
      </div>
    </div>
  `;try{const t=await l.getLineage(),s=document.getElementById("lineage-content");if(!s)return;if(t.length===0){s.innerHTML='<p class="text-slate-400">No lineage data found.</p>';return}s.innerHTML=t.map((a,n)=>`
      <div class="flex items-center gap-3 p-3 rounded-lg bg-slate-50 border border-slate-100 hover:bg-slate-100 transition">
        <span class="text-slate-500 font-bold w-6">${n+1}</span>
        <span class="font-medium text-slate-800 bg-white px-3 py-1 rounded border border-slate-200">${a.source}</span>
        <span class="text-slate-400">&rarr;</span>
        <span class="font-medium text-slate-800 bg-white px-3 py-1 rounded border border-slate-200">${a.target}</span>
        <span class="ml-auto text-xs px-2 py-1 rounded bg-slate-200 text-slate-600">${a.type}</span>
      </div>
    `).join("")}catch{const t=document.getElementById("lineage-content");t&&(t.innerHTML='<p class="text-red-500">Failed to load lineage data.</p>')}}async function D(e){e.innerHTML=`
    <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
      <div class="flex items-center justify-between mb-6">
        <h3 class="text-lg font-semibold text-slate-800">System Settings</h3>
        <span class="text-xs text-slate-500">Live data from /api/settings</span>
      </div>
      <div id="settings-content" class="space-y-4 max-w-2xl">
        <p class="text-slate-400">Loading...</p>
      </div>
    </div>
  `;try{const t=await l.getSettings(),s=document.getElementById("settings-content");if(!s)return;s.innerHTML=`
      <div class="grid grid-cols-1 gap-4">
        ${c("Version",t.version)}
        ${c("Environment",t.environment)}
        ${c("Database URL",j(t.database_url))}
        ${c("Log Level",t.log_level)}
      </div>
    `}catch{const t=document.getElementById("settings-content");t&&(t.innerHTML='<p class="text-red-500">Failed to load settings.</p>')}}function c(e,t){return`
    <div class="flex items-center justify-between p-4 bg-slate-50 rounded-lg border border-slate-100">
      <span class="text-sm font-medium text-slate-600">${e}</span>
      <span class="text-sm font-semibold text-slate-800 font-mono">${t}</span>
    </div>
  `}function j(e){try{const t=new URL(e);return`${t.protocol}//${t.hostname}:****${t.pathname}`}catch{return"***masked***"}}const u={"#dashboard":{title:"Dashboard",render:w},"#workflows":{title:"Workflows",render:v},"#executions":{title:"Executions",render:M},"#models":{title:"Models",render:f},"#lineage":{title:"Lineage",render:C},"#settings":{title:"Settings",render:D}};let i=null;async function m(){const e=window.location.hash||"#dashboard",t=u[e]||u["#dashboard"];i||(i=g()),i.updateTitle(t.title),i.updateSidebar(),await t.render(i.contentContainer)}window.addEventListener("hashchange",m);document.addEventListener("DOMContentLoaded",m);

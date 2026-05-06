import { initLayout } from './components/Layout';
import { renderDashboard } from './pages/Dashboard';
import { renderWorkflows } from './pages/Workflows';
import { renderExecutions } from './pages/Executions';
import { renderModels } from './pages/Models';
import { renderLineage } from './pages/Lineage';
import { renderSettings } from './pages/Settings';
import './styles/main.css';

const routes: Record<string, { title: string; render: (container: HTMLElement) => Promise<void> | void }> = {
  '#dashboard': { title: 'Dashboard', render: renderDashboard },
  '#workflows': { title: 'Workflows', render: renderWorkflows },
  '#executions': { title: 'Executions', render: renderExecutions },
  '#models': { title: 'Models', render: renderModels },
  '#lineage': { title: 'Lineage', render: renderLineage },
  '#settings': { title: 'Settings', render: renderSettings },
};

let layout: ReturnType<typeof initLayout> | null = null;

async function router() {
  const hash = window.location.hash || '#dashboard';
  const route = routes[hash] || routes['#dashboard'];

  if (!layout) {
    layout = initLayout();
  }

  layout.updateTitle(route.title);
  layout.updateSidebar();
  await route.render(layout.contentContainer);
}

window.addEventListener('hashchange', router);
document.addEventListener('DOMContentLoaded', router);

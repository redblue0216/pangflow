import { renderSidebar } from './Sidebar';
import { renderHeader } from './Header';

export function initLayout(): {
  contentContainer: HTMLElement;
  updateTitle: (title: string) => void;
  updateSidebar: () => void;
} {
  document.body.innerHTML = '';
  document.body.className = 'bg-slate-50 text-slate-800 font-sans antialiased';

  const app = document.createElement('div');
  app.className = 'flex min-h-screen';

  const sidebarContainer = document.createElement('div');
  renderSidebar(sidebarContainer);

  const mainWrap = document.createElement('div');
  mainWrap.className = 'flex-1 ml-64 flex flex-col min-h-screen';

  const headerContainer = document.createElement('div');
  const contentContainer = document.createElement('main');
  contentContainer.className = 'flex-1 p-8';

  mainWrap.appendChild(headerContainer);
  mainWrap.appendChild(contentContainer);

  app.appendChild(sidebarContainer);
  app.appendChild(mainWrap);
  document.body.appendChild(app);

  return {
    contentContainer,
    updateTitle: (title: string) => renderHeader(headerContainer, title),
    updateSidebar: () => renderSidebar(sidebarContainer),
  };
}

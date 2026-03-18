import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import './index.css'
import App from './app/App.tsx'
import { Providers } from './app/providers.tsx'

// Dynamic basename: proxy OR Domino
let basename = '/';
const path = window.location.pathname;

// 1. Proxy pattern: /34f34f43f3/proxy/8888 → everything before /proxy/
const proxyMatch = path.match(/^(.*?)\/proxy\/\d+$/);
if (proxyMatch) {
  basename = proxyMatch[1];
}
// 2. Domino pattern: /aice-studio/app/.../inventory-analyzer/
else if (path.includes('/inventory-analyzer')) {
  basename = path.substring(0, path.indexOf('/inventory-analyzer') + '/inventory-analyzer'.length);
}

console.log('Dynamic basename:', basename, 'Full path:', path);  // DEBUG

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <Providers>
      <BrowserRouter basename={basename}>
        <App />
      </BrowserRouter>
    </Providers>
  </StrictMode>
);

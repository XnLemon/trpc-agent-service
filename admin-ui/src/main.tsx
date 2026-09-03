import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import { ConfigProvider } from 'tdesign-react';
import zhCN from 'tdesign-react/es/locale/zh_CN';
import App from './App';
import './styles/tdesign.less';
import './styles/app.less';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ConfigProvider globalConfig={zhCN}>
      <BrowserRouter basename={window.location.pathname === '/admin' || window.location.pathname.startsWith('/admin/') ? '/admin' : undefined}>
        <App />
      </BrowserRouter>
    </ConfigProvider>
  </React.StrictMode>,
);

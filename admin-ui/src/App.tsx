import type { ReactElement } from 'react';
import { Navigate, Route, Routes, useLocation } from 'react-router-dom';
import { AppShell } from '@/components/AppShell';
import { ConnectionProvider, useConnection } from '@/lib/connection';
import { ConnectPage } from '@/pages/ConnectPage';
import { TenantsPage } from '@/pages/TenantsPage';
import { TenantLayout } from '@/pages/tenant/TenantLayout';
import { TenantOverview } from '@/pages/tenant/TenantOverview';
import { AppsPage } from '@/pages/tenant/AppsPage';
import { AppDetailPage } from '@/pages/tenant/AppDetailPage';
import { ModelsPage } from '@/pages/tenant/ModelsPage';
import { ModelDetailPage } from '@/pages/tenant/ModelDetailPage';
import { BackendsPage } from '@/pages/tenant/BackendsPage';
import { BackendDetailPage } from '@/pages/tenant/BackendDetailPage';
import { BindingsPage } from '@/pages/tenant/BindingsPage';
import { BindingDetailPage } from '@/pages/tenant/BindingDetailPage';

function RequireConnection({ children }: { children: ReactElement }) {
  const { status } = useConnection();
  const location = useLocation();
  if (status === 'loading') {
    return <div className="admin-load-state" />;
  }
  if (status !== 'authenticated') {
    return <Navigate to="/admin/login" state={{ from: location.pathname }} replace />;
  }
  return children;
}

export default function App() {
  return (
    <ConnectionProvider>
      <Routes>
        <Route path="/admin/login" element={<ConnectPage />} />
        <Route path="/connect" element={<Navigate to="/admin/login" replace />} />
        <Route
          path="/admin"
          element={
            <RequireConnection>
              <Navigate to="/tenants" replace />
            </RequireConnection>
          }
        />
        <Route
          element={
            <RequireConnection>
              <AppShell />
            </RequireConnection>
          }
        >
          <Route path="/" element={<Navigate to="/tenants" replace />} />
          <Route path="/tenants" element={<TenantsPage />} />
          <Route path="/tenants/:tenantId" element={<TenantLayout />}>
            <Route index element={<TenantOverview />} />
            <Route path="apps" element={<AppsPage />} />
            <Route path="apps/:appId" element={<AppDetailPage />} />
            <Route path="models" element={<ModelsPage />} />
            <Route path="models/:profileId" element={<ModelDetailPage />} />
            <Route path="backends" element={<BackendsPage />} />
            <Route path="backends/:profileId" element={<BackendDetailPage />} />
            <Route path="bindings" element={<BindingsPage />} />
            <Route path="bindings/:bindingId" element={<BindingDetailPage />} />
          </Route>
        </Route>
        <Route path="*" element={<Navigate to="/admin/login" replace />} />
      </Routes>
    </ConnectionProvider>
  );
}

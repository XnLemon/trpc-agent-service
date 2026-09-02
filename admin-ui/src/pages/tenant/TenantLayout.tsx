import { useCallback, useEffect, useState } from 'react';
import { Card, Descriptions, Space } from 'tdesign-react';
import { Outlet, useOutletContext, useParams } from 'react-router-dom';
import { LoadState } from '@/components/LoadState';
import { StatusTag } from '@/components/StatusTag';
import { useAdminClient } from '@/lib/connection';
import { categoryMessage } from '@/lib/mutation';
import { addRecent } from '@/lib/recents';
import { formatDateTime } from '@/lib/format';
import { ApiError } from '@/api/client';
import type { Tenant } from '@/api/types';

export interface TenantOutletContext {
  tenant: Tenant;
  refreshTenant: () => Promise<void>;
}

export function useTenantOutlet(): TenantOutletContext {
  return useOutletContext<TenantOutletContext>();
}

export function TenantLayout() {
  const { tenantId = '' } = useParams();
  const client = useAdminClient();
  const [tenant, setTenant] = useState<Tenant | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const { data } = await client.getTenant(tenantId);
      setTenant(data);
      addRecent('tenant', null, { id: data.TenantID, label: data.DisplayName });
    } catch (err) {
      setTenant(null);
      setError(
        err instanceof ApiError && err.category === 'not_found'
          ? '租户不存在，或当前凭证无权访问该租户（未授权查询统一返回 404，以避免暴露跨租户信息）。'
          : categoryMessage(err instanceof ApiError ? err.category : 'internal_error'),
      );
    } finally {
      setLoading(false);
    }
  }, [client, tenantId]);

  useEffect(() => {
    void load();
  }, [load]);

  return (
    <div className="admin-page" style={{ maxWidth: 'none' }}>
      <LoadState loading={loading} error={error} onRetry={load}>
        {tenant ? (
          <>
            <Card bordered>
              <Space align="center" size="large" breakLine style={{ width: '100%' }}>
                <div>
                  <Space align="center">
                    <h2 className="admin-page-title">{tenant.DisplayName}</h2>
                    <StatusTag status={tenant.Status} />
                  </Space>
                  <Descriptions
                    size="small"
                    colon
                    style={{ marginTop: 8 }}
                    items={[
                      { label: '租户 Key', content: tenant.TenantKey },
                      { label: '租户 ID', content: <span className="admin-mono">{tenant.TenantID}</span> },
                      { label: '配置版本', content: `v${tenant.Version}` },
                      { label: '最近更新', content: formatDateTime(tenant.UpdatedAt) },
                    ]}
                  />
                </div>
              </Space>
            </Card>
            <Outlet context={{ tenant, refreshTenant: load } satisfies TenantOutletContext} />
          </>
        ) : null}
      </LoadState>
    </div>
  );
}

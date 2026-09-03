import { useCallback, useState } from 'react';
import { Button, Card, Form, Input, MessagePlugin } from 'tdesign-react';
import { useNavigate } from 'react-router-dom';
import { ResourceListPanel } from '@/components/ResourceListPanel';
import { StatusTag } from '@/components/StatusTag';
import { useAdminClient } from '@/lib/connection';
import { formatDateTime } from '@/lib/format';
import { runMutation } from '@/lib/mutation';
import type { Tenant } from '@/api/types';

export function TenantsPage() {
  const client = useAdminClient();
  const navigate = useNavigate();
  const loadTenants = useCallback((params: Parameters<typeof client.listTenants>[0]) => client.listTenants(params), [client]);
  const [tenantKey, setTenantKey] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [creating, setCreating] = useState(false);

  const createTenant = async () => {
    if (!tenantKey.trim() || !displayName.trim() || creating) {
      return;
    }
    setCreating(true);
    try {
      const result = await runMutation(() =>
        client.createTenant({ tenant_key: tenantKey.trim(), display_name: displayName.trim() }),
      );
      if (result.ok) {
        MessagePlugin.success('租户已创建。请将新的租户 ID 加入服务端 TRPC_ADMIN_TENANTS scope 后再进行配置。');
        navigate(`/tenants/${encodeURIComponent(result.value.TenantID)}`);
      }
    } finally {
      setCreating(false);
    }
  };

  return <div className="admin-page">
    <div className="admin-page-heading"><div className="admin-page-eyebrow">控制面资源</div><h1 className="admin-page-title">租户管理</h1><div className="admin-page-subtitle">查看并管理当前账号获授权的租户，所有数据由服务端按权限范围返回。</div></div>
    <ResourceListPanel<Tenant>
      title="租户列表"
      loader={loadTenants}
      rowKey="TenantID"
      columns={[
        { colKey: 'DisplayName', title: '租户名称', cell: ({ row }) => <Button variant="text" theme="primary" onClick={() => navigate(`/tenants/${encodeURIComponent(row.TenantID)}`)}>{row.DisplayName}</Button> },
        { colKey: 'TenantKey', title: 'Tenant Key' },
        { colKey: 'Status', title: '状态', cell: ({ row }) => <StatusTag status={row.Status} /> },
        { colKey: 'TenantID', title: 'Tenant ID', ellipsis: true },
        { colKey: 'UpdatedAt', title: '更新时间', cell: ({ row }) => formatDateTime(row.UpdatedAt) },
      ]}
    />
    <Card className="admin-panel" title="创建租户" bordered><Form layout="vertical" labelAlign="top" colon><Form.FormItem label="租户 Key"><Input value={tenantKey} onChange={(v) => setTenantKey(String(v))} /></Form.FormItem><Form.FormItem label="展示名"><Input value={displayName} onChange={(v) => setDisplayName(String(v))} /></Form.FormItem><Button theme="primary" loading={creating} disabled={!tenantKey.trim() || !displayName.trim()} onClick={createTenant}>创建</Button></Form></Card>
  </div>;
}

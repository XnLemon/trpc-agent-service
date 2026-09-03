import { useCallback, useState } from 'react';
import { Button, Drawer, Form, Input, MessagePlugin } from 'tdesign-react';
import { AddIcon } from 'tdesign-icons-react';
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
  const [createOpen, setCreateOpen] = useState(false);

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
    <div className="admin-page-heading admin-page-heading-row">
      <div><div className="admin-page-eyebrow">控制面资源</div><h1 className="admin-page-title">租户管理</h1><div className="admin-page-subtitle">查看并管理当前账号获授权的租户，所有数据由服务端按权限范围返回。</div></div>
      <Button theme="primary" icon={<AddIcon />} onClick={() => setCreateOpen(true)}>创建租户</Button>
    </div>
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
    <Drawer className="admin-create-drawer" header="创建租户" visible={createOpen} placement="right" size="min(520px, 100vw)" footer={false} destroyOnClose onClose={() => setCreateOpen(false)}>
      <div className="admin-create-form">
        <div className="admin-page-subtitle admin-panel-description">创建后会进入租户详情页，可继续配置应用、模型、存储后端和渠道绑定。</div>
        <Form layout="vertical" labelAlign="top" colon>
          <Form.FormItem label="租户 Key" help="创建后不可修改，建议使用小写短横线格式。"><Input value={tenantKey} placeholder="例如 acme-prod" onChange={(v) => setTenantKey(String(v))} /></Form.FormItem>
          <Form.FormItem label="展示名"><Input value={displayName} placeholder="例如 Acme 生产环境" onChange={(v) => setDisplayName(String(v))} /></Form.FormItem>
          <Button theme="primary" loading={creating} disabled={!tenantKey.trim() || !displayName.trim()} onClick={createTenant}>创建租户</Button>
        </Form>
      </div>
    </Drawer>
  </div>;
}

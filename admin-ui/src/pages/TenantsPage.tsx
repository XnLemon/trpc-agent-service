import { useCallback, useEffect, useState } from 'react';
import { Button, Card, Form, Input, MessagePlugin } from 'tdesign-react';
import { useNavigate } from 'react-router-dom';
import { ResourceTable, StatusTag, type ResourceTableColumn } from '@/components/ResourceTable';
import { useAdminClient } from '@/lib/connection';
import { runMutation } from '@/lib/mutation';
import type { Tenant } from '@/api/types';

export function TenantsPage() {
  const client = useAdminClient();
  const navigate = useNavigate();
  const [items, setItems] = useState<Tenant[]>([]);
  const [nextCursor, setNextCursor] = useState('');
  const [loading, setLoading] = useState(false);
  const [query, setQuery] = useState('');
  const [status, setStatus] = useState('');
  const [tenantKey, setTenantKey] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [creating, setCreating] = useState(false);

  const load = useCallback(async (append = false) => {
    setLoading(true);
    try { const result = await client.listTenants({ q: query, status, cursor: append ? nextCursor : undefined, limit: 25 }); setItems((current) => append ? [...current, ...result.data.items] : result.data.items); setNextCursor(result.data.next_cursor ?? ''); } finally { setLoading(false); }
  }, [client, query, status, nextCursor]);
  useEffect(() => { void load(); }, []); // eslint-disable-line react-hooks/exhaustive-deps

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

  return (
    <div className="admin-page"><div className="admin-page-heading"><div className="admin-page-eyebrow">控制面资源</div><h1 className="admin-page-title">租户管理</h1><div className="admin-page-subtitle">查看并管理当前账号获授权的租户，所有数据由服务端按权限范围返回。</div></div><Card className="admin-panel" bordered><ResourceTable data={items} loading={loading} columns={[
      { colKey: 'DisplayName', title: '租户名称', cell: ({ row }: { row: Tenant }) => <Button variant="text" theme="primary" onClick={() => navigate(`/tenants/${encodeURIComponent(row.TenantID)}`)}>{row.DisplayName}</Button> },
      { colKey: 'TenantKey', title: 'Tenant Key' }, { colKey: 'Status', title: '状态', cell: ({ row }: { row: Tenant }) => <StatusTag value={row.Status} /> }, { colKey: 'TenantID', title: 'Tenant ID' }, { colKey: 'UpdatedAt', title: '更新时间' },
    ] as ResourceTableColumn<Tenant>[]} rowKey="TenantID" query={query} status={status} onQueryChange={setQuery} onStatusChange={setStatus} onSearch={() => void load()} onReset={() => { setQuery(''); setStatus(''); setNextCursor(''); void load(); }} onLoadMore={() => void load(true)} hasMore={Boolean(nextCursor)} /></Card><Card className="admin-panel" title="创建租户" bordered><Form layout="vertical" labelAlign="top" colon><Form.FormItem label="租户 Key"><Input value={tenantKey} onChange={(v) => setTenantKey(String(v))} /></Form.FormItem><Form.FormItem label="展示名"><Input value={displayName} onChange={(v) => setDisplayName(String(v))} /></Form.FormItem><Button theme="primary" loading={creating} disabled={!tenantKey.trim() || !displayName.trim()} onClick={createTenant}>创建</Button></Form></Card></div>
  );
}

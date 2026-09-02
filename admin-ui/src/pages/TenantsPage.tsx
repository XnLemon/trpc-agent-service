import { useState } from 'react';
import { Alert, Button, Form, Input, MessagePlugin } from 'tdesign-react';
import { useNavigate } from 'react-router-dom';
import { ResourceLobby } from '@/components/ResourceLobby';
import { useAdminClient } from '@/lib/connection';
import { runMutation } from '@/lib/mutation';
import { addRecent, listRecents, removeRecent } from '@/lib/recents';

export function TenantsPage() {
  const client = useAdminClient();
  const navigate = useNavigate();
  const [recents, setRecents] = useState(() => listRecents('tenant', null));
  const [tenantKey, setTenantKey] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [creating, setCreating] = useState(false);

  const openTenant = (id: string, label?: string) => {
    setRecents(addRecent('tenant', null, { id, label: label ?? id }));
    navigate(`/tenants/${encodeURIComponent(id)}`);
  };

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
        openTenant(result.value.TenantID, result.value.DisplayName);
      }
    } finally {
      setCreating(false);
    }
  };

  return (
    <ResourceLobby
      title="租户入口"
      subtitle="控制面暂无租户列表接口（P0 缺口），请通过已知租户 ID 打开，或在全局凭证下创建首个租户。"
      alert={
        <Alert
          theme="info"
          message="关于权限"
          description="TRPC_ADMIN_TENANTS=* 仅允许创建首个租户，并不代表已有租户的通读权限；创建后需在服务端配置明确的租户 ID scope。未授权的查询统一返回 404。"
        />
      }
      idLabel="租户 ID"
      idPlaceholder="例如 t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
      onOpen={(id) => openTenant(id)}
      createTitle="创建首个租户"
      createDescription="仅当当前凭证为 global principal 且系统中尚无租户时可用。"
      createForm={
        <Form layout="vertical" colon>
          <Form.FormItem label="租户 Key" help="创建后不可修改；小写字母、数字与中划线。">
            <Input value={tenantKey} onChange={(value) => setTenantKey(String(value))} placeholder="例如 acme" />
          </Form.FormItem>
          <Form.FormItem label="展示名">
            <Input value={displayName} onChange={(value) => setDisplayName(String(value))} placeholder="例如 Acme 有限公司" />
          </Form.FormItem>
          <Button theme="primary" loading={creating} disabled={!tenantKey.trim() || !displayName.trim()} onClick={createTenant}>
            创建租户
          </Button>
        </Form>
      }
      recents={recents}
      onOpenRecent={(id) => openTenant(id)}
      onRemoveRecent={(id) => setRecents(removeRecent('tenant', null, id))}
    />
  );
}

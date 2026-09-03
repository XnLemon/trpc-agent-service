import { useCallback, useState } from 'react';
import { Button, Form, Input, MessagePlugin } from 'tdesign-react';
import { useNavigate } from 'react-router-dom';
import type { BackendProfile } from '@/api/types';
import { AuditFields } from '@/components/AuditFields';
import { BindingsEditor, emptyCapabilityBinding, serializeBindings, type CapabilityBindingForm } from '@/components/BindingsEditor';
import { ResourceListPanel } from '@/components/ResourceListPanel';
import { ResourceLobby } from '@/components/ResourceLobby';
import { StatusTag } from '@/components/StatusTag';
import { useAdminClient } from '@/lib/connection';
import { formatDateTime, newCorrelationId } from '@/lib/format';
import { runMutation } from '@/lib/mutation';
import { addRecent, listRecents, removeRecent } from '@/lib/recents';
import { useTenantOutlet } from './TenantLayout';

export function BackendsPage() {
  const { tenant } = useTenantOutlet();
  const client = useAdminClient();
  const navigate = useNavigate();
  const loadBackends = useCallback((params: Parameters<typeof client.listBackends>[1]) => client.listBackends(tenant.TenantID, params), [client, tenant.TenantID]);
  const [recents, setRecents] = useState(() => listRecents('backend', tenant.TenantID));
  const [profileKey, setProfileKey] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [bindings, setBindings] = useState<CapabilityBindingForm[]>([emptyCapabilityBinding()]);
  const [reason, setReason] = useState('');
  const [correlationId, setCorrelationId] = useState(newCorrelationId);
  const [creating, setCreating] = useState(false);

  const openBackend = (id: string, label?: string) => {
    setRecents(addRecent('backend', tenant.TenantID, { id, label: label ?? id }));
    navigate(`/tenants/${encodeURIComponent(tenant.TenantID)}/backends/${encodeURIComponent(id)}`);
  };

  const createBackend = async () => {
    if (creating) {
      return;
    }
    setCreating(true);
    try {
      const result = await runMutation(() =>
        client.createBackend(tenant.TenantID, {
          profile_key: profileKey.trim(),
          display_name: displayName.trim(),
          schema_version: 1,
          bindings: serializeBindings(bindings),
          reason: reason.trim(),
          correlation_id: correlationId,
        }),
      );
      if (result.ok) {
        MessagePlugin.success('存储后端配置已创建。');
        openBackend(result.value.profile.ProfileID, result.value.profile.DisplayName);
      }
    } finally {
      setCreating(false);
    }
  };

  const canSubmit = profileKey.trim() && displayName.trim() && reason.trim() && serializeBindings(bindings).length > 0;

  return (
    <ResourceLobby
      title="存储后端"
      subtitle="浏览当前租户的存储后端 Profile，进入详情页查看能力绑定、编辑和状态。"
      idLabel="Profile ID"
      idPlaceholder="Backend Profile ID"
      onOpen={(id) => openBackend(id)}
      hideOpenPanel
      resourceList={
        <ResourceListPanel<BackendProfile>
          title="存储后端列表"
          loader={loadBackends}
          rowKey="ProfileID"
          columns={[
            {
              colKey: 'DisplayName',
              title: '配置名称',
              cell: ({ row }) => (
                <Button variant="text" theme="primary" onClick={() => openBackend(row.ProfileID, row.DisplayName)}>
                  {row.DisplayName}
                </Button>
              ),
            },
            { colKey: 'ProfileKey', title: 'Profile Key' },
            { colKey: 'Bindings', title: '能力绑定', cell: ({ row }) => `${row.Bindings?.length ?? 0} 项` },
            { colKey: 'Status', title: '状态', cell: ({ row }) => <StatusTag status={row.Status} /> },
            { colKey: 'Version', title: '版本', cell: ({ row }) => `v${row.Version}` },
            { colKey: 'UpdatedAt', title: '更新时间', cell: ({ row }) => formatDateTime(row.UpdatedAt) },
          ]}
          createLabel="创建存储后端"
        />
      }
      createTitle="创建存储后端配置"
      createDescription="绑定 session / memory / summary / knowledge / artifact / audit 能力的 Provider；active 状态至少需要一条 session 绑定。"
      createForm={
        <Form layout="vertical" labelAlign="top" colon>
          <Form.FormItem label="Profile Key" help="创建后不可修改，租户内唯一。">
            <Input value={profileKey} onChange={(v) => setProfileKey(String(v))} placeholder="例如 primary-store" />
          </Form.FormItem>
          <Form.FormItem label="展示名">
            <Input value={displayName} onChange={(v) => setDisplayName(String(v))} placeholder="例如 主力存储后端" />
          </Form.FormItem>
          <Form.FormItem label="能力绑定">
            <BindingsEditor value={bindings} onChange={setBindings} />
          </Form.FormItem>
          <AuditFields reason={reason} correlationId={correlationId} onReasonChange={setReason} />
          <Button theme="primary" loading={creating} disabled={!canSubmit} onClick={createBackend}>
            创建存储后端
          </Button>
        </Form>
      }
      recents={recents}
      onOpenRecent={(id) => openBackend(id)}
      onRemoveRecent={(id) => setRecents(removeRecent('backend', tenant.TenantID, id))}
    />
  );
}

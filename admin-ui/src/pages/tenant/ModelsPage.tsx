import { useCallback, useState } from 'react';
import { Button, Form, Input, MessagePlugin } from 'tdesign-react';
import { useNavigate } from 'react-router-dom';
import type { ModelProfile } from '@/api/types';
import { AuditFields } from '@/components/AuditFields';
import { ResourceListPanel } from '@/components/ResourceListPanel';
import { ResourceLobby } from '@/components/ResourceLobby';
import { StatusTag } from '@/components/StatusTag';
import { useAdminClient } from '@/lib/connection';
import { formatDateTime, newCorrelationId } from '@/lib/format';
import { runMutation } from '@/lib/mutation';
import { addRecent, listRecents, removeRecent } from '@/lib/recents';
import { useTenantOutlet } from './TenantLayout';

export function ModelsPage() {
  const { tenant } = useTenantOutlet();
  const client = useAdminClient();
  const navigate = useNavigate();
  const loadModels = useCallback((params: Parameters<typeof client.listModels>[1]) => client.listModels(tenant.TenantID, params), [client, tenant.TenantID]);
  const [recents, setRecents] = useState(() => listRecents('model', tenant.TenantID));
  const [profileKey, setProfileKey] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [provider, setProvider] = useState('');
  const [model, setModel] = useState('');
  const [endpoint, setEndpoint] = useState('');
  const [secretRef, setSecretRef] = useState('');
  const [reason, setReason] = useState('');
  const [correlationId] = useState(newCorrelationId);
  const [creating, setCreating] = useState(false);

  const openModel = (id: string, label?: string) => {
    setRecents(addRecent('model', tenant.TenantID, { id, label: label ?? id }));
    navigate(`/tenants/${encodeURIComponent(tenant.TenantID)}/models/${encodeURIComponent(id)}`);
  };

  const createModel = async () => {
    if (creating) return;
    setCreating(true);
    try {
      const configuration: Record<string, unknown> = { provider: provider.trim(), model: model.trim() };
      if (endpoint.trim()) configuration.endpoint = endpoint.trim();
      if (secretRef.trim()) configuration.secret_ref = secretRef.trim();
      const result = await runMutation(() => client.createModel(tenant.TenantID, {
        profile_key: profileKey.trim(), display_name: displayName.trim(), schema_version: 1,
        configuration, reason: reason.trim(), correlation_id: correlationId,
      }));
      if (result.ok) { MessagePlugin.success('模型配置已创建。'); openModel(result.value.profile.ProfileID, result.value.profile.DisplayName); }
    } finally { setCreating(false); }
  };

  return <ResourceLobby
    title="模型配置" subtitle="浏览当前租户的模型 Profile，进入详情页查看配置、编辑和迁移状态。"
    idLabel="Profile ID" idPlaceholder="Model Profile ID" onOpen={(id) => openModel(id)} hideOpenPanel
    resourceList={<ResourceListPanel<ModelProfile> title="模型配置列表" loader={loadModels} rowKey="ProfileID" columns={[
      { colKey: 'DisplayName', title: '配置名称', cell: ({ row }) => <Button variant="text" theme="primary" onClick={() => openModel(row.ProfileID, row.DisplayName)}>{row.DisplayName}</Button> },
      { colKey: 'ProfileKey', title: 'Profile Key' }, { colKey: 'Provider', title: 'Provider', cell: ({ row }) => row.Configuration?.provider || '-' },
      { colKey: 'Model', title: 'Model', cell: ({ row }) => row.Configuration?.model || '-' }, { colKey: 'Status', title: '状态', cell: ({ row }) => <StatusTag status={row.Status} /> },
      { colKey: 'Version', title: '版本', cell: ({ row }) => `v${row.Version}` }, { colKey: 'UpdatedAt', title: '更新时间', cell: ({ row }) => formatDateTime(row.UpdatedAt) },
    ]} createLabel="创建模型配置" />}
    createTitle="创建模型配置" createDescription="provider / model / options 受服务端 Catalog 限制；secret_ref 仅为不透明引用，禁止填写明文密钥。"
    createForm={<Form layout="vertical" labelAlign="top" colon><Form.FormItem label="Profile Key" help="创建后不可修改，租户内唯一。"><Input value={profileKey} onChange={(v) => setProfileKey(String(v))} placeholder="例如 gpt-main" /></Form.FormItem><Form.FormItem label="展示名"><Input value={displayName} onChange={(v) => setDisplayName(String(v))} placeholder="例如 主力 GPT 模型" /></Form.FormItem><Form.FormItem label="Provider"><Input value={provider} onChange={(v) => setProvider(String(v))} placeholder="例如 openai" /></Form.FormItem><Form.FormItem label="Model"><Input value={model} onChange={(v) => setModel(String(v))} placeholder="Catalog 允许的模型名" /></Form.FormItem><Form.FormItem label="Endpoint（可选）" help="是否允许由 Catalog 的 EndpointPolicy 决定。"><Input value={endpoint} onChange={(v) => setEndpoint(String(v))} placeholder="https://…" /></Form.FormItem><Form.FormItem label="Secret 引用（可选）" help="仅填写 Secret Manager 引用，不填写明文。"><Input value={secretRef} onChange={(v) => setSecretRef(String(v))} placeholder="例如 sm://tenant/model-key" /></Form.FormItem><AuditFields reason={reason} correlationId={correlationId} onReasonChange={setReason} /><Button theme="primary" loading={creating} disabled={!profileKey.trim() || !displayName.trim() || !provider.trim() || !model.trim() || !reason.trim()} onClick={createModel}>创建模型配置</Button></Form>}
    recents={recents} onOpenRecent={(id) => openModel(id)} onRemoveRecent={(id) => setRecents(removeRecent('model', tenant.TenantID, id))}
  />;
}

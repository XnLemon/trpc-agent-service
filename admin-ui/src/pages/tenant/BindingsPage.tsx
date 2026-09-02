import { useState } from 'react';
import { Button, Form, Input, MessagePlugin, Select } from 'tdesign-react';
import { useNavigate } from 'react-router-dom';
import { AuditFields } from '@/components/AuditFields';
import { emptyProtocolForm, ProtocolFields, serializeProtocol, type ProtocolFormState } from '@/components/ProtocolFields';
import { ResourceLobby } from '@/components/ResourceLobby';
import type { ChannelKind } from '@/api/types';
import { useAdminClient } from '@/lib/connection';
import { newCorrelationId } from '@/lib/format';
import { runMutation } from '@/lib/mutation';
import { addRecent, listRecents, removeRecent } from '@/lib/recents';
import { useTenantOutlet } from './TenantLayout';

export function BindingsPage() {
  const { tenant } = useTenantOutlet();
  const client = useAdminClient();
  const navigate = useNavigate();
  const [recents, setRecents] = useState(() => listRecents('binding', tenant.TenantID));
  const [bindingKey, setBindingKey] = useState('');
  const [channel, setChannel] = useState<ChannelKind>('wecom');
  const [appId, setAppId] = useState('');
  const [providerAccountId, setProviderAccountId] = useState('');
  const [routeKeyDigest, setRouteKeyDigest] = useState('');
  const [secretRef, setSecretRef] = useState('');
  const [protocol, setProtocol] = useState<ProtocolFormState>(emptyProtocolForm());
  const [reason, setReason] = useState('');
  const [correlationId, setCorrelationId] = useState(newCorrelationId);
  const [creating, setCreating] = useState(false);

  const openBinding = (id: string, label?: string) => {
    setRecents(addRecent('binding', tenant.TenantID, { id, label: label ?? id }));
    navigate(`/tenants/${encodeURIComponent(tenant.TenantID)}/bindings/${encodeURIComponent(id)}`);
  };

  const createBinding = async () => {
    if (creating) {
      return;
    }
    setCreating(true);
    try {
      const result = await runMutation(() =>
        client.createBinding(tenant.TenantID, {
          binding_key: bindingKey.trim(),
          channel,
          app_id: appId.trim(),
          provider_account_id: providerAccountId.trim(),
          public_route_key_digest: routeKeyDigest.trim(),
          secret_ref: secretRef.trim(),
          protocol: serializeProtocol(channel, protocol),
          reason: reason.trim(),
          correlation_id: correlationId,
        }),
      );
      if (result.ok) {
        MessagePlugin.success('渠道绑定已创建（draft 状态），验证配置后可激活。');
        openBinding(result.value.binding.BindingID, result.value.binding.BindingKey);
      }
    } finally {
      setCreating(false);
    }
  };

  const canSubmit = bindingKey.trim() && appId.trim() && routeKeyDigest.trim() && reason.trim();

  return (
    <ResourceLobby
      title="渠道绑定"
      subtitle="控制面暂无列表接口（P0 缺口），请通过已知 Binding ID 打开。"
      idLabel="Binding ID"
      idPlaceholder="Channel Binding ID"
      onOpen={(id) => openBinding(id)}
      createTitle="创建渠道绑定"
      createDescription="将企业微信 / Telegram 入站流量路由到指定 Agent App。服务端只保存路由摘要与 Secret 引用，不保存路由原文与渠道凭据。"
      createForm={
        <Form layout="vertical" colon>
          <Form.FormItem label="Binding Key" help="创建后不可修改，租户内唯一。">
            <Input value={bindingKey} onChange={(v) => setBindingKey(String(v))} placeholder="例如 wecom-support" />
          </Form.FormItem>
          <Form.FormItem label="渠道">
            <Select
              value={channel}
              options={[
                { label: '企业微信（wecom）', value: 'wecom' },
                { label: 'Telegram', value: 'telegram' },
              ]}
              onChange={(v) => {
                setChannel(v as ChannelKind);
                setProtocol(emptyProtocolForm());
              }}
            />
          </Form.FormItem>
          <Form.FormItem label="目标应用 ID" help="入站消息将路由到该 Agent App。">
            <Input value={appId} onChange={(v) => setAppId(String(v))} placeholder="Agent App ID" />
          </Form.FormItem>
          <Form.FormItem label="Provider 账号 ID">
            <Input value={providerAccountId} onChange={(v) => setProviderAccountId(String(v))} placeholder="渠道侧的应用/账号标识" />
          </Form.FormItem>
          <Form.FormItem label="公开路由 Key 摘要" help="提交摘要（digest），不要提交路由原文。">
            <Input value={routeKeyDigest} onChange={(v) => setRouteKeyDigest(String(v))} placeholder="public_route_key_digest" />
          </Form.FormItem>
          <Form.FormItem label="Secret 引用（可选）" help="渠道凭据的 Secret Manager 引用，禁止明文。">
            <Input value={secretRef} onChange={(v) => setSecretRef(String(v))} placeholder="例如 sm://tenant/wecom-token" />
          </Form.FormItem>
          <ProtocolFields channel={channel} value={protocol} onChange={setProtocol} />
          <AuditFields reason={reason} correlationId={correlationId} onReasonChange={setReason} />
          <Button theme="primary" loading={creating} disabled={!canSubmit} onClick={createBinding}>
            创建渠道绑定
          </Button>
        </Form>
      }
      recents={recents}
      onOpenRecent={(id) => openBinding(id)}
      onRemoveRecent={(id) => setRecents(removeRecent('binding', tenant.TenantID, id))}
    />
  );
}

import { useCallback, useEffect, useState } from 'react';
import { Button, Card, Descriptions, Form, Input, MessagePlugin, Space } from 'tdesign-react';
import { useParams } from 'react-router-dom';
import { AuditFields } from '@/components/AuditFields';
import { LoadState } from '@/components/LoadState';
import { ProtocolFields, serializeProtocol, type ProtocolFormState } from '@/components/ProtocolFields';
import { StatusActions } from '@/components/StatusActions';
import { StatusTag } from '@/components/StatusTag';
import { ApiError } from '@/api/client';
import type { ChannelBinding, LifecycleStatus } from '@/api/types';
import { useAdminClient } from '@/lib/connection';
import { categoryMessage, runMutation } from '@/lib/mutation';
import { addRecent } from '@/lib/recents';
import { formatDateTime, newCorrelationId } from '@/lib/format';
import { useTenantOutlet } from './TenantLayout';

interface BindingFormState {
  providerAccountId: string;
  routeKeyDigest: string;
  appId: string;
  secretRef: string;
  protocol: ProtocolFormState;
  reason: string;
}

export function BindingDetailPage() {
  const { tenant } = useTenantOutlet();
  const { bindingId = '' } = useParams();
  const client = useAdminClient();
  const [binding, setBinding] = useState<ChannelBinding | null>(null);
  const [form, setForm] = useState<BindingFormState | null>(null);
  const [correlationId, setCorrelationId] = useState(newCorrelationId);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const { data } = await client.getBinding(tenant.TenantID, bindingId);
      setBinding(data);
      setForm({
        providerAccountId: data.ProviderAccountID ?? '',
        routeKeyDigest: data.PublicRouteKeyDigest ?? '',
        appId: data.AppID ?? '',
        secretRef: data.SecretRef ?? '',
        protocol: {
          corpId: data.Protocol?.wecom?.corp_id ?? '',
          agentId: data.Protocol?.wecom?.agent_id ?? '',
          receiveId: data.Protocol?.wecom?.receive_id ?? '',
          apiBaseUrl: data.Protocol?.telegram?.api_base_url ?? '',
          webhookPath: data.Protocol?.telegram?.webhook_path ?? '',
        },
        reason: '',
      });
      setCorrelationId(newCorrelationId());
      addRecent('binding', tenant.TenantID, { id: data.BindingID, label: data.BindingKey });
    } catch (err) {
      setBinding(null);
      setError(
        err instanceof ApiError && err.category === 'not_found'
          ? '渠道绑定不存在，或当前凭证无权访问（未授权查询统一返回 404）。'
          : categoryMessage(err instanceof ApiError ? err.category : 'internal_error'),
      );
    } finally {
      setLoading(false);
    }
  }, [client, tenant.TenantID, bindingId]);

  useEffect(() => {
    void load();
  }, [load]);

  const save = async () => {
    if (!binding || !form || saving) {
      return;
    }
    setSaving(true);
    try {
      const result = await runMutation(
        () =>
          client.updateBinding(tenant.TenantID, binding.BindingID, {
            expected_version: binding.Version,
            provider_account_id: form.providerAccountId.trim(),
            public_route_key_digest: form.routeKeyDigest.trim(),
            app_id: form.appId.trim(),
            secret_ref: form.secretRef.trim(),
            protocol: serializeProtocol(binding.Channel, form.protocol),
            reason: form.reason.trim(),
            correlation_id: correlationId,
          }),
        { reload: load },
      );
      if (result.ok) {
        MessagePlugin.success('渠道绑定已保存。');
        await load();
      }
    } finally {
      setSaving(false);
    }
  };

  const transition = async (next: LifecycleStatus, meta: { reason: string; correlationId: string }) => {
    if (!binding) {
      return false;
    }
    const result = await runMutation(
      () =>
        client.transitionBindingStatus(tenant.TenantID, binding.BindingID, {
          expected_version: binding.Version,
          next_status: next,
          reason: meta.reason,
          correlation_id: meta.correlationId,
        }),
      { reload: load },
    );
    if (result.ok) {
      MessagePlugin.success('状态已迁移。');
      await load();
      return true;
    }
    return false;
  };

  return (
    <LoadState loading={loading} error={error} onRetry={load}>
      {binding && form ? (
        <div className="admin-page">
          <Card bordered>
            <Space align="center">
              <h2 className="admin-page-title">{binding.BindingKey}</h2>
              <StatusTag status={binding.Status} />
            </Space>
            <Descriptions
              size="small"
              colon
              className="admin-description-meta"
              items={[
                { label: '渠道', content: binding.Channel === 'wecom' ? '企业微信' : 'Telegram' },
                { label: 'Binding ID', content: <span className="admin-mono">{binding.BindingID}</span> },
                { label: '配置版本', content: `v${binding.Version}` },
                { label: '最近更新', content: formatDateTime(binding.UpdatedAt) },
              ]}
            />
          </Card>

          <Card
            title="配置（完整替换保存）"
            bordered
            actions={
              <Button
                theme="primary"
                size="small"
                loading={saving}
                disabled={binding.Status === 'disabled' || !form.appId.trim() || !form.routeKeyDigest.trim() || !form.reason.trim()}
                onClick={save}
              >
                保存全部修改
              </Button>
            }
          >
            <Form layout="vertical" labelAlign="top" colon className="admin-form-grid">
              <Form.FormItem label="目标应用 ID">
                <Input value={form.appId} onChange={(v) => setForm({ ...form, appId: String(v) })} />
              </Form.FormItem>
              <Form.FormItem label="Provider 账号 ID">
                <Input value={form.providerAccountId} onChange={(v) => setForm({ ...form, providerAccountId: String(v) })} />
              </Form.FormItem>
              <Form.FormItem label="公开路由 Key 摘要" help="仅摘要，不提交路由原文。">
                <Input value={form.routeKeyDigest} onChange={(v) => setForm({ ...form, routeKeyDigest: String(v) })} />
              </Form.FormItem>
              <Form.FormItem label="Secret 引用" help="仅引用，禁止明文渠道凭据。">
                <Input value={form.secretRef} onChange={(v) => setForm({ ...form, secretRef: String(v) })} />
              </Form.FormItem>
            </Form>
            <Form layout="vertical" labelAlign="top" colon>
              <ProtocolFields channel={binding.Channel} value={form.protocol} onChange={(protocol) => setForm({ ...form, protocol })} />
              <AuditFields reason={form.reason} correlationId={correlationId} onReasonChange={(reason) => setForm({ ...form, reason })} />
            </Form>
          </Card>

          <Card title="状态操作" bordered>
            <Space direction="vertical" size="small" className="admin-stack-full">
              <div className="admin-page-subtitle">
                draft → active / disabled；active ↔ suspended；disabled 为终态。激活前请确认协议配置与 Secret 引用已验证。
              </div>
              <StatusActions status={binding.Status} busy={saving} allowActivate onTransition={transition} />
            </Space>
          </Card>
        </div>
      ) : null}
    </LoadState>
  );
}

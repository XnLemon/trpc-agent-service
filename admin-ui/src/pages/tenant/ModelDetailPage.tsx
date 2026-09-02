import { useCallback, useEffect, useState } from 'react';
import { Button, Card, Descriptions, Form, Input, InputNumber, MessagePlugin, Space, Textarea } from 'tdesign-react';
import { useParams } from 'react-router-dom';
import { AuditFields } from '@/components/AuditFields';
import { KeyValueEditor } from '@/components/KeyValueEditor';
import { LoadState } from '@/components/LoadState';
import { StatusActions } from '@/components/StatusActions';
import { StatusTag } from '@/components/StatusTag';
import { ApiError } from '@/api/client';
import type { LifecycleStatus, ModelProfile } from '@/api/types';
import { useAdminClient } from '@/lib/connection';
import { categoryMessage, runMutation } from '@/lib/mutation';
import { addRecent } from '@/lib/recents';
import { formatDateTime, newCorrelationId } from '@/lib/format';
import { useTenantOutlet } from './TenantLayout';

interface ModelFormState {
  displayName: string;
  description: string;
  provider: string;
  model: string;
  endpoint: string;
  secretRef: string;
  options: Record<string, string>;
  temperature: number | null;
  topP: number | null;
  maxOutputTokens: number | null;
  reason: string;
}

function nullableNumber(value: unknown): number | null {
  if (value === undefined || value === null || value === '') {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function ModelDetailPage() {
  const { tenant } = useTenantOutlet();
  const { profileId = '' } = useParams();
  const client = useAdminClient();
  const [profile, setProfile] = useState<ModelProfile | null>(null);
  const [form, setForm] = useState<ModelFormState | null>(null);
  const [correlationId, setCorrelationId] = useState(newCorrelationId);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const { data } = await client.getModel(tenant.TenantID, profileId);
      setProfile(data);
      setForm({
        displayName: data.DisplayName,
        description: data.Description,
        provider: data.Configuration?.provider ?? '',
        model: data.Configuration?.model ?? '',
        endpoint: data.Configuration?.endpoint ?? '',
        secretRef: data.Configuration?.secret_ref ?? '',
        options: { ...(data.Configuration?.options ?? {}) },
        temperature: data.Configuration?.generation?.temperature ?? null,
        topP: data.Configuration?.generation?.top_p ?? null,
        maxOutputTokens: data.Configuration?.generation?.max_output_tokens ?? null,
        reason: '',
      });
      setCorrelationId(newCorrelationId());
      addRecent('model', tenant.TenantID, { id: data.ProfileID, label: data.DisplayName });
    } catch (err) {
      setProfile(null);
      setError(
        err instanceof ApiError && err.category === 'not_found'
          ? '模型配置不存在，或当前凭证无权访问（未授权查询统一返回 404）。'
          : categoryMessage(err instanceof ApiError ? err.category : 'internal_error'),
      );
    } finally {
      setLoading(false);
    }
  }, [client, tenant.TenantID, profileId]);

  useEffect(() => {
    void load();
  }, [load]);

  const patch = (partial: Partial<ModelFormState>) => setForm((prev) => (prev ? { ...prev, ...partial } : prev));

  const save = async () => {
    if (!profile || !form || saving) {
      return;
    }
    setSaving(true);
    try {
      const generation: Record<string, unknown> = {};
      if (form.temperature !== null) {
        generation.temperature = form.temperature;
      }
      if (form.topP !== null) {
        generation.top_p = form.topP;
      }
      if (form.maxOutputTokens !== null) {
        generation.max_output_tokens = form.maxOutputTokens;
      }
      const configuration: Record<string, unknown> = {
        provider: form.provider.trim(),
        model: form.model.trim(),
        endpoint: form.endpoint.trim(),
        secret_ref: form.secretRef.trim(),
      };
      if (Object.keys(form.options).length > 0) {
        configuration.options = form.options;
      }
      if (Object.keys(generation).length > 0) {
        configuration.generation = generation;
      }
      // PATCH 为完整替换：所有可变字段随最新版本号一起提交。
      const result = await runMutation(
        () =>
          client.updateModel(tenant.TenantID, profile.ProfileID, {
            expected_version: profile.Version,
            display_name: form.displayName.trim(),
            description: form.description,
            schema_version: profile.SchemaVersion,
            configuration,
            reason: form.reason.trim(),
            correlation_id: correlationId,
          }),
        { reload: load },
      );
      if (result.ok) {
        MessagePlugin.success('模型配置已保存。');
        await load();
      }
    } finally {
      setSaving(false);
    }
  };

  const transition = async (next: LifecycleStatus, meta: { reason: string; correlationId: string }) => {
    if (!profile) {
      return false;
    }
    const result = await runMutation(
      () =>
        client.transitionModelStatus(tenant.TenantID, profile.ProfileID, {
          expected_version: profile.Version,
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
      {profile && form ? (
        <div className="admin-page">
          <Card bordered>
            <Space align="center">
              <h2 className="admin-page-title">{profile.DisplayName}</h2>
              <StatusTag status={profile.Status} />
            </Space>
            <Descriptions
              size="small"
              colon
              style={{ marginTop: 8 }}
              items={[
                { label: 'Profile Key', content: profile.ProfileKey },
                { label: 'Profile ID', content: <span className="admin-mono">{profile.ProfileID}</span> },
                { label: '配置版本', content: `v${profile.Version}` },
                { label: '最近更新', content: formatDateTime(profile.UpdatedAt) },
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
                disabled={profile.Status === 'disabled' || !form.displayName.trim() || !form.provider.trim() || !form.model.trim() || !form.reason.trim()}
                onClick={save}
              >
                保存全部修改
              </Button>
            }
          >
            <Form layout="vertical" colon className="admin-form-grid">
              <Form.FormItem label="展示名">
                <Input value={form.displayName} onChange={(v) => patch({ displayName: String(v) })} />
              </Form.FormItem>
              <Form.FormItem label="Provider">
                <Input value={form.provider} onChange={(v) => patch({ provider: String(v) })} />
              </Form.FormItem>
              <Form.FormItem label="Model">
                <Input value={form.model} onChange={(v) => patch({ model: String(v) })} />
              </Form.FormItem>
              <Form.FormItem label="Endpoint">
                <Input value={form.endpoint} onChange={(v) => patch({ endpoint: String(v) })} placeholder="留空使用默认" />
              </Form.FormItem>
              <Form.FormItem label="Secret 引用" help="仅引用，不回显也不接受明文密钥。">
                <Input value={form.secretRef} onChange={(v) => patch({ secretRef: String(v) })} />
              </Form.FormItem>
              <Form.FormItem label="Temperature">
                <InputNumber
                  value={form.temperature ?? undefined}
                  min={0}
                  max={2}
                  step={0.1}
                  decimalPlaces={2}
                  placeholder="默认"
                  style={{ width: '100%' }}
                  onChange={(v) => patch({ temperature: nullableNumber(v) })}
                />
              </Form.FormItem>
              <Form.FormItem label="Top P">
                <InputNumber
                  value={form.topP ?? undefined}
                  min={0}
                  max={1}
                  step={0.05}
                  decimalPlaces={2}
                  placeholder="默认"
                  style={{ width: '100%' }}
                  onChange={(v) => patch({ topP: nullableNumber(v) })}
                />
              </Form.FormItem>
              <Form.FormItem label="最大输出 Tokens">
                <InputNumber
                  value={form.maxOutputTokens ?? undefined}
                  min={1}
                  placeholder="默认"
                  style={{ width: '100%' }}
                  onChange={(v) => patch({ maxOutputTokens: nullableNumber(v) })}
                />
              </Form.FormItem>
            </Form>
            <Form layout="vertical" colon>
              <Form.FormItem label="描述">
                <Textarea value={form.description} onChange={(v) => patch({ description: String(v) })} autosize={{ minRows: 2, maxRows: 4 }} />
              </Form.FormItem>
              <Form.FormItem label="Provider Options" help="键集合受服务端 Catalog 限制，键名保持原样提交。">
                <KeyValueEditor value={form.options} onChange={(options) => patch({ options })} />
              </Form.FormItem>
              <AuditFields reason={form.reason} correlationId={correlationId} onReasonChange={(reason) => patch({ reason })} />
            </Form>
          </Card>

          <Card title="状态操作" bordered>
            <Space direction="vertical" size="small" style={{ width: '100%' }}>
              <div className="admin-page-subtitle">active ↔ suspended；disabled 为终态。暂停后运行时将拒绝新执行。</div>
              <StatusActions status={profile.Status} busy={saving} onTransition={transition} />
            </Space>
          </Card>
        </div>
      ) : null}
    </LoadState>
  );
}

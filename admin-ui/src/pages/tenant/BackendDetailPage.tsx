import { useCallback, useEffect, useState } from 'react';
import { Button, Card, Descriptions, Form, Input, MessagePlugin, Space, Textarea } from 'tdesign-react';
import { useParams } from 'react-router-dom';
import { AuditFields } from '@/components/AuditFields';
import { BindingsEditor, bindingsReady, serializeBindings, type CapabilityBindingForm } from '@/components/BindingsEditor';
import { LoadState } from '@/components/LoadState';
import { StatusActions } from '@/components/StatusActions';
import { StatusTag } from '@/components/StatusTag';
import { ApiError } from '@/api/client';
import type { BackendProfile, LifecycleStatus } from '@/api/types';
import { useAdminClient } from '@/lib/connection';
import { categoryMessage, runMutation } from '@/lib/mutation';
import { addRecent } from '@/lib/recents';
import { formatDateTime, newCorrelationId } from '@/lib/format';
import { useTenantOutlet } from './TenantLayout';

interface BackendFormState {
  displayName: string;
  description: string;
  bindings: CapabilityBindingForm[];
  reason: string;
}

export function BackendDetailPage() {
  const { tenant } = useTenantOutlet();
  const { profileId = '' } = useParams();
  const client = useAdminClient();
  const [profile, setProfile] = useState<BackendProfile | null>(null);
  const [form, setForm] = useState<BackendFormState | null>(null);
  const [correlationId, setCorrelationId] = useState(newCorrelationId);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const { data } = await client.getBackend(tenant.TenantID, profileId);
      setProfile(data);
      setForm({
        displayName: data.DisplayName,
        description: data.Description,
        // 服务端 CapabilityBinding 无 json tag，响应为 PascalCase 键。
        bindings: (data.Bindings ?? []).map((binding) => ({
          capability: binding.Capability ?? '',
          provider: binding.Provider ?? '',
          endpoint: binding.Endpoint ?? '',
          secretRef: binding.SecretRef ?? '',
          options: { ...(binding.Options ?? {}) },
        })),
        reason: '',
      });
      setCorrelationId(newCorrelationId());
      addRecent('backend', tenant.TenantID, { id: data.ProfileID, label: data.DisplayName });
    } catch (err) {
      setProfile(null);
      setError(
        err instanceof ApiError && err.category === 'not_found'
          ? '存储后端配置不存在，或当前凭证无权访问（未授权查询统一返回 404）。'
          : categoryMessage(err instanceof ApiError ? err.category : 'internal_error'),
      );
    } finally {
      setLoading(false);
    }
  }, [client, tenant.TenantID, profileId]);

  useEffect(() => {
    void load();
  }, [load]);

  const save = async () => {
    if (!profile || !form || saving) {
      return;
    }
    setSaving(true);
    try {
      const result = await runMutation(
        () =>
          client.updateBackend(tenant.TenantID, profile.ProfileID, {
            expected_version: profile.Version,
            display_name: form.displayName.trim(),
            description: form.description,
            schema_version: profile.SchemaVersion,
            bindings: serializeBindings(form.bindings),
            reason: form.reason.trim(),
            correlation_id: correlationId,
          }),
        { reload: load },
      );
      if (result.ok) {
        MessagePlugin.success('存储后端配置已保存。');
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
        client.transitionBackendStatus(tenant.TenantID, profile.ProfileID, {
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

  const knowledgeBinding = profile?.Bindings?.find((binding) => binding.Capability === 'knowledge');
  const knowledgeOptions = knowledgeBinding?.Options ?? {};

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
              className="admin-description-meta"
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
                disabled={
                  profile.Status === 'disabled' ||
                  !form.displayName.trim() ||
                  !form.reason.trim() ||
                  !bindingsReady(form.bindings)
                }
                onClick={save}
              >
                保存全部修改
              </Button>
            }
          >
            <Form layout="vertical" labelAlign="top" colon>
              <Form.FormItem label="展示名">
                <Input value={form.displayName} onChange={(v) => setForm({ ...form, displayName: String(v) })} />
              </Form.FormItem>
              <Form.FormItem label="描述">
                <Textarea value={form.description} onChange={(v) => setForm({ ...form, description: String(v) })} autosize={{ minRows: 2, maxRows: 4 }} />
              </Form.FormItem>
              <Form.FormItem label="能力绑定">
                <BindingsEditor value={form.bindings} onChange={(bindings) => setForm({ ...form, bindings })} />
              </Form.FormItem>
              <AuditFields reason={form.reason} correlationId={correlationId} onReasonChange={(reason) => setForm({ ...form, reason })} />
            </Form>
          </Card>

          {knowledgeBinding?.Provider === 'pgvector' ? (
            <Card title="Knowledge / pgvector" bordered>
              <Descriptions
                size="small"
                colon
                items={[
                  { label: '索引状态', content: '异步 pending → ready；失败后按重试上限进入 dead-letter' },
                  { label: '命名空间', content: `${knowledgeOptions.schema ?? 'public'}.${knowledgeOptions.collection ?? 'knowledge'}` },
                  { label: 'Embedding', content: `${knowledgeOptions.embedding_model ?? 'deterministic'} / ${knowledgeOptions.embedding_version ?? 'v1'}` },
                  { label: '向量维度', content: knowledgeOptions.dimension ?? '32' },
                  { label: '索引队列', content: `${knowledgeOptions.queue_size ?? '128'} / ${knowledgeOptions.workers ?? '1'} worker` },
                  { label: '最大尝试', content: knowledgeOptions.max_attempts ?? '3' },
                ]}
              />
              <div className="admin-page-subtitle admin-space-top">
                检索只返回当前租户、ready 且未删除的数据，并在相似度排序前应用授权过滤。更换模型或维度需要新 Profile 版本并执行显式重建。
              </div>
            </Card>
          ) : null}

          <Card title="状态操作" bordered>
            <Space direction="vertical" size="small" className="admin-stack-full">
              <div className="admin-page-subtitle">active ↔ suspended；disabled 为终态。暂停后运行时将拒绝新执行。</div>
              <StatusActions status={profile.Status} busy={saving} onTransition={transition} />
            </Space>
          </Card>
        </div>
      ) : null}
    </LoadState>
  );
}

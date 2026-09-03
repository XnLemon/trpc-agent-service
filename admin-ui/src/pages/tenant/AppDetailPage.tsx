import { useCallback, useEffect, useState } from 'react';
import { Alert, Button, Card, Descriptions, Form, Input, InputNumber, MessagePlugin, Space, Textarea } from 'tdesign-react';
import { useParams } from 'react-router-dom';
import { DraftEditor, draftFormFromRevision, draftFormToConfiguration, emptyDraftForm, type DraftFormState } from '@/components/DraftEditor';
import { LoadState } from '@/components/LoadState';
import { ResourceListPanel } from '@/components/ResourceListPanel';
import { ReasonDialog, type ReasonSubmit } from '@/components/ReasonDialog';
import { StatusActions } from '@/components/StatusActions';
import { StatusTag } from '@/components/StatusTag';
import { ApiError } from '@/api/client';
import type { App, LifecycleStatus, Revision } from '@/api/types';
import { useAdminClient } from '@/lib/connection';
import { categoryMessage, runMutation } from '@/lib/mutation';
import { clearDraftSession, loadDraftSession, saveDraftSession } from '@/lib/draftSession';
import { addRecent } from '@/lib/recents';
import { formatDateTime } from '@/lib/format';
import { useTenantOutlet } from './TenantLayout';

export function AppDetailPage() {
  const { tenant } = useTenantOutlet();
  const { appId = '' } = useParams();
  const client = useAdminClient();
  const loadRevisions = useCallback(
    (params: Parameters<typeof client.listRevisions>[2]) => client.listRevisions(tenant.TenantID, appId, params),
    [client, tenant.TenantID, appId],
  );

  const [app, setApp] = useState<App | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [displayName, setDisplayName] = useState('');
  const [description, setDescription] = useState('');
  const [savingMeta, setSavingMeta] = useState(false);

  const [draft, setDraft] = useState<Revision | null>(null);
  const [draftForm, setDraftForm] = useState<DraftFormState | null>(null);
  const [draftBusy, setDraftBusy] = useState(false);
  const [rollbackTarget, setRollbackTarget] = useState<number | null>(null);
  const [rollbackOpen, setRollbackOpen] = useState(false);
  const [canaryTarget, setCanaryTarget] = useState<number | null>(null);
  const [canaryMode, setCanaryMode] = useState<'set' | 'clear' | null>(null);
  const [publishOpen, setPublishOpen] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const { data } = await client.getApp(tenant.TenantID, appId);
      setApp(data);
      setDisplayName(data.DisplayName);
      setDescription(data.Description);
      addRecent('app', tenant.TenantID, { id: data.AppID, label: data.DisplayName });
    } catch (err) {
      setApp(null);
      setError(
        err instanceof ApiError && err.category === 'not_found'
          ? '应用不存在，或当前凭证无权访问（未授权查询统一返回 404）。'
          : categoryMessage(err instanceof ApiError ? err.category : 'internal_error'),
      );
    } finally {
      setLoading(false);
    }
  }, [client, tenant.TenantID, appId]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    const stored = loadDraftSession(tenant.TenantID, appId);
    if (stored) {
      setDraft(stored);
      setDraftForm(draftFormFromRevision(stored));
    }
  }, [tenant.TenantID, appId]);

  const saveMetadata = async () => {
    if (!app || savingMeta) {
      return;
    }
    setSavingMeta(true);
    try {
      const result = await runMutation(
        () =>
          client.updateApp(tenant.TenantID, app.AppID, {
            expected_version: app.Version,
            display_name: displayName.trim(),
            description,
          }),
        { reload: load },
      );
      if (result.ok) {
        MessagePlugin.success('应用信息已保存。');
        await load();
      }
    } finally {
      setSavingMeta(false);
    }
  };

  const transition = async (next: LifecycleStatus, meta: ReasonSubmit) => {
    if (!app) {
      return false;
    }
    const result = await runMutation(
      () =>
        client.transitionAppStatus(tenant.TenantID, app.AppID, {
          expected_version: app.Version,
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

  const createDraft = async () => {
    if (!app || !draftForm || draftBusy) {
      return;
    }
    setDraftBusy(true);
    try {
      const result = await runMutation(
        () =>
          client.createDraft(tenant.TenantID, app.AppID, {
            expected_app_version: app.Version,
            kind: 'llm',
            schema_version: 1,
            configuration: draftFormToConfiguration(draftForm),
          }),
        { reload: load },
      );
      if (result.ok) {
        setDraft(result.value);
        setDraftForm(draftFormFromRevision(result.value));
        saveDraftSession(result.value);
        MessagePlugin.success(`草稿 r${result.value.Revision} 已创建。`);
        await load();
      }
    } finally {
      setDraftBusy(false);
    }
  };

  const saveDraft = async () => {
    if (!app || !draft || !draftForm || draftBusy) {
      return;
    }
    setDraftBusy(true);
    try {
      const result = await runMutation(
        () =>
          client.updateDraft(tenant.TenantID, app.AppID, draft.Revision, {
            expected_app_version: app.Version,
            expected_draft_version: draft.DraftVersion,
            configuration: draftFormToConfiguration(draftForm),
          }),
        { reload: load },
      );
      if (result.ok) {
        setDraft(result.value);
        setDraftForm(draftFormFromRevision(result.value));
        saveDraftSession(result.value);
        MessagePlugin.success('草稿已保存。');
        await load();
      }
    } finally {
      setDraftBusy(false);
    }
  };

  const publish = async (meta: ReasonSubmit) => {
    if (!app || !draft) {
      return false;
    }
    const result = await runMutation(
      () =>
        client.publishDraft(tenant.TenantID, app.AppID, draft.Revision, {
          expected_app_version: app.Version,
          expected_draft_version: draft.DraftVersion,
          reason: meta.reason,
          correlation_id: meta.correlationId,
        }),
      { reload: load },
    );
    if (result.ok) {
      MessagePlugin.success(`r${draft.Revision} 已发布为当前版本。`);
      clearDraftSession(tenant.TenantID, app.AppID);
      setDraft(null);
      setDraftForm(null);
      await load();
      return true;
    }
    return false;
  };

  const rollback = async (meta: ReasonSubmit) => {
    if (!app || rollbackTarget === null) {
      return false;
    }
    const result = await runMutation(
      () =>
        client.rollbackApp(tenant.TenantID, app.AppID, {
          target_revision: rollbackTarget,
          expected_app_version: app.Version,
          reason: meta.reason,
          correlation_id: meta.correlationId,
        }),
      { reload: load },
    );
    if (result.ok) {
      MessagePlugin.success(`已回滚到 r${rollbackTarget}。`);
      await load();
      return true;
    }
    return false;
  };

  const submitCanary = async (meta: ReasonSubmit) => {
    if (!app || canaryMode === null) {
      return false;
    }
    const result = await runMutation(
      () =>
        client.setCanary(tenant.TenantID, app.AppID, {
          expected_app_version: app.Version,
          candidate_revision: canaryMode === 'clear' ? null : canaryTarget,
          reason: meta.reason,
          correlation_id: meta.correlationId,
        }),
      { reload: load },
    );
    if (result.ok) {
      MessagePlugin.success(canaryMode === 'clear' ? 'Canary 已清除。' : `Canary 已设置为 r${canaryTarget}。`);
      await load();
      return true;
    }
    return false;
  };

  return (
    <LoadState loading={loading} error={error} onRetry={load}>
      {app ? (
        <div className="admin-page">
          <Card bordered>
            <Space align="center">
              <h2 className="admin-page-title">{app.DisplayName}</h2>
              <StatusTag status={app.Status} />
            </Space>
            <Descriptions
              size="small"
              colon
              className="admin-description-meta"
              items={[
                { label: '应用 Key', content: app.AppKey },
                { label: '应用 ID', content: <span className="admin-mono">{app.AppID}</span> },
                { label: '当前版本', content: app.CurrentRevision !== null ? `r${app.CurrentRevision}` : '未发布' },
                { label: 'Canary 版本', content: app.CanaryRevision !== null ? `r${app.CanaryRevision}` : '无' },
                { label: '配置版本', content: `v${app.Version}` },
                { label: '最近更新', content: formatDateTime(app.UpdatedAt) },
              ]}
            />
          </Card>

          <Card
            title="应用信息"
            bordered
            actions={
              <Button theme="primary" size="small" loading={savingMeta} disabled={app.Status === 'disabled' || !displayName.trim()} onClick={saveMetadata}>
                保存
              </Button>
            }
          >
            <Form layout="vertical" labelAlign="top" colon>
              <Form.FormItem label="展示名">
                <Input value={displayName} onChange={(value) => setDisplayName(String(value))} />
              </Form.FormItem>
              <Form.FormItem label="描述">
                <Textarea value={description} onChange={(value) => setDescription(String(value))} autosize={{ minRows: 2, maxRows: 5 }} />
              </Form.FormItem>
            </Form>
          </Card>

          <Card title="状态操作" bordered>
            <Space direction="vertical" size="small" className="admin-stack-full">
              <div className="admin-page-subtitle">
                首次发布版本会使 draft → active；之后 active ↔ suspended；disabled 为终态。
              </div>
              <StatusActions status={app.Status} onTransition={transition} />
            </Space>
          </Card>

          <Card
            title="版本草稿"
            bordered
            actions={
              draft ? (
                <Space>
                  <Button variant="outline" size="small" loading={draftBusy} disabled={app.Status === 'disabled'} onClick={saveDraft}>
                    保存草稿
                  </Button>
                  <Button theme="primary" size="small" disabled={app.Status === 'disabled'} onClick={() => setPublishOpen(true)}>
                    发布 r{draft.Revision}
                  </Button>
                </Space>
              ) : (
                <Button theme="primary" size="small" loading={draftBusy} disabled={app.Status === 'disabled'} onClick={createDraft}>
                  创建草稿
                </Button>
              )
            }
          >
            {draftForm ? (
              <Space direction="vertical" size="small" className="admin-stack-full">
                {draft ? (
                  <Alert
                    theme="info"
                    message={`正在编辑草稿 r${draft.Revision}（DraftVersion v${draft.DraftVersion}）`}
                    description="草稿快照保存在本次浏览器会话中；版本列表会显示服务端已保存的草稿和发布历史。若他处修改过该草稿，提交时会收到版本冲突提示。"
                  />
                ) : (
                  <Alert theme="info" message="新草稿尚未创建" description="填写配置后点击“创建草稿”；kind 固定为 llm，schema_version 固定为 1。" />
                )}
                <DraftEditor value={draftForm} onChange={setDraftForm} />
                {draft ? (
                  <Button
                    variant="text"
                    theme="danger"
                    size="small"
                    onClick={() => {
                      clearDraftSession(tenant.TenantID, app.AppID);
                      setDraft(null);
                      setDraftForm(null);
                    }}
                  >
                    放弃本地草稿视图（不会删除服务端草稿）
                  </Button>
                ) : (
                  <Button variant="text" size="small" onClick={() => setDraftForm(null)}>
                    取消
                  </Button>
                )}
              </Space>
            ) : (
              <div className="admin-page-subtitle">点击右上角“创建草稿”开始编辑下一个版本。</div>
            )}
          </Card>

          <ResourceListPanel<Revision>
            title="版本列表"
            loader={loadRevisions}
            rowKey="Revision"
            statusOptions={[{ label: '草稿', value: 'draft' }, { label: '已发布', value: 'published' }]}
            columns={[
              { colKey: 'Revision', title: 'Revision', cell: ({ row }) => `r${row.Revision}` },
              { colKey: 'State', title: '状态', cell: ({ row }) => <StatusTag status={row.State} /> },
              { colKey: 'DraftVersion', title: 'Draft Version', cell: ({ row }) => `v${row.DraftVersion}` },
              { colKey: 'Kind', title: '类型' },
              { colKey: 'ContentDigest', title: '内容摘要', ellipsis: true },
              { colKey: 'CreatedAt', title: '创建时间', cell: ({ row }) => formatDateTime(row.CreatedAt) },
              { colKey: 'PublishedAt', title: '发布时间', cell: ({ row }) => row.PublishedAt ? formatDateTime(row.PublishedAt) : '-' },
            ]}
          />

          <Card title="发布控制" bordered>
            <div className="admin-form-grid">
              <Form layout="vertical" labelAlign="top" colon>
                <Form.FormItem label="回滚到已发布版本" help="将当前版本指针指回一个已发布的 Revision，不复制内容。">
                  <InputNumber
                    value={rollbackTarget ?? undefined}
                    min={1}
                    placeholder="目标 Revision 号"
                    className="admin-full-width"
                    onChange={(value) => setRollbackTarget(value === undefined || value === null ? null : Number(value))}
                  />
                </Form.FormItem>
                <Button theme="warning" variant="outline" disabled={app.CurrentRevision === null || rollbackTarget === null} onClick={() => setRollbackOpen(true)}>
                  回滚
                </Button>
              </Form>
              <Form layout="vertical" labelAlign="top" colon>
                <Form.FormItem label="Canary 候选版本" help="仅允许选择已发布的 Revision；清除后流量不再分流。">
                  <InputNumber
                    value={canaryTarget ?? undefined}
                    min={1}
                    placeholder="候选 Revision 号"
                    className="admin-full-width"
                    onChange={(value) => setCanaryTarget(value === undefined || value === null ? null : Number(value))}
                  />
                </Form.FormItem>
                <Space>
                  <Button
                    theme="primary"
                    variant="outline"
                    disabled={app.Status !== 'active' || canaryTarget === null}
                    onClick={() => setCanaryMode('set')}
                  >
                    设置 Canary
                  </Button>
                  <Button variant="outline" disabled={app.CanaryRevision === null} onClick={() => setCanaryMode('clear')}>
                    清除 Canary
                  </Button>
                </Space>
              </Form>
            </div>
          </Card>

          <ReasonDialog
            visible={publishOpen}
            title={`发布草稿 r${draft?.Revision ?? ''}`}
            description={
              <div className="admin-page-subtitle">
                发布是原子操作：草稿变为不可变的已发布版本，并成为应用的当前版本，立即影响后续运行时流量。
              </div>
            }
            confirmTheme="primary"
            onCancel={() => setPublishOpen(false)}
            onConfirm={publish}
          />
          <ReasonDialog
            visible={rollbackOpen}
            title={`回滚到 r${rollbackTarget ?? ''}`}
            description={<div className="admin-page-subtitle">回滚会立即改变运行时使用的版本，请确认目标版本内容符合预期。</div>}
            confirmTheme="warning"
            onCancel={() => setRollbackOpen(false)}
            onConfirm={rollback}
          />
          <ReasonDialog
            visible={canaryMode !== null}
            title={canaryMode === 'clear' ? '清除 Canary' : `设置 Canary 为 r${canaryTarget ?? ''}`}
            description={
              <div className="admin-page-subtitle">
                {canaryMode === 'clear' ? '清除后所有流量回到当前版本。' : '设置后部分流量将使用候选版本，请确认候选版本已发布且经过验证。'}
              </div>
            }
            confirmTheme={canaryMode === 'clear' ? 'warning' : 'primary'}
            onCancel={() => setCanaryMode(null)}
            onConfirm={submitCanary}
          />
        </div>
      ) : null}
    </LoadState>
  );
}

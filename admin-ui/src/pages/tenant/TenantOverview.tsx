import { useEffect, useState } from 'react';
import { Button, Card, Form, Input, InputNumber, MessagePlugin, Select, Space } from 'tdesign-react';
import { StatusActions } from '@/components/StatusActions';
import { useAdminClient } from '@/lib/connection';
import { runMutation } from '@/lib/mutation';
import { useTenantOutlet } from './TenantLayout';
import type { LifecycleStatus, LogMaskingLevel } from '@/api/types';

interface TenantFormState {
  displayName: string;
  rateLimitRPM: number | null;
  maxConcurrentExecutions: number | null;
  monthlyTokenBudget: number | null;
  monthlySpendLimitMinor: number | null;
  billingCurrency: string;
  auditRetentionDays: number;
  logMaskingLevel: LogMaskingLevel;
  traceSamplingRate: number;
  defaultAgentAppID: string;
  defaultBackendProfileID: string;
}

function nullableNumber(value: unknown): number | null {
  if (value === undefined || value === null || value === '') {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function TenantOverview() {
  const { tenant, refreshTenant } = useTenantOutlet();
  const client = useAdminClient();
  const [form, setForm] = useState<TenantFormState | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    setForm({
      displayName: tenant.DisplayName,
      rateLimitRPM: tenant.RateLimitRPM,
      maxConcurrentExecutions: tenant.MaxConcurrentExecutions,
      monthlyTokenBudget: tenant.MonthlyTokenBudget,
      monthlySpendLimitMinor: tenant.MonthlySpendLimitMinor,
      billingCurrency: tenant.BillingCurrency,
      auditRetentionDays: tenant.AuditRetentionDays,
      logMaskingLevel: tenant.LogMaskingLevel,
      traceSamplingRate: tenant.TraceSamplingRate,
      defaultAgentAppID: tenant.DefaultAgentAppID ?? '',
      defaultBackendProfileID: tenant.DefaultBackendProfileID ?? '',
    });
  }, [tenant]);

  if (!form) {
    return null;
  }

  const patch = (partial: Partial<TenantFormState>) => setForm((prev) => (prev ? { ...prev, ...partial } : prev));

  const save = async () => {
    if (saving) {
      return;
    }
    setSaving(true);
    try {
      // PATCH is a complete replacement: submit every mutable field together
      // with the latest optimistic-lock version.
      const result = await runMutation(
        () =>
          client.updateTenant(tenant.TenantID, {
            expected_version: tenant.Version,
            display_name: form.displayName.trim(),
            rate_limit_rpm: form.rateLimitRPM,
            max_concurrent_executions: form.maxConcurrentExecutions,
            monthly_token_budget: form.monthlyTokenBudget,
            monthly_spend_limit_minor: form.monthlySpendLimitMinor,
            billing_currency: form.billingCurrency.trim(),
            audit_retention_days: form.auditRetentionDays,
            log_masking_level: form.logMaskingLevel,
            trace_sampling_rate: form.traceSamplingRate,
            default_agent_app_id: form.defaultAgentAppID.trim() || null,
            default_backend_profile_id: form.defaultBackendProfileID.trim() || null,
          }),
        { reload: refreshTenant },
      );
      if (result.ok) {
        MessagePlugin.success('租户配置已保存。');
        await refreshTenant();
      }
    } finally {
      setSaving(false);
    }
  };

  const transition = async (next: LifecycleStatus, meta: { reason: string; correlationId: string }) => {
    const result = await runMutation(
      () =>
        client.transitionTenantStatus(tenant.TenantID, {
          expected_version: tenant.Version,
          next_status: next,
          reason: meta.reason,
          correlation_id: meta.correlationId,
        }),
      { reload: refreshTenant },
    );
    if (result.ok) {
      MessagePlugin.success('状态已迁移。');
      await refreshTenant();
      return true;
    }
    return false;
  };

  return (
    <>
      <Card
        title="租户设置"
        bordered
        actions={
          <Space>
            <Button variant="outline" disabled={saving} onClick={() => void refreshTenant()}>
              重置
            </Button>
            <Button theme="primary" loading={saving} disabled={tenant.Status === 'disabled' || !form.displayName.trim()} onClick={save}>
              保存全部修改
            </Button>
          </Space>
        }
      >
        <Form layout="vertical" colon className="admin-form-grid">
          <Form.FormItem label="展示名">
            <Input value={form.displayName} onChange={(value) => patch({ displayName: String(value) })} />
          </Form.FormItem>
          <Form.FormItem label="限流 RPM" help="留空表示不限制。">
            <InputNumber
              value={form.rateLimitRPM ?? undefined}
              min={0}
              placeholder="不限制"
              style={{ width: '100%' }}
              onChange={(value) => patch({ rateLimitRPM: nullableNumber(value) })}
            />
          </Form.FormItem>
          <Form.FormItem label="最大并发执行数" help="留空表示不限制。">
            <InputNumber
              value={form.maxConcurrentExecutions ?? undefined}
              min={0}
              placeholder="不限制"
              style={{ width: '100%' }}
              onChange={(value) => patch({ maxConcurrentExecutions: nullableNumber(value) })}
            />
          </Form.FormItem>
          <Form.FormItem label="月度 Token 预算" help="留空表示不限制。">
            <InputNumber
              value={form.monthlyTokenBudget ?? undefined}
              min={0}
              placeholder="不限制"
              style={{ width: '100%' }}
              onChange={(value) => patch({ monthlyTokenBudget: nullableNumber(value) })}
            />
          </Form.FormItem>
          <Form.FormItem label="月度金额上限（最小货币单位）" help="留空表示不限制。">
            <InputNumber
              value={form.monthlySpendLimitMinor ?? undefined}
              min={0}
              placeholder="不限制"
              style={{ width: '100%' }}
              onChange={(value) => patch({ monthlySpendLimitMinor: nullableNumber(value) })}
            />
          </Form.FormItem>
          <Form.FormItem label="账单币种">
            <Input
              value={form.billingCurrency}
              placeholder="例如 CNY / USD"
              onChange={(value) => patch({ billingCurrency: String(value) })}
            />
          </Form.FormItem>
          <Form.FormItem label="审计保留天数">
            <InputNumber
              value={form.auditRetentionDays}
              min={1}
              style={{ width: '100%' }}
              onChange={(value) => patch({ auditRetentionDays: nullableNumber(value) ?? 90 })}
            />
          </Form.FormItem>
          <Form.FormItem label="日志脱敏级别">
            <Select
              value={form.logMaskingLevel}
              options={[
                { label: 'none（不脱敏）', value: 'none' },
                { label: 'basic（标准脱敏）', value: 'basic' },
                { label: 'strict（最强脱敏）', value: 'strict' },
              ]}
              onChange={(value) => patch({ logMaskingLevel: value as LogMaskingLevel })}
            />
          </Form.FormItem>
          <Form.FormItem label="Trace 采样率（0 - 1）">
            <InputNumber
              value={form.traceSamplingRate}
              min={0}
              max={1}
              step={0.05}
              decimalPlaces={2}
              style={{ width: '100%' }}
              onChange={(value) => patch({ traceSamplingRate: nullableNumber(value) ?? 0 })}
            />
          </Form.FormItem>
          <Form.FormItem label="默认应用 ID" help="留空表示未设置。">
            <Input
              value={form.defaultAgentAppID}
              placeholder="Agent App ID"
              onChange={(value) => patch({ defaultAgentAppID: String(value) })}
            />
          </Form.FormItem>
          <Form.FormItem label="默认存储后端 ID" help="留空表示未设置。">
            <Input
              value={form.defaultBackendProfileID}
              placeholder="Backend Profile ID"
              onChange={(value) => patch({ defaultBackendProfileID: String(value) })}
            />
          </Form.FormItem>
        </Form>
      </Card>
      <Card title="状态操作" bordered>
        <Space direction="vertical" size="small" style={{ width: '100%' }}>
          <div className="admin-page-subtitle">
            合法迁移：active → suspended / disabled；suspended → active / disabled；disabled 为终态。所有状态操作都会写入审计。
          </div>
          <StatusActions status={tenant.Status} busy={saving} onTransition={transition} />
        </Space>
      </Card>
    </>
  );
}

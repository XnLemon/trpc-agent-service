import { Button, Card, Form, Input, InputNumber, Select, Space } from 'tdesign-react';
import { AddIcon, DeleteIcon } from 'tdesign-icons-react';
import { KeyValueEditor } from './KeyValueEditor';

export interface CapabilityBindingForm {
  capability: string;
  provider: string;
  endpoint: string;
  secretRef: string;
  options: Record<string, string>;
}

export const CAPABILITY_OPTIONS = ['session', 'memory', 'summary', 'knowledge', 'artifact', 'audit'].map((value) => ({
  label: value,
  value,
}));

const PGVECTOR_DEFAULTS = {
  schema: 'public',
  collection: 'knowledge',
  embedding_model: 'deterministic',
  embedding_version: 'v1',
  dimension: '32',
  queue_size: '128',
  workers: '1',
  max_attempts: '3',
} as const;

const PGVECTOR_OPTION_LABELS: Record<string, string> = {
  schema: 'PostgreSQL Schema',
  collection: 'Collection',
  embedding_model: 'Embedding Model',
  embedding_version: 'Embedding Version',
  dimension: '向量维度',
  queue_size: '索引队列容量',
  workers: '索引 Worker 数',
  max_attempts: '最大重试次数',
};

function isPGVectorKnowledge(binding: CapabilityBindingForm) {
  return binding.capability === 'knowledge' && binding.provider.trim().toLowerCase() === 'pgvector';
}

function isPGVectorProvider(provider: string) {
  return provider.trim().toLowerCase() === 'pgvector';
}

function withoutPGVectorOptions(options: Record<string, string>) {
  return Object.fromEntries(Object.entries(options).filter(([key]) => !(key in PGVECTOR_DEFAULTS)));
}

function validPGVectorIdentifier(value: string) {
  return /^[A-Za-z_][A-Za-z0-9_]{0,62}$/.test(value);
}

export function emptyCapabilityBinding(): CapabilityBindingForm {
  return { capability: 'session', provider: '', endpoint: '', secretRef: '', options: {} };
}

/** 序列化为 snake_case 请求项；服务端 normalizeKeys 会映射为 PascalCase 字段。 */
export function serializeBindings(bindings: CapabilityBindingForm[]): Record<string, unknown>[] {
  return bindings
    .filter((binding) => binding.capability && binding.provider.trim())
    .map((binding) => {
      const item: Record<string, unknown> = {
        capability: binding.capability,
        provider: binding.provider.trim(),
      };
      if (binding.endpoint.trim()) {
        item.endpoint = binding.endpoint.trim();
      }
      if (binding.secretRef.trim()) {
        item.secret_ref = binding.secretRef.trim();
      }
      const bindingOptions = binding.options ?? {};
      const options = isPGVectorKnowledge(binding)
        ? Object.fromEntries(Object.keys(PGVECTOR_DEFAULTS).flatMap((key) => bindingOptions[key] === undefined ? [] : [[key, bindingOptions[key]]]))
        : bindingOptions;
      if (Object.keys(options).length > 0) {
        item.options = options;
      }
      return item;
    });
}

/** Returns false when a configured pgvector binding cannot pass Catalog validation. */
export function bindingsReady(bindings: CapabilityBindingForm[]) {
  const configured = (bindings ?? []).filter((binding) => binding.capability && binding.provider.trim());
  if (configured.length === 0) return false;
  return configured.every((binding) => {
    if (!isPGVectorKnowledge(binding)) return true;
    const options = binding.options ?? {};
    const dimension = Number(options.dimension ?? PGVECTOR_DEFAULTS.dimension);
    const queueSize = Number(options.queue_size ?? PGVECTOR_DEFAULTS.queue_size);
    const workers = Number(options.workers ?? PGVECTOR_DEFAULTS.workers);
    const maxAttempts = Number(options.max_attempts ?? PGVECTOR_DEFAULTS.max_attempts);
    return binding.endpoint.trim() !== '' &&
      validPGVectorIdentifier(options.schema ?? PGVECTOR_DEFAULTS.schema) &&
      validPGVectorIdentifier(options.collection ?? PGVECTOR_DEFAULTS.collection) &&
      Number.isInteger(dimension) && dimension >= 1 && dimension <= 2000 &&
      Number.isInteger(queueSize) && queueSize >= 1 && queueSize <= 10000 &&
      Number.isInteger(workers) && workers >= 1 && workers <= 32 &&
      Number.isInteger(maxAttempts) && maxAttempts >= 1 && maxAttempts <= 100;
  });
}

interface BindingsEditorProps {
  value: CapabilityBindingForm[];
  onChange: (value: CapabilityBindingForm[]) => void;
}

/** Editor for Backend Profile capability bindings. */
export function BindingsEditor({ value, onChange }: BindingsEditorProps) {
  // TDesign FormItem injects `value` (often undefined) into custom-component
  // children unconditionally; treat the prop defensively.
  const list = value ?? [];
  const patchAt = (index: number, partial: Partial<CapabilityBindingForm>) => {
    onChange(list.map((item, i) => (i === index ? { ...item, ...partial } : item)));
  };
  const patchOptionAt = (index: number, key: string, value: string) => {
    const binding = list[index];
    if (!binding) return;
    patchAt(index, { options: { ...(binding.options ?? {}), [key]: value } });
  };
  const patchProviderAt = (index: number, provider: string) => {
    const binding = list[index];
    if (!binding) return;
    const enteringPGVector = binding.capability === 'knowledge' && isPGVectorProvider(provider);
    const leavingPGVector = isPGVectorKnowledge(binding) && !isPGVectorProvider(provider);
    patchAt(index, {
      provider,
      secretRef: enteringPGVector || leavingPGVector ? '' : binding.secretRef,
      options: enteringPGVector ? binding.options : leavingPGVector ? withoutPGVectorOptions(binding.options ?? {}) : binding.options,
    });
  };
  const patchCapabilityAt = (index: number, capability: string) => {
    const binding = list[index];
    if (!binding) return;
    const staysPGVector = capability === 'knowledge' && isPGVectorProvider(binding.provider);
    const leavingPGVector = isPGVectorKnowledge(binding) && !staysPGVector;
    patchAt(index, {
      capability,
      secretRef: staysPGVector || leavingPGVector ? '' : binding.secretRef,
      options: leavingPGVector ? withoutPGVectorOptions(binding.options ?? {}) : binding.options,
    });
  };

  return (
    <Space direction="vertical" size="small" className="admin-stack-full">
      {list.map((binding, index) => (
        <Card
          key={index}
          size="small"
          bordered
          title={`绑定 #${index + 1}`}
          actions={
            <Button size="small" variant="text" theme="danger" icon={<DeleteIcon />} onClick={() => onChange(list.filter((_, i) => i !== index))}>
              移除
            </Button>
          }
        >
          <Form layout="vertical" labelAlign="top" colon className="admin-form-grid">
            <Form.FormItem label="能力（Capability）">
              <Select value={binding.capability} options={CAPABILITY_OPTIONS} onChange={(v) => patchCapabilityAt(index, String(v))} />
            </Form.FormItem>
            <Form.FormItem label="Provider" help="由部署的 Backend Catalog 决定。">
              <Input value={binding.provider} onChange={(v) => patchProviderAt(index, String(v))} placeholder="例如 pgvector / postgres / inmemory" />
            </Form.FormItem>
            <Form.FormItem label={isPGVectorKnowledge(binding) ? 'Endpoint（必填）' : 'Endpoint（可选）'} help={isPGVectorKnowledge(binding) ? '必须与服务端已配置的 PostgreSQL 数据库完全匹配。' : undefined}>
              <Input
                value={binding.endpoint}
                onChange={(v) => patchAt(index, { endpoint: String(v) })}
                placeholder={isPGVectorKnowledge(binding) ? 'postgresql://db.example:5432/database' : undefined}
              />
            </Form.FormItem>
            <Form.FormItem label={isPGVectorKnowledge(binding) ? 'Secret 引用（禁止）' : 'Secret 引用（可选）'} help={isPGVectorKnowledge(binding) ? 'pgvector 使用部署侧数据库连接，不接受租户 Secret 引用。' : '仅引用，禁止明文。'}>
              <Input value={binding.secretRef} disabled={isPGVectorKnowledge(binding)} onChange={(v) => patchAt(index, { secretRef: String(v) })} />
            </Form.FormItem>
          </Form>
          {isPGVectorKnowledge(binding) ? (
            <div className="admin-space-top">
              <div className="admin-page-subtitle">
                pgvector 使用已注册的 PostgreSQL 连接。租户不能填写 DSN 用户名、密码或查询参数；凭据由部署连接管理。
              </div>
              <Form layout="vertical" labelAlign="top" colon className="admin-form-grid admin-space-top">
                <Form.FormItem label={PGVECTOR_OPTION_LABELS.schema} help="仅允许字母、数字和下划线，且不能以数字开头。">
                  <Input value={binding.options.schema ?? PGVECTOR_DEFAULTS.schema} onChange={(v) => patchOptionAt(index, 'schema', String(v))} />
                </Form.FormItem>
                <Form.FormItem label={PGVECTOR_OPTION_LABELS.collection} help="仅允许字母、数字和下划线，且不能以数字开头。">
                  <Input value={binding.options.collection ?? PGVECTOR_DEFAULTS.collection} onChange={(v) => patchOptionAt(index, 'collection', String(v))} />
                </Form.FormItem>
                <Form.FormItem label={PGVECTOR_OPTION_LABELS.embedding_model}>
                  <Input value={binding.options.embedding_model ?? PGVECTOR_DEFAULTS.embedding_model} onChange={(v) => patchOptionAt(index, 'embedding_model', String(v))} />
                </Form.FormItem>
                <Form.FormItem label={PGVECTOR_OPTION_LABELS.embedding_version}>
                  <Input value={binding.options.embedding_version ?? PGVECTOR_DEFAULTS.embedding_version} onChange={(v) => patchOptionAt(index, 'embedding_version', String(v))} />
                </Form.FormItem>
                <Form.FormItem label={PGVECTOR_OPTION_LABELS.dimension} help="范围 1–2000；更换维度需要新 Profile 版本并重建索引。">
                  <InputNumber className="admin-full-width" value={Number(binding.options.dimension ?? PGVECTOR_DEFAULTS.dimension)} min={1} max={2000} step={1} onChange={(v) => patchOptionAt(index, 'dimension', String(v ?? PGVECTOR_DEFAULTS.dimension))} />
                </Form.FormItem>
                <Form.FormItem label={PGVECTOR_OPTION_LABELS.queue_size} help="范围 1–10000，队列满时写入仍会保持 pending。">
                  <InputNumber className="admin-full-width" value={Number(binding.options.queue_size ?? PGVECTOR_DEFAULTS.queue_size)} min={1} max={10000} step={1} onChange={(v) => patchOptionAt(index, 'queue_size', String(v ?? PGVECTOR_DEFAULTS.queue_size))} />
                </Form.FormItem>
                <Form.FormItem label={PGVECTOR_OPTION_LABELS.workers} help="范围 1–32。">
                  <InputNumber className="admin-full-width" value={Number(binding.options.workers ?? PGVECTOR_DEFAULTS.workers)} min={1} max={32} step={1} onChange={(v) => patchOptionAt(index, 'workers', String(v ?? PGVECTOR_DEFAULTS.workers))} />
                </Form.FormItem>
                <Form.FormItem label={PGVECTOR_OPTION_LABELS.max_attempts} help="范围 1–100，超过后进入 dead-letter。">
                  <InputNumber className="admin-full-width" value={Number(binding.options.max_attempts ?? PGVECTOR_DEFAULTS.max_attempts)} min={1} max={100} step={1} onChange={(v) => patchOptionAt(index, 'max_attempts', String(v ?? PGVECTOR_DEFAULTS.max_attempts))} />
                </Form.FormItem>
              </Form>
            </div>
          ) : (
            <Form layout="vertical" labelAlign="top" colon className="admin-space-top">
              <Form.FormItem label="Options" help="键集合受 Catalog 限制，键名保持原样提交。">
                <KeyValueEditor value={binding.options} onChange={(options) => patchAt(index, { options })} />
              </Form.FormItem>
            </Form>
          )}
        </Card>
      ))}
      <Button variant="dashed" icon={<AddIcon />} onClick={() => onChange([...list, emptyCapabilityBinding()])}>
        添加能力绑定
      </Button>
      <div className="admin-page-subtitle">提示：active 状态的 Backend Profile 至少需要一条 session 绑定。</div>
    </Space>
  );
}

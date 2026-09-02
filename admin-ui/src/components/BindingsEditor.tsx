import { Button, Card, Form, Input, Select, Space } from 'tdesign-react';
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
      if (Object.keys(binding.options).length > 0) {
        item.options = binding.options;
      }
      return item;
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

  return (
    <Space direction="vertical" size="small" style={{ width: '100%' }}>
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
          <Form layout="vertical" colon className="admin-form-grid">
            <Form.FormItem label="能力（Capability）">
              <Select value={binding.capability} options={CAPABILITY_OPTIONS} onChange={(v) => patchAt(index, { capability: String(v) })} />
            </Form.FormItem>
            <Form.FormItem label="Provider" help="由部署的 Backend Catalog 决定。">
              <Input value={binding.provider} onChange={(v) => patchAt(index, { provider: String(v) })} placeholder="例如 postgres / redis / inmemory" />
            </Form.FormItem>
            <Form.FormItem label="Endpoint（可选）">
              <Input value={binding.endpoint} onChange={(v) => patchAt(index, { endpoint: String(v) })} />
            </Form.FormItem>
            <Form.FormItem label="Secret 引用（可选）" help="仅引用，禁止明文。">
              <Input value={binding.secretRef} onChange={(v) => patchAt(index, { secretRef: String(v) })} />
            </Form.FormItem>
          </Form>
          <Form layout="vertical" colon style={{ marginTop: 8 }}>
            <Form.FormItem label="Options" help="键集合受 Catalog 限制，键名保持原样提交。">
              <KeyValueEditor value={binding.options} onChange={(options) => patchAt(index, { options })} />
            </Form.FormItem>
          </Form>
        </Card>
      ))}
      <Button variant="dashed" icon={<AddIcon />} onClick={() => onChange([...list, emptyCapabilityBinding()])}>
        添加能力绑定
      </Button>
      <div className="admin-page-subtitle">提示：active 状态的 Backend Profile 至少需要一条 session 绑定。</div>
    </Space>
  );
}

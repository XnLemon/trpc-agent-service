import { Button, Form, Input, InputNumber, Switch, Textarea } from 'tdesign-react';
import { AddIcon, DeleteIcon } from 'tdesign-icons-react';
import type { Revision, ToolAuthorization } from '@/api/types';

export interface DraftFormState {
  description: string;
  instruction: string;
  globalInstruction: string;
  modelProfileId: string;
  temperature: number | null;
  topP: number | null;
  maxOutputTokens: number | null;
  maxLlmCalls: number;
  maxToolCalls: number;
  enableParallelTools: boolean;
  maxParallelTools: number;
  executionTimeoutSeconds: number;
  tools: ToolAuthorization[];
}

export const DEFAULT_RUNTIME = {
  maxLlmCalls: 16,
  maxToolCalls: 64,
  enableParallelTools: false,
  maxParallelTools: 1,
  executionTimeoutSeconds: 300,
};

export function emptyDraftForm(): DraftFormState {
  return {
    description: '',
    instruction: '',
    globalInstruction: '',
    modelProfileId: '',
    temperature: null,
    topP: null,
    maxOutputTokens: null,
    ...DEFAULT_RUNTIME,
    tools: [],
  };
}

export function draftFormFromRevision(revision: Revision): DraftFormState {
  return {
    description: revision.Description ?? '',
    instruction: revision.Instruction ?? '',
    globalInstruction: revision.GlobalInstruction ?? '',
    modelProfileId: revision.ModelProfileID ?? '',
    temperature: revision.Generation?.temperature ?? null,
    topP: revision.Generation?.top_p ?? null,
    maxOutputTokens: revision.Generation?.max_output_tokens ?? null,
    maxLlmCalls: revision.Runtime?.max_llm_calls ?? DEFAULT_RUNTIME.maxLlmCalls,
    maxToolCalls: revision.Runtime?.max_tool_calls ?? DEFAULT_RUNTIME.maxToolCalls,
    enableParallelTools: revision.Runtime?.enable_parallel_tools ?? false,
    maxParallelTools: revision.Runtime?.max_parallel_tools ?? DEFAULT_RUNTIME.maxParallelTools,
    executionTimeoutSeconds: revision.Runtime?.execution_timeout_seconds ?? DEFAULT_RUNTIME.executionTimeoutSeconds,
    tools: Array.isArray(revision.Tools) ? revision.Tools.map((tool) => ({ ...tool })) : [],
  };
}

/**
 * Serializes to the snake_case wire shape. `GlobalInstruction` must keep its
 * PascalCase spelling because the server's request-key normalization does not
 * map `global_instruction` (known contract gap, see admin-web-ui.md).
 */
export function draftFormToConfiguration(state: DraftFormState): Record<string, unknown> {
  const generation: Record<string, unknown> = {};
  if (state.temperature !== null) {
    generation.temperature = state.temperature;
  }
  if (state.topP !== null) {
    generation.top_p = state.topP;
  }
  if (state.maxOutputTokens !== null) {
    generation.max_output_tokens = state.maxOutputTokens;
  }
  return {
    description: state.description,
    instruction: state.instruction,
    GlobalInstruction: state.globalInstruction,
    model_profile_id: state.modelProfileId.trim(),
    generation,
    runtime: {
      max_llm_calls: state.maxLlmCalls,
      max_tool_calls: state.maxToolCalls,
      enable_parallel_tools: state.enableParallelTools,
      max_parallel_tools: state.maxParallelTools,
      execution_timeout_seconds: state.executionTimeoutSeconds,
    },
    tools: state.tools.filter((tool) => tool.tool_id.trim() !== ''),
  };
}

function nullableNumber(value: unknown): number | null {
  if (value === undefined || value === null || value === '') {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

interface DraftEditorProps {
  value: DraftFormState;
  onChange: (value: DraftFormState) => void;
}

/** Editor for one revision draft configuration (schema_version 1, kind=llm). */
export function DraftEditor({ value, onChange }: DraftEditorProps) {
  const patch = (partial: Partial<DraftFormState>) => onChange({ ...value, ...partial });

  return (
    <div className="admin-stack">
      <Form layout="vertical" labelAlign="top" colon className="admin-form-grid">
        <Form.FormItem label="版本说明">
          <Input value={value.description} onChange={(v) => patch({ description: String(v) })} placeholder="本版本的变更摘要" />
        </Form.FormItem>
        <Form.FormItem label="模型配置 ID" help="引用一个 Model Profile。">
          <Input
            value={value.modelProfileId}
            onChange={(v) => patch({ modelProfileId: String(v) })}
            placeholder="Model Profile ID"
          />
        </Form.FormItem>
      </Form>
      <Form layout="vertical" labelAlign="top" colon>
        <Form.FormItem label="Instruction（系统指令）">
          <Textarea value={value.instruction} onChange={(v) => patch({ instruction: String(v) })} autosize={{ minRows: 5, maxRows: 14 }} />
        </Form.FormItem>
        <Form.FormItem label="Global Instruction（全局指令，可选）">
          <Textarea
            value={value.globalInstruction}
            onChange={(v) => patch({ globalInstruction: String(v) })}
            autosize={{ minRows: 3, maxRows: 8 }}
          />
        </Form.FormItem>
      </Form>
      <Form layout="vertical" labelAlign="top" colon className="admin-form-grid">
        <Form.FormItem label="Temperature" help="留空使用 Provider 默认值。">
          <InputNumber
            value={value.temperature ?? undefined}
            min={0}
            max={2}
            step={0.1}
            decimalPlaces={2}
            placeholder="默认"
            className="admin-full-width"
            onChange={(v) => patch({ temperature: nullableNumber(v) })}
          />
        </Form.FormItem>
        <Form.FormItem label="Top P" help="留空使用 Provider 默认值。">
          <InputNumber
            value={value.topP ?? undefined}
            min={0}
            max={1}
            step={0.05}
            decimalPlaces={2}
            placeholder="默认"
            className="admin-full-width"
            onChange={(v) => patch({ topP: nullableNumber(v) })}
          />
        </Form.FormItem>
        <Form.FormItem label="最大输出 Tokens" help="留空使用 Provider 默认值。">
          <InputNumber
            value={value.maxOutputTokens ?? undefined}
            min={1}
            placeholder="默认"
            className="admin-full-width"
            onChange={(v) => patch({ maxOutputTokens: nullableNumber(v) })}
          />
        </Form.FormItem>
        <Form.FormItem label="最大 LLM 调用次数">
          <InputNumber
            value={value.maxLlmCalls}
            min={1}
            className="admin-full-width"
            onChange={(v) => patch({ maxLlmCalls: nullableNumber(v) ?? DEFAULT_RUNTIME.maxLlmCalls })}
          />
        </Form.FormItem>
        <Form.FormItem label="最大工具调用次数">
          <InputNumber
            value={value.maxToolCalls}
            min={0}
            className="admin-full-width"
            onChange={(v) => patch({ maxToolCalls: nullableNumber(v) ?? DEFAULT_RUNTIME.maxToolCalls })}
          />
        </Form.FormItem>
        <Form.FormItem label="最大并行工具数">
          <InputNumber
            value={value.maxParallelTools}
            min={1}
            disabled={!value.enableParallelTools}
            className="admin-full-width"
            onChange={(v) => patch({ maxParallelTools: nullableNumber(v) ?? DEFAULT_RUNTIME.maxParallelTools })}
          />
        </Form.FormItem>
        <Form.FormItem label="执行超时（秒）">
          <InputNumber
            value={value.executionTimeoutSeconds}
            min={1}
            className="admin-full-width"
            onChange={(v) => patch({ executionTimeoutSeconds: nullableNumber(v) ?? DEFAULT_RUNTIME.executionTimeoutSeconds })}
          />
        </Form.FormItem>
        <Form.FormItem label="启用并行工具调用">
          <Switch value={value.enableParallelTools} onChange={(v) => patch({ enableParallelTools: Boolean(v) })} />
        </Form.FormItem>
      </Form>
      <Form layout="vertical" labelAlign="top" colon>
        <Form.FormItem label="工具白名单" help="按 tool_id 逐项授权（deny-by-default）。">
          <div>
            {value.tools.map((tool, index) => (
              <div className="admin-kv-row" key={index}>
                <Input
                  value={tool.tool_id}
                  placeholder="tool_id"
                  onChange={(v) => {
                    const tools = value.tools.map((item, i) => (i === index ? { ...item, tool_id: String(v) } : item));
                    patch({ tools });
                  }}
                />
                <span className="admin-page-subtitle admin-inline-label">
                  必需
                </span>
                <Switch
                  value={tool.required}
                  onChange={(v) => {
                    const tools = value.tools.map((item, i) => (i === index ? { ...item, required: Boolean(v) } : item));
                    patch({ tools });
                  }}
                />
                <Button
                  shape="square"
                  variant="outline"
                  theme="danger"
                  icon={<DeleteIcon />}
                  aria-label={`移除工具 ${index + 1}`}
                  onClick={() => patch({ tools: value.tools.filter((_, i) => i !== index) })}
                />
              </div>
            ))}
            <Button variant="dashed" icon={<AddIcon />} onClick={() => patch({ tools: [...value.tools, { tool_id: '', required: false }] })}>
              添加工具
            </Button>
          </div>
        </Form.FormItem>
      </Form>
    </div>
  );
}

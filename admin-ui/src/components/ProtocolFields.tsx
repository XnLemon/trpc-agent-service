import { Form, Input } from 'tdesign-react';
import type { ChannelKind } from '@/api/types';

export interface ProtocolFormState {
  corpId: string;
  agentId: string;
  receiveId: string;
  apiBaseUrl: string;
  webhookPath: string;
}

export function emptyProtocolForm(): ProtocolFormState {
  return { corpId: '', agentId: '', receiveId: '', apiBaseUrl: '', webhookPath: '' };
}

/** 序列化为 ProtocolConfiguration（仅填充与 channel 匹配的一侧）。 */
export function serializeProtocol(channel: ChannelKind, state: ProtocolFormState): Record<string, unknown> {
  if (channel === 'wecom') {
    const wecom: Record<string, unknown> = {};
    if (state.corpId.trim()) {
      wecom.corp_id = state.corpId.trim();
    }
    if (state.agentId.trim()) {
      wecom.agent_id = state.agentId.trim();
    }
    if (state.receiveId.trim()) {
      wecom.receive_id = state.receiveId.trim();
    }
    return Object.keys(wecom).length > 0 ? { wecom } : {};
  }
  const telegram: Record<string, unknown> = {};
  if (state.apiBaseUrl.trim()) {
    telegram.api_base_url = state.apiBaseUrl.trim();
  }
  if (state.webhookPath.trim()) {
    telegram.webhook_path = state.webhookPath.trim();
  }
  return Object.keys(telegram).length > 0 ? { telegram } : {};
}

interface ProtocolFieldsProps {
  channel: ChannelKind;
  value: ProtocolFormState;
  onChange: (value: ProtocolFormState) => void;
}

/** 渠道协议的非秘密配置字段（凭据一律走 secret_ref）。 */
export function ProtocolFields({ channel, value, onChange }: ProtocolFieldsProps) {
  const patch = (partial: Partial<ProtocolFormState>) => onChange({ ...value, ...partial });

  if (channel === 'wecom') {
    return (
      <>
        <Form.FormItem label="Corp ID">
          <Input value={value.corpId} onChange={(v) => patch({ corpId: String(v) })} placeholder="企业微信 Corp ID" />
        </Form.FormItem>
        <Form.FormItem label="Agent ID">
          <Input value={value.agentId} onChange={(v) => patch({ agentId: String(v) })} placeholder="企业微信应用 Agent ID" />
        </Form.FormItem>
        <Form.FormItem label="Receive ID（可选）">
          <Input value={value.receiveId} onChange={(v) => patch({ receiveId: String(v) })} />
        </Form.FormItem>
      </>
    );
  }
  return (
    <>
      <Form.FormItem label="API Base URL" help="必须为 HTTPS。">
        <Input value={value.apiBaseUrl} onChange={(v) => patch({ apiBaseUrl: String(v) })} placeholder="https://api.telegram.org" />
      </Form.FormItem>
      <Form.FormItem label="Webhook Path" help="必须以 / 开头。">
        <Input value={value.webhookPath} onChange={(v) => patch({ webhookPath: String(v) })} placeholder="/telegram/webhook" />
      </Form.FormItem>
    </>
  );
}

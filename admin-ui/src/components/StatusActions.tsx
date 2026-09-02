import { useState } from 'react';
import { Button, Space } from 'tdesign-react';
import type { LifecycleStatus } from '@/api/types';
import { ReasonDialog, type ReasonSubmit } from './ReasonDialog';

interface PendingTransition {
  next: LifecycleStatus;
  label: string;
  description: string;
  confirmTheme: 'primary' | 'danger' | 'warning';
}

interface StatusActionsProps {
  status: LifecycleStatus;
  busy?: boolean;
  /** allowActivate enables draft -> active (Channel Binding). */
  allowActivate?: boolean;
  onTransition: (next: LifecycleStatus, meta: ReasonSubmit) => Promise<boolean>;
}

const TRANSITION_META: Record<string, { label: string; description: string; confirmTheme: 'primary' | 'danger' | 'warning' }> = {
  suspend: {
    label: '暂停',
    description: '暂停后该资源不再接受新的运行时流量，但配置保留，可恢复。',
    confirmTheme: 'warning',
  },
  activate: {
    label: '激活',
    description: '激活后该资源开始对入站流量生效，请确认配置已经过验证。',
    confirmTheme: 'primary',
  },
  resume: {
    label: '恢复',
    description: '恢复后该资源重新接受运行时流量。',
    confirmTheme: 'primary',
  },
  disable: {
    label: '禁用',
    description: '禁用是终态操作，不可恢复。请确认该资源不再被任何运行时配置引用。',
    confirmTheme: 'danger',
  },
};

function legalTransitions(status: LifecycleStatus, allowActivate: boolean): PendingTransition[] {
  const meta = TRANSITION_META;
  switch (status) {
    case 'draft':
      return [
        ...(allowActivate ? [{ next: 'active' as const, label: meta.activate.label, ...meta.activate }] : []),
        { next: 'disabled', label: meta.disable.label, ...meta.disable },
      ];
    case 'active':
      return [
        { next: 'suspended', label: meta.suspend.label, ...meta.suspend },
        { next: 'disabled', label: meta.disable.label, ...meta.disable },
      ];
    case 'suspended':
      return [
        { next: 'active', label: meta.resume.label, ...meta.resume },
        { next: 'disabled', label: meta.disable.label, ...meta.disable },
      ];
    default:
      return [];
  }
}

/** Renders only the lifecycle transitions that are legal for the current state. */
export function StatusActions({ status, busy, allowActivate, onTransition }: StatusActionsProps) {
  const [pending, setPending] = useState<PendingTransition | null>(null);
  const transitions = legalTransitions(status, Boolean(allowActivate));
  if (transitions.length === 0) {
    return null;
  }
  return (
    <>
      <Space breakLine={false}>
        {transitions.map((transition) => (
          <Button
            key={transition.next}
            theme={transition.confirmTheme === 'danger' ? 'danger' : 'default'}
            variant={transition.confirmTheme === 'danger' ? 'outline' : 'base'}
            disabled={busy}
            onClick={() => setPending(transition)}
          >
            {transition.label}
          </Button>
        ))}
      </Space>
      <ReasonDialog
        visible={pending !== null}
        title={`${pending?.label ?? ''}确认`}
        description={<div className="admin-page-subtitle">{pending?.description}</div>}
        confirmTheme={pending?.confirmTheme}
        onCancel={() => setPending(null)}
        onConfirm={async (submit) => {
          if (!pending) {
            return false;
          }
          return onTransition(pending.next, submit);
        }}
      />
    </>
  );
}

import { useEffect, useState, type ReactNode } from 'react';
import { Dialog, Textarea } from 'tdesign-react';
import { newCorrelationId } from '@/lib/format';

export interface ReasonSubmit {
  reason: string;
  correlationId: string;
}

interface ReasonDialogProps {
  visible: boolean;
  title: string;
  description?: ReactNode;
  confirmContent?: string;
  confirmTheme?: 'primary' | 'danger' | 'warning';
  onCancel: () => void;
  onConfirm: (submit: ReasonSubmit) => Promise<boolean | void> | boolean | void;
}

/**
 * Confirmation dialog for high-impact control-plane operations. A fresh
 * correlation_id is generated each time the dialog opens and is shown to the
 * operator so it can be cross-checked with audit records later.
 */
export function ReasonDialog({
  visible,
  title,
  description,
  confirmContent = '确认执行',
  confirmTheme = 'primary',
  onCancel,
  onConfirm,
}: ReasonDialogProps) {
  const [reason, setReason] = useState('');
  const [correlationId, setCorrelationId] = useState('');
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (visible) {
      setReason('');
      setCorrelationId(newCorrelationId());
      setSubmitting(false);
    }
  }, [visible]);

  const handleConfirm = async () => {
    if (!reason.trim() || submitting) {
      return;
    }
    setSubmitting(true);
    try {
      const result = await onConfirm({ reason: reason.trim(), correlationId });
      if (result !== false) {
        onCancel();
      }
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Dialog
      visible={visible}
      header={title}
      confirmBtn={{ content: confirmContent, theme: confirmTheme, loading: submitting, disabled: !reason.trim() }}
      cancelBtn="取消"
      onClose={onCancel}
      onConfirm={handleConfirm}
      destroyOnClose
      width={520}
    >
      <div className="admin-stack">
        {description}
        <div>
          <div className="admin-page-subtitle admin-description-spaced">
            操作原因（必填，将写入审计记录）
          </div>
          <Textarea
            value={reason}
            onChange={(value) => setReason(String(value))}
            placeholder="例如：例行发布 r3 修复指令错误"
            autosize={{ minRows: 2, maxRows: 4 }}
            maxlength={256}
          />
        </div>
        <div className="admin-page-subtitle">
          本次操作 correlation_id：<span className="admin-mono">{correlationId}</span>
        </div>
      </div>
    </Dialog>
  );
}

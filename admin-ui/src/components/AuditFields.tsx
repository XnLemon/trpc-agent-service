import { Form, Input } from 'tdesign-react';

interface AuditFieldsProps {
  reason: string;
  correlationId: string;
  onReasonChange: (reason: string) => void;
}

/**
 * Model/Backend/Binding 的创建、配置修改与状态迁移都要求 reason +
 * correlation_id（服务端强制校验并写入审计）。correlation_id 在每次表单
 * 初始化/提交成功后由调用方重新生成，操作期间保持不变，便于人工核对。
 */
export function AuditFields({ reason, correlationId, onReasonChange }: AuditFieldsProps) {
  return (
    <>
      <Form.FormItem label="操作原因" help="必填，将写入审计记录。">
        <Input value={reason} maxlength={256} placeholder="例如：接入新的模型提供方" onChange={(value) => onReasonChange(String(value))} />
      </Form.FormItem>
      <div className="admin-page-subtitle admin-description-spaced">
        本次操作 correlation_id：<span className="admin-mono">{correlationId}</span>
      </div>
    </>
  );
}

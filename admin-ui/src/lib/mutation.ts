import { DialogPlugin, MessagePlugin, NotificationPlugin } from 'tdesign-react';
import { ApiError } from '@/api/client';

export type MutationFailureKind =
  | 'conflict'
  | 'audit_unavailable'
  | 'unauthorized'
  | 'forbidden'
  | 'not_found'
  | 'invalid_request'
  | 'unavailable'
  | 'network_error'
  | 'error';

export type MutationResult<T> = { ok: true; value: T; requestId: string } | { ok: false; kind: MutationFailureKind; requestId?: string };

const CATEGORY_MESSAGES: Record<string, string> = {
  invalid_request: '请求不合法：请检查表单内容（例如必填项、状态机约束、Catalog 限制）。',
  unauthorized: '凭证无效或已过期，请重新连接。',
  forbidden: '当前凭证没有该租户/资源的写权限。',
  not_found: '资源不存在，或当前凭证无权查看该资源。',
  conflict: '版本冲突：服务端数据已被其他操作修改。',
  storage_unavailable: '控制面存储暂时不可用，请稍后重试。',
  audit_unavailable: '审计链路不可用：写入可能已生效但未被确认。',
  internal_error: '服务端内部错误，请查看服务端日志。',
  network_error: '无法连接 Admin API：请检查网关地址与网络。',
};

export function categoryMessage(category: string): string {
  return CATEGORY_MESSAGES[category] ?? `操作失败（${category}）。`;
}

/**
 * Executes one control-plane mutation and normalizes failure handling:
 * - conflict:            keeps user input, offers an explicit reload (never auto-retries)
 * - audit_unavailable:   marks the result as "待确认" and refreshes the resource
 * - other categories:    shows the stable generic message with the request id
 */
export async function runMutation<T extends object>(
  fn: () => Promise<{ data: T; requestId: string }>,
  options?: { reload?: () => void | Promise<void> },
): Promise<MutationResult<T>> {
  try {
    const { data, requestId } = await fn();
    return { ok: true, value: data, requestId };
  } catch (error) {
    if (error instanceof ApiError) {
      if (error.category === 'conflict') {
        const dialog = DialogPlugin.confirm({
          header: '版本冲突',
          body: '服务端数据已被其他操作修改（乐观锁冲突）。你的修改未提交；为避免覆盖他人变更，系统不会自动重试。请重新加载最新数据，确认内容后再提交。',
          confirmBtn: '重新加载',
          cancelBtn: '保留我的修改',
          theme: 'warning',
          onConfirm: async () => {
            await options?.reload?.();
            dialog.hide();
          },
          onClose: () => dialog.hide(),
        });
        return { ok: false, kind: 'conflict', requestId: error.requestId };
      }
      if (error.category === 'audit_unavailable') {
        NotificationPlugin.warning({
          title: '提交状态待确认',
          content: `审计写入失败，但配置可能已生效，且不会自动重试。已为你重新读取最新数据。请记录 request_id（${error.requestId ?? '未知'}）与你本次操作使用的 correlation_id，排查审计链路。`,
          duration: 10000,
        });
        await options?.reload?.();
        return { ok: false, kind: 'audit_unavailable', requestId: error.requestId };
      }
      const kind: MutationFailureKind =
        error.category === 'unauthorized' ||
        error.category === 'forbidden' ||
        error.category === 'not_found' ||
        error.category === 'invalid_request' ||
        error.category === 'network_error'
          ? (error.category as MutationFailureKind)
          : error.category === 'storage_unavailable'
            ? 'unavailable'
            : 'error';
      MessagePlugin.error({
        content: `${categoryMessage(error.category)}${error.requestId ? `（request_id: ${error.requestId}）` : ''}`,
        duration: 6000,
      });
      return { ok: false, kind, requestId: error.requestId };
    }
    MessagePlugin.error('操作失败：未知错误。');
    return { ok: false, kind: 'error' };
  }
}

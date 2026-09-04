import { Tag } from 'tdesign-react';

const STATUS_META: Record<string, { theme: 'success' | 'default' | 'warning' | 'danger' | 'primary'; label: string }> = {
  active: { theme: 'success', label: '运行中' },
  draft: { theme: 'default', label: '草稿' },
  suspended: { theme: 'warning', label: '已暂停' },
  disabled: { theme: 'danger', label: '已禁用' },
  published: { theme: 'primary', label: '已发布' },
};

export function StatusTag({ status }: { status: string }) {
  const meta = STATUS_META[status] ?? { theme: 'default' as const, label: status || '未知' };
  return (
    <Tag theme={meta.theme} variant="light-outline">
      {meta.label}
    </Tag>
  );
}

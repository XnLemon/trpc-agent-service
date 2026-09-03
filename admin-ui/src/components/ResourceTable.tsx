import { Alert, Button, Empty, Input, Pagination, Select, Table } from 'tdesign-react';
import type { ReactNode } from 'react';

export interface ResourceTableColumn<T extends object> {
  colKey: string;
  title: string;
  width?: number;
  ellipsis?: boolean;
  cell?: (ctx: { row: T }) => ReactNode;
  [key: string]: unknown;
}

interface Props<T extends object> {
  data: T[];
  loading?: boolean;
  columns: ResourceTableColumn<T>[];
  rowKey: string;
  query: string;
  status: string;
  statusOptions?: Array<{ label: string; value: string }>;
  onQueryChange: (value: string) => void;
  onStatusChange: (value: string) => void;
  onSearch: () => void;
  onReset: () => void;
  onLoadMore?: () => void;
  hasMore?: boolean;
  page?: number;
  pageSize?: number;
  total?: number | null;
  onPageChange?: (page: number) => void;
  error?: string | null;
  onRetry?: () => void;
  createLabel?: string;
  onCreate?: () => void;
}

export function ResourceTable<T extends object>({ data, loading, columns, rowKey, query, status, statusOptions, onQueryChange, onStatusChange, onSearch, onReset, onLoadMore, hasMore, page, pageSize = 25, total, onPageChange, error, onRetry, createLabel, onCreate }: Props<T>) {
  const estimatedTotal = total ?? (hasMore ? pageSize * (page ?? 1) + 1 : Math.max(0, pageSize * ((page ?? 1) - 1) + data.length));

  return <div className="admin-resource-list">
    <div className="admin-list-toolbar">
      <Input value={query} clearable placeholder="搜索名称、Key 或 ID" onChange={(value) => onQueryChange(String(value))} onEnter={onSearch} />
      <Select value={status} clearable placeholder="全部状态" options={statusOptions ?? [{ label: '运行中', value: 'active' }, { label: '已暂停', value: 'suspended' }, { label: '草稿', value: 'draft' }, { label: '已禁用', value: 'disabled' }]} onChange={(value) => onStatusChange(String(value ?? ''))} />
      <Button theme="primary" onClick={onSearch}>搜索</Button>
      <Button variant="text" onClick={onReset}>重置</Button>
      <span className="admin-list-toolbar-spacer" />
      {onCreate ? <Button theme="primary" variant="outline" onClick={onCreate}>{createLabel ?? '新建'}</Button> : null}
    </div>
    {error ? <Alert theme="error" message="列表加载失败" description={error} operation={onRetry ? <Button size="small" variant="text" theme="primary" onClick={onRetry}>重试</Button> : undefined} /> : null}
    <Table rowKey={rowKey} bordered={false} stripe hover loading={loading} data={data} columns={columns} empty={<Empty description="暂无匹配资源" />} />
    {page !== undefined && onPageChange ? (
      <Pagination
        current={page}
        pageSize={pageSize}
        total={estimatedTotal}
        totalContent={false}
        showPageSize={false}
        disabled={loading}
        onChange={({ current }) => onPageChange(current)}
      />
    ) : hasMore && onLoadMore ? <div className="admin-list-more"><Button variant="outline" loading={loading} onClick={onLoadMore}>加载更多</Button></div> : null}
  </div>;
}

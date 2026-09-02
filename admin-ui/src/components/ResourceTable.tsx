import { Button, Empty, Input, Select, Table, Tag } from 'tdesign-react';
import type { ReactNode } from 'react';

export interface ResourceTableColumn<T> {
  colKey: string;
  title: string;
  width?: number;
  cell?: (ctx: { row: T }) => ReactNode;
}

interface Props<T> {
  data: T[];
  loading?: boolean;
  columns: ResourceTableColumn<T>[];
  rowKey: string;
  query: string;
  status: string;
  onQueryChange: (value: string) => void;
  onStatusChange: (value: string) => void;
  onSearch: () => void;
  onReset: () => void;
  onLoadMore?: () => void;
  hasMore?: boolean;
  createLabel?: string;
  onCreate?: () => void;
}

export function ResourceTable<T>({ data, loading, columns, rowKey, query, status, onQueryChange, onStatusChange, onSearch, onReset, onLoadMore, hasMore, createLabel, onCreate }: Props<T>) {
  return <div className="admin-resource-list">
    <div className="admin-list-toolbar">
      <Input value={query} clearable placeholder="搜索名称、Key 或 ID" onChange={(value) => onQueryChange(String(value))} onEnter={onSearch} />
      <Select value={status} clearable placeholder="全部状态" options={[{ label: 'Active', value: 'active' }, { label: 'Suspended', value: 'suspended' }, { label: 'Draft', value: 'draft' }, { label: 'Disabled', value: 'disabled' }]} onChange={(value) => onStatusChange(String(value ?? ''))} />
      <Button theme="primary" onClick={onSearch}>搜索</Button>
      <Button variant="text" onClick={onReset}>重置</Button>
      <span className="admin-list-toolbar-spacer" />
      {onCreate ? <Button theme="primary" variant="outline" onClick={onCreate}>{createLabel ?? '新建'}</Button> : null}
    </div>
    <Table rowKey={rowKey} bordered={false} stripe hover loading={loading} data={data} columns={columns} empty={<Empty description="暂无匹配资源" />} />
    {hasMore && onLoadMore ? <div className="admin-list-more"><Button variant="outline" loading={loading} onClick={onLoadMore}>加载更多</Button></div> : null}
  </div>;
}

export function StatusTag({ value }: { value?: string }) {
  const theme = value === 'active' ? 'success' : value === 'disabled' ? 'danger' : value === 'suspended' ? 'warning' : 'default';
  return <Tag theme={theme as any} variant="light">{value ?? 'unknown'}</Tag>;
}

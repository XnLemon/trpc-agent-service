import { Card } from 'tdesign-react';
import { ResourceTable, type ResourceTableColumn } from '@/components/ResourceTable';
import { useCursorResourceList, type CursorListLoader } from '@/lib/useCursorResourceList';

interface ResourceListPanelProps<T extends object> {
  title: string;
  loader: CursorListLoader<T>;
  rowKey: string;
  columns: ResourceTableColumn<T>[];
  statusOptions?: Array<{ label: string; value: string }>;
  createLabel?: string;
  onCreate?: () => void;
}

export function ResourceListPanel<T extends object>({ title, loader, rowKey, columns, statusOptions, createLabel, onCreate }: ResourceListPanelProps<T>) {
  const list = useCursorResourceList(loader);

  return (
    <Card className="admin-panel admin-resource-list-panel" title={title} bordered>
      <ResourceTable
        data={list.items}
        loading={list.loading}
        error={list.error}
        onRetry={list.search}
        columns={columns}
        rowKey={rowKey}
        query={list.query}
        status={list.status}
        statusOptions={statusOptions}
        onQueryChange={list.setQuery}
        onStatusChange={list.setStatus}
        onSearch={list.search}
        onReset={list.reset}
        page={list.page}
        pageSize={list.pageSize}
        hasMore={list.hasMore}
        onPageChange={list.changePage}
        createLabel={createLabel}
        onCreate={onCreate}
      />
    </Card>
  );
}

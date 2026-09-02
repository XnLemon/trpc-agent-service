import { useState, type ReactNode } from 'react';
import { Button, Card, Empty, Form, Input, Link, Table } from 'tdesign-react';
import type { RecentItem } from '@/lib/recents';
import { formatDateTime } from '@/lib/format';

interface ResourceLobbyProps {
  title: string;
  subtitle?: ReactNode;
  alert?: ReactNode;
  idLabel: string;
  idPlaceholder?: string;
  openLabel?: string;
  onOpen: (id: string) => void;
  createTitle?: string;
  createDescription?: ReactNode;
  createForm?: ReactNode;
  recents: RecentItem[];
  onOpenRecent: (id: string) => void;
  onRemoveRecent: (id: string) => void;
}

/**
 * Landing page for one resource kind. The control plane has no list APIs yet
 * (P0 gap in docs/docs/admin-web-ui.md), so navigation is by known ID plus a
 * session-scoped "recently opened" list.
 */
export function ResourceLobby({
  title,
  subtitle,
  alert,
  idLabel,
  idPlaceholder,
  openLabel = '打开',
  onOpen,
  createTitle,
  createDescription,
  createForm,
  recents,
  onOpenRecent,
  onRemoveRecent,
}: ResourceLobbyProps) {
  const [id, setId] = useState('');

  return (
    <div className="admin-page">
      <div>
        <h2 className="admin-page-title">{title}</h2>
        {subtitle ? <div className="admin-page-subtitle">{subtitle}</div> : null}
      </div>
      {alert}
      <div className="admin-form-grid" style={{ alignItems: 'start' }}>
        <Card title="按 ID 打开" bordered>
          <Form layout="vertical" colon>
            <Form.FormItem label={idLabel}>
              <Input
                value={id}
                placeholder={idPlaceholder}
                onChange={(value) => setId(String(value).trim())}
                onEnter={() => id && onOpen(id)}
              />
            </Form.FormItem>
            <Button theme="primary" disabled={!id} onClick={() => onOpen(id)}>
              {openLabel}
            </Button>
          </Form>
        </Card>
        {createForm ? (
          <Card title={createTitle ?? '新建'} bordered>
            {createDescription ? <div className="admin-page-subtitle" style={{ marginBottom: 12 }}>{createDescription}</div> : null}
            {createForm}
          </Card>
        ) : null}
      </div>
      <Card title="最近打开（仅本次浏览器会话）" bordered>
        {recents.length === 0 ? (
          <Empty description="暂无记录。服务端暂无列表接口，请通过已知 ID 打开资源。" />
        ) : (
          <Table
            rowKey="id"
            size="small"
            hover
            data={recents}
            columns={[
              {
                colKey: 'label',
                title: '名称',
                cell: ({ row }) => (
                  <Link theme="primary" hover="underline" onClick={() => onOpenRecent(row.id)}>
                    {row.label || row.id}
                  </Link>
                ),
              },
              { colKey: 'id', title: 'ID', cell: ({ row }) => <span className="admin-mono">{row.id}</span>, ellipsis: true },
              { colKey: 'openedAt', title: '最近打开', width: 180, cell: ({ row }) => formatDateTime(new Date(row.openedAt).toISOString()) },
              {
                colKey: 'ops',
                title: '操作',
                width: 80,
                cell: ({ row }) => (
                  <Link theme="danger" hover="color" onClick={() => onRemoveRecent(row.id)}>
                    移除
                  </Link>
                ),
              },
            ]}
          />
        )}
      </Card>
    </div>
  );
}

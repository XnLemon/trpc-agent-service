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
    <div className="admin-page admin-lobby-page">
      <div className="admin-page-heading">
        <div className="admin-page-eyebrow">控制面资源</div>
        <h1 className="admin-page-title">{title}</h1>
        {subtitle ? <div className="admin-page-subtitle">{subtitle}</div> : null}
      </div>
      {alert}
      <div className="admin-lobby-grid">
        <Card className="admin-panel admin-open-panel" title="打开已有资源" bordered>
          <Form layout="vertical" labelAlign="top" colon>
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
          <Card className="admin-panel admin-create-panel" title={createTitle ?? '新建'} bordered>
            {createDescription ? <div className="admin-page-subtitle admin-panel-description">{createDescription}</div> : null}
            {createForm}
          </Card>
        ) : null}
      </div>
      <Card className="admin-panel admin-recent-panel" title="最近打开" bordered>
        <div className="admin-panel-description">仅保存在当前浏览器会话中，不会写入服务端。</div>
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

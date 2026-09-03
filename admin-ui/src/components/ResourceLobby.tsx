import { useState, type ReactNode } from 'react';
import { Button, Card, Drawer, Empty, Form, Input, Link, Table } from 'tdesign-react';
import { AddIcon } from 'tdesign-icons-react';
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
  resourceList?: ReactNode;
  hideOpenPanel?: boolean;
}

/**
 * Shared workspace shell for one tenant resource kind. Resource pages provide
 * a server-backed list while this component keeps the create form and known-ID
 * compatibility entry point together.
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
  resourceList,
  hideOpenPanel = false,
}: ResourceLobbyProps) {
  const [id, setId] = useState('');
  const [createOpen, setCreateOpen] = useState(false);

  return (
    <div className="admin-page admin-lobby-page">
      <div className="admin-page-heading admin-page-heading-row">
        <div>
          <div className="admin-page-eyebrow">控制面资源</div>
          <h1 className="admin-page-title">{title}</h1>
          {subtitle ? <div className="admin-page-subtitle">{subtitle}</div> : null}
        </div>
        {createForm ? (
          <Button theme="primary" icon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            {createTitle ?? '新建'}
          </Button>
        ) : null}
      </div>
      {alert}
      {resourceList}
      {!hideOpenPanel ? <Card className="admin-panel admin-open-panel" title="打开已有资源" bordered>
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
        </Card> : null}
      <Drawer
        className="admin-create-drawer"
        header={createTitle ?? '新建'}
        visible={createOpen}
        placement="right"
        size="min(520px, 100vw)"
        footer={false}
        destroyOnClose
        onClose={() => setCreateOpen(false)}
      >
        <div className="admin-create-form">
          {createDescription ? <div className="admin-page-subtitle admin-panel-description">{createDescription}</div> : null}
          {createForm}
        </div>
      </Drawer>
      <Card className="admin-panel admin-recent-panel" title="最近打开" bordered>
        <div className="admin-panel-description">仅保存在当前浏览器会话中，不会写入服务端。</div>
        {recents.length === 0 ? (
          <Empty description="暂无最近打开记录。" />
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

import { useState } from 'react';
import { Button, Form, Input, MessagePlugin, Textarea } from 'tdesign-react';
import { useNavigate } from 'react-router-dom';
import { ResourceLobby } from '@/components/ResourceLobby';
import { useAdminClient } from '@/lib/connection';
import { runMutation } from '@/lib/mutation';
import { addRecent, listRecents, removeRecent } from '@/lib/recents';
import { useTenantOutlet } from './TenantLayout';

export function AppsPage() {
  const { tenant } = useTenantOutlet();
  const client = useAdminClient();
  const navigate = useNavigate();
  const [recents, setRecents] = useState(() => listRecents('app', tenant.TenantID));
  const [appKey, setAppKey] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [description, setDescription] = useState('');
  const [creating, setCreating] = useState(false);

  const openApp = (id: string, label?: string) => {
    setRecents(addRecent('app', tenant.TenantID, { id, label: label ?? id }));
    navigate(`/tenants/${encodeURIComponent(tenant.TenantID)}/apps/${encodeURIComponent(id)}`);
  };

  const createApp = async () => {
    if (!appKey.trim() || !displayName.trim() || creating) {
      return;
    }
    setCreating(true);
    try {
      const result = await runMutation(() =>
        client.createApp(tenant.TenantID, {
          app_key: appKey.trim(),
          display_name: displayName.trim(),
          description: description.trim(),
        }),
      );
      if (result.ok) {
        MessagePlugin.success('应用已创建（draft 状态），请继续创建并发布首个版本。');
        openApp(result.value.AppID, result.value.DisplayName);
      }
    } finally {
      setCreating(false);
    }
  };

  return (
    <ResourceLobby
      title="应用与版本"
      subtitle="控制面暂无应用列表接口（P0 缺口），请通过已知应用 ID 打开。"
      idLabel="应用 ID"
      idPlaceholder="Agent App ID"
      onOpen={(id) => openApp(id)}
      createTitle="创建应用"
      createDescription="新应用为 draft 状态；首次发布版本后自动转为 active。"
      createForm={
        <Form layout="vertical" colon>
          <Form.FormItem label="应用 Key" help="创建后不可修改，租户内唯一。">
            <Input value={appKey} onChange={(value) => setAppKey(String(value))} placeholder="例如 support-bot" />
          </Form.FormItem>
          <Form.FormItem label="展示名">
            <Input value={displayName} onChange={(value) => setDisplayName(String(value))} placeholder="例如 客服机器人" />
          </Form.FormItem>
          <Form.FormItem label="描述">
            <Textarea value={description} onChange={(value) => setDescription(String(value))} autosize={{ minRows: 2, maxRows: 4 }} />
          </Form.FormItem>
          <Button theme="primary" loading={creating} disabled={!appKey.trim() || !displayName.trim()} onClick={createApp}>
            创建应用
          </Button>
        </Form>
      }
      recents={recents}
      onOpenRecent={(id) => openApp(id)}
      onRemoveRecent={(id) => setRecents(removeRecent('app', tenant.TenantID, id))}
    />
  );
}

import { useState } from 'react';
import { Alert, Button, Card, Form, Input } from 'tdesign-react';
import { LayersIcon } from 'tdesign-icons-react';
import { useLocation, useNavigate } from 'react-router-dom';
import { readStoredBaseUrl, useConnection } from '@/lib/connection';

/**
 * Credential entry point. The admin token is only kept in memory for the
 * duration of this tab (never localStorage / cookies / bundled config), per
 * the security boundary in docs/docs/admin-web-ui.md. Production deployments
 * should front this UI with a BFF that holds the token server-side.
 */
export function ConnectPage() {
  const { connect } = useConnection();
  const navigate = useNavigate();
  const location = useLocation();
  const [baseUrl, setBaseUrl] = useState(readStoredBaseUrl());
  const [token, setToken] = useState('');
  const from = (location.state as { from?: string } | null)?.from ?? '/tenants';

  const submit = () => {
    if (!token.trim()) {
      return;
    }
    connect({ baseUrl: baseUrl.trim().replace(/\/+$/, ''), token: token.trim() });
    navigate(from, { replace: true });
  };

  return (
    <div className="admin-connect-page">
      <div className="admin-connect-intro">
        <div className="admin-connect-mark" aria-hidden="true"><LayersIcon /></div>
        <div className="admin-connect-kicker">tRPC Agent / CONTROL PLANE</div>
        <h1>管理控制台</h1>
        <p>集中管理租户、Agent 应用、模型、存储后端与渠道绑定。</p>
      </div>
      <Card className="admin-connect-card" title="连接 Admin API" bordered>
        <div className="admin-connect-stack">
          <Alert
            theme="warning"
            message="凭证仅保存在内存中"
            description="Admin token 不会写入 localStorage、Cookie 或前端构建产物；刷新页面后需重新输入。请勿在不可信的设备上使用高权限凭证。"
          />
          <Form layout="vertical" labelAlign="top" colon>
            <Form.FormItem label="API 地址" help="留空表示同源 /admin（开发环境走 Vite 代理，生产环境由 BFF/反代转发）。">
              <Input
                value={baseUrl}
                placeholder="例如 https://gateway.internal.example.com"
                onChange={(value) => setBaseUrl(String(value))}
              />
            </Form.FormItem>
            <Form.FormItem label="Admin Token" help="即服务端 TRPC_ADMIN_TOKEN；其租户范围由 TRPC_ADMIN_TENANTS 决定。">
              <Input
                type="password"
                value={token}
                placeholder="输入受控 Bearer 凭证"
                onChange={(value) => setToken(String(value))}
                onEnter={submit}
              />
            </Form.FormItem>
            <Button theme="primary" block disabled={!token.trim()} onClick={submit}>
              连接
            </Button>
          </Form>
        </div>
      </Card>
    </div>
  );
}

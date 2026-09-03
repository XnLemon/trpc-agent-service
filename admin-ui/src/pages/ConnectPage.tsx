import { useState } from 'react';
import { Alert, Button, Card, Form, Input } from 'tdesign-react';
import { LayersIcon } from 'tdesign-icons-react';
import { useLocation, useNavigate } from 'react-router-dom';
import { useConnection } from '@/lib/connection';

/**
 * Same-origin administrator login. The server owns the HttpOnly session cookie;
 * the browser never handles a bearer token.
 */
export function ConnectPage() {
  const { client, connect } = useConnection();
  const navigate = useNavigate();
  const location = useLocation();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const from = (location.state as { from?: string } | null)?.from ?? '/tenants';

  const submit = async () => {
    if (!client || !username.trim() || !password || loading) return;
    setLoading(true); setError('');
    try {
      const { data } = await client.login(username.trim(), password);
      connect(data);
      navigate(from, { replace: true });
    } catch {
      setError('账号或密码错误，请重试。');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="admin-connect-page">
      <div className="admin-connect-intro">
        <div className="admin-connect-mark" aria-hidden="true"><LayersIcon /></div>
        <div className="admin-connect-kicker">tRPC Agent / CONTROL PLANE</div>
        <h1>管理控制台</h1>
        <p>集中管理租户、Agent 应用、模型、存储后端与渠道绑定。</p>
      </div>
      <Card className="admin-connect-card" title="管理员登录" bordered>
        <div className="admin-connect-stack">
          {error ? <Alert theme="error" message={error} /> : null}
          <Form layout="vertical" labelAlign="top" colon>
            <Form.FormItem label="管理员账号">
              <Input
                value={username}
                placeholder="输入账号"
                onChange={(value) => setUsername(String(value))}
              />
            </Form.FormItem>
            <Form.FormItem label="密码">
              <Input
                type="password"
                value={password}
                placeholder="输入密码"
                onChange={(value) => setPassword(String(value))}
                onEnter={submit}
              />
            </Form.FormItem>
            <Button theme="primary" block loading={loading} disabled={!username.trim() || !password} onClick={submit}>
              登录
            </Button>
          </Form>
        </div>
      </Card>
    </div>
  );
}

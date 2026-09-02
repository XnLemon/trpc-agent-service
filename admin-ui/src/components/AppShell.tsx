import { useMemo } from 'react';
import { Button, Layout, Menu } from 'tdesign-react';
import { AppIcon, DataBaseIcon, HomeIcon, LayersIcon, LinkIcon, LogoutIcon, ViewListIcon } from 'tdesign-icons-react';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useConnection } from '@/lib/connection';

const { Header, Aside, Content } = Layout;

interface NavItem {
  value: string;
  label: string;
  icon: JSX.Element;
}

export function AppShell() {
  const location = useLocation();
  const navigate = useNavigate();
  const { connection, disconnect } = useConnection();

  const tenantMatch = location.pathname.match(/^\/tenants\/([^/]+)/);
  const tenantId = tenantMatch?.[1];

  const items = useMemo<NavItem[]>(() => {
    const base: NavItem[] = [{ value: '/tenants', label: '租户入口', icon: <HomeIcon /> }];
    if (tenantId) {
      base.push(
        { value: `/tenants/${tenantId}`, label: '概览与设置', icon: <ViewListIcon /> },
        { value: `/tenants/${tenantId}/apps`, label: '应用与版本', icon: <AppIcon /> },
        { value: `/tenants/${tenantId}/models`, label: '模型配置', icon: <LayersIcon /> },
        { value: `/tenants/${tenantId}/backends`, label: '存储后端', icon: <DataBaseIcon /> },
        { value: `/tenants/${tenantId}/bindings`, label: '渠道绑定', icon: <LinkIcon /> },
      );
    }
    return base;
  }, [tenantId]);

  const activeValue = useMemo(() => {
    let best = '';
    for (const item of items) {
      const exact = location.pathname === item.value;
      const nested = item.value !== '/tenants' && location.pathname.startsWith(`${item.value}/`);
      if ((exact || nested) && item.value.length > best.length) {
        best = item.value;
      }
    }
    return best || '/tenants';
  }, [items, location.pathname]);

  return (
    <Layout className="admin-shell">
      <Header className="admin-shell-header">
        <span className="admin-shell-logo">tRPC Agent 管理控制台</span>
        {tenantId ? (
          <span className="admin-page-subtitle">
            当前租户：<span className="admin-mono">{tenantId}</span>
          </span>
        ) : null}
        <span className="admin-shell-header-spacer" />
        <span className="admin-page-subtitle">API：{connection?.baseUrl || '同源 /admin（开发代理或 BFF）'}</span>
        <Button
          size="small"
          variant="outline"
          icon={<LogoutIcon />}
          onClick={() => {
            disconnect();
            navigate('/connect');
          }}
        >
          断开连接
        </Button>
      </Header>
      <Layout style={{ height: 'calc(100vh - 64px)' }}>
        <Aside width="220px" className="admin-shell-aside">
          <Menu value={activeValue} onChange={(value) => navigate(String(value))} style={{ width: '100%' }}>
            {items.map((item) => (
              <Menu.MenuItem key={item.value} value={item.value} icon={item.icon}>
                {item.label}
              </Menu.MenuItem>
            ))}
          </Menu>
        </Aside>
        <Layout>
          <Content className="admin-shell-content">
            <Outlet />
          </Content>
        </Layout>
      </Layout>
    </Layout>
  );
}

import { useMemo } from 'react';
import { Button, Layout, Menu, Tag } from 'tdesign-react';
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
  const { principal, disconnect } = useConnection();

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
        <div className="admin-shell-brand">
          <span className="admin-shell-brand-mark" aria-hidden="true">
            <LayersIcon />
          </span>
          <div className="admin-shell-brand-copy">
            <strong className="admin-shell-logo">tRPC Agent</strong>
            <span>管理控制台</span>
          </div>
        </div>
        {tenantId ? (
          <div className="admin-shell-context">
            <span className="admin-shell-context-label">当前租户</span>
            <span className="admin-mono">{tenantId}</span>
          </div>
        ) : null}
        <span className="admin-shell-header-spacer" />
        <div className="admin-shell-endpoint">
          <span className="admin-shell-endpoint-label">管理员</span>
          <span>{principal?.subject_id || 'admin'}</span>
        </div>
        <Tag theme="primary" variant="light">Admin</Tag>
        <Button
          size="small"
          variant="outline"
          icon={<LogoutIcon />}
          onClick={() => {
            disconnect();
            navigate('/admin/login');
          }}
        >
          退出登录
        </Button>
      </Header>
      <Layout className="admin-shell-body">
        <Aside width="220px" className="admin-shell-aside">
          <div className="admin-nav-caption">工作区</div>
          <Menu theme="light" width="100%" value={activeValue} onChange={(value) => navigate(String(value))}>
            {items.map((item) => (
              <Menu.MenuItem key={item.value} value={item.value} icon={item.icon}>
                {item.label}
              </Menu.MenuItem>
            ))}
          </Menu>
          <div className="admin-aside-footer">
            <span className="admin-aside-footer-dot" aria-hidden="true" />
            <span>控制面配置</span>
            <span className="admin-mono">v1</span>
          </div>
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

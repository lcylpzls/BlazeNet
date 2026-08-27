import { Suspense, lazy, useState, type ComponentType } from 'react';
import { Button, Card, Form, Input, Layout, Menu, message } from 'antd';
import { login, logout } from './api';

// 三期 P3.5：页面按需加载（代码分割），首屏只加载登录页与布局。
const UsersPage = lazy(() => import('./pages/UsersPage'));
const NodesPage = lazy(() => import('./pages/NodesPage'));
const GamesPage = lazy(() => import('./pages/GamesPage'));
const TasksPage = lazy(() => import('./pages/TasksPage'));
const StatusPage = lazy(() => import('./pages/StatusPage'));
const AuditPage = lazy(() => import('./pages/AuditPage'));

const { Sider, Content, Header } = Layout;

const PAGES: Record<string, ComponentType> = {
  users: UsersPage,
  nodes: NodesPage,
  games: GamesPage,
  tasks: TasksPage,
  status: StatusPage,
  audit: AuditPage,
};

function PageView({ page }: { page: ComponentType }) {
  const Page = page;
  return <Page />;
}

function AdminLayout() {
  const [selected, setSelected] = useState('users');
  const items = [
    { key: 'users', label: '账号/场所/分组' },
    { key: 'nodes', label: '节点管理' },
    { key: 'games', label: '游戏管理' },
    { key: 'tasks', label: '任务管理' },
    { key: 'status', label: '基础状态' },
    { key: 'audit', label: '审计日志' },
  ];
  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider theme="dark">
        <div style={{ color: '#fff', padding: 16 }}>BlazeNet 管理后台</div>
        <Menu
          theme="dark"
          mode="inline"
          items={items}
          selectedKeys={[selected]}
          onClick={(e) => setSelected(e.key)}
        />
      </Sider>
      <Layout>
        <Header style={{ background: '#fff', display: 'flex', justifyContent: 'flex-end' }}>
          <Button
            onClick={() => {
              logout();
              window.location.reload();
            }}
          >
            退出登录
          </Button>
        </Header>
        <Content style={{ margin: 16 }}>
          <Suspense fallback={<div>页面加载中…</div>}>
            <PageView page={PAGES[selected]} />
          </Suspense>
        </Content>
      </Layout>
    </Layout>
  );
}

function LoginPage() {
  const [form] = Form.useForm();
  const onFinish = async (values: { username: string; password: string }) => {
    try {
      await login(values.username, values.password);
      window.location.reload();
    } catch (error) {
      message.error(String(error));
    }
  };
  return (
    <div style={{ maxWidth: 360, margin: '120px auto' }}>
      <Card title="登录">
        <Form form={form} onFinish={onFinish}>
          <Form.Item name="username" rules={[{ required: true, message: '请输入用户名' }]}>
            <Input placeholder="用户名" />
          </Form.Item>
          <Form.Item name="password" rules={[{ required: true, message: '请输入密码' }]}>
            <Input.Password placeholder="密码" />
          </Form.Item>
          <Button type="primary" htmlType="submit" block>
            登录
          </Button>
        </Form>
      </Card>
    </div>
  );
}

export default function App() {
  const [hasToken, setHasToken] = useState(() => Boolean(localStorage.getItem('blazenet-token')));
  return hasToken ? <AdminLayout /> : <LoginPage />;
}

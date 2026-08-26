import { useState, type ReactNode } from 'react';
import { Button, Card, Form, Input, Layout, Menu, Table, Tag, Typography, message } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { api, login, logout } from './api';

const { Sider, Content, Header } = Layout;

function useList<T>(endpoint: string) {
  const [data, setData] = useState<T[]>([]);
  const [loading, setLoading] = useState(false);
  const load = async () => {
    setLoading(true);
    try {
      setData(await api<T[]>(endpoint));
    } catch (error) {
      message.error(String(error));
    } finally {
      setLoading(false);
    }
  };
  return { data, loading, load };
}

function UsersPage() {
  const users = useList<{ id: number; username: string }>('/api/users');
  const places = useList<{ id: number; name: string; region: string }>('/api/places');
  const groups = useList<{ id: number; name: string }>('/api/groups');
  useState(() => {
    void users.load();
    void places.load();
    void groups.load();
  });
  return (
    <SpaceCards>
      <Table
        rowKey="id"
        loading={users.loading}
        dataSource={users.data}
        columns={[
          { title: 'ID', dataIndex: 'id' },
          { title: '用户名', dataIndex: 'username' },
        ]}
        pagination={false}
      />
      <Table
        rowKey="id"
        loading={places.loading}
        dataSource={places.data}
        columns={[
          { title: 'ID', dataIndex: 'id' },
          { title: '名称', dataIndex: 'name' },
          { title: '地区', dataIndex: 'region' },
        ]}
        pagination={false}
      />
      <Table
        rowKey="id"
        loading={groups.loading}
        dataSource={groups.data}
        columns={[
          { title: 'ID', dataIndex: 'id' },
          { title: '名称', dataIndex: 'name' },
        ]}
        pagination={false}
      />
    </SpaceCards>
  );
}

function SpaceCards({ children }: { children: ReactNode }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <Card title="账号">{(children as ReactNode[])[0]}</Card>
      <Card title="场所">{(children as ReactNode[])[1]}</Card>
      <Card title="分组">{(children as ReactNode[])[2]}</Card>
    </div>
  );
}

function NodesPage() {
  const nodes = useList<{
    id: number;
    node_type: string;
    endpoint_id: string;
    status: string;
  }>('/api/nodes');
  useState(() => {
    void nodes.load();
  });
  const columns: ColumnsType<(typeof nodes.data)[number]> = [
    { title: 'ID', dataIndex: 'id' },
    { title: '类型', dataIndex: 'node_type' },
    { title: '端点', dataIndex: 'endpoint_id' },
    {
      title: '状态',
      dataIndex: 'status',
      render: (value: string) => <Tag color={value === 'online' ? 'green' : 'red'}>{value}</Tag>,
    },
  ];
  return <Table rowKey="id" loading={nodes.loading} dataSource={nodes.data} columns={columns} />;
}

function GamesPage() {
  const games = useList<{
    id: number;
    name: string;
    status: string;
    current_version: number;
    latest_version: number;
  }>('/api/games');
  useState(() => {
    void games.load();
  });
  const columns: ColumnsType<(typeof games.data)[number]> = [
    { title: 'ID', dataIndex: 'id' },
    { title: '名称', dataIndex: 'name' },
    { title: '状态', dataIndex: 'status' },
    { title: '当前版本', dataIndex: 'current_version' },
    { title: '最新版本', dataIndex: 'latest_version' },
  ];
  return <Table rowKey="id" loading={games.loading} dataSource={games.data} columns={columns} />;
}

function TasksPage() {
  const tasks = useList<{
    id: number;
    node_id: number;
    game_id: number;
    status: string;
  }>('/api/tasks');
  useState(() => {
    void tasks.load();
  });
  const columns: ColumnsType<(typeof tasks.data)[number]> = [
    { title: 'ID', dataIndex: 'id' },
    { title: '节点', dataIndex: 'node_id' },
    { title: '游戏', dataIndex: 'game_id' },
    { title: '状态', dataIndex: 'status' },
  ];
  return <Table rowKey="id" loading={tasks.loading} dataSource={tasks.data} columns={columns} />;
}

function StatusPage() {
  const [health, setHealth] = useState('未知');
  useState(() => {
    void api<string>('/healthz')
      .then(setHealth)
      .catch(() => setHealth('不可达'));
  });
  return (
    <Card title="系统状态">
      <Typography.Text>健康检查：</Typography.Text>
      <Tag color={health === 'ok' ? 'green' : 'red'}>{health}</Tag>
    </Card>
  );
}

function AuditPage() {
  const audits = useList<{
    id: number;
    time_ms: number;
    actor: string;
    action: string;
    detail: string;
  }>('/api/audit');
  useState(() => {
    void audits.load();
  });
  const columns: ColumnsType<(typeof audits.data)[number]> = [
    { title: 'ID', dataIndex: 'id' },
    {
      title: '时间',
      dataIndex: 'time_ms',
      render: (value: number) => new Date(value).toLocaleString(),
    },
    { title: '操作人', dataIndex: 'actor' },
    { title: '动作', dataIndex: 'action' },
    { title: '详情', dataIndex: 'detail' },
  ];
  return <Table rowKey="id" loading={audits.loading} dataSource={audits.data} columns={columns} />;
}

const PAGES: Record<string, ReactNode> = {
  users: <UsersPage />,
  nodes: <NodesPage />,
  games: <GamesPage />,
  tasks: <TasksPage />,
  status: <StatusPage />,
  audit: <AuditPage />,
};

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
        <Menu theme="dark" mode="inline" items={items} selectedKeys={[selected]} onClick={(e) => setSelected(e.key)} />
      </Sider>
      <Layout>
        <Header style={{ background: '#fff', display: 'flex', justifyContent: 'flex-end' }}>
          <Button onClick={() => { logout(); window.location.reload(); }}>退出登录</Button>
        </Header>
        <Content style={{ margin: 16 }}>{PAGES[selected]}</Content>
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

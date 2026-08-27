import { useState } from 'react';
import { Button, Card, Form, InputNumber, Select, Table, message } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { api } from '../api';
import { useList } from '../list';

type GameRow = {
  id: number;
  name: string;
  status: string;
  current_version: number;
  latest_version: number;
};

export default function GamesPage() {
  const games = useList<GameRow>('/api/games');
  const nodes = useList<{ id: number; node_type: string }>('/api/nodes');
  const [form] = Form.useForm();
  useState(() => {
    void games.load();
    void nodes.load();
  });
  const columns: ColumnsType<GameRow> = [
    { title: 'ID', dataIndex: 'id' },
    { title: '名称', dataIndex: 'name' },
    { title: '状态', dataIndex: 'status' },
    { title: '当前版本', dataIndex: 'current_version' },
    { title: '最新版本', dataIndex: 'latest_version' },
  ];
  const onRollback = async (values: { node_id: number; version: number }) => {
    const gameId = form.getFieldValue('game_id');
    if (!gameId) {
      message.warning('请先选择游戏');
      return;
    }
    await api(`/api/games/${gameId}/rollback`, {
      method: 'POST',
      body: JSON.stringify({ node_id: values.node_id, version: values.version }),
    });
    message.success('回滚任务已创建');
    form.resetFields();
    await games.load();
  };
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <Table rowKey="id" loading={games.loading} dataSource={games.data} columns={columns} />
      <Card title="发起回滚">
        <Form form={form} layout="inline" onFinish={onRollback}>
          <Form.Item name="game_id" rules={[{ required: true, message: '请选择游戏' }]}>
            <Select
              placeholder="游戏"
              style={{ width: 180 }}
              options={games.data.map((g) => ({ value: g.id, label: `${g.name}(${g.id})` }))}
            />
          </Form.Item>
          <Form.Item name="node_id" rules={[{ required: true, message: '请输入节点 ID' }]}>
            <InputNumber placeholder="节点 ID" style={{ width: 140 }} />
          </Form.Item>
          <Form.Item name="version" rules={[{ required: true, message: '请输入目标版本' }]}>
            <InputNumber placeholder="目标版本" min={1} style={{ width: 140 }} />
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit">
              回滚
            </Button>
          </Form.Item>
        </Form>
      </Card>
    </div>
  );
}

import { useEffect, useState } from 'react';
import { Button, Card, Form, InputNumber, Select, Table, message } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { api, subscribeEvents } from '../api';
import { useList } from '../list';

type TaskRow = { id: number; node_id: number; game_id: number; status: string };

export default function TasksPage() {
  const tasks = useList<TaskRow>('/api/tasks');
  const nodes = useList<{ id: number; node_type: string }>('/api/nodes');
  const games = useList<{ id: number; latest_version: number }>('/api/games');
  const [form] = Form.useForm();
  useState(() => {
    void tasks.load();
    void nodes.load();
    void games.load();
  });
  useEffect(
    () =>
      subscribeEvents(() => {
        void tasks.load();
        void nodes.load();
        void games.load();
      }),
    [],
  );
  const columns: ColumnsType<TaskRow> = [
    { title: 'ID', dataIndex: 'id' },
    { title: '节点', dataIndex: 'node_id' },
    { title: '游戏', dataIndex: 'game_id' },
    { title: '状态', dataIndex: 'status' },
  ];
  const onCreate = async (values: {
    node_id: number;
    game_id: number;
    version: number;
    kind: string;
  }) => {
    await api('/api/tasks', { method: 'POST', body: JSON.stringify(values) });
    message.success('任务已创建');
    form.resetFields();
    await tasks.load();
  };
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <Card title="创建任务">
        <Form form={form} layout="inline" onFinish={onCreate}>
          <Form.Item name="node_id" rules={[{ required: true, message: '请选择节点' }]}>
            <Select
              placeholder="节点"
              style={{ width: 180 }}
              options={nodes.data.map((n) => ({
                value: n.id,
                label: `节点 ${n.id}(${n.node_type})`,
              }))}
            />
          </Form.Item>
          <Form.Item name="game_id" rules={[{ required: true, message: '请选择游戏' }]}>
            <Select
              placeholder="游戏"
              style={{ width: 160 }}
              options={games.data.map((g) => ({ value: g.id, label: `游戏 ${g.id}` }))}
            />
          </Form.Item>
          <Form.Item name="version" rules={[{ required: true, message: '请输入版本' }]}>
            <InputNumber placeholder="版本" min={1} style={{ width: 120 }} />
          </Form.Item>
          <Form.Item name="kind" initialValue="UPDATE" rules={[{ required: true }]}>
            <Select
              style={{ width: 130 }}
              options={[
                { value: 'DOWNLOAD', label: '下载' },
                { value: 'UPDATE', label: '更新' },
                { value: 'ROLLBACK', label: '回滚' },
              ]}
            />
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit">
              创建
            </Button>
          </Form.Item>
        </Form>
      </Card>
      <Table
        rowKey="id"
        loading={tasks.loading}
        dataSource={tasks.data}
        columns={columns}
        virtual
        scroll={{ y: 480 }}
        pagination={false}
      />
    </div>
  );
}

import { useState } from 'react';
import { Table, Tag } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useList } from '../list';

type NodeRow = { id: number; node_type: string; endpoint_id: string; status: string };

export default function NodesPage() {
  const nodes = useList<NodeRow>('/api/nodes');
  useState(() => {
    void nodes.load();
  });
  const columns: ColumnsType<NodeRow> = [
    { title: 'ID', dataIndex: 'id' },
    { title: '类型', dataIndex: 'node_type' },
    { title: '端点', dataIndex: 'endpoint_id' },
    {
      title: '状态',
      dataIndex: 'status',
      render: (value: string) => <Tag color={value === 'online' ? 'green' : 'red'}>{value}</Tag>,
    },
  ];
  return (
    <Table
      rowKey="id"
      loading={nodes.loading}
      dataSource={nodes.data}
      columns={columns}
      virtual
      scroll={{ y: 480 }}
      pagination={false}
    />
  );
}

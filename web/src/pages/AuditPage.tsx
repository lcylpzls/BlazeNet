import { useState } from 'react';
import { Table } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useList } from '../list';

type AuditRow = { id: number; time_ms: number; actor: string; action: string; detail: string };

export default function AuditPage() {
  const audits = useList<AuditRow>('/api/audit');
  useState(() => {
    void audits.load();
  });
  const columns: ColumnsType<AuditRow> = [
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
  return (
    <Table
      rowKey="id"
      loading={audits.loading}
      dataSource={audits.data}
      columns={columns}
      virtual
      scroll={{ y: 480 }}
      pagination={false}
    />
  );
}

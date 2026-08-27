import { useState } from 'react';
import { Table } from 'antd';
import { SpaceCards, useList } from '../list';

export default function UsersPage() {
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

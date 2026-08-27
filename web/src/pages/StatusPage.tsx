import { useState } from 'react';
import { Card, Tag, Typography } from 'antd';
import { api } from '../api';

export default function StatusPage() {
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

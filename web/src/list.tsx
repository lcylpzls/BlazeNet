import { useState, type ReactNode } from 'react';
import { Card, message } from 'antd';
import { api } from './api';

/** 通用列表加载 hook：拉取、错误提示、加载态。 */
export function useList<T>(endpoint: string) {
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

/** 账号/场所/分组三卡片布局（首屏性能：配合代码分包按需加载）。 */
export function SpaceCards({ children }: { children: ReactNode }) {
  const cards = children as ReactNode[];
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <Card title="账号">{cards[0]}</Card>
      <Card title="场所">{cards[1]}</Card>
      <Card title="分组">{cards[2]}</Card>
    </div>
  );
}

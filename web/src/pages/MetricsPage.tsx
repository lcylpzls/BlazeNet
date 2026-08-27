import { useEffect, useRef, useState } from 'react';
import { Card, Typography } from 'antd';
import { api } from '../api';
import { parseMetrics, type MetricsSnapshot } from '../metrics';

const LABELS: Record<string, string> = {
  blazenet_nodes_total: '节点总数',
  blazenet_nodes_online: '在线节点',
  blazenet_tasks_total: '任务总数',
  blazenet_tasks_running: '运行中任务',
  blazenet_tasks_done: '完成任务',
  blazenet_tasks_failed: '失败任务',
  blazenet_games_total: '游戏总数',
  blazenet_audits_total: '审计日志数',
};

/** 每 5 秒拉取一次 /metrics，保留最近 20 个采样点用于轻量趋势图（SVG，无额外依赖）。 */
export default function MetricsPage() {
  const [snapshot, setSnapshot] = useState<MetricsSnapshot>({});
  const [history, setHistory] = useState<MetricsSnapshot[]>([]);
  const timer = useRef<ReturnType<typeof setInterval> | null>(null);
  useEffect(() => {
    const load = async () => {
      try {
        const text = await api<string>('/metrics');
        const parsed = parseMetrics(text);
        setSnapshot(parsed);
        setHistory((prev) => [...prev.slice(-19), parsed]);
      } catch {
        // 指标拉取失败时保留上次数据
      }
    };
    void load();
    timer.current = setInterval(() => void load(), 5000);
    return () => {
      if (timer.current) {
        clearInterval(timer.current);
      }
    };
  }, []);
  const entries = Object.entries(snapshot).filter(([name]) => LABELS[name]);
  const points = history.map((item, index) => ({
    index,
    nodes: item.blazenet_nodes_online ?? 0,
    tasks: item.blazenet_tasks_running ?? 0,
  }));
  const width = 320;
  const height = 80;
  const maxNodes = Math.max(1, ...points.map((p) => p.nodes));
  const maxTasks = Math.max(1, ...points.map((p) => p.tasks));
  const polyline = (key: 'nodes' | 'tasks', max: number) =>
    points
      .map((p, i) => {
        const x = points.length > 1 ? (i / (points.length - 1)) * width : width / 2;
        const y = height - (p[key] / max) * height;
        return `${x},${y}`;
      })
      .join(' ');
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12 }}>
        {entries.map(([name, value]) => (
          <Card key={name} size="small" style={{ minWidth: 150 }}>
            <Typography.Text type="secondary">{LABELS[name]}</Typography.Text>
            <div style={{ fontSize: 24 }}>{value}</div>
          </Card>
        ))}
      </div>
      <Card title="在线节点/运行中任务趋势（最近 20 个采样）">
        <svg width={width} height={height} role="img" aria-label="指标趋势图">
          <polyline points={polyline('nodes', maxNodes)} fill="none" stroke="#1677ff" strokeWidth="2" />
          <polyline points={polyline('tasks', maxTasks)} fill="none" stroke="#52c41a" strokeWidth="2" />
        </svg>
      </Card>
    </div>
  );
}

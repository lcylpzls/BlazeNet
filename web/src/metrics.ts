// 三期 P3.5b：Prometheus 文本指标解析（仅取 blazenet_* 数值项）。
export type MetricsSnapshot = Record<string, number>;

export function parseMetrics(text: string): MetricsSnapshot {
  const out: MetricsSnapshot = {};
  for (const line of text.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }
    const space = trimmed.indexOf(' ');
    if (space <= 0) {
      continue;
    }
    const name = trimmed.slice(0, space);
    const value = Number(trimmed.slice(space + 1));
    if (name.startsWith('blazenet_') && Number.isFinite(value)) {
      out[name] = value;
    }
  }
  return out;
}

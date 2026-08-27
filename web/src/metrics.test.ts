import { describe, expect, it } from 'vitest';
import { parseMetrics } from './metrics';

describe('parseMetrics', () => {
  it('解析 blazenet 数值指标并忽略注释与其他行', () => {
    const text = [
      '# HELP blazenet_nodes_total 节点总数',
      '# TYPE blazenet_nodes_total gauge',
      'blazenet_nodes_total 3',
      'blazenet_tasks_done 7',
      'other_metric 1',
      'not-a-number x',
    ].join('\n');
    expect(parseMetrics(text)).toEqual({
      blazenet_nodes_total: 3,
      blazenet_tasks_done: 7,
    });
  });

  it('空文本返回空对象', () => {
    expect(parseMetrics('')).toEqual({});
  });
});

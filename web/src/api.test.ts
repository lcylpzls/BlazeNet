import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api, login, logout } from './api';

const memory = new Map<string, string>();

describe('后台 API 客户端', () => {
  beforeEach(() => {
    memory.clear();
    vi.stubGlobal('localStorage', {
      getItem: (key: string) => memory.get(key) ?? null,
      setItem: (key: string, value: string) => {
        memory.set(key, value);
      },
      removeItem: (key: string) => {
        memory.delete(key);
      },
      clear: () => memory.clear(),
    });
  });

  afterEach(() => {
    memory.clear();
    vi.unstubAllGlobals();
  });

  it('登录成功保存 token', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        new Response(JSON.stringify({ token: 't1' }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        }),
      ),
    );
    const token = await login('admin', 'secret');
    expect(token).toBe('t1');
    expect(localStorage.getItem('blazenet-token')).toBe('t1');
  });

  it('请求自动携带 Bearer token', async () => {
    localStorage.setItem('blazenet-token', 'tok');
    const fetchMock = vi.fn(async () =>
      new Response(JSON.stringify([1]), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    );
    vi.stubGlobal('fetch', fetchMock);
    const data = await api<number[]>('/api/nodes');
    expect(data).toEqual([1]);
    const init = fetchMock.mock.calls[0]?.[1] as RequestInit;
    expect(init.headers).toMatchObject({ Authorization: 'Bearer tok' });
  });

  it('非 2xx 响应抛出错误', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response('', { status: 500 })));
    await expect(api('/api/x')).rejects.toThrow('请求失败: 500');
  });

  it('204 响应返回 undefined', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(null, { status: 204 })));
    const result = await api('/api/x');
    expect(result).toBeUndefined();
  });

  it('退出登录清除 token', () => {
    localStorage.setItem('blazenet-token', 'x');
    logout();
    expect(localStorage.getItem('blazenet-token')).toBeNull();
  });
});

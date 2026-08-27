// 后台 API 客户端：同一源相对路径，Token 存本地会话。
const REQUEST_TIMEOUT_MS = 8000;

async function request<T>(path: string, init?: RequestInit, retries = 1): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(path, {
      ...init,
      signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
        ...(localStorage.getItem('blazenet-token')
          ? { Authorization: `Bearer ${localStorage.getItem('blazenet-token')}` }
          : {}),
      },
    });
    if (!response.ok) {
      throw new Error(`请求失败: ${response.status}`);
    }
    if (response.status === 204) {
      return undefined as T;
    }
    const contentType = response.headers.get('content-type') ?? '';
    return (contentType.includes('application/json')
      ? await response.json()
      : await response.text()) as T;
  } catch (error) {
    if (
      retries > 0 &&
      (!(error instanceof Error) || !error.message.startsWith('请求失败'))
    ) {
      return request(path, init, retries - 1);
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  return request<T>(path, init);
}

export async function login(username: string, password: string): Promise<string> {
  const reply = await api<{ token: string }>('/api/login', {
    method: 'POST',
    body: JSON.stringify({ username, password }),
  });
  localStorage.setItem('blazenet-token', reply.token);
  return reply.token;
}

export function logout(): void {
  localStorage.removeItem('blazenet-token');
}

/**
 * 三期 P3.5b：订阅调度中心 SSE 增量事件（任务创建/状态变化/取消），
 * 返回取消订阅函数；浏览器不支持 EventSource 时返回空操作。
 */
export function subscribeEvents(onEvent: () => void): () => void {
  if (typeof EventSource === 'undefined') {
    return () => undefined;
  }
  const source = new EventSource('/api/events');
  source.onmessage = () => onEvent();
  return () => source.close();
}

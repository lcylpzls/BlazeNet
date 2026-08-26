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
    return (await response.json()) as T;
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

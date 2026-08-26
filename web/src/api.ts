// 后台 API 客户端：同一源相对路径，Token 存本地会话。
export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const token = localStorage.getItem('blazenet-token') ?? '';
  const response = await fetch(path, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });
  if (!response.ok) {
    throw new Error(`请求失败: ${response.status}`);
  }
  if (response.status === 204) {
    return undefined as T;
  }
  return (await response.json()) as T;
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

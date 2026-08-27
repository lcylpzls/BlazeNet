import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  build: {
    rollupOptions: {
      output: {
        // 三期 P3.5：第三方库独立 chunk，配合页面分包实现稳定缓存与按需加载。
        manualChunks: {
          antd: ['antd'],
        },
      },
    },
  },
  test: {
    environment: 'jsdom',
  },
  server: {
    proxy: {
      '/api': 'http://127.0.0.1:50051',
    },
  },
});

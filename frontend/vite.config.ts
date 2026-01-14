import path from 'node:path'
import { defineConfig } from 'vite'
import tailwindcss from '@tailwindcss/vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue(), tailwindcss()],
  server: {
    proxy: {
      // Whenever you fetch something starting with /api, 
      // Vite will send it to your backend.
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      }
    },
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  logLevel: 'info'
})
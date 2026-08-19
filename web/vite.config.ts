import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Build output must land inside the Go embed package so the binary can
// serve the SPA. The relative path resolves from this file's directory.
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: '../internal/web/dist',
    emptyOutDir: true,
  },
  server: {
    proxy: {
      '/events': 'http://localhost:8080',
      '/api': 'http://localhost:8080',
      '/healthz': 'http://localhost:8080',
      '/metrics': 'http://localhost:8080',
    },
  },
})

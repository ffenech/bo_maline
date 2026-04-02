import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      }
    }
  },
  build: {
    rollupOptions: {
      output: {
        manualChunks: {
          'react-vendor': ['react', 'react-dom', 'react-router-dom'],
          'charts': ['recharts'],
          'mermaid': ['mermaid', 'cytoscape', 'cytoscape-cose-bilkent', 'cytoscape-fcose'],
          'supabase': ['@supabase/supabase-js']
        }
      }
    },
    chunkSizeWarningLimit: 1000
  }
})

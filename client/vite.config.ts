import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// Everything under /graphql (including /graphql/ws, ws: true handles the
// upgrade) is proxied to the Rust server, so the SPA needs no CORS setup.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      '/graphql': {
        target: 'http://localhost:8088',
        ws: true,
      },
    },
  },
})

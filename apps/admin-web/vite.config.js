import { sveltekit } from '@sveltejs/kit/vite';
import adapter from '@sveltejs/adapter-static';
import { defineConfig } from 'vite';

export default defineConfig({
  server: {
    proxy: {
      '/api': {
        target: process.env.IPTO_API_URL || 'http://localhost:8080',
        changeOrigin: true
      }
    }
  },
  plugins: [
    sveltekit({
      adapter: adapter({
        pages: 'build',
        assets: 'build',
        fallback: '200.html'
      }),
      prerender: {
        handleUnseenRoutes: 'ignore'
      }
    })
  ]
});

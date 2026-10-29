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
        pages: '../../implementations/java/quarkus-app/src/main/resources/META-INF/resources',
        assets: '../../implementations/java/quarkus-app/src/main/resources/META-INF/resources'
      }),
      prerender: {
        handleUnseenRoutes: 'ignore'
      }
    })
  ]
});

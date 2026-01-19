import adapter from '@sveltejs/adapter-static';

const config = {
  kit: {
    adapter: adapter({
      pages: '../quarkus-app/src/main/resources/META-INF/resources',
      assets: '../quarkus-app/src/main/resources/META-INF/resources'
    }),
    prerender: {
      handleUnseenRoutes: 'ignore'
    }
  }
};

export default config;

package org.gautelis.ipto.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.List;

@ConfigMapping(prefix = "ipto")
public interface IptoConfig {
    Repository repository();

    Graphql graphql();

    interface Repository {
        @WithDefault("org.gautelis.ipto.repo.search.query.adapters.PostgresAdapter")
        String databaseAdapter();

        @WithDefault("1000")
        int eventsThreshold();

        @WithDefault("org.gautelis.ipto.repo.listeners.NopActionListener")
        List<String> eventsListeners();

        Cache cache();
    }

    interface Cache {
        @WithDefault("true")
        boolean lookBehind();

        @WithDefault("1000")
        int maxSize();

        @WithDefault("60")
        int idleCheckInterval();
    }

    interface Graphql {
        @WithDefault("")
        String sdlResource();
    }
}

package org.gautelis.ipto.api;

import io.vertx.ext.web.Router;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

/** Serve the SvelteKit shell for known UI routes, including dynamic editor URLs. */
@ApplicationScoped
public class AdminWebRoutes {
    void register(@Observes Router router) {
        if (getClass().getClassLoader().getResource("META-INF/resources/200.html") != null) {
            router.getWithRegex("/(?:admin(?:/attributes/(?:new|[0-9]+/edit)|/records/[0-9]+/edit)?|trees|search)/?")
                    .handler(context -> context.reroute("/200.html"));
        }
    }
}

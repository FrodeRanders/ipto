package org.gautelis.ipto.repo.model;

import org.gautelis.ipto.repo.db.Database;
import org.gautelis.ipto.repo.exceptions.DatabaseConnectionException;
import org.gautelis.ipto.repo.exceptions.DatabaseReadException;
import org.gautelis.ipto.repo.model.utils.TimedExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Tenant {
    private static final Logger log = LoggerFactory.getLogger(Tenant.class);

    public static class TenantInfo {
        public int id = 0;
        public String name = null;
        public String description = null;
        public Timestamp created = null;
    }

    private static final Map<String, Tenant.TenantInfo> tenants = new HashMap<>();

    private Tenant() {}

    private static synchronized Map<String, Tenant.TenantInfo> fetchTenants(Context ctx) throws DatabaseConnectionException, DatabaseReadException {

        if (tenants.isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace("Fetching known tenants");
            }

            TimedExecution.run(ctx.getTimingData(), "fetch tenants", () -> Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().tenantsGetAll(), pStmt -> {
                try (ResultSet rs = Database.executeQuery(pStmt)) {
                    while (rs.next()) {
                        Tenant.TenantInfo info = new Tenant.TenantInfo();
                        info.id = rs.getInt("tenantid");
                        info.name = rs.getString("name");
                        info.description = rs.getString("description");
                        info.created = rs.getTimestamp("created");

                        tenants.put(info.name.toLowerCase(), info);
                    }
                }
            }));
        }
        return tenants;
    }

    /**
     * Get tenant identified by id
     *
     * @param tenantId id of tenant
     * @return TenantInfo if attribute exists
     */
    /* package accessible only */
    static Optional<Tenant.TenantInfo> getTenant(Context ctx, int tenantId) throws DatabaseConnectionException, DatabaseReadException {

        Map<String, Tenant.TenantInfo> data = fetchTenants(ctx);

        for (Tenant.TenantInfo info : data.values()) {
            if (info.id == tenantId) {
                return Optional.of(info);
            }
        }
        return Optional.empty();
    }

    /**
     * Get tenant identified by name.
     *
     * @param name name of tenant
     * @return TenantInfo if tenant exists
     */
    /* package accessible only */
    static Optional<Tenant.TenantInfo> getTenant(Context ctx, String name) throws DatabaseConnectionException, DatabaseReadException {
        Map<String, Tenant.TenantInfo> info = fetchTenants(ctx);
        return Optional.ofNullable(info.get(name.toLowerCase()));
    }

}

package org.gautelis.repo;

import org.postgresql.ds.PGSimpleDataSource;
import org.gautelis.vopn.db.Database;

public class PostgresDataSourcePreparer implements Database.DataSourcePreparation<PGSimpleDataSource> {
    @Override
    public PGSimpleDataSource prepare(
            PGSimpleDataSource ds,
            Database.Configuration cf
    ) {
        String[] servers = new String[]{"localhost"};
        ds.setServerNames(servers);

        int[] ports = new int[]{ 1402 };
        ports[0] = cf.port();

        ds.setPortNumbers(ports);
        ds.setDatabaseName(cf.database()); // std
        ds.setUser(cf.user()); // std
        ds.setPassword(cf.password()); // std
        return ds;
    }
}

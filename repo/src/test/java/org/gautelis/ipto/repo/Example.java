package org.gautelis.ipto.repo;

import org.gautelis.ipto.repo.db.Database;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.search.model.*;
import org.gautelis.ipto.repo.search.query.DatabaseAdapter;
import org.gautelis.ipto.repo.search.query.QueryBuilder;
import org.gautelis.ipto.repo.search.query.SearchExpression;
import org.gautelis.ipto.repo.search.query.SearchOrder;
import org.gautelis.ipto.repo.search.SearchResult;
import org.gautelis.ipto.repo.search.UnitSearch;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

public class Example {

    private int getTenantId(String tenantName, Repository repo) {
        Optional<Integer> tenantId = repo.tenantNameToId(tenantName);
        if (tenantId.isEmpty()) {
            throw new RuntimeException("Unknown tenant: " + tenantName);
        }
        return tenantId.get();
    }

    private int getAttributeId(String attributeName, Repository repo) {
        Optional<Integer> attributeId = repo.attributeNameToId(attributeName);
        if (attributeId.isEmpty()) {
             throw new RuntimeException("Unknown attribute: " + attributeName);
        }
        return attributeId.get();
    }
    
    public void createAUnitAndAssignAttributes() {
        Repository repo = RepositoryFactory.getRepository();
        int tenantId = getTenantId("SCRATCH", repo); // SCRATCH is the default space

        // Create a unit, with some random name. Names does not have to be unique
        // as they are not normally used to identify units.
        Unit unit = repo.createUnit(tenantId, "unit five");

        // Add "dc:title" (already known to the system).
        unit.withAttributeValue("dce:title", String.class, value -> {
            value.add("Æ e nordlending æ");
        });

        // Add "dc:date" (already known to the system).
        unit.withAttributeValue("dce:date", Instant.class, value -> {
            value.add(Instant.now());
        });

        // Store this new unit to database
        repo.storeUnit(unit);
    }

    public void searchForAUnitBasedOnAttributes() {
        Repository repo = RepositoryFactory.getRepository();
        int tenantId = getTenantId("SCRATCH", repo); // SCRATCH is the default space

        // Constraints specified for the unit itself (such as being 'EFFECTIVE')
        SearchExpression expr = QueryBuilder.constrainToSpecificTenant(tenantId);
        expr = QueryBuilder.assembleAnd(expr, QueryBuilder.constrainToSpecificStatus(Unit.Status.EFFECTIVE));

        // Constrain to time-related attribute
        SearchItem<Instant> timestampSearchItem = new TimeAttributeSearchItem("dce:date", Operator.LEQ, Instant.now());
        expr = QueryBuilder.assembleAnd(expr, timestampSearchItem);

        // Constrain to string attribute
        SearchItem<String> stringSearchItem = new StringAttributeSearchItem("dce:title", Operator.EQ, "Ajj som bara den");
        expr = QueryBuilder.assembleAnd(expr, stringSearchItem);

        // 
        SearchOrder order = SearchOrder.getDefaultOrder(); // descending on creation time

        // Now we can either use canned search (that produces units) or
        // search "manually" where we retrieve individual fields of units
        // without actually creating Unit objects.
        if (/* canned search? */ true) {
            SearchResult result = repo.searchUnit(
                    /* paging stuff */ 0, 5, 100,
                    expr, order
            );

            Collection<Unit> units = result.results();
            for (Unit unit : units) {
                System.out.println("Found: " + unit);
            }
        } else {
            // Search "manually", in which case no Unit:s are instantiated
            DatabaseAdapter searchAdapter = repo.getDatabaseAdapter();
            UnitSearch usd = new UnitSearch(expr, order, /* selectionSize */ 5);

            try {
                repo.withConnection(conn -> searchAdapter.search(conn, usd, repo.getTimingData(),
                        rs -> {
                            while (rs.next()) {
                                int i = 0;
                                int _tenantId = rs.getInt(++i);
                                long _unitId = rs.getLong(++i);
                                Timestamp _created = rs.getTimestamp(++i);

                                System.out.println("Found: tenantId=" + _tenantId + " unitId=" + _unitId + " created=" + _created);
                            }
                        }));
            } catch (SQLException sqle) {
                throw new RuntimeException("Could not search: " + Database.squeeze(sqle), sqle);
            }
        }
    }
}
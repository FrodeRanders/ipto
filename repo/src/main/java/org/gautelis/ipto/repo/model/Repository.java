/*
 * Copyright (C) 2024-2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gautelis.ipto.repo.model;

import com.fasterxml.uuid.Generators;
import org.gautelis.ipto.repo.db.Database;
import org.gautelis.ipto.repo.exceptions.*;
import org.gautelis.ipto.repo.listeners.ActionListener;
import org.gautelis.ipto.repo.model.associations.ExternalAssociation;
import org.gautelis.ipto.repo.model.associations.InternalRelation;
import org.gautelis.ipto.repo.model.attributes.Attribute;
import org.gautelis.ipto.repo.model.cache.UnitFactory;
import org.gautelis.ipto.repo.model.locks.Lock;
import org.gautelis.ipto.repo.model.locks.LockType;
import org.gautelis.ipto.repo.model.utils.TimedExecution;
import org.gautelis.ipto.repo.model.utils.TimingData;
import org.gautelis.ipto.repo.search.SearchResult;
import org.gautelis.ipto.repo.search.UnitSearch;
import org.gautelis.ipto.repo.search.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.*;


public class Repository {
    private static final Logger log = LoggerFactory.getLogger(Repository.class);
    private final Map<String, ActionListener> actionListeners = new HashMap<>();

    private final Context context;
    private final int eventThreshold;

    public Repository(Context context, int eventThreshold, Map<String, ActionListener> actionListeners) {
        this.context = context;
        this.eventThreshold = eventThreshold;
        this.actionListeners.putAll(actionListeners);
    }

    /**
     * Gives access to a database adapter, from which solution-related SQL can
     * be retrieved.
     */
    public DatabaseAdapter getDatabaseAdapter() {
        return context.getDatabaseAdapter();
    }


    /**
     * Creates a unit for specified tenant with the specified name.
     * @param tenantId id of tenant
     * @param name name of unit (may be null)
     * @param correlationId a correlation id specific for this unit
     * @return a new unit, not yet persisted
     * @throws DatabaseConnectionException
     * @throws DatabaseReadException
     * @throws DatabaseWriteException
     * @throws ConfigurationException
     */
    public Unit createUnit(
            int tenantId,
            String name,
            UUID correlationId
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, ConfigurationException {
        return new Unit(context, tenantId, name, correlationId);
    }

    /**
     * Creates a unit for specified tenant with the specified name.
     * @param tenantId id of tenant
     * @param name name of unit (may be null)
     * <p>
     * A correlation id specific for this unit is automatically created (UUID v7)
     * <p>
     * @return a new unit, not yet persisted
     * @throws DatabaseConnectionException
     * @throws DatabaseReadException
     * @throws DatabaseWriteException
     * @throws ConfigurationException
     */
    public Unit createUnit(
            int tenantId,
            String name
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, ConfigurationException {
        UUID correlationId = Generators.timeBasedEpochGenerator().generate(); // UUID v7
        return new Unit(context, tenantId, name, correlationId);
    }

    /**
     * Creates a unit for specified tenant. The unit is not given a
     * name -- which is quite OK.
     * @param tenantId id of tenant
     * <p>
     * A correlation id specific for this unit is automatically created (UUID v7)
     * <p>
     * @return a new unit, not yet persisted
     * @throws DatabaseConnectionException
     * @throws DatabaseReadException
     * @throws DatabaseWriteException
     * @throws ConfigurationException
     */
    public Unit createUnit(
            int tenantId
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, ConfigurationException {
        UUID correlationId = Generators.timeBasedEpochGenerator().generate(); // UUID v7
        return new Unit(context, tenantId, /* no name */ null, correlationId);
    }

    /**
     * Creates a unit for specified tenant with the specified correlation ID.
     * @param tenantId id of tenant
     * @param correlationId a correlation id specific for this unit
     * @return a new unit, not yet persisted
     * @throws DatabaseConnectionException
     * @throws DatabaseReadException
     * @throws DatabaseWriteException
     * @throws ConfigurationException
     */
    public Unit createUnit(
            int tenantId,
            UUID correlationId
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, ConfigurationException {
        return new Unit(context, tenantId, /* no name */ null, correlationId);
    }

    /**
     * Searches for <B>all</B> units with unit status 'PENDING_DISPOSITION'.
     */
    private SearchResult getDisposedUnits(
            int tenantId,
            int low, int high, int max
    ) throws InvalidParameterException {
        return getUnits(Unit.Status.PENDING_DISPOSITION, tenantId, low, high, max);
    }

    /**
     * Disposes specified units (specified by type) in <I>chunks</I>
     * (1000 at a time to avoid memory exhaustion).
     * <p>
     * The method returns when <B>all</B> objects are removed.
     * <p>
     * Observe that this method will not return until all objects are removed
     * and thus, if some units may not be removed, it will not return :-)
     */
    private void disposeUnits(
            int tenantId,
            PrintWriter writer
    ) throws InvalidParameterException {

        final int MAX_HITS = 1000;
        final boolean printProgress = (null != writer);

        SearchResult result = getDisposedUnits(tenantId, /* low */ 0, /* high */ MAX_HITS, MAX_HITS);

        while (result.totalNumberOfHits() > 0) {
            Collection<Unit> units = result.results();
            int count = 0;
            for (Unit unit : units) {
                try {
                    disposeUnit(unit);
                    ++count;
                } catch (Exception e) {
                    // Log problem, but do continue...
                    String info = "Failed to dispose unit " + unit.getReference();
                    info += ": " + e.getMessage();
                    log.warn(info);
                }
            }

            if (printProgress) {
                writer.println(" * chunk of " + count + " were removed");
                writer.flush();
            }

            if (count == 0) {
                writer.println(" ! ending");
                writer.flush();
                break;
            }
            result = getDisposedUnits(tenantId, /* low */ 0, /* high */ MAX_HITS, MAX_HITS);
        }
    }


    /**
     * Dispose all objects marked as 'pending disposition'.
     * <p>
     * Only objects with unit status of <B>PENDING_DISPOSITION</B>
     * are disposed.
     *
     * @param writer an (optional) writer onto which progress information is printed
     */
    public void dispose(
            int tenantId,
            PrintWriter writer
    ) throws InvalidParameterException {

        final boolean printProgress = (null != writer);

        if (printProgress) {
            writer.println("Disposing units for tenant " + tenantId);
            writer.flush();
        }
        disposeUnits(tenantId, writer);
    }


    /**
     * Searches for <B>all</B> units with specified status.
     */
    private SearchResult getUnits(
            Unit.Status status,
            int tenantId,
            int low, int high, int max
    ) throws InvalidParameterException {

        SearchExpression expr = QueryBuilder.constrainToSpecificStatus(status);

        // -- of specific tenant (if applicable) --
        if (tenantId > 0) {
            expr = QueryBuilder.assembleAnd(expr, QueryBuilder.constrainToSpecificTenant(tenantId));
        }

        // Search order (pick any)
        SearchOrder order = SearchOrder.getDefaultOrder();

        // Perform search
        SearchExpression finalExpr = expr;
        return TimedExecution.run(context.getTimingData(), "search", () -> searchUnit(low, high, max, finalExpr, order));
    }

    /**
     * Fetch an existing unit of latest version.
     */
    public Optional<Unit> getUnit(
            int tenantId,
            long unitId
    ) throws DatabaseConnectionException, DatabaseReadException {

        Optional<Unit> unit = TimedExecution.run(context.getTimingData(), "resurrect unit", () -> UnitFactory.resurrectUnit(context, tenantId, unitId));
        unit.ifPresent(_unit -> generateActionEvent(
                _unit,
                ActionEvent.Type.ACCESSED,
                "Unit accessed"
        ));
        return unit;
    }

    /**
     * Fetch an existing unit of specific version.
     */
    public Optional<Unit> getUnit(
            int tenantId,
            long unitId,
            int unitVersion
    ) throws DatabaseConnectionException, DatabaseReadException {

        Optional<Unit> unit = TimedExecution.run(context.getTimingData(), "resurrect unit", () -> UnitFactory.resurrectUnit(context, tenantId, unitId, unitVersion));
        unit.ifPresent(_unit -> generateActionEvent(
                _unit,
                ActionEvent.Type.ACCESSED,
                "Unit accessed"
        ));
        return unit;
    }


    /**
     * Fetches an existing unit from resultset.
     */
    private Optional<Unit> getUnit(
            ResultSet rs
    ) throws DatabaseReadException {

        Optional<Unit> unit = TimedExecution.run(context.getTimingData(), "resurrect unit", () -> UnitFactory.resurrectUnit(context, rs));
        unit.ifPresent(_unit -> generateActionEvent(
                _unit,
                ActionEvent.Type.ACCESSED,
                "Unit accessed"
        ));
        return unit;
    }

    /**
     * Checks existence of unit.
     *
     * @param tenantId type id
     * @param unitId unit id requested
     * @return boolean true if unit exists, false otherwise
     */
    public boolean unitExists(
            int tenantId,
            long unitId
    ) throws DatabaseConnectionException, DatabaseReadException {
        return TimedExecution.run(context.getTimingData(), "unit exists", () -> UnitFactory.unitExists(context, tenantId, unitId));
    }

    /**
     * Flush unit cache.
     */
    public void flushCache() {
        TimedExecution.run(context.getTimingData(), "unit cache flush", UnitFactory::flush);
    }

    /**
     * Store a unit to persistent storage.
     */
    public void storeUnit(
            Unit unit
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, AttributeTypeException, AttributeValueException, UnitReadOnlyException, UnitLockedException, InvalidParameterException, ConfigurationException, SystemInconsistencyException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }

        // --- Applicability control ---
        if (unit.isReadOnly()) {
            throw new UnitReadOnlyException(
                    "Unit " + unit.getReference() + " is read only");
        }

        if (!unit.isNew()) {
            if (unit.isLocked()) {
                for (Lock lock : unit.getLocks()) {
                    throw new UnitLockedException(
                            "Unit " + unit.getReference() + " has lock: " + lock);
                }
            }
        }

        log.debug("Storing unit {}", unit.getReference());
        TimedExecution.run(context.getTimingData(), "store unit", unit::store);

        generateActionEvent(
                unit,
                ActionEvent.Type.UPDATED,
                "Unit stored"
        );
    }

    /**
     * Associates a unit with an external resource (reference).
     * <p>
     */
    public void addRelation(
            Unit unit, AssociationType assocType, Unit otherUnit
    ) throws DatabaseConnectionException, DatabaseWriteException, InvalidParameterException, ConfigurationException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }
        if (null == otherUnit) {
            throw new InvalidParameterException("no other unit");
        }

        TimedExecution.run(context.getTimingData(), "create relation", () -> InternalRelation.create(
                context, unit.getTenantId(), unit.getUnitId(), assocType, otherUnit.getTenantId(), otherUnit.getUnitId()
        ));

        generateActionEvent(
                unit,
                ActionEvent.Type.ASSOCIATION_ADDED,
                assocType + " to " + otherUnit.getReference() + " created"
        );
    }

    /**
     * Removes an external association (reference) from a unit.
     * <p>
     * <I>This method has side effects: If, as a result of calling
     * this method, the internal status of the unit should be
     * updated, a request for this status transition is issued.
     * This information is not persisted to database though. You
     * should be aware of the fact that you may have a <B>modified</B>
     * unit after this call.</I>
     * <p>
     * Currently only accepts the Association.CASE_ASSOCIATION
     * association.
     */
    public void removeRelation(
            Unit unit, AssociationType assocType, Unit otherUnit
    ) throws DatabaseConnectionException, DatabaseWriteException, InvalidParameterException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }
        if (null == otherUnit) {
            throw new InvalidParameterException("no other unit");
        }

        TimedExecution.run(context.getTimingData(), "remove relation", () -> InternalRelation.remove(
                context, unit.getTenantId(), unit.getUnitId(), assocType, otherUnit.getTenantId(), otherUnit.getUnitId()
        ));

        generateActionEvent(
                unit,
                ActionEvent.Type.ASSOCIATION_REMOVED,
                assocType + " to " + otherUnit.getReference() + " removed"
        );
    }


    /**
     * Associates a unit with an external resource (reference).
     * <p>
     * <I>This method has side effects: If, as a result of calling
     * this method, the internal status of the unit should be
     * updated, a request for this status transition is issued.
     * This information is not persisted to database though. You
     * should be aware of the fact that you may have a <B>modified</B>
     * unit after this call.</I>
     * <p>
     * Currently only accepts the Association.CASE_ASSOCIATION
     * association.
     */
    public void addAssociation(
            Unit unit, AssociationType assocType, String reference
    ) throws DatabaseConnectionException, DatabaseWriteException, InvalidParameterException, ConfigurationException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }
        if (null == reference || reference.isEmpty()) {
            throw new InvalidParameterException("no (external) reference");
        }

        TimedExecution.run(context.getTimingData(), "create assoc", () -> ExternalAssociation.create(
                context, unit.getTenantId(), unit.getUnitId(), assocType, reference
        ));

        generateActionEvent(
                unit,
                ActionEvent.Type.ASSOCIATION_ADDED,
                assocType + " to " + reference + " created"
        );
    }

    /**
     * Removes an external association (reference) from a unit.
     * <p>
     * <I>This method has side effects: If, as a result of calling
     * this method, the internal status of the unit should be
     * updated, a request for this status transition is issued.
     * This information is not persisted to database though. You
     * should be aware of the fact that you may have a <B>modified</B>
     * unit after this call.</I>
     */
    public void removeAssociation(
            Unit unit, AssociationType assocType, String reference
    ) throws DatabaseConnectionException, DatabaseWriteException, InvalidParameterException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }
        if (null == reference || reference.isEmpty()) {
            throw new InvalidParameterException("no (external) reference");
        }

        TimedExecution.run(context.getTimingData(), "remove assoc", () -> ExternalAssociation.remove(
                context, unit.getTenantId(), unit.getUnitId(), assocType, reference
        ));

        generateActionEvent(
                unit,
                ActionEvent.Type.ASSOCIATION_REMOVED,
                assocType + " to " + reference + " removed"
        );
    }


    /**
     * Lock unit.
     */
    public boolean lockUnit(
            Unit unit,
            LockType type,
            String purpose
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, InvalidParameterException, IllegalRequestException, ConfigurationException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }

        boolean success = TimedExecution.run(context.getTimingData(), "lock unit", () -> unit.lock(type, purpose));
        if (success) {
            generateActionEvent(
                    unit,
                    ActionEvent.Type.LOCKED,
                    "Unit locked"
            );
        }
        return success;
    }

    /**
     * Unlock unit.
     */
    public void unlockUnit(
            Unit unit
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, InvalidParameterException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }

        if (unit.isLocked()) {
            TimedExecution.run(context.getTimingData(), "unlock unit", unit::unlock);

            generateActionEvent(
                    unit,
                    ActionEvent.Type.UNLOCKED,
                    "Unit unlocked"
            );
        }
    }

    /**
     * Dispose unit if not locked
     */
    private boolean disposeUnit(
            Unit unit
    ) throws DatabaseConnectionException, DatabaseWriteException, DatabaseReadException, InvalidParameterException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }

        // Delete unit (all versions) if not locked.
        boolean isUndisposable = false;
        {
            for (Lock _lock : unit.getLocks()) {
                isUndisposable = true;
                break;
            }
        }
        if (isUndisposable) {
            return false;
        }

        TimedExecution.run(context.getTimingData(), "delete unit", unit::delete);

        generateActionEvent(
                unit,
                ActionEvent.Type.DELETED,
                "Unit deleted"
        );
        return true;
    }

    private void deleteUnit(
            Unit unit
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, AttributeTypeException, AttributeValueException, InvalidParameterException, IllegalRequestException, ConfigurationException, SystemInconsistencyException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }
        // We want to set state to pending disposition.
        TimedExecution.run(context.getTimingData(), "status transition", () -> unit.requestStatusTransition(Unit.Status.PENDING_DISPOSITION));

        generateActionEvent(
                unit,
                ActionEvent.Type.DELETED,
                "Unit deleted"
        );
    }


    /**
     * Activates unit.
     */
    public void activateUnit(
            Unit unit
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, AttributeTypeException, AttributeValueException, InvalidParameterException, IllegalRequestException, ConfigurationException, SystemInconsistencyException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }

        TimedExecution.run(context.getTimingData(), "activate unit", unit::activate);

        generateActionEvent(
                unit,
                ActionEvent.Type.UPDATED,
                "Unit activated"
        );
    }

    /**
     * Inactivates unit.
     */
    public void inactivateUnit(
            Unit unit
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, AttributeTypeException, AttributeValueException, UnitLockedException, InvalidParameterException, IllegalRequestException, ConfigurationException, SystemInconsistencyException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }

        // Check if unit is locked
        if (unit.isLocked()) {
            for (Lock lock : unit.getLocks()) {
                throw new UnitLockedException("Unit " + unit.getReference() + " has lock: " + lock);
            }
        }

        TimedExecution.run(context.getTimingData(), "inactivate unit", unit::inactivate);

        generateActionEvent(
                unit,
                ActionEvent.Type.UPDATED,
                "Unit inactivated"
        );
    }

    /**
     * Attribute search.
     *
     * @param reqlow  Lower number of units to retrieve
     * @param reqhigh Upper number of units to retrieve
     * @param maxhits Max number of units to search for
     * @param expr    Search expression
     * @param order   Sort order
     */
    public SearchResult searchUnit(
            final int reqlow, final int reqhigh, final int maxhits,
            SearchExpression expr,
            SearchOrder order
    ) throws InvalidParameterException {

        if (null == expr) {
            throw new InvalidParameterException("no search expression");
        }
        if (null == order) {
            throw new InvalidParameterException("no search order");
        }

        Collection<Unit> result = new LinkedList<>();
        int totalNumberOfHits[] = { 0 };

        try {
            UnitSearch sd;
            if (maxhits > 0) {
                sd = new UnitSearch(expr, SearchStrategy.SET_OPS, order, maxhits);
            } else {
                sd = new UnitSearch(expr, SearchStrategy.SET_OPS, order, reqlow, (reqhigh - reqlow));
            }

            Database.useConnection(context.getDataSource(),
            conn -> context.getDatabaseAdapter().search(conn, sd, context.getTimingData(), rs -> {
                int pagedPosition = 1;

                while (rs.next()) {
                    totalNumberOfHits[0]++;

                    int tenantId = rs.getInt("tenantid");
                    long unitId = rs.getLong("unitid");

                    try {
                        // Skip entries that does not fit the specified "page".
                        if (pagedPosition >= reqlow && pagedPosition <= reqhigh) {
                            Optional<Unit> unit = UnitFactory.resurrectUnit(context, tenantId, unitId);
                            unit.ifPresent(result::add);
                        }
                        pagedPosition++;

                    } catch (Exception e) {
                        if (log.isDebugEnabled()) {
                            String info = "Failed to resurrect unit from search result: " + e.getMessage();
                            log.debug(info);
                        }
                    }
                }
            }));
        } catch (Exception e) {
            log.error("Failure in search", e);
        }
        return new SearchResult(result, totalNumberOfHits[0]);
    }

    /**
     * Instantiates an attribute
     */
    public Optional<Attribute<?>> instantiateAttribute(String attributeName) throws DatabaseConnectionException, DatabaseReadException {
        Optional<KnownAttributes.AttributeInfo> attributeInfo = getAttributeInfo(attributeName);
        if (attributeInfo.isPresent()) {
            Attribute<?> attribute = new Attribute<>(attributeInfo.get());
            return Optional.of(attribute);
        }

        return Optional.empty();
    }

    public Optional<Attribute<?>> instantiateAttribute(int attributeId) throws DatabaseConnectionException, DatabaseReadException {
        Optional<KnownAttributes.AttributeInfo> attributeInfo = getAttributeInfo(attributeId);
        if (attributeInfo.isPresent()) {
            Attribute<?> attribute = new Attribute<>(attributeInfo.get());
            return Optional.of(attribute);
        }

        return Optional.empty();
    }

    /**
     * Get attribute information identified by name.
     *
     * @param attributeName name of attribute
     * @return AttributeInfo if attribute exists
     */
    public Optional<KnownAttributes.AttributeInfo> getAttributeInfo(String attributeName) throws DatabaseConnectionException, DatabaseReadException {
        return KnownAttributes.getAttribute(context, attributeName);
    }

    /**
     * Get attribute information identified by id
     *
     * @param attributeId id of attribute
     * @return AttributeInfo if attribute exists
     */
    public Optional<KnownAttributes.AttributeInfo> getAttributeInfo(int attributeId) throws DatabaseConnectionException, DatabaseReadException {
        return KnownAttributes.getAttribute(context, attributeId);
    }

    public Optional<Integer> attributeNameToId(String attributeName) {
        Optional<KnownAttributes.AttributeInfo> attributeInfo = getAttributeInfo(attributeName);
        Integer[] attributeId = { null };
        attributeInfo.ifPresent(attr -> attributeId[0] = attr.id);
        return Optional.ofNullable(attributeId[0]);
    }

    public Optional<String> attributeIdToName(int attributeId) {
        Optional<KnownAttributes.AttributeInfo> attributeInfo = getAttributeInfo(attributeId);
        String[] attributeName = { null };
        attributeInfo.ifPresent(attr -> attributeName[0] = attr.name);
        return Optional.ofNullable(attributeName[0]);
    }

    public Optional<Tenant.TenantInfo> getTenantInfo(String tenantName) throws DatabaseConnectionException, DatabaseReadException {
        return Tenant.getTenant(context, tenantName);
    }

    public Optional<Tenant.TenantInfo> getTenantInfo(int tenantId) throws DatabaseConnectionException, DatabaseReadException {
        return Tenant.getTenant(context, tenantId);
    }

    public Optional<Integer> tenantNameToId(String tenantName) {
        Optional<Tenant.TenantInfo> tenantInfo = getTenantInfo(tenantName);
        Integer[] tenantId = { null };
        tenantInfo.ifPresent(info -> tenantId[0] = info.id);
        return Optional.ofNullable(tenantId[0]);
    }

    public Optional<String> tenantIdToName(int tenantId) {
        Optional<Tenant.TenantInfo> tenantInfo = getTenantInfo(tenantId);
        String[] tenantName = { null };
        tenantInfo.ifPresent(info -> tenantName[0] = info.name);
        return Optional.ofNullable(tenantName[0]);
    }

    /**
     * Convenience function for generating an Action event
     *
     * @param source      source object action applies to
     * @param actionType  action type
     * @param description text explaining what has happened
     */
    private void generateActionEvent(
            Object source,
            ActionEvent.Type actionType,
            String description
    ) {
        if (actionListeners.isEmpty())
            return;

        if (actionType.getLevel() < eventThreshold)
            return;

        ActionEvent event = new ActionEvent(source, actionType, description);
        for (ActionListener l : actionListeners.values()) {
            TimedExecution.run(context.getTimingData(), "event action", () -> {
                l.actionPerformed(event);
                return null;
            });
        }
    }

    public TimingData getTimingData() {
        return context.getTimingData();
    }

    public interface InternalDataSourceRunnable {
        void run(javax.sql.DataSource dataSource);
    }

    public void withDataSource(InternalDataSourceRunnable runnable) {
        runnable.run(context.getDataSource());
    }

    public interface InternalConnectionRunnable {
        void run(java.sql.Connection connection);
    }

    public void withConnection(InternalConnectionRunnable runnable) throws java.sql.SQLException {
        try (Connection conn = context.getDataSource().getConnection()) {
            runnable.run(conn);
        }
    }

    /**
     * Synchronize state after external initialisation.
     */
    public void sync() {
        Map<String, Integer> attributeNameToIdMap = context.getDatabaseAdapter().getAttributeNameToIdMap();
        attributeNameToIdMap.putAll(KnownAttributes.fetchAttributeNameToIdMap(context));
    }
}


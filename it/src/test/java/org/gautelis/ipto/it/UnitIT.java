/*
 * Copyright (C) 2024-2025 Frode Randers
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
package org.gautelis.ipto.it;

import org.gautelis.ipto.repo.db.Database;
import org.gautelis.ipto.repo.exceptions.DatabaseReadException;
import org.gautelis.ipto.repo.exceptions.UnitLockedException;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.attributes.Attribute;
import org.gautelis.ipto.repo.model.locks.LockType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.Stack;

import static org.junit.jupiter.api.Assertions.*;

/**
 *
 */
@Tag("units")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@IptoIT
public class UnitIT {
    private static final Logger log = LoggerFactory.getLogger(UnitIT.class);

    private record Counters(
            long unitCount,
            long versionCount
    ) {
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Counters(long count, long versionCount1))) return false;
            return unitCount == count && versionCount == versionCount1;
        }

        @Override
        public int hashCode() {
            return Objects.hash(unitCount, versionCount);
        }
    }

    private final Stack<Counters> counters = new Stack<>();

    private Counters getCounters(Repository repo) {
        Long[] unitCount = { 0L };
        Long[] versionCount = { 0L };

        repo.withDataSource(dataSource -> {
            String sql = "SELECT COUNT(*) FROM repo_unit_kernel";
            Database.useReadonlyStatement(dataSource, sql, rs -> {
                if (rs.next()) {
                    unitCount[0] = rs.getLong(1);
                }
            });

            sql = "SELECT COUNT(*) FROM repo_unit_version";
            Database.useReadonlyStatement(dataSource, sql, rs -> {
                if (rs.next()) {
                    versionCount[0] = rs.getLong(1);
                }
            });
        });

        return new Counters(unitCount[0], versionCount[0]);
    }

    @BeforeEach
    public void pre(Repository repo) {
        counters.push(getCounters(repo));
    }

    @AfterEach
    public void post(Repository repo) {
        Counters _pre = counters.pop();
        Counters _post = getCounters(repo);

        if (_pre != null) {
            if (!_pre.equals(_post)) {
                System.out.println();
                System.out.println("   <<<Imbalance>>>   pre != post --> delete and disposition failed");
                System.out.println();
            }
        }
    }

    private void assertVersionIs(final int expectedVersion, Unit unit, Repository repo) {
        try {
            repo.withConnection(conn -> {
                //
                String sql = "SELECT lastver FROM repo_unit_kernel uk WHERE tenantid = ? AND unitid = ?";
                try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                    int i = 0;
                    pStmt.setInt(++i, unit.getTenantId());
                    pStmt.setLong(++i, unit.getUnitId());

                    try (ResultSet rs = pStmt.executeQuery()) {
                        if (!rs.next()) {
                            fail("Could not locate unit in repo_unit_kernel: " + unit.getReference());
                        }

                        int lastver = rs.getInt("lastver");
                        if (lastver != expectedVersion) {
                            fail("Unexpected last version: " + lastver + " != " + expectedVersion);
                        }
                    }
                } catch (SQLException sqle) {
                    String info = "Could not query repo_unit_kernel: " + Database.squeeze(sqle);
                    log.warn(info, sqle);
                    fail(info);
                }

                //
                sql = "SELECT COUNT(*) FROM repo_unit_version WHERE tenantid = ? AND unitid = ?";
                try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                    int i = 0;
                    pStmt.setInt(++i, unit.getTenantId());
                    pStmt.setLong(++i, unit.getUnitId());

                    try (ResultSet rs = pStmt.executeQuery()) {
                        if (!rs.next()) {
                            fail("Could not count versions in repo_unit_version: " + unit.getReference());
                        }

                        int count = rs.getInt(1);
                        if (count != expectedVersion) {
                            fail("Unexpected number of versions: " + count + " != " + expectedVersion);
                        }
                    }
                } catch (SQLException sqle) {
                    String info = "Could not assert unit versions: " + Database.squeeze(sqle);
                    log.warn(info, sqle);
                    fail(info);
                }
            });
        } catch (SQLException sqle) {
            String info = "Could not verify data in database: " + Database.squeeze(sqle);
            log.warn(info, sqle);
            fail(info);
        }
    }

    private int getAttributeId(String attributeName, Repository repo) {
        Optional<Integer> attributeId = repo.attributeNameToId(attributeName);
        if (attributeId.isEmpty()) {
            throw new RuntimeException("Unknown attribute: " + attributeName);
        }
        return attributeId.get();
    }

    private void assertAttributeExists(final String attributeName, int versionFrom, int versionTo, Unit unit, Repository repo) {

        final int attrId = getAttributeId(attributeName, repo);
        try {
            repo.withConnection(conn -> {
                String sql = """
                    SELECT valueid, unitverfrom, unitverto
                    FROM repo_attribute_value av
                    WHERE tenantid = ?
                      AND unitid = ?
                      AND attrid = ?
                    ORDER BY valueid DESC
                    """;

                try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                    int i = 0;
                    pStmt.setInt(++i, unit.getTenantId());
                    pStmt.setLong(++i, unit.getUnitId());
                    pStmt.setInt(++i, attrId);

                    try (ResultSet rs = pStmt.executeQuery()) {
                        if (!rs.next()) {
                            fail("Could not locate value in repo_attribute_value: unit=" + unit.getReference() + " attribute=" + attributeName + "(" + attrId + ")");
                        }

                        long valueId = rs.getLong("valueid");
                        int verFrom = rs.getInt("unitverfrom");
                        int verTo = rs.getInt("unitverto");
                        //System.out.println(unit.getReference() + " attribute=" + attributeName + "(" + attrId + ") : from=" + verFrom + " to=" + verTo + " valueid=" + valueId);

                        if (verFrom != versionFrom) {
                            fail("Version-from: " + verFrom + " != " + versionFrom + ": unit=" + unit.getReference() + " attribute=" + attributeName + "(" + attrId + ")");
                        }

                        if (verTo != versionTo) {
                            fail("Version-to: " + verTo + " != " + versionTo + ": unit=" + unit.getReference() + " attribute=" + attributeName + "(" + attrId + ")");
                        }
                    }
                } catch (SQLException sqle) {
                    String info = "Could not query repo_attribute_value: " + Database.squeeze(sqle);
                    log.warn(info, sqle);
                    fail(info);
                }
            });
        } catch (SQLException sqle) {
            String info = "Could not verify data in database: " + Database.squeeze(sqle);
            log.warn(info, sqle);
            fail(info);
        }
   }

    @Test
    public void test(Repository repo) {
        final int tenantId = 1;

        //
        Unit unit = repo.createUnit(tenantId, "a record instance");

        unit.withAttributeValue("dce:title", String.class, value -> {
            value.add("This testcase has a title");
        });

        unit.withAttributeValue("dce:description", String.class, value -> {
            value.add("First description");
        });

        assertTrue(unit.isNew());
        assertFalse(unit.isReadOnly());

        //
        repo.storeUnit(unit);

        assertVersionIs(1, unit, repo);
        assertAttributeExists("dce:description", 1, 1, unit, repo);
        assertAttributeExists("dce:title", 1, 1, unit, repo);
        assertFalse(unit.isNew());
        assertFalse(unit.isReadOnly());

        repo.lockUnit(unit, LockType.WRITE, "unit must not be modified");
        assertTrue(unit.isLocked());

        unit.withAttributeValue("dce:description", String.class, value -> {
            value.add("Second additional description");
        });

        //
        assertThrows(UnitLockedException.class, () -> {
            repo.storeUnit(unit);
        });
        assertVersionIs(/* still */ 1, unit, repo);

        repo.unlockUnit(unit);
        assertFalse(unit.isLocked());
        assertFalse(unit.isNew());
        assertFalse(unit.isReadOnly());

        //
        assertDoesNotThrow(() -> {
            repo.storeUnit(unit);
        });

        assertVersionIs(2, unit, repo);
        assertAttributeExists("dce:description", 2, 2, unit, repo);
        assertAttributeExists("dce:title", 1, 2, unit, repo);
        assertFalse(unit.isNew());
        assertFalse(unit.isReadOnly());

        unit.withAttributeValue("dce:description", String.class, value -> {
            value.add("Third additional description");
        });

        assertDoesNotThrow(() -> {
            repo.storeUnit(unit);
        });

        assertVersionIs(3, unit, repo);
        assertAttributeExists("dce:description", 3, 3, unit, repo);
        assertAttributeExists("dce:title", 1, 3, unit, repo);

        unit.requestStatusTransition(Unit.Status.PENDING_DISPOSITION);

        System.out.println("---------------------------------------------------------------------------------------");
        repo.dispose(tenantId, new PrintWriter(System.out));
        System.out.println("---------------------------------------------------------------------------------------");
    }
}

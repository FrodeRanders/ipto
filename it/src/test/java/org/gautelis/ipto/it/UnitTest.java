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
import org.gautelis.ipto.repo.exceptions.UnitLockedException;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.locks.LockType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.Objects;
import java.util.Stack;

import static org.junit.jupiter.api.Assertions.*;

/**
 *
 */
@Tag("units")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(RepositorySetupExtension.class)
public class UnitTest {
    private static final Logger log = LoggerFactory.getLogger(UnitTest.class);

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
                System.out.println("Imbalance");
            }
        }
    }

    @Test
    public void test(Repository repo) {
        final int tenantId = 1;

        //
        Unit unit = repo.createUnit(tenantId, "a record instance");

        unit.withAttributeValue("dce:title", String.class, value -> {
            value.add("First title");
        });

        assertTrue(unit.isNew());
        assertFalse(unit.isReadOnly());

        //
        repo.storeUnit(unit);

        assertFalse(unit.isNew());
        assertFalse(unit.isReadOnly());

        repo.lockUnit(unit, LockType.WRITE, "unit must not be modified");
        assertTrue(unit.isLocked());

        unit.withAttributeValue("dce:title", String.class, value -> {
            value.add("Second title");
        });

        //
        assertThrows(UnitLockedException.class, () -> {
            repo.storeUnit(unit);
        });

        repo.unlockUnit(unit);
        assertFalse(unit.isLocked());
        assertFalse(unit.isNew());
        assertFalse(unit.isReadOnly());

        //
        assertDoesNotThrow(() -> {
            repo.storeUnit(unit);
        });
        assertFalse(unit.isNew());
        assertFalse(unit.isReadOnly());

        unit.withAttributeValue("dce:title", String.class, value -> {
            value.add("Third title");
        });

        assertDoesNotThrow(() -> {
            repo.storeUnit(unit);
        });

        unit.requestStatusTransition(Unit.Status.PENDING_DISPOSITION);
        repo.dispose(tenantId, new PrintWriter(System.out));
    }
}

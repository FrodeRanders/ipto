/*
 * Copyright (C) 2026 Frode Randers
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
package org.gautelis.ipto.repo.benchmark;

import org.gautelis.ipto.repo.RepositoryFactory;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.utils.RunningStatistics;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
import java.time.Instant;
import java.util.Locale;

class StoreBenchmarkTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void benchmarkStoreUnits() {
        boolean enabled = Boolean.parseBoolean(System.getProperty("ipto.bench.enabled", "false"));
        Assumptions.assumeTrue(
                enabled,
                "Set -Dipto.bench.enabled=true to run the benchmark test"
        );

        int units = Integer.parseInt(System.getProperty("ipto.bench.units", "500000"));
        int reportEvery = Integer.parseInt(System.getProperty("ipto.bench.reportEvery", "10000"));
        String tenantName = System.getProperty("ipto.bench.tenant", "SCRATCH");
        String prefix = System.getProperty(
                "ipto.bench.prefix",
                "java-soak-" + Instant.now().toString().replace(":", "").replace(".", "")
        );
        boolean printTimingData = Boolean.parseBoolean(System.getProperty("ipto.bench.printTimingData", "true"));

        Repository repo = RepositoryFactory.getRepository();
        int tenantId = repo.tenantNameToId(tenantName)
                .orElseThrow(() -> new IllegalArgumentException("Unknown tenant: " + tenantName));

        RunningStatistics stats = new RunningStatistics();
        long[] errors = {0L};
        long[] firstUnitId = {-1L};
        long[] lastUnitId = {-1L};
        long startedNs = System.nanoTime();

        System.out.println("====================================================================================================");
        System.out.println("IPTO Java store benchmark");
        System.out.println("Started: " + Instant.now());
        System.out.println("tenant=" + tenantId + " units=" + units + " reportEvery=" + reportEvery + " prefix=" + prefix);
        System.out.println("====================================================================================================");

        repo.withDataSource((DataSource dataSource) -> {
            try (Connection conn = dataSource.getConnection();
                 CallableStatement cs = conn.prepareCall("CALL ingest_new_unit_json(?, ?, ?, ?, ?)")) {
                conn.setAutoCommit(false);
                cs.registerOutParameter(2, Types.BIGINT);
                cs.registerOutParameter(3, Types.INTEGER);
                cs.registerOutParameter(4, Types.TIMESTAMP);
                cs.registerOutParameter(5, Types.TIMESTAMP);

                for (int i = 1; i <= units; i++) {
                    String unitName = String.format("%s-java-%08d", prefix, i);
                    long t0 = System.nanoTime();
                    try {
                        ObjectNode payload = MAPPER.createObjectNode();
                        payload.put("tenantid", tenantId);
                        payload.put("status", 30);
                        payload.put("unitname", unitName);
                        payload.put("corrid", java.util.UUID.randomUUID().toString());
                        payload.putArray("attributes");

                        cs.setString(1, payload.toString());
                        cs.execute();
                        conn.commit();

                        long unitId = cs.getLong(2);
                        if (firstUnitId[0] < 0) {
                            firstUnitId[0] = unitId;
                        }
                        lastUnitId[0] = unitId;
                        stats.addSample((System.nanoTime() - t0) / 1_000_000.0);
                    } catch (Exception e) {
                        errors[0]++;
                        try {
                            conn.rollback();
                        } catch (Exception ignored) {
                        }
                        throw new RuntimeException("Benchmark failed at i=" + i, e);
                    }

                    if (i % reportEvery == 0 || i == units) {
                        double elapsedS = (System.nanoTime() - startedNs) / 1_000_000_000.0;
                        double throughput = elapsedS > 0.0 ? stats.getCount() / elapsedS : 0.0;
                        double remaining = units - i;
                        double etaS = throughput > 0.0 ? remaining / throughput : Double.NaN;
                        String etaText = Double.isNaN(etaS) ? "n/a" : String.format(Locale.US, "%.1fs", etaS);
                        System.out.printf(
                                Locale.US,
                                "[java] progress=%d/%d (%.1f%%) ok=%d err=%d mean=%.3fms throughput=%.1f ops/s eta=%s%n",
                                i, units, (100.0 * i) / units, stats.getCount(), errors[0], stats.getMean(), throughput, etaText
                        );
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Benchmark failed to initialize JDBC path", e);
            }
        });

        double elapsedS = (System.nanoTime() - startedNs) / 1_000_000_000.0;
        double throughput = elapsedS > 0.0 ? stats.getCount() / elapsedS : 0.0;
        double p95Est = Double.NaN;
        if (stats.getCount() > 1) {
            double stddev = stats.getStdDev();
            if (!Double.isNaN(stddev)) {
                p95Est = Math.max(stats.getMin(), stats.getMean() + 1.645 * stddev);
            }
        }

        System.out.println();
        System.out.println("====================================================================================================");
        System.out.println("Benchmark summary");
        System.out.println("====================================================================================================");
        System.out.printf(
                Locale.US,
                "requested=%d ok=%d errors=%d elapsed=%.2fs throughput=%.2f ops/s%n",
                units, stats.getCount(), errors[0], elapsedS, throughput
        );
        System.out.printf(
                Locale.US,
                "latency_ms: min=%.3f mean=%.3f max=%.3f stddev=%.3f cv=%.2f%% p95_est=%s%n",
                stats.getMin(), stats.getMean(), stats.getMax(), stats.getStdDev(), stats.getCV(),
                Double.isNaN(p95Est) ? "n/a" : String.format(Locale.US, "%.3f", p95Est)
        );
        System.out.printf("first_unit_id=%d last_unit_id=%d%n", firstUnitId[0], lastUnitId[0]);
        if (printTimingData) {
            System.out.println();
            System.out.println("Repository timing report:");
            System.out.print(repo.getTimingData().report());
        }
        System.out.println("====================================================================================================");
    }
}

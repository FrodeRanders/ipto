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
package org.gautelis.ipto.repo.model.utils;

/**
 * Helpers for measuring execution time and accumulating timing statistics.
 */
public class TimedExecution {

    private TimedExecution() {}

    /**
     * Runs a task and records its elapsed time in one statistics bucket.
     *
     * @param rs the statistics bucket to update
     * @param task the task to run
     * @return the task result
     * @param <T> the task result type
     */
    public static <T> T run(final RunningStatistics rs, RepoRunnable<T> task) {
        long t0 = System.nanoTime();
        try {
            return task.run();
        } finally {
            long elapsedNanos = System.nanoTime() - t0;

            // double elapsedMicros = elapsedNanos / 1_000.0;
            double elapsedMillis = elapsedNanos / 1_000_000.0;
            rs.addSample(elapsedMillis);
        }
    }

    /**
     * Runs a task and records its elapsed time under one named measurement.
     *
     * @param timingData the timing registry to update
     * @param timing the measurement name
     * @param task the task to run
     * @return the task result
     * @param <T> the task result type
     */
    public static <T> T run(final TimingData timingData, String timing, RepoRunnable<T> task) {
        RunningStatistics rs = timingData.computeIfAbsent(timing, k -> new RunningStatistics());
        return run(rs, task);
    }

    /**
     * Runs a void task and records its elapsed time in one statistics bucket.
     *
     * @param rs the statistics bucket to update
     * @param task the task to run
     */
    public static void run(final RunningStatistics rs, Runnable task) {
        long t0 = System.nanoTime();
        try {
            task.run();
        } finally {
            long elapsedNanos = System.nanoTime() - t0;

            // double elapsedMicros = elapsedNanos / 1_000.0;
            double elapsedMillis = elapsedNanos / 1_000_000.0;
            rs.addSample(elapsedMillis);
        }
    }

    /**
     * Runs a void task and records its elapsed time under one named
     * measurement.
     *
     * @param timingData the timing registry to update
     * @param timing the measurement name
     * @param task the task to run
     */
    public static void run(final TimingData timingData, String timing, Runnable task) {
        RunningStatistics rs = timingData.computeIfAbsent(timing, k -> new RunningStatistics());
        run(rs, task);
    }
}

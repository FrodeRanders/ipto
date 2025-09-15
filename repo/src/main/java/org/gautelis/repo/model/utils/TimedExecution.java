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
package org.gautelis.repo.model.utils;

public class TimedExecution {

    private TimedExecution() {}

    public static <T> T run(final MovingAverage movingAverage, RepoRunnable<T> task) {
        long t0 = System.nanoTime();
        try {
            return task.run();
        } finally {
            long elapsedNanos = System.nanoTime() - t0;

            // double elapsedMicros = elapsedNanos / 1_000.0;
            double elapsedMillis = elapsedNanos / 1_000_000.0;
            movingAverage.update(elapsedMillis);
        }
    }

    public static <T> T run(final TimingData timingData, String timing, RepoRunnable<T> task) {
        MovingAverage ma = timingData.computeIfAbsent(timing, k -> new MovingAverage());
        return run(ma, task);
    }

    public static void run(final MovingAverage movingAverage, Runnable task) {
        long t0 = System.nanoTime();
        try {
            task.run();
        } finally {
            long elapsedNanos = System.nanoTime() - t0;

            // double elapsedMicros = elapsedNanos / 1_000.0;
            double elapsedMillis = elapsedNanos / 1_000_000.0;
            movingAverage.update(elapsedMillis);
        }
    }

    public static void run(final TimingData timingData, String timing, Runnable task) {
        MovingAverage ma = timingData.computeIfAbsent(timing, k -> new MovingAverage());
        run(ma, task);
    }
}

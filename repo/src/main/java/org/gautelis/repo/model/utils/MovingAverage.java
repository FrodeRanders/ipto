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

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/*
 * Calculates statistics based on a Moving Average (MA) algorithm over a set of data points.
 * <pre>
 * (defun running_average (avg new n)
 *    "Calculate new average given previous average over n data points"
 *    (/ (+ new (* avg n)) (1+ n)))
 * </pre>
 */
public class MovingAverage {

    private double min = 0.0;
    private double max = 0.0;
    private double average = 0.0;
    private double stdDev = 0.0;
    private double cv = 0.0;
    private BigInteger total = BigInteger.ZERO;

    private long count = 0L;
    private double _pwrSumAverage = 0.0; // computational use

    public MovingAverage() {
    }

    public long getCount() {
        return count;
    }

    /**
     * Get average.
     * @return average of all samples
     */
    public double getAverage() {
        return average;
    }

    /**
     * Get min value.
     * @return minimal sample
     */
    public double getMin() {
        return min;
    }

    /**
     * Get max value.
     * @return maximal sample
     */
    public double getMax() {
        return max;
    }

    /**
     * Get standard deviation (if we have enough samples)
     * @return standard deviation for series of samples
     */
    public Optional<Double> getStdDev() {
        if (count > 1L) {
            return Optional.of(stdDev);
        }
        return Optional.empty();
    }

    /**
     * Get CV (if we have enough samples)
     * @return CV for series of samples
     */
    public Optional<Double> getCV() {
        if (count > 1L && (average > 0 || average < 0)) {
            return Optional.of(cv);
        }
        return Optional.empty();
    }

    public BigInteger getTotal() {
        return total;
    }

    /**
     * Updates statistics with 'sample'
     * <p>
     * @param sample a sample added to the series
     */
    public void update(double sample) {
        total = total.add(BigInteger.valueOf(Math.round(sample)));

        // Adjust min&max
        if (0L == count) {
            min = max = sample;
        } else {
            min = Math.min(sample, min);
            max = Math.max(sample, max);
        }

        // Update average
        average += (sample - average) / ++count;
        _pwrSumAverage += ( sample * sample - _pwrSumAverage) / count;

        // Update variance
        if (count > 1L) {
            stdDev = Math.sqrt((_pwrSumAverage * count - count * average * average) / (count - 1L));
            if (average > 0 || average < 0) {
                cv = 100.0 * (stdDev / average);
            }
        }
    }

    public void update(Instant startTime, Instant endTime) {
        // update(Duration.between(startTime, endTime).toMillis());
        update(endTime.toEpochMilli() - startTime.toEpochMilli());
    }
}

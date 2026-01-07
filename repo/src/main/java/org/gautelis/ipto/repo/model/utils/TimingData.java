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

import org.gautelis.vopn.lang.TimeDelta;

import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.IntStream;

public class TimingData extends HashMap<String, RunningStatistics> {

    public void addSample(String name, double sample) {
        computeIfAbsent(name, k -> new RunningStatistics()).addSample(sample);
    }

    public String report() {
        StringBuilder buf = new StringBuilder();

        String[] cols = {"Measurement", "Count", "Min", "Max", "Mean", "CV(%)", "Total time", "i.e."};
        int[] colSize = Arrays.stream(cols).flatMapToInt(str -> IntStream.of(str.length() + 1)).toArray(); // minimum

        final int fluff = 5; // e.g. space around numbers
        long maxCount = 0L;
        for (String key : keySet()) {
            RunningStatistics rs = get(key);
            colSize[0] = Math.max(colSize[0], key.length());
            maxCount = Math.max(maxCount, rs.getCount());
            colSize[2] = Math.max(colSize[2], (int)Math.log10(Math.abs(rs.getMin())) + fluff);
            colSize[3] = Math.max(colSize[3], (int)Math.log10(Math.abs(rs.getMax())) + fluff);
            colSize[4] = Math.max(colSize[4], (int)Math.log10(Math.abs(rs.getMean())) + fluff);
            colSize[5] = Math.max(colSize[5], (int)Math.log10(Math.abs(rs.getCV())) + fluff);
            colSize[6] = Math.max(colSize[6], (int)Math.log10(Math.abs(rs.getTotal())) + 1);
            colSize[7] = Math.max(colSize[7], TimeDelta.asHumanApproximate(rs.getTotal()).length());
        }
        colSize[1] = Math.max(colSize[1], (int)Math.floor(Math.log10(maxCount)) + 1);

        StringBuilder hdrFormat = new StringBuilder();
        for (int i = 0; i < cols.length; i++) {
            hdrFormat.append("%").append(colSize[i]).append("s");
            if (i < cols.length - 2) {  // no separator between 'Total time' and 'i.e.'
                hdrFormat.append(" | ");
            }
        }
        hdrFormat.append("\n");

        StringBuilder line = new StringBuilder();
        for (int i = 0; i < colSize.length; i++) {
            line.append("-".repeat(Math.max(0, colSize[i] + 1)));
            if (i < colSize.length - 1) {
                line.append("+-");
            }
        }
        line.append("\n");

        buf.append(line);
        buf.append(String.format(hdrFormat.toString(), (Object[]) cols));
        buf.append(line);

        final String format =
            "%" + colSize[0] + "s | " +
            "%" + colSize[1] + "d | " +
            "%" + colSize[2] + ".2f | " +
            "%" + colSize[3] + ".2f | " +
            "%" + colSize[4] + ".2f | " +
            "%" + colSize[5] + ".2f | " +
            "%" + colSize[6] + "d | " +
            "%" + colSize[7] + "s\n";

        keySet().stream().sorted().forEach(key -> {
            RunningStatistics rs = get(key);
            buf.append(String.format(
                    format, key,
                    rs.getCount(),
                    rs.getMin(),
                    rs.getMax(),
                    rs.getMean(),
                    rs.getCV(),
                    rs.getTotal(),
                    TimeDelta.asHumanApproximate(rs.getTotal())));
        });
        buf.append("\n");
        return buf.toString();
    }
}

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
package org.gautelis.repo.search.query;


import org.gautelis.repo.db.Adapter;
import org.gautelis.repo.db.Column;
import org.gautelis.repo.model.attributes.Value;
import org.gautelis.repo.model.utils.TimingData;
import org.gautelis.repo.search.UnitSearch;
import org.gautelis.repo.search.model.*;
import org.gautelis.repo.utils.CheckedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Database adapter base class implementation.
 */
public abstract class DatabaseAdapter implements Adapter {
    private static final Logger log = LoggerFactory.getLogger(DatabaseAdapter.class);

    public DatabaseAdapter() {
    }

    public abstract String getTimePattern();

    public abstract String asTimeLiteral(String timeStr);

    public abstract String asTimeLiteral(Instant instant);

    /**
     * Generates fragment: a OPERATOR b
     * for Strings.
     * <p>
     * Operator is one of =, &gt;=, &gt;, &lt;, &lt;=, &lt;&gt;, LIKE, ...
     */
    protected StringBuilder compare(
            StringBuilder buf, Column column, Operator operator, String b, boolean append
    ) {

        if (append) {
            buf.append("AND ");
        }
        buf.append("LOWER(");
        buf.append(column);
        buf.append(") ");

        // Detect wildcards
        boolean hasWildcards = false;

        if (Operator.EQ == operator || Operator.LIKE == operator) {
            hasWildcards =
                    b.indexOf('*') >= 0
                 || b.indexOf('%') >= 0
                 || b.indexOf('_') >= 0;

            if (hasWildcards) {
                // Replace any '*' with a '%'
                b = b.replace('*', '%');
            }
        }

        if (Operator.LIKE == operator && !hasWildcards) {
            // Use an EQ compare instead.
            buf.append(Operator.EQ.toString());

        } else if (Operator.EQ == operator && hasWildcards) {
            // Use a LIKE compare instead.
            buf.append(Operator.LIKE.toString());

        } else {
            buf.append(operator);
        }

        b = b.replace('\'', ' '); // Protect against escape characters in value.
        buf.append("'");
        buf.append(b.toLowerCase());
        buf.append("' ");

        return buf;
    }

    /**
     * Generates fragment: a OPERATOR b
     * for byte[].
     * <p>
     * Operator is one of =, &gt;=, &gt;, &lt;, &lt;=, &lt;&gt;, LIKE, ...
     */
    protected StringBuilder compare(
            StringBuilder buf, Column column, Operator operator, byte[] b, boolean append
    ) {

        if (append) {
            buf.append("AND ");
        }
        buf.append(column);
        buf.append(operator);
        buf.append(Arrays.toString(b)); // May not be correct
        buf.append(" ");

        return buf;
    }

    /**
     * Generates fragment: a OPERATOR b
     * for java.sql.Timestamp.
     * <p>
     * Operator is one of =, &gt;=, &gt;, &lt;, &lt;=, &lt;&gt;, LIKE, ...
     * <p>
     * The 'adjust' is used when searching for attributes to allow
     * equality matches where the 'b' is not specified down to the
     * individual millis.
     */
    protected StringBuilder compare(
            StringBuilder buf, Column column, Operator operator, java.time.Instant b, boolean append, boolean adjust
    ) {

        if (append) {
            buf.append("AND ");
        }

        // Convert date to a known format
        final DateTimeFormatter dtf = DateTimeFormatter.ofPattern(getTimePattern());

        if (adjust && Operator.EQ == operator) {
            // Since we are checking for equality and we are advised to adjust
            // the search (practically indicating that we are comparing dates),
            // we will do so depending on the granularity of the provided timestamp.
            //
            // Otherwise, we will never get a match on a date attribute if we did
            // not have the exact value including all millis. This is probably not
            // what the users of the system would expect.
            //
            // Thus, we try to establish some kind of interval in which we seek
            // a match. If we got a date "2008-01-23 23:12", then we will actually
            // accept matches in the range
            // from (including) "2008-01-23 23:12:00.000000000"
            // to (including) "2008-01-23 23:12:59.999999999"
            //
            // "In the date format for DT_DBTIMESTAMP, fffffffff is a value between 0 and 999999999",
            // but since some DBMS does not record more than 3 fractional seconds, we will use
            // only 3 digits.
            //
            // SELECT TO_TIMESTAMP('2008-01-23 00:00:00.123456789', 'YYYY-MM-DD HH24:MI:SS.FF3') FROM DUAL
            // --> 23-JAN-08 12.00.00.123 AM

            // Match on the time pattern (above)
            String d = "(\\d\\d+)"; // number
            String s = ".*?"; // filler
            Pattern p = Pattern.compile(d + s + d + s + d + s + d + s + d + s + d + s + d, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

            //
            Locale some24HourLocale = Locale.of("sv", "SE");
            Calendar lower = Calendar.getInstance(some24HourLocale);
            lower.setTimeInMillis(b.toEpochMilli());
            Calendar higher = Calendar.getInstance(some24HourLocale);
            higher.setTimeInMillis(b.toEpochMilli());
            Calendar bc = Calendar.getInstance(some24HourLocale);
            bc.setTimeInMillis(b.toEpochMilli());
            boolean compareWithInterval = false;

            String dateStr = dtf.format(b);
            Matcher m = p.matcher(dateStr);
            if (m.find()) {
                String _year = m.group(1);
                if (null != _year && !_year.isEmpty()) {
                    int year = Integer.parseInt(_year);
                    if (0 == year) {
                        // year not specified
                        lower.set(Calendar.YEAR, lower.getActualMinimum(Calendar.YEAR));
                        higher.set(Calendar.YEAR, higher.getActualMaximum(Calendar.YEAR));
                        compareWithInterval = true;
                    }
                }

                String _month = m.group(2);
                if (compareWithInterval || (null != _month && !_month.isEmpty())) {
                    int month = Integer.parseInt(_month);
                    if (compareWithInterval || 0 == month) {
                        // month not specified
                        lower.set(Calendar.MONTH, lower.getActualMinimum(Calendar.MONTH));
                        higher.set(Calendar.MONTH, higher.getActualMaximum(Calendar.MONTH));
                        compareWithInterval = true;
                    }
                }

                String _day = m.group(3);
                if (compareWithInterval || (null != _day && !_day.isEmpty())) {
                    int day = Integer.parseInt(_day);
                    if (compareWithInterval || 0 == day) {
                        // day not specified
                        lower.set(Calendar.DAY_OF_MONTH, lower.getActualMinimum(Calendar.DAY_OF_MONTH));
                        higher.set(Calendar.DAY_OF_MONTH, higher.getActualMaximum(Calendar.DAY_OF_MONTH));
                        compareWithInterval = true;
                    }
                }

                String _hour = m.group(4);
                if (compareWithInterval || (null != _hour && !_hour.isEmpty())) {
                    int hour = Integer.parseInt(_hour);
                    if (compareWithInterval || 0 == hour) {
                        // hour not specified
                        lower.set(Calendar.HOUR, lower.getActualMinimum(Calendar.HOUR));
                        higher.set(Calendar.HOUR, 23); // higher.getActualMaximum(Calendar.HOUR) returns 11 ???
                        compareWithInterval = true;
                    }
                }

                String _minutes = m.group(5);
                if (compareWithInterval || (null != _minutes && !_minutes.isEmpty())) {
                    int minutes = Integer.parseInt(_minutes);
                    if (compareWithInterval || 0 == minutes) {
                        // minutes not specified
                        lower.set(Calendar.MINUTE, lower.getActualMinimum(Calendar.MINUTE));
                        higher.set(Calendar.MINUTE, higher.getActualMaximum(Calendar.MINUTE));
                        compareWithInterval = true;
                    }
                }

                String _seconds = m.group(6);
                if (compareWithInterval || (null != _seconds && !_seconds.isEmpty())) {
                    int seconds = Integer.parseInt(_seconds);
                    if (compareWithInterval || 0 == seconds) {
                        // seconds not specified
                        lower.set(Calendar.SECOND, lower.getActualMinimum(Calendar.SECOND));
                        higher.set(Calendar.SECOND, higher.getActualMaximum(Calendar.SECOND));
                        compareWithInterval = true;
                    }
                }

                String _millis = m.group(7);
                if (compareWithInterval || (null != _millis && !_millis.isEmpty())) {
                    int millis = Integer.parseInt(_millis);
                    if (compareWithInterval || 0 == millis) {
                        // milliseconds not specified
                        lower.set(Calendar.MILLISECOND, lower.getActualMinimum(Calendar.MILLISECOND));
                        higher.set(Calendar.MILLISECOND, higher.getActualMaximum(Calendar.MILLISECOND));
                        compareWithInterval = true;
                    }
                }
            }
            if (compareWithInterval) {
                buf.append("( ");
                buf.append(column);
                buf.append(Operator.GEQ);
                buf.append(asTimeLiteral(lower.getTime().toInstant()));
                buf.append(" AND ");
                buf.append(column);
                buf.append(Operator.LEQ);
                buf.append(asTimeLiteral(higher.getTime().toInstant()));
                buf.append(" ) ");
            } else {
                buf.append(column);
                buf.append(operator); // operator is EQ
                buf.append(asTimeLiteral(lower.getTime().toInstant()));
                buf.append(" ");
            }
        } else {
            buf.append(column);
            buf.append(operator);
            buf.append(asTimeLiteral(b));
            buf.append(" ");
        }

        return buf;
    }

    /**
     * Generates fragment: a OPERATOR b
     * for integers.
     * <p>
     * Operator is one of =, &gt;=, &gt;, &lt;, &lt;=, &lt;&gt;, LIKE, ...
     */
    protected StringBuilder compare(
            StringBuilder buf, Column column, Operator operator, int b, boolean append
    ) {

        if (append) {
            buf.append("AND ");
        }
        buf.append(column);
        buf.append(operator);
        buf.append(b);
        buf.append(" ");

        return buf;
    }

    /**
     * Generates fragment: a OPERATOR b
     * for booleans.
     * <p>
     * Operator is one of =, &gt;=, &gt;, &lt;, &lt;=, &lt;&gt;, LIKE, ...
     */
    protected StringBuilder compare(
            StringBuilder buf, Column column, Operator operator, boolean b, boolean append
    ) {

        if (append) {
            buf.append("AND ");
        }
        buf.append(column);
        buf.append(operator);
        buf.append(b ? 1 : 0); // Treat as integer
        buf.append(" ");

        return buf;
    }

    /**
     * Generates fragment: a OPERATOR b
     * for float.
     * <p>
     * Operator is one of =, &gt;=, &gt;, &lt;, &lt;=, &lt;&gt;, LIKE, ...
     */
    protected StringBuilder compare(
            StringBuilder buf, Column column, Operator operator, double d, boolean append
    ) throws IllegalArgumentException {

        if (append) {
            buf.append("AND ");
        }
        buf.append(column);
        buf.append(operator);
        buf.append(d);
        buf.append(" ");

        return buf;
    }

    //---------------------------------------------------------------
    // GENERIC STRUCTURAL
    //---------------------------------------------------------------

    public abstract void search(
            Connection conn,
            UnitSearch sd,
            TimingData timimgData,
            CheckedConsumer<ResultSet> rsBlock
    ) throws IllegalArgumentException;

    //---------------------------------------------------------------
    // DBMS SPECIFIC GENERATORS
    //---------------------------------------------------------------
    abstract protected SearchExpression optimize(
            SearchExpression sex
    );

    public record GeneratedStatement(
            String statement,
            Collection<SearchItem<?>> preparedItems,
            Map<String, SearchItem<?>> commonConstraintValues
    ) {}

    protected abstract GeneratedStatement generateStatement(
            UnitSearch sd
    );
}

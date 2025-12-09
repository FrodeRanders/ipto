package org.gautelis.ipto.repo.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.Locale;

public class TimeHelper {
    private static final Logger log = LoggerFactory.getLogger(TimeHelper.class);

    private static final DateTimeFormatter FMT = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 3, 6, true) // 000—999999
            .toFormatter();

    public static Timestamp parseTimestamp(String s) {
        // Local time
        LocalDateTime ldt = LocalDateTime.parse(s, FMT);

        // local time to UTC
        Instant inst = ldt.toInstant(ZoneOffset.UTC);

        // time in UTC to epoch milliseconds
        long millis = inst.toEpochMilli();

        return new Timestamp(millis);
    }

    /**
     * Converts a string to a date given a specific locale.
     *
     * @param date
     * @param locale
     * @return
     * @throws ParseException
     */
    public static java.util.Date parseDate(
            String date, Locale locale
    ) throws ParseException {
        if (null != date && date.length() == 1) {
            Calendar now = Calendar.getInstance(); // locale not important
            now.setTimeInMillis(System.currentTimeMillis());

            switch (date.toLowerCase().charAt(0)) {
                case 'd':
                case 't': {
                    // This date, "today" or whatever
                    Calendar c = Calendar.getInstance(); // locale not important
                    c.clear();
                    c.set(now.get(Calendar.YEAR), now.get(Calendar.MONTH), now.get(Calendar.DATE));
                    return c.getTime();
                }

                case 'n':
                    // This very moment ("now")
                    return now.getTime();
            }
        }

        // Parse the date and possibly time, minding the locale
        try {
            // -----------------------------
            // Format: 2024-08-26 19:15:23
            // -----------------------------
            DateFormat df = DateFormat.getDateTimeInstance(
                    /* date style */ DateFormat.SHORT, /* time style */ DateFormat.MEDIUM, locale
            );
            return df.parse(date);

        } catch (ParseException pe1) {
            try {
                // ---------------------------
                // Format: 2024-08-26 19:15
                // ---------------------------
                DateFormat df = DateFormat.getDateTimeInstance(
                        /* date style */ DateFormat.SHORT, /* time style */ DateFormat.SHORT, locale
                );
                return df.parse(date);

            } catch (ParseException pe2) {
                // ---------------------------
                // Format: 2024-08-26
                // ---------------------------
                DateFormat df = DateFormat.getDateInstance(
                        /* date style */ DateFormat.SHORT, locale
                );
                return df.parse(date);
            }
        }
    }

    public static Instant parseInstant(String s) {
        return Instant.parse(s);
    }

    public static Instant timestamp2Instant(Timestamp timestamp) {
        return timestamp.toInstant();
    }

    public static Timestamp instant2Timestamp(Instant instant) {
        return new Timestamp(instant.toEpochMilli());
    }
}

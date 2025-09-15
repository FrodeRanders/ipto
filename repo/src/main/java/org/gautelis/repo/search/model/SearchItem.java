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
package org.gautelis.repo.search.model;

import org.gautelis.repo.model.Type;
import org.gautelis.repo.utils.TimeHelper;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Locale;
import java.util.Objects;

/**
 * Handles individual criteria, specified on units and attributes.
 */
public abstract class SearchItem<T> {
    //
    public enum Variant {
        UNIT,
        ATTRIBUTE,
        ASSOCIATION
    }

    //
    protected final Variant variant;
    protected final Type type;
    protected final Operator operator;

    protected SearchItem(Variant variant, Type type, Operator operator) {
        Objects.requireNonNull(variant, "variant");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(operator, "operator");

        this.variant = variant;
        this.type = type;
        this.operator = operator;
    }

    public Variant getVariant() {
        return variant;
    }

    public Type getType() {
        return type;
    }

    public Operator getOperator() {
        return operator;
    }

    public abstract T getValue();

    /**
     * Returns an appropriate from-date, given a date as a string.
     * <p>
     * The general idea is to specify date + time, time being
     * 00:00:00.000 of that date.
     * <p>
     * Match call with call to #toDate.
     *
     * @param str
     * @param locale
     * @return
     */
    public static Timestamp early(
            String str, Locale locale
    ) throws ParseException {
        Objects.requireNonNull(str, "str");

        java.util.Date d = TimeHelper.parseDate(str, locale);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        return new Timestamp(calendar.getTime().getTime());
    }

    /**
     * Returns an appropriate to-date, given a date as a string.
     * <p>
     * The general idea is to specify date + time, time being
     * 23:59:59.999 of that date.
     * <p>
     * Match call with call to #fromDate.
     *
     * @param str
     * @param locale
     * @return
     */
    public static Timestamp late(
            String str, Locale locale
    ) throws ParseException {
        Objects.requireNonNull(str, "str");

        java.util.Date d = TimeHelper.parseDate(str, locale);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        calendar.set(Calendar.MILLISECOND, 999);

        return new Timestamp(calendar.getTime().getTime());
    }
}

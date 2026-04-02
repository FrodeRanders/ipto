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
package org.gautelis.ipto.repo.utils;

import org.gautelis.ipto.repo.exceptions.BaseException;

import java.sql.SQLException;
import java.util.function.Consumer;

/**
 * Variant of {@link Consumer} whose implementation is allowed to throw checked
 * SQL and repository exceptions.
 *
 * @param <T> consumed element type
 */
@FunctionalInterface
public interface CheckedConsumer<T> extends Consumer<T> {

    @Override
    default void accept(final T elem) {
        try {
            checkedAccept(elem);

        } catch (final SQLException | BaseException e) {

            throw new RuntimeException(e);
        }
    }

    /**
     * Consumes the supplied element and may throw checked exceptions.
     *
     * @param elem element to consume
     * @throws SQLException if SQL-backed processing fails
     * @throws BaseException if repository processing fails
     */
    void checkedAccept(T elem) throws SQLException, BaseException;
}

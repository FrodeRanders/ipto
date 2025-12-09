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
package org.gautelis.ipto.repo.exceptions;

/**
 * General exception used when problems occur with the database. This exception works as a catch-all for
 * the various more specific database exceptions.
 * <p>
 * Choose one of {@link DatabaseConnectionException}, {@link DatabaseReadException} or
 * {@link DatabaseWriteException} when throwing an exception.
 */
public abstract class DatabaseException extends BaseException {
    private final java.sql.SQLException sqle;

    /**
     *
     */
    public DatabaseException(java.sql.SQLException sqle) {
        this.sqle = sqle;
    }

    /**
     * Overridden constructor from {@link BaseException }
     *
     * @param msg message
     */
    public DatabaseException(String msg, java.sql.SQLException sqle) {
        super(msg);
        this.sqle = sqle;
    }

    public java.sql.SQLException getSQLException() {
        return sqle;
    }
}





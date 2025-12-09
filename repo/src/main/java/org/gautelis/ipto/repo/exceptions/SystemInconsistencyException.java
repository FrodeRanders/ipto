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
 * Exception thrown if system is found to be inconsistent, e.g. due to inconsistencies in the database.
 */
public class SystemInconsistencyException extends BaseException {
    /**
     * Default constructor
     */
    public SystemInconsistencyException() {
    }

    /**
     * Overridden constructor from {@link BaseException}.
     *
     * @param msg error message
     */
    public SystemInconsistencyException(String msg) {
        super(msg);
    }

    /**
     * Overridden constructor from {@link BaseException }
     *
     * @param msg message
     */
    public SystemInconsistencyException(String msg, Throwable t) {
        super(msg, t);
    }
}


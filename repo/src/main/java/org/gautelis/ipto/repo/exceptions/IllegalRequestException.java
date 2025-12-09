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
 * Exception used when problems occur due requesting operations that
 * are not allowed.
 */
public class IllegalRequestException extends BaseException {
    /**
     * Default constructor
     */
    public IllegalRequestException() {
    }

    /**
     * Overridden constructor from {@link BaseException}.
     *
     * @param msg error message
     */
    public IllegalRequestException(String msg) {
        super(msg);
    }
}





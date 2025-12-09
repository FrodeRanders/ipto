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

import java.io.PrintWriter;
import java.util.Vector;

/**
 * Base exception, based on RuntimeException, that does not have to be declared to be thrown but
 * functions as a catch-all for the various exceptions used.
 */
public abstract class BaseException extends RuntimeException {

    private Throwable originalException = null;

    /**
     * Not allowed
     */
    public BaseException() {
    }

    /**
     * Overridden constructor from {@link Exception }
     *
     * @param msg message
     */
    public BaseException(String msg) {
        super(msg);
    }

    /**
     * Overridden constructor from {@link Exception }
     *
     * @param msg message
     */
    public BaseException(String msg, Throwable originalException) {
        this(msg);
        this.originalException = originalException;
    }


    public Vector<Throwable> getExceptionRecursive() {
        Vector<Throwable> vector;
        if (null != originalException) {
            if (originalException instanceof BaseException) {
                vector = ((BaseException) originalException).getExceptionRecursive();
            } else {
                vector = new Vector<>();
                vector.add(originalException);
            }
        } else {
            vector = new Vector<>();
        }
        vector.insertElementAt(this, 0);
        return vector;
    }


    public Vector<String> getMessageRecursive() {
        Vector<String> vector;
        if (null != originalException) {
            if (originalException instanceof BaseException) {
                vector = ((BaseException) originalException).getMessageRecursive();
            } else {
                vector = new Vector<>();
                vector.add(originalException.getMessage());
            }
        } else {
            vector = new Vector<>();
        }
        vector.insertElementAt(getMessage(), 0);
        return vector;
    }

    public void printStackTraceRecursive() {
        if (null != originalException) {
            if (originalException instanceof BaseException) {
                ((BaseException) originalException).printStackTraceRecursive();
            } else {
                originalException.printStackTrace();
            }
        }
        printStackTrace();
    }

    public void printStackTraceRecursive(PrintWriter out) {
        if (null != originalException) {
            if (originalException instanceof BaseException) {
                ((BaseException) originalException).printStackTraceRecursive(out);
            } else {
                originalException.printStackTrace(out);
            }
        }
        printStackTrace(out);
    }
}


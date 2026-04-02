/*
 * Copyright (C) 2025-2026 Frode Randers
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
package org.gautelis.ipto.repo.model.attributes;

import java.util.ArrayList;

/**
 * Callback receiving the mutable value vector of a resolved attribute.
 *
 * @param <A> the Java element type of the value vector
 */
@FunctionalInterface
public interface AttributeValueRunnable<A> {
    /**
     * Invoked with the attribute value vector.
     *
     * @param value the mutable value vector
     */
    void run(ArrayList<A> value);
}

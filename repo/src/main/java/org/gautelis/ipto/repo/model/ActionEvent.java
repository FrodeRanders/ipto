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
package org.gautelis.ipto.repo.model;

import java.util.EventObject;

/**
 * Base action event class
 */
public class ActionEvent extends EventObject {

    private final Type actionType;
    private final String description;
    /**
     * Constructor for an event.
     *
     * @param source     Source object affected by the action
     * @param actionType action type
     */
    public ActionEvent(
            Object source,
            Type actionType,
            String description) {

        super(source);
        this.actionType = actionType;
        this.description = description;
    }

    /**
     * Get action id for action
     *
     * @return action id
     */
    public Type getActionType() {
        return actionType;
    }

    /**
     * Get description of action
     *
     * @return Action description
     */
    public String getDescription() {
        return description;
    }

    public String toString() {
        return actionType + " - " + description;
    }


    public enum Type {
        ACCESSED(10, "accessed"),
        ASSOCIATION_ADDED(20, "associated"),
        ASSOCIATION_REMOVED(30, "deassociated"),
        LOCKED(40, "locked"),
        UNLOCKED(50, "unlocked"),
        CREATED(60, "created"),
        UPDATED(80, "updated"),
        DELETED(90, "deleted");

        private final int eventLevel;
        private final String description;

        Type(int eventLevel, String description) {
            this.eventLevel = eventLevel;
            this.description = description;
        }

        public int getLevel() {
            return eventLevel;
        }

        public String getDescription() {
            return description;
        }
    }
}



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
package org.gautelis.ipto.repo.listeners;

import org.gautelis.ipto.repo.db.Database;
import org.gautelis.ipto.repo.model.ActionEvent;
import org.gautelis.ipto.repo.model.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;


public final class LoggingActionListener implements ActionListener {
    private static final Logger log = LoggerFactory.getLogger(LoggingActionListener.class);
    private static final String LOG_STATEMENT = "INSERT INTO repo_log (tenantid,unitid,unitver,event,logentry) VALUES (?,?,?,?,?)";
    private DataSource ds;

    public LoggingActionListener() {
    }

    public void initialize(DataSource ds) {
        this.ds = ds;
    }

    public void actionPerformed(ActionEvent e) {

        Object o = e.getSource();
        if (o instanceof final Unit unit) {
            try {
                Database.usePreparedStatement(ds, LOG_STATEMENT, pStmt -> {
                    int i = 0;
                    pStmt.setInt(++i, unit.getTenantId());
                    pStmt.setLong(++i, unit.getUnitId());
                    pStmt.setInt(++i, unit.getVersion());
                    pStmt.setInt(++i, e.getActionType().getLevel()); // event
                    pStmt.setString(++i, e.getDescription()); // logentry

                    Database.executeUpdate(pStmt);
                });
            } catch (Throwable t) {
                log.warn("Failed to write to audit log: {}", t.getMessage(), t);
            }
        }
    }
}


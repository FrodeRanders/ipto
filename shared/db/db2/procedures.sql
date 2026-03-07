---------------------------------------------------------------
-- Copyright (C) 2025-2026 Frode Randers
-- All rights reserved
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
---------------------------------------------------------------

SET CURRENT SCHEMA REPO;

CREATE OR REPLACE PROCEDURE INGEST_NEW_UNIT_JSON (
    IN  p_unit     CLOB(2M), -- alternatively VARCHAR(32704)
    OUT p_unitid   BIGINT,
    OUT p_unitver  INTEGER,
    OUT p_created  TIMESTAMP,
    OUT p_modified TIMESTAMP
)
    EXTERNAL NAME 'IPTO_JAR:org.gautelis.ipto.repo.udpf.Routines.ingestNewUnit'
    LANGUAGE JAVA
    PARAMETER STYLE JAVA
    NO DBINFO
    FENCED
    MODIFIES SQL DATA;


CREATE OR REPLACE PROCEDURE INGEST_NEW_VERSION_JSON (
   IN  P_UNIT     CLOB(2M), -- alternatively VARCHAR(32704)
   OUT P_UNITVER  INTEGER,
   OUT P_MODIFIED TIMESTAMP
)
    EXTERNAL NAME 'IPTO_JAR:org.gautelis.ipto.repo.udpf.Routines.ingestNewVersion'
    LANGUAGE JAVA
    PARAMETER STYLE JAVA
    NO DBINFO
    FENCED
    MODIFIES SQL DATA;


CREATE OR REPLACE PROCEDURE EXTRACT_UNIT_JSON (
    IN  p_tenantid INTEGER,
    IN  p_unitid   BIGINT,
    IN  p_unitver  INTEGER, -- -1 == latest version
    OUT p_json     CLOB(2M)
)
    EXTERNAL NAME 'IPTO_JAR:org.gautelis.ipto.repo.udpf.Routines.extractUnit'
    LANGUAGE JAVA
    PARAMETER STYLE JAVA
    DETERMINISTIC
    NO DBINFO
    FENCED
    THREADSAFE
    READS SQL DATA;


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

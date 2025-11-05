---------------------------------------------------------------
-- Copyright (C) 2025 Frode Randers
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


---------------------------------------------------------------
-- Analysis of index usage
--
SELECT
    relname                                               AS tablename,
    to_char(seq_scan, '999,999,999,999')                  AS totalseqscan,
    to_char(idx_scan, '999,999,999,999')                  AS totalindexscan,
    to_char(n_live_tup, '999,999,999,999')                AS tablerows,
    pg_size_pretty(pg_relation_size(relname :: regclass)) AS tablesize
FROM pg_catalog.pg_stat_user_tables
--WHERE 50 * seq_scan > idx_scan -- more than 2%
-- AND n_live_tup > 10000
-- AND pg_relation_size(relname :: regclass) > 5000000
ORDER BY relname ASC;


---------------------------------------------------------------
-- In order to understand why a sequential scan at times are
-- chosen before using an index, an 'EXPLAIN (ANALYZE,BUFFERS)´
-- can be issued on relevant queries to observe the chosen
-- plan and confirm why the seq-scan was cheaper.

-- Needs theese two lines in postgresql.conf:
--
-- shared_preload_libraries = 'pg_stat_statements'
-- pg_stat_statements.track = all

-- Configuration file is here:
SHOW config_file;

-- Needs this extension (which has to be enabled by super user)
--
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- This is the query
SELECT calls,
       round(total_exec_time/1000) AS s,
       query
FROM   pg_stat_statements
WHERE  query LIKE '%repo_attribute_value%'
  AND  (upper(query) LIKE '%SEQSCAN%' OR total_plan_time = 0)
ORDER BY total_exec_time DESC
LIMIT 10;

-- This is an answer
-- [
--   {
--     "calls": 1,
--     "s": 96,
--     "query": "WITH c1 AS (SELECT av.tenantid, av.unitid FROM repo_attribute_value av JOIN repo_time_vector vv ON av.valueid = vv.valueid WHERE av.tenantid = $1 AND av.attrid = $8 AND vv.value >= $2), c2 AS (SELECT av.tenantid, av.unitid FROM repo_attribute_value av JOIN repo_string_vector vv ON av.valueid = vv.valueid WHERE av.tenantid = $3 AND av.attrid = $9 AND lower(vv.value) = lower($4)), final AS ((SELECT * FROM c1) INTERSECT (SELECT * FROM c2)) SELECT uk.tenantid, uk.unitid, uv.unitver, uk.created, uv.modified FROM repo_unit_kernel uk, repo_unit_version uv JOIN final f USING (tenantid, unitid) WHERE uk.tenantid = uv.tenantid AND uk.unitid = uv.unitid AND uk.lastver = uv.unitver AND ((uk.tenantid = $5 AND uk.status = $6) AND uk.created >= $7) ORDER BY uk.created DESC LIMIT $10"
--   },
--   {
--     "calls": 351030,
--     "s": 5,
--     "query": "INSERT INTO repo_attribute_value (tenantid, unitid, attrid, unitverfrom, unitverto)\n            VALUES (v_tenantid, p_unitid, attr.attrid, p_unitver, p_unitver)\n            RETURNING valueid"
--   },
--   {
--     "calls": 50005,
--     "s": 3,
--     "query": "UPDATE repo_record_vector cv\n        SET    ref_valueid = child.valueid\n        FROM   repo_attribute_value parent,\n               repo_attribute_value child\n        WHERE  parent.valueid  = cv.valueid        -- tie cv to its parent row\n          AND  parent.tenantid = v_tenantid        -- limit to current unit\n          AND  parent.unitid   = p_unitid\n          AND  child.tenantid  = parent.tenantid   -- child in same unit\n          AND  child.unitid    = parent.unitid\n          AND  child.attrid    = cv.ref_attrid\n          AND  cv.ref_valueid IS NULL"
--   },
--   {
--     "calls": 802576,
--     "s": 1,
--     "query": "SELECT $2 FROM ONLY \"repo\".\"repo_attribute_value\" x WHERE \"valueid\" OPERATOR(pg_catalog.=) $1 FOR KEY SHARE OF x"
-- },
--   {
--     "calls": 500,
--     "s": 0,
--     "query": "UPDATE repo_attribute_value\n                        SET unitverto = p_unitver\n                        WHERE tenantid = v_tenantid\n                          AND unitid = v_unitid\n                          AND attrid = r_prim.attrid\n                          AND unitverfrom <= prev_ver\n                          AND unitverto   >= prev_ver"
--   },
--   {
--     "calls": 500,
--     "s": 0,
--     "query": "INSERT INTO repo_attribute_value(tenantid, unitid, attrid, unitverfrom, unitverto)\n    VALUES (p_tenantid,p_unitid,p_attrid,p_from,p_to)\n    RETURNING valueid"
--   },
--   {
--     "calls": 1,
--     "s": 0,
--     "query": "SELECT COUNT(*) FROM repo_attribute_value"
--   },
--   {
--     "calls": 500,
--     "s": 0,
--     "query": "UPDATE repo_attribute_value av\n    SET unitverto = p_unitver - $11\n    WHERE av.tenantid = v_tenantid\n      AND av.unitid = v_unitid\n      AND av.unitverfrom <= prev_ver\n      AND av.unitverto >= prev_ver\n      AND av.attrid NOT IN (SELECT attrid FROM in_attrs)"
--   },
--   {
--     "calls": 5,
--     "s": 0,
--     "query": "WITH unit_hdr AS (\n    SELECT uk.tenantid,\n           uk.unitid,\n           uv.unitver,\n           uk.lastver,\n           uk.corrid,\n           uk.status,\n           uk.created,\n           uv.modified,\n           uv.unitname\n    FROM repo_unit_kernel uk\n    JOIN repo_unit_version uv ON uk.tenantid = uv.tenantid AND uk.unitid = uv.unitid AND uv.unitver = COALESCE(p_unitver, uk.lastver)\n    WHERE uk.tenantid = p_tenantid\n      AND uk.unitid = p_unitid\n),\n     attrs AS (\n         SELECT av.attrid,\n                a.attrtype,\n                a.attrname,\n                av.unitverfrom,\n                av.unitverto,\n                av.valueid,\n\n                -- one array per primitive type (NULL when not applicable)\n                CASE a.attrtype\n                    WHEN $4  -- STRING\n                         THEN (SELECT jsonb_agg(sv.value ORDER BY sv.idx)\n                               FROM repo_string_vector sv\n                               WHERE sv.valueid = av.valueid)\n                    WHEN $5  -- TIME\n                         THEN (SELECT jsonb_agg(tv.value ORDER BY tv.idx)\n                               FROM repo_time_vector tv\n                               WHERE tv.valueid = av.valueid)\n                    WHEN $6  -- INTEGER\n                         THEN (SELECT jsonb_agg(iv.value ORDER BY iv.idx)\n                               FROM repo_integer_vector iv\n                               WHERE iv.valueid = av.valueid)\n                    WHEN $7  -- LONG\n                         THEN (SELECT jsonb_agg(lv.value ORDER BY lv.idx)\n                               FROM repo_long_vector lv\n                               WHERE lv.valueid = av.valueid)\n                    WHEN $8  -- DOUBLE\n                         THEN (SELECT jsonb_agg(dv.value ORDER BY dv.idx)\n                               FROM repo_double_vector dv\n                               WHERE dv.valueid = av.valueid)\n                    WHEN $9  -- BOOLEAN\n                         THEN (SELECT jsonb_agg(bv.value ORDER BY bv.idx)\n                               FROM repo_boolean_vector bv\n                               WHERE bv.valueid = av.valueid)\n                    WHEN $10  -- DATA / BLOB\n                         THEN (SELECT jsonb_agg( encode(dat.value, $11) ORDER BY dat.idx )\n                               FROM repo_data_vector dat\n                               WHERE dat.valueid = av.valueid)\n                    WHEN $12 -- RECORD\n                         THEN (SELECT jsonb_agg(jsonb_build_object(\n                             $13,  rv.ref_attrid,\n                             $14, rv.ref_valueid) ORDER BY rv.idx)\n                         FROM repo_record_vector rv\n                         WHERE rv.valueid = av.valueid)\n               END AS value\n         FROM repo_attribute_value av\n         JOIN repo_attribute a ON a.attrid = av.attrid\n         JOIN repo_unit_kernel uk ON uk.tenantid = av.tenantid AND uk.unitid = av.unitid\n         WHERE uk.lastver <= av.unitverto\n           AND uk.lastver >= av.unitverfrom\n           AND av.tenantid = p_tenantid\n           AND av.unitid = p_unitid\n     )\nSELECT jsonb_build_object(\n           $15, $16,\n           $17, $18,\n           $19, u.tenantid,\n           $20,   u.unitid,\n           $21,  u.unitver,\n           $22,   u.corrid,\n           $23,   u.status,\n           $24, u.unitname,\n           $25,  u.created,\n           $26, u.modified,\n           $27, u.lastver > u.unitver,\n           $28,\n           (SELECT jsonb_agg( -- array of attribute objects\n                   jsonb_strip_nulls( -- omit NULL fields to keep size down?\n                           jsonb_build_object(\n                                   $29,       attrid,\n                                   $30,     attrtype,\n                                   $31,     attrname,\n                                   $32,  unitverfrom,\n                                   $33,    unitverto,\n                                   $34,      valueid,\n                                   $35,        value\n                           )\n                   )\n            )\n            FROM attrs)\n       )\nFROM unit_hdr u"
--   },
--   {
--     "calls": 500,
--     "s": 0,
--     "query": "UPDATE in_record_elems e\n    SET ref_valueid = (\n        SELECT av.valueid\n        FROM repo_attribute_value av\n        WHERE av.tenantid = v_tenantid\n          AND av.unitid = v_unitid\n          AND av.attrid = e.ref_attrid\n          AND av.unitverfrom <= p_unitver\n          AND av.unitverto >= p_unitver\n        LIMIT $8\n    )"
--   }
-- ]

---------------------------------------------------------------
--
SELECT t.relname AS tablename, t.indexrelname AS indexname, t.idx_blks_read, t.idx_blks_hit
FROM pg_catalog.pg_statio_all_indexes t
WHERE schemaname = 'repo'
ORDER BY idx_blks_read DESC, relname ASC;


----------------------------------------------------------------
--
-- https://pglinter.readthedocs.io/en/latest/
--
-- Can we go ahead with creating the extension or do we have to fetch it first?
SELECT *
FROM pg_available_extensions
WHERE name = 'pglinter';

-- Replace XX with your PG major version (13, 14, 15, 16, 17, or 18)
-- wget https://github.com/pmpetit/pglinter/releases/download/0.0.19/postgresql_pglinter_XX_0.0.19_amd64.deb
-- sudo dpkg -i postgresql_pglinter_XX_0.0.19_amd64.deb
-- sudo apt-get -f install   # if dependencies are needed

CREATE EXTENSION pglinter;

-- Analyze entire database
SELECT pglinter.perform_base_check();

-- Save results to file
SELECT pglinter.perform_base_check('/tmp/results.sarif');


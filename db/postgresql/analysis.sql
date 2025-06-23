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

-- Needs this extension (which has to be enabled by super user)
--
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

--
SELECT calls,
       round(total_exec_time/1000) AS s,
       query
FROM   pg_stat_statements
WHERE  query LIKE '%repo_attribute_value%'
  AND  (upper(query) LIKE '%SEQSCAN%' OR total_plan_time = 0)
ORDER BY total_exec_time DESC
LIMIT 10;


---------------------------------------------------------------
--
SELECT t.relname AS tablename, t.indexrelname AS indexname, t.idx_blks_read, t.idx_blks_hit
FROM pg_catalog.pg_statio_all_indexes t
WHERE schemaname = 'repo'
ORDER BY idx_blks_read DESC, relname ASC;

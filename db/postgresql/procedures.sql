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
-- Store units, with attributes, with value vectors, from JSON
--
CREATE OR REPLACE PROCEDURE ingest_unit_json (
    IN  p_unit     jsonb,
    OUT p_unitid   bigint,
    OUT p_created  timestamp
)
    LANGUAGE plpgsql
AS $$
DECLARE
    v_tenantid   int         := (p_unit ->> 'tenantid')::int;
    v_corrid     uuid        := (p_unit ->> 'corrid')::uuid;
    v_status     int         := (p_unit ->> 'status')::int;
    v_name       text        :=  p_unit ->> 'name';

    v_valueid    bigint;          -- last INSERT … RETURNING value
    attr         record;          -- loop variable
    has_record_attribs boolean := false;
BEGIN
    --------------------------------------------------------------------
    -- 1. insert unit header row, capture unitid + created timestamp
    --------------------------------------------------------------------
    INSERT INTO repo_unit (tenantid, corrid, status, name)
    VALUES (v_tenantid, v_corrid, v_status, v_name)
    RETURNING unitid, created
        INTO  p_unitid, p_created;

    --------------------------------------------------------------------
    -- 2. single scan over attribute array (JSON)
    --------------------------------------------------------------------
    FOR attr IN
        SELECT *
        FROM jsonb_to_recordset(p_unit -> 'attributes') AS
                 (  attrid       int
                  , attrtype     int
                  , val          jsonb
                 )
        LOOP
            /* 2a. (tenantid,unitid,attrid) → repo_attribute_value → valueid ----*/
            INSERT INTO repo_attribute_value (tenantid, unitid, attrid)
            VALUES (v_tenantid, p_unitid, attr.attrid)
            RETURNING valueid INTO v_valueid;

            /* 2b. vector tables by attrtype --------------------------------*/
            CASE attr.attrtype
                WHEN 1 THEN  -- STRING
                    INSERT INTO repo_string_vector(valueid, idx, val)
                    SELECT v_valueid, ord-1, elem
                    FROM jsonb_array_elements_text(attr.val) WITH ORDINALITY AS t(elem, ord);

                WHEN 2 THEN  -- TIME
                    INSERT INTO repo_time_vector(valueid, idx, val)
                    SELECT v_valueid, ord-1, elem::timestamp
                    FROM jsonb_array_elements_text(attr.val) WITH ORDINALITY AS t(elem, ord);

                WHEN 3 THEN  -- INT
                    INSERT INTO repo_integer_vector(valueid, idx, val)
                    SELECT v_valueid, ord-1, elem::int
                    FROM jsonb_array_elements_text(attr.val) WITH ORDINALITY AS t(elem, ord);

                WHEN 4 THEN  -- LONG
                    INSERT INTO repo_long_vector(valueid, idx, val)
                    SELECT v_valueid, ord-1, elem::bigint
                    FROM jsonb_array_elements_text(attr.val) WITH ORDINALITY AS t(elem, ord);

                WHEN 5 THEN  -- DOUBLE
                    INSERT INTO repo_double_vector(valueid, idx, val)
                    SELECT v_valueid, ord-1, elem::double precision
                    FROM jsonb_array_elements_text(attr.val) WITH ORDINALITY AS t(elem, ord);

                WHEN 6 THEN  -- BOOLEAN
                    INSERT INTO repo_boolean_vector(valueid, idx, val)
                    SELECT v_valueid, ord-1, elem::boolean
                    FROM jsonb_array_elements_text(attr.val) WITH ORDINALITY AS t(elem, ord);

                WHEN 7 THEN  -- DATA / BLOB (base-64)
                    INSERT INTO repo_data_vector(valueid, idx, val)
                    SELECT v_valueid, ord-1, decode(elem,'base64')
                    FROM jsonb_array_elements_text(attr.val) WITH ORDINALITY AS t(elem, ord);

                WHEN 99 THEN -- RECORD  (placeholder ref_valueid NULL)
                    has_record_attribs := true;
                    INSERT INTO repo_record_vector(valueid, idx, ref_attrid, ref_valueid)
                    SELECT v_valueid, ord-1, t.ref_attrid, NULL
                    FROM ROWS FROM (
                        jsonb_to_recordset(attr.val) AS (ref_attrid int, ref_valueid bigint)
                    ) WITH ORDINALITY AS t(ref_attrid, ref_valueid, ord);

                ELSE
                    RAISE EXCEPTION
                        'Unknown attrtype % for attrid %', attr.attrtype, attr.attrid
                        USING ERRCODE = '22023';
                END CASE;
        END LOOP;

    --------------------------------------------------------------------
    -- 3. resolve record placeholders
    --------------------------------------------------------------------
    IF has_record_attribs THEN
        UPDATE repo_record_vector cv
        SET    ref_valueid = child.valueid
        FROM   repo_attribute_value parent,
               repo_attribute_value child
        WHERE  parent.valueid  = cv.valueid        -- tie cv to its parent row
          AND  parent.tenantid = v_tenantid        -- limit to current unit
          AND  parent.unitid   = p_unitid
          AND  child.tenantid  = parent.tenantid   -- child in same unit
          AND  child.unitid    = parent.unitid
          AND  child.attrid    = cv.ref_attrid
          AND  cv.ref_valueid IS NULL;             -- update only placeholders
    END IF;
END;
$$;


---------------------------------------------------------------
-- Load units, with attributes, with value vectors, as JSON
--
CREATE OR REPLACE FUNCTION export_unit_json(
    p_tenantid integer,
    p_unitid   bigint
)
    RETURNS jsonb
    LANGUAGE sql STABLE
AS $func$
WITH unit_hdr AS (
    SELECT tenantid,
           unitid,
           corrid,
           status,
           name,
           created
    FROM   repo_unit
    WHERE  tenantid = p_tenantid
      AND  unitid   = p_unitid
),
     attrs AS (
         SELECT av.attrid,
                a.attrtype,
                a.attrname,
                av.valueid,

                -- one array per primitive type (NULL when not applicable)
                CASE a.attrtype
                    WHEN 1  -- STRING
                         THEN (SELECT jsonb_agg(sv.val ORDER BY sv.idx)
                               FROM repo_string_vector sv
                               WHERE sv.valueid = av.valueid)
                    WHEN 2  -- TIME
                         THEN (SELECT jsonb_agg(tv.val ORDER BY tv.idx)
                               FROM repo_time_vector tv
                               WHERE tv.valueid = av.valueid)
                    WHEN 3  -- INTEGER
                         THEN (SELECT jsonb_agg(iv.val ORDER BY iv.idx)
                               FROM repo_integer_vector iv
                               WHERE iv.valueid = av.valueid)
                    WHEN 4  -- LONG
                         THEN (SELECT jsonb_agg(lv.val ORDER BY lv.idx)
                               FROM repo_long_vector lv
                               WHERE lv.valueid = av.valueid)
                    WHEN 5  -- DOUBLE
                         THEN (SELECT jsonb_agg(dv.val ORDER BY dv.idx)
                               FROM repo_double_vector dv
                               WHERE dv.valueid = av.valueid)
                    WHEN 6  -- BOOLEAN
                         THEN (SELECT jsonb_agg(bv.val ORDER BY bv.idx)
                               FROM repo_boolean_vector bv
                               WHERE bv.valueid = av.valueid)
                    WHEN 7  -- DATA / BLOB
                         THEN (SELECT jsonb_agg( encode(dat.val, 'base64') ORDER BY dat.idx )
                               FROM repo_data_vector dat
                               WHERE dat.valueid = av.valueid)
                    WHEN 99 -- RECORD
                         THEN (SELECT jsonb_agg(jsonb_build_object(
                             'ref_attrid',  rv.ref_attrid,
                             'ref_valueid', rv.ref_valueid) ORDER BY rv.idx)
                         FROM repo_record_vector rv
                         WHERE rv.valueid = av.valueid)
               END AS val
         FROM repo_attribute_value av
         JOIN repo_attribute a ON a.attrid = av.attrid
         WHERE av.tenantid = p_tenantid
           AND av.unitid = p_unitid
     )
SELECT jsonb_build_object(
           '@type', 'unit',
           '@version', 1,
           'tenantid', u.tenantid,
           'unitid',   u.unitid,
           'corrid',   u.corrid,
           'status',   u.status,
           'name',     u.name,
           'created',  u.created,
           'attributes',
           (SELECT jsonb_agg( -- array of attribute objects
                   jsonb_strip_nulls( -- omit NULL fields to keep size down?
                           jsonb_build_object(
                                   'attrid',       attrid,
                                   'attrtype',     attrtype,
                                   'attrname',     attrname,
                                   'valueid',      valueid,
                                   'val',          val
                           )
                   )
            )
            FROM attrs)
       )
FROM unit_hdr u;
$func$;
;


---------------------------------------------------------------
-- Load attributes, with value vectors, for a given unit
--
CREATE OR REPLACE FUNCTION load_unit_vectors (
        p_tenantid INTEGER,
        p_unitid   BIGINT
)
RETURNS TABLE (
    valueid        BIGINT,
    attrid         INTEGER,
    attrtype       INTEGER,
    attrname       TEXT,
    parent_valueid BIGINT,   -- parent record’s valueid (NULL means top-level)
    record_idx     INTEGER,  -- ordinal inside parent record
    depth          INTEGER,

    idx            INTEGER,
    string_val     TEXT,
    time_val       TIMESTAMP,
    int_val        INTEGER,
    long_val       BIGINT,
    double_val     DOUBLE PRECISION,
    bool_val       BOOLEAN,
    data_val       BYTEA
)
LANGUAGE sql
STABLE
AS $func$
WITH RECURSIVE attr_tree AS (
    -- Depth 0: unit attributes
    SELECT av.valueid,
           av.attrid,
           p.attrtype,
           p.attrname,
           NULL::bigint AS parent_valueid,   -- top level
           NULL::int    AS record_idx,
           0 AS depth
    FROM repo_attribute_value av
    JOIN repo_attribute p ON p.attrid = av.attrid
    WHERE av.tenantid = p_tenantid
      AND av.unitid = p_unitid
      AND NOT EXISTS (SELECT 1 FROM repo_record_vector cv WHERE cv.ref_valueid = av.valueid)
  UNION ALL
    -- Depth n: follows record vectors
    SELECT cv.ref_valueid,
           cv.ref_attrid,
           p.attrtype,
           p.attrname,
           cv.valueid AS parent_valueid,
           cv.idx     AS record_idx,
           at.depth + 1
    FROM attr_tree at
           JOIN repo_record_vector cv ON cv.valueid = at.valueid
           JOIN repo_attribute p ON p.attrid = cv.ref_attrid
)
SELECT
    t.valueid,
    t.attrid,
    t.attrtype,
    t.attrname,
    t.parent_valueid,
    t.record_idx,
    t.depth,

    COALESCE(sv.idx, tv.idx, iv.idx, lv.idx, dov.idx, bv.idx, dv.idx) AS idx,

    sv.val  AS string_val,
    tv.val  AS time_val,
    iv.val  AS int_val,
    lv.val  AS long_val,
    dov.val AS double_val,
    bv.val  AS bool_val,
    dv.val  AS data_val
FROM attr_tree t
       LEFT JOIN repo_string_vector  sv  ON t.attrtype = 1 AND t.valueid = sv.valueid
       LEFT JOIN repo_time_vector    tv  ON t.attrtype = 2 AND t.valueid = tv.valueid
       LEFT JOIN repo_integer_vector iv  ON t.attrtype = 3 AND t.valueid = iv.valueid
       LEFT JOIN repo_long_vector    lv  ON t.attrtype = 4 AND t.valueid = lv.valueid
       LEFT JOIN repo_double_vector  dov ON t.attrtype = 5 AND t.valueid = dov.valueid
       LEFT JOIN repo_boolean_vector bv  ON t.attrtype = 6 AND t.valueid = bv.valueid
       LEFT JOIN repo_data_vector    dv  ON t.attrtype = 7 AND t.valueid = dv.valueid
ORDER BY t.depth,
         COALESCE(t.parent_valueid, t.valueid),   -- group siblings
         t.record_idx,                            -- preserve order in record
         idx
$func$;
;


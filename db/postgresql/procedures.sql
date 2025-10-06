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
-- Writes a primitive (string, integer, long, ...) attribute value row and its value vector
--
CREATE OR REPLACE FUNCTION repo_write_vector(
    p_tenantid int, p_unitid bigint, p_attrid int,
    p_from int, p_to int,
    p_attrtype int, p_value jsonb
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE v_valueid bigint;
BEGIN
    INSERT INTO repo_attribute_value(tenantid, unitid, attrid, unitverfrom, unitverto)
    VALUES (p_tenantid,p_unitid,p_attrid,p_from,p_to)
    RETURNING valueid INTO v_valueid;

    /*
     * If we anticipate a lot of versions of units with multiple
     * attribute (value) modifications, the cost of keeping a 'current'
     * representation of attribute values becomes more palatable
     *
    INSERT INTO repo_attribute_current_value(tenantid, unitid, attrid, valueid)
    VALUES (p_tenantid, p_unitid, p_attrid, v_valueid)
    ON CONFLICT ON CONSTRAINT repo_attribute_current_value_pk
        DO UPDATE SET valueid = v_valueid;
     */

    CASE p_attrtype
        WHEN 1 THEN  -- STRING
            INSERT INTO repo_string_vector(valueid, idx, value)
            SELECT v_valueid, ord-1, elem
            FROM jsonb_array_elements_text(p_value) WITH ORDINALITY AS t(elem,ord);

        WHEN 2 THEN  -- TIME
            INSERT INTO repo_time_vector(valueid, idx, value)
            SELECT v_valueid, ord-1, (elem)::timestamp
            FROM jsonb_array_elements_text(p_value) WITH ORDINALITY AS t(elem,ord);

        WHEN 3 THEN  -- INT
            INSERT INTO repo_integer_vector(valueid, idx, value)
            SELECT v_valueid, ord-1, (elem)::int
            FROM jsonb_array_elements_text(p_value) WITH ORDINALITY AS t(elem,ord);

        WHEN 4 THEN  -- LONG
            INSERT INTO repo_long_vector(valueid, idx, value)
            SELECT v_valueid, ord-1, (elem)::bigint
            FROM jsonb_array_elements_text(p_value) WITH ORDINALITY AS t(elem,ord);

        WHEN 5 THEN  -- DOUBLE
            INSERT INTO repo_double_vector(valueid, idx, value)
            SELECT v_valueid, ord-1, (elem)::double precision
            FROM jsonb_array_elements_text(p_value) WITH ORDINALITY AS t(elem,ord);

        WHEN 6 THEN  -- BOOLEAN
            INSERT INTO repo_boolean_vector(valueid, idx, value)
            SELECT v_valueid, ord-1, (elem)::boolean
            FROM jsonb_array_elements_text(p_value) WITH ORDINALITY AS t(elem,ord);

        WHEN 7 THEN  -- DATA / BLOB (base-64)
            INSERT INTO repo_data_vector(valueid, idx, value)
            SELECT v_valueid, ord-1, decode(elem,'base64')
            FROM jsonb_array_elements_text(p_value) WITH ORDINALITY AS t(elem,ord);

        ELSE
            RAISE EXCEPTION 'Unsupported attrtype % for primitive attribute % in unit %.%',
                p_attrtype, p_attrid, p_tenantid, p_unitid;
        END CASE;
END $$;

---------------------------------------------------------------
-- Writes a record parent row; assumes record_element:s already have resolved ref_valueid
--
CREATE TYPE record_element AS (
    idx         int,
    ref_attrid  int,
    ref_valueid bigint
);

CREATE OR REPLACE FUNCTION repo_write_record(
    p_tenantid int, p_unitid bigint, p_attrid int,
    p_from int, p_to int,
    p_elems record_element[]
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE v_valueid bigint;
BEGIN
    INSERT INTO repo_attribute_value(tenantid, unitid, attrid, unitverfrom, unitverto)
    VALUES (p_tenantid,p_unitid,p_attrid,p_from,p_to)
    RETURNING valueid INTO v_valueid;

    INSERT INTO repo_record_vector(valueid, idx, ref_attrid, ref_valueid)
    SELECT v_valueid, e.idx, e.ref_attrid, e.ref_valueid
    FROM unnest(p_elems) AS e
    ORDER BY e.idx;
END $$;


---------------------------------------------------------------
-- Store a new unit, with attributes, with value vectors, from JSON
--
CREATE OR REPLACE PROCEDURE ingest_new_unit_json (
    IN  p_unit     jsonb,
    OUT p_unitid   bigint,
    OUT p_unitver  int,
    OUT p_created  timestamp,
    OUT p_modified timestamp
) LANGUAGE plpgsql AS $$
DECLARE
    v_tenantid   int         := (p_unit ->> 'tenantid')::int;
    v_corrid     uuid        := (p_unit ->> 'corrid')::uuid;
    v_status     int         := (p_unit ->> 'status')::int;
    v_unitname   text        :=  p_unit ->> 'unitname';

    v_valueid    bigint;
    attr         record;     -- loop variable
    has_record_attribs boolean := false;
BEGIN
    -- Insert unit header row, capture unitid + created timestamp
    INSERT INTO repo_unit_kernel (tenantid, corrid, status)
    VALUES (v_tenantid, v_corrid, v_status)
    RETURNING lastver, unitid, created
        INTO  p_unitver, p_unitid, p_created;

    INSERT INTO repo_unit_version (tenantid, unitid, unitver, unitname)
    VALUES (v_tenantid, p_unitid, p_unitver, v_unitname)
    RETURNING modified
         INTO p_modified;

    -- Single scan over attribute array (JSON)
    FOR attr IN
        SELECT *
        FROM jsonb_to_recordset(p_unit -> 'attributes') AS
                 (  attrid       int
                  , attrtype     int
                  , value        jsonb
                 )
        LOOP
            -- Write attribute value row, and retrieve new valueid
            INSERT INTO repo_attribute_value (tenantid, unitid, attrid, unitverfrom, unitverto)
            VALUES (v_tenantid, p_unitid, attr.attrid, p_unitver, p_unitver)
            RETURNING valueid INTO v_valueid;

            /*
             * If we anticipate a lot of versions of units with multiple
             * attribute (value) modifications, the cost of keeping a 'current'
             * representation of attribute values becomes more palatable
             *
            INSERT INTO repo_attribute_current_value (tenantid, unitid, attrid, valueid)
            VALUES (v_tenantid, p_unitid, attr.attrid, v_valueid);
             */

            -- Write corresponding value vector
            CASE attr.attrtype
                WHEN 1 THEN  -- STRING
                    INSERT INTO repo_string_vector(valueid, idx, value)
                    SELECT v_valueid, ord-1, elem
                    FROM jsonb_array_elements_text(attr.value) WITH ORDINALITY AS t(elem, ord);

                WHEN 2 THEN  -- TIME
                    INSERT INTO repo_time_vector(valueid, idx, value)
                    SELECT v_valueid, ord-1, elem::timestamp
                    FROM jsonb_array_elements_text(attr.value) WITH ORDINALITY AS t(elem, ord);

                WHEN 3 THEN  -- INT
                    INSERT INTO repo_integer_vector(valueid, idx, value)
                    SELECT v_valueid, ord-1, elem::int
                    FROM jsonb_array_elements_text(attr.value) WITH ORDINALITY AS t(elem, ord);

                WHEN 4 THEN  -- LONG
                    INSERT INTO repo_long_vector(valueid, idx, value)
                    SELECT v_valueid, ord-1, elem::bigint
                    FROM jsonb_array_elements_text(attr.value) WITH ORDINALITY AS t(elem, ord);

                WHEN 5 THEN  -- DOUBLE
                    INSERT INTO repo_double_vector(valueid, idx, value)
                    SELECT v_valueid, ord-1, elem::double precision
                    FROM jsonb_array_elements_text(attr.value) WITH ORDINALITY AS t(elem, ord);

                WHEN 6 THEN  -- BOOLEAN
                    INSERT INTO repo_boolean_vector(valueid, idx, value)
                    SELECT v_valueid, ord-1, elem::boolean
                    FROM jsonb_array_elements_text(attr.value) WITH ORDINALITY AS t(elem, ord);

                WHEN 7 THEN  -- DATA / BLOB (base-64)
                    INSERT INTO repo_data_vector(valueid, idx, value)
                    SELECT v_valueid, ord-1, decode(elem,'base64')
                    FROM jsonb_array_elements_text(attr.value) WITH ORDINALITY AS t(elem, ord);

                WHEN 99 THEN -- RECORD  (placeholder ref_valueid NULL)
                    has_record_attribs := true;
                    INSERT INTO repo_record_vector(valueid, idx, ref_attrid, ref_valueid)
                    SELECT v_valueid, ord-1, t.ref_attrid, NULL -- placeholder
                    FROM ROWS FROM (
                        jsonb_to_recordset(attr.value) AS (ref_attrid int, ref_valueid bigint)
                    ) WITH ORDINALITY AS t(ref_attrid, ref_valueid, ord);

                ELSE
                    RAISE EXCEPTION
                        'Unknown attrtype % for attrid %', attr.attrtype, attr.attrid
                        USING ERRCODE = '22023';
                END CASE;
        END LOOP;

    -- Resolve record placeholders (where ref_valued was set to NULL previously)
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
END $$;

---------------------------------------------------------------
-- Store a new version of an existing unit, with attributes, with value vectors, from JSON
--
CREATE OR REPLACE PROCEDURE ingest_new_version_json (
    IN  p_unit     jsonb,
    OUT p_unitver  int,
    OUT p_modified timestamp
) LANGUAGE plpgsql AS $$
DECLARE
    v_tenantid int    := (p_unit ->> 'tenantid')::int;
    v_unitid   bigint := (p_unit ->> 'unitid')::bigint;
    v_status   int    := (p_unit ->> 'status')::int;
    v_unitname text   :=  p_unit ->> 'unitname';
    prev_ver    int;
BEGIN
    -- Bump unit kernel and insert new version
    UPDATE repo_unit_kernel
    SET status = v_status, lastver = lastver + 1
    WHERE tenantid = v_tenantid AND unitid = v_unitid
    RETURNING lastver-1, lastver INTO prev_ver, p_unitver;

    INSERT INTO repo_unit_version (tenantid, unitid, unitver, unitname)
    VALUES (v_tenantid, v_unitid, p_unitver, v_unitname)
    RETURNING modified INTO p_modified;

    -- Setup temp tables for incoming attributes, discriminating between
    -- primitive attributes and record attributes.
    CREATE TEMP TABLE in_attrs (
       attrid      int,
       attrtype    int,
       ismodified  boolean DEFAULT false,
       value       jsonb
    ) ON COMMIT DROP;

    INSERT INTO in_attrs(attrid, attrtype, ismodified, value)
    SELECT (a->>'attrid')::int,
           (a->>'attrtype')::int,
           COALESCE((a->>'ismodified')::boolean, false),
           a->'value'
    FROM jsonb_array_elements(p_unit->'attributes') a;

    CREATE TEMP TABLE in_prim ON COMMIT DROP AS SELECT * FROM in_attrs WHERE attrtype <> 99;
    CREATE TEMP TABLE in_rec ON COMMIT DROP AS SELECT * FROM in_attrs WHERE attrtype = 99;

    -- Handle existing (modified and unmodified) primitive attributes
    DECLARE
        r_prim  RECORD;
    BEGIN
        FOR r_prim IN SELECT attrid, ismodified, attrtype, value FROM in_prim
            LOOP
                BEGIN
                    IF r_prim.ismodified THEN
                        -- Since attribute was modified, insert new value and corresponding value vector
                        PERFORM repo_write_vector(v_tenantid, v_unitid, r_prim.attrid,
                                                  p_unitver, p_unitver, r_prim.attrtype,
                                                  r_prim.value);
                    ELSE
                        -- Since attribute was unchanged, extend range of attribute value to new version
                        UPDATE repo_attribute_value
                        SET unitverto = p_unitver
                        WHERE tenantid = v_tenantid
                          AND unitid = v_unitid
                          AND attrid = r_prim.attrid
                          AND unitverfrom <= prev_ver
                          AND unitverto   >= prev_ver;
                    END IF;
                END;
            END LOOP;
    END;

    -- Handle primitive attributes that was removed from new version
    UPDATE repo_attribute_value av
    SET unitverto = p_unitver - 1
    WHERE av.tenantid = v_tenantid
      AND av.unitid = v_unitid
      AND av.unitverfrom <= prev_ver
      AND av.unitverto >= prev_ver
      AND av.attrid NOT IN (SELECT attrid FROM in_attrs);

    -- Handle record attributes, expand incoming record entries by
    -- resolving to child valueids effective at p_unitver
    CREATE TEMP TABLE in_record_elems (
        parent_attrid int,
        idx           int,
        ref_attrid    int,
        ref_valueid   bigint
    ) ON COMMIT DROP;

    INSERT INTO in_record_elems(parent_attrid, idx, ref_attrid)
    SELECT ir.attrid, t.ord-1, (t.elem->>'ref_attrid')::int
    FROM in_rec ir,
         LATERAL jsonb_array_elements(ir.value) WITH ORDINALITY AS t(elem, ord);

    -- Resolve each child ref to its *current* valueid at the new version.
    -- noinspection SqlUpdateWithoutWhere (since operating on temporary table)
    UPDATE in_record_elems e
    SET ref_valueid = (
        SELECT av.valueid
        FROM repo_attribute_value av
        WHERE av.tenantid = v_tenantid
          AND av.unitid = v_unitid
          AND av.attrid = e.ref_attrid
          AND av.unitverfrom <= p_unitver
          AND av.unitverto >= p_unitver
        LIMIT 1
    );

    -- Ensure that no unresolved children remain
    IF EXISTS (SELECT 1 FROM in_record_elems WHERE ref_valueid IS NULL) THEN
        RAISE EXCEPTION 'Record references attribute(s) without an effective value at %.%v%', v_tenantid, v_unitid, p_unitver
            USING ERRCODE = '22023';
    END IF;

    -- For each parent record attrid, extend or insert
    DECLARE
        record_id INT;
    BEGIN
        FOR record_id IN SELECT DISTINCT parent_attrid FROM in_record_elems
            LOOP
                DECLARE
                    prev_parent_valueid bigint;
                    same boolean;
                BEGIN
                    SELECT av.valueid
                    INTO prev_parent_valueid
                    FROM repo_attribute_value av
                    WHERE av.tenantid = v_tenantid
                      AND av.unitid   = v_unitid
                      AND av.attrid   = record_id
                      AND av.unitverfrom <= prev_ver
                      AND av.unitverto   >= prev_ver
                    LIMIT 1;

                    IF prev_parent_valueid IS NULL THEN
                        -- New record
                        DECLARE v_elems record_element[];
                        BEGIN
                            SELECT array_agg((idx, ref_attrid, ref_valueid)::record_element ORDER BY idx)
                            INTO   v_elems
                            FROM   in_record_elems
                            WHERE  parent_attrid = record_id;

                            PERFORM repo_write_record(v_tenantid, v_unitid, record_id, 1,
                                                      p_unitver, p_unitver, v_elems);
                        END;
                    ELSE
                        -- Compare ordered pairs (ref_attrid, ref_valueid)
                        SELECT
                            (SELECT array_agg(row(e.ref_attrid, e.ref_valueid) ORDER BY e.idx)
                             FROM in_record_elems e WHERE e.parent_attrid = record_id)
                                =
                            (SELECT array_agg(row(rv.ref_attrid, rv.ref_valueid) ORDER BY rv.idx)
                             FROM repo_record_vector rv WHERE rv.valueid = prev_parent_valueid)
                        INTO same;

                        IF same THEN
                            UPDATE repo_attribute_value
                            SET unitverto = p_unitver
                            WHERE tenantid = v_tenantid
                              AND unitid   = v_unitid
                              AND attrid   = record_id
                              AND unitverfrom <= prev_ver
                              AND unitverto   >= prev_ver;
                        ELSE
                            -- Insert record row + its elements
                            DECLARE v_elems record_element[];
                            BEGIN
                                SELECT array_agg((idx, ref_attrid, ref_valueid)::record_element ORDER BY idx)
                                INTO   v_elems
                                FROM   in_record_elems
                                WHERE  parent_attrid = record_id;

                                PERFORM repo_write_record(v_tenantid, v_unitid, record_id,
                                                          1,p_unitver, p_unitver, v_elems);
                            END;
                        END IF;
                    END IF;
                END;
            END LOOP;
    END;
END $$;


---------------------------------------------------------------
-- Load units, with attributes, with value vectors, as JSON
--
CREATE OR REPLACE FUNCTION extract_unit_json(
    p_tenantid integer,
    p_unitid   bigint,
    p_unitver  integer DEFAULT NULL
) RETURNS jsonb LANGUAGE sql STABLE AS $$
WITH unit_hdr AS (
    SELECT uk.tenantid,
           uk.unitid,
           uv.unitver,
           uk.lastver,
           uk.corrid,
           uk.status,
           uk.created,
           uv.modified,
           uv.unitname
    FROM repo_unit_kernel uk
    JOIN repo_unit_version uv ON uk.tenantid = uv.tenantid AND uk.unitid = uv.unitid AND uv.unitver = COALESCE(p_unitver, uk.lastver)
    WHERE uk.tenantid = p_tenantid
      AND uk.unitid = p_unitid
),
     attrs AS (
         SELECT av.attrid,
                a.attrtype,
                a.attrname,
                av.unitverfrom,
                av.unitverto,
                av.valueid,

                -- one array per primitive type (NULL when not applicable)
                CASE a.attrtype
                    WHEN 1  -- STRING
                         THEN (SELECT jsonb_agg(sv.value ORDER BY sv.idx)
                               FROM repo_string_vector sv
                               WHERE sv.valueid = av.valueid)
                    WHEN 2  -- TIME
                         THEN (SELECT jsonb_agg(tv.value ORDER BY tv.idx)
                               FROM repo_time_vector tv
                               WHERE tv.valueid = av.valueid)
                    WHEN 3  -- INTEGER
                         THEN (SELECT jsonb_agg(iv.value ORDER BY iv.idx)
                               FROM repo_integer_vector iv
                               WHERE iv.valueid = av.valueid)
                    WHEN 4  -- LONG
                         THEN (SELECT jsonb_agg(lv.value ORDER BY lv.idx)
                               FROM repo_long_vector lv
                               WHERE lv.valueid = av.valueid)
                    WHEN 5  -- DOUBLE
                         THEN (SELECT jsonb_agg(dv.value ORDER BY dv.idx)
                               FROM repo_double_vector dv
                               WHERE dv.valueid = av.valueid)
                    WHEN 6  -- BOOLEAN
                         THEN (SELECT jsonb_agg(bv.value ORDER BY bv.idx)
                               FROM repo_boolean_vector bv
                               WHERE bv.valueid = av.valueid)
                    WHEN 7  -- DATA / BLOB
                         THEN (SELECT jsonb_agg( encode(dat.value, 'base64') ORDER BY dat.idx )
                               FROM repo_data_vector dat
                               WHERE dat.valueid = av.valueid)
                    WHEN 99 -- RECORD
                         THEN (SELECT jsonb_agg(jsonb_build_object(
                             'ref_attrid',  rv.ref_attrid,
                             'ref_valueid', rv.ref_valueid) ORDER BY rv.idx)
                         FROM repo_record_vector rv
                         WHERE rv.valueid = av.valueid)
               END AS value
         FROM repo_attribute_value av
         JOIN repo_attribute a ON a.attrid = av.attrid
         JOIN repo_unit_kernel uk ON uk.tenantid = av.tenantid AND uk.unitid = av.unitid
         WHERE uk.lastver <= av.unitverto
           AND uk.lastver >= av.unitverfrom
           AND av.tenantid = p_tenantid
           AND av.unitid = p_unitid
     )
SELECT jsonb_build_object(
           '@type', 'unit',
           '@version', 2,
           'tenantid', u.tenantid,
           'unitid',   u.unitid,
           'unitver',  u.unitver,
           'corrid',   u.corrid,
           'status',   u.status,
           'unitname', u.unitname,
           'created',  u.created,
           'modified', u.modified,
           'isreadonly', u.lastver > u.unitver,
           'attributes',
           (SELECT jsonb_agg( -- array of attribute objects
                   jsonb_strip_nulls( -- omit NULL fields to keep size down?
                           jsonb_build_object(
                                   'attrid',       attrid,
                                   'attrtype',     attrtype,
                                   'attrname',     attrname,
                                   'unitverfrom',  unitverfrom,
                                   'unitverto',    unitverto,
                                   'valueid',      valueid,
                                   'value',        value
                           )
                   )
            )
            FROM attrs)
       )
FROM unit_hdr u;
$$;


---------------------------------------------------------------
-- Load attributes, with value vectors, for a given unit
--
CREATE OR REPLACE FUNCTION load_unit_vectors (
        p_tenantid INTEGER,
        p_unitid   BIGINT
) RETURNS TABLE (
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
) LANGUAGE sql STABLE AS $$
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
    sv.value  AS string_val,
    tv.value  AS time_val,
    iv.value  AS int_val,
    lv.value  AS long_val,
    dov.value AS double_val,
    bv.value  AS bool_val,
    dv.value  AS data_val
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
$$;



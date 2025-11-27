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
-- Function: repo_ingest_attributes
--
-- This function walks an array of attribute JSON objects ('p_attrs');
-- for each attribute {
--    inserts a row in repo_attribute_value,
--    if primitive {
--       writes the correct vector table (repo_string_vector, repo_time_vector, …);
--    }
--    if record (attrtype = 99) {
--       recurses into its nested value array;
--       if p_parent_record_valueid is non-NULL {
--           inserts a row into repo_record_vector
--           linking the parent record to this attribute occurrence
--       }
--    }
-- }
--
-- 'v_idx' is local per call, so every record gets its own 0…N-1 indexing
-- of children. 'repo_record_vector.valueid' is always the parent record’s
-- valueid.
---------------------------------------------------------------
CREATE OR REPLACE FUNCTION repo_ingest_attributes(
    p_tenantid               int,
    p_unitid                 bigint,
    p_unitver                int,
    p_attrs                  jsonb,
    p_parent_record_valueid  bigint DEFAULT NULL
) RETURNS void
    LANGUAGE plpgsql AS $$
DECLARE
    v_attr       jsonb;
    v_attrid     int;
    v_attrtype   int;
    v_value      jsonb;  -- for primitive values
    v_children   jsonb;  -- for nested attributes in records
    v_valueid    bigint;
    v_idx        int := 0; -- index within this record (if any)
BEGIN
    -- Nothing to do if null or not an array
    IF p_attrs IS NULL OR jsonb_typeof(p_attrs) <> 'array' THEN
        RETURN;
    END IF;

    FOR v_attr IN
        SELECT value
        FROM jsonb_array_elements(p_attrs)
        LOOP
            v_attrid   := (v_attr ->> 'attrid')::int;
            v_attrtype := (v_attr ->> 'attrtype')::int;

            IF v_attrid IS NULL OR v_attrtype IS NULL THEN
                RAISE EXCEPTION 'Attribute missing attrid or attrtype: %', v_attr;
            END IF;

            -----------------------------------------------------------------------
            -- Insert attribute_value row for this occurrence, capturing 'valueid'
            -----------------------------------------------------------------------
            INSERT INTO repo_attribute_value(
                tenantid, unitid, attrid, unitverfrom, unitverto
            )
            VALUES (
                       p_tenantid,
                       p_unitid,
                       v_attrid,
                       p_unitver,
                       p_unitver
                   )
            RETURNING valueid INTO v_valueid;

            -----------------------------------------------------------------------
            -- Depending on type: record vs primitive
            -----------------------------------------------------------------------
            IF v_attrtype = 99 THEN -- RECORD attribute
                -- If this record is a *child* of another record, link it.
                IF p_parent_record_valueid IS NOT NULL THEN
                    INSERT INTO repo_record_vector(valueid, idx, ref_attrid, ref_valueid)
                    VALUES (p_parent_record_valueid, v_idx, v_attrid, v_valueid);
                END IF;

                -- Children of this record live under "value",
                -- but we also handle "attributes" for the top unit
                v_children := COALESCE(v_attr->'attributes', v_attr->'value');

                IF v_children IS NOT NULL THEN
                    PERFORM repo_ingest_attributes(
                            p_tenantid,
                            p_unitid,
                            p_unitver,
                            v_children,
                            v_valueid -- this record is now the parent
                            );
                END IF;

            ELSE -- PRIMITIVE attribute
                -- write vector based on attrtype
                v_value := v_attr->'value';

                IF v_value IS NULL THEN
                    RAISE EXCEPTION
                        'Primitive attribute %.% (attrid=%) missing ''value'' array: %',
                        p_tenantid, p_unitid, v_attrid, v_attr;
                END IF;

                CASE v_attrtype
                    WHEN 1 THEN  -- STRING
                    INSERT INTO repo_string_vector(valueid, idx, value)
                    SELECT v_valueid, ord - 1, elem
                    FROM jsonb_array_elements_text(v_value)
                             WITH ORDINALITY AS t(elem, ord);

                    WHEN 2 THEN  -- TIME
                    INSERT INTO repo_time_vector(valueid, idx, value)
                    SELECT v_valueid, ord - 1, (elem)::timestamp
                    FROM jsonb_array_elements_text(v_value)
                             WITH ORDINALITY AS t(elem, ord);

                    WHEN 3 THEN  -- INT
                    INSERT INTO repo_integer_vector(valueid, idx, value)
                    SELECT v_valueid, ord - 1, (elem)::int
                    FROM jsonb_array_elements_text(v_value)
                             WITH ORDINALITY AS t(elem, ord);

                    WHEN 4 THEN  -- LONG
                    INSERT INTO repo_long_vector(valueid, idx, value)
                    SELECT v_valueid, ord - 1, (elem)::bigint
                    FROM jsonb_array_elements_text(v_value)
                             WITH ORDINALITY AS t(elem, ord);

                    WHEN 5 THEN  -- DOUBLE
                    INSERT INTO repo_double_vector(valueid, idx, value)
                    SELECT v_valueid, ord - 1, (elem)::double precision
                    FROM jsonb_array_elements_text(v_value)
                             WITH ORDINALITY AS t(elem, ord);

                    WHEN 6 THEN  -- BOOLEAN
                    INSERT INTO repo_boolean_vector(valueid, idx, value)
                    SELECT v_valueid, ord - 1, (elem)::boolean
                    FROM jsonb_array_elements_text(v_value)
                             WITH ORDINALITY AS t(elem, ord);

                    WHEN 7 THEN  -- DATA / BLOB (base-64)
                    INSERT INTO repo_data_vector(valueid, idx, value)
                    SELECT v_valueid, ord - 1, decode(elem, 'base64')
                    FROM jsonb_array_elements_text(v_value)
                             WITH ORDINALITY AS t(elem, ord);

                    ELSE
                        RAISE EXCEPTION
                            'Unknown attrtype % for attrid % in unit %.%',
                            v_attrtype, v_attrid, p_tenantid, p_unitid
                            USING ERRCODE = '22023';
                    END CASE;

                -- If this primitive is a child of a record, link it.
                IF p_parent_record_valueid IS NOT NULL THEN
                    INSERT INTO repo_record_vector(valueid, idx, ref_attrid, ref_valueid)
                    VALUES (p_parent_record_valueid, v_idx, v_attrid, v_valueid);
                END IF;
            END IF;

            v_idx := v_idx + 1;
        END LOOP;
END;
$$;


---------------------------------------------------------------
-- Procedure: ingest_new_unit_json
---------------------------------------------------------------
CREATE OR REPLACE PROCEDURE ingest_new_unit_json (
    IN  p_unit_text text,
    OUT p_unitid    bigint,
    OUT p_unitver   int,
    OUT p_created   timestamp,
    OUT p_modified  timestamp
) LANGUAGE plpgsql AS $$
DECLARE
    p_unit     jsonb := p_unit_text::jsonb;

    v_tenantid int   := (p_unit ->> 'tenantid')::int;
    v_corrid   uuid  := (p_unit ->> 'corrid')::uuid;
    v_status   int   := COALESCE((p_unit ->> 'status')::int, 30);
    v_unitname text  :=  p_unit ->> 'unitname';
BEGIN
    IF v_tenantid IS NULL THEN
        RAISE EXCEPTION 'tenantid is required in unit JSON: %', p_unit_text;
    END IF;
    IF v_corrid IS NULL THEN
        -- generate a v7 here?
        RAISE EXCEPTION 'corrid is required in unit JSON: %', p_unit_text;
    END IF;

    -- Insert unit header row, capture 'unitid' and 'created' timestamp
    INSERT INTO repo_unit_kernel (tenantid, corrid, status)
    VALUES (v_tenantid, v_corrid, v_status)
    RETURNING lastver, unitid, created
        INTO  p_unitver, p_unitid, p_created;

    -- Insert version row, capture 'modified' timestamp
    INSERT INTO repo_unit_version (tenantid, unitid, unitver, unitname)
    VALUES (v_tenantid, p_unitid, p_unitver, v_unitname)
    RETURNING modified
        INTO p_modified;

    -- Recursively ingest all attributes, including nested record attributes
    PERFORM repo_ingest_attributes(
            v_tenantid,
            p_unitid,
            p_unitver,
            p_unit -> 'attributes', -- top-level attributes array
            NULL -- no parent (record) at the top level
            );
END;
$$;

---------------------------------------------------------------
-- Procedure: ingest_new_version_json
---------------------------------------------------------------
CREATE OR REPLACE PROCEDURE ingest_new_version_json (
    IN  p_unit_text text,
    OUT p_unitver   int,
    OUT p_modified  timestamp
) LANGUAGE plpgsql AS $$
DECLARE
    -- Parsed JSON document
    p_unit     jsonb := p_unit_text::jsonb;

    v_tenantid int    := (p_unit ->> 'tenantid')::int;
    v_unitid   bigint := (p_unit ->> 'unitid')::bigint;
    v_status   int    := COALESCE((p_unit ->> 'status')::int, 30);
    v_unitname text   :=  p_unit ->> 'unitname';
BEGIN
    IF v_tenantid IS NULL OR v_unitid IS NULL THEN
        RAISE EXCEPTION 'tenantid and unitid are required in unit JSON: %', p_unit_text;
    END IF;

    ----------------------------------------------------------------------
    -- Bump kernel version and update status, capturing 'lastver'
    ----------------------------------------------------------------------
    UPDATE repo_unit_kernel
    SET status  = v_status,
        lastver = lastver + 1
    WHERE tenantid = v_tenantid
      AND unitid   = v_unitid
    RETURNING lastver INTO p_unitver;

    IF p_unitver IS NULL THEN
        RAISE EXCEPTION 'No such unit %.% when ingesting new version',
            v_tenantid, v_unitid;
    END IF;

    ----------------------------------------------------------------------
    -- Insert new version row, capturing 'modified'
    ----------------------------------------------------------------------
    INSERT INTO repo_unit_version (tenantid, unitid, unitver, unitname)
    VALUES (v_tenantid, v_unitid, p_unitver, v_unitname)
    RETURNING modified INTO p_modified;

    ----------------------------------------------------------------------
    -- Ingest all attributes for this version (records included)
    --
    --    This will:
    --      * insert new repo_attribute_value rows with
    --            unitverfrom = unitverto = p_unitver
    --      * insert primitive vectors (string/integer/…)
    --      * insert record vectors recursively
    --
    --    We *do not* extend old ranges or close them; each version is
    --    a full snapshot of attribute occurrences.
    ----------------------------------------------------------------------
    PERFORM repo_ingest_attributes(
            v_tenantid,
            v_unitid,
            p_unitver,
            p_unit -> 'attributes', -- top-level attributes array
            NULL -- no parent (record) at the top level
            );
END;
$$;


---------------------------------------------------------------
-- Procedure: extract_unit_json
--
-- Loads attributes, with value vectors, for a given unit
---------------------------------------------------------------
CREATE OR REPLACE PROCEDURE extract_unit_json(
    IN  p_tenantid integer,
    IN  p_unitid   bigint,
    IN  p_unitver  integer,   -- -1 == latest version
    INOUT p_json   jsonb
)
    LANGUAGE plpgsql
AS $$
BEGIN
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
                 JOIN repo_unit_version uv
                      ON uk.tenantid = uv.tenantid
                          AND uk.unitid = uv.unitid
                          AND uv.unitver = COALESCE(NULLIF(p_unitver, -1), uk.lastver)
        WHERE uk.tenantid = p_tenantid
          AND uk.unitid   = p_unitid
    ),
         attrs AS (
             SELECT av.attrid,
                    a.attrtype,
                    a.attrname,
                    a.alias,
                    av.unitverfrom,
                    av.unitverto,
                    av.valueid,

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
                            THEN (SELECT jsonb_agg(encode(dat.value, 'base64') ORDER BY dat.idx)
                                  FROM repo_data_vector dat
                                  WHERE dat.valueid = av.valueid)
                        WHEN 99 -- RECORD
                            THEN (SELECT jsonb_agg(
                                                 jsonb_build_object(
                                                         'ref_attrid',  rv.ref_attrid,
                                                         'ref_valueid', rv.ref_valueid
                                                 )
                                                 ORDER BY rv.idx)
                                  FROM repo_record_vector rv
                                  WHERE rv.valueid = av.valueid)
                        END AS value
             FROM repo_attribute_value av
                      JOIN repo_attribute a
                           ON a.attrid = av.attrid
                      JOIN repo_unit_kernel uk
                           ON uk.tenantid = av.tenantid
                               AND uk.unitid   = av.unitid
             WHERE uk.lastver <= av.unitverto
               AND uk.lastver >= av.unitverfrom
               AND av.tenantid = p_tenantid
               AND av.unitid   = p_unitid
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
                   (
                       SELECT jsonb_agg(
                                      jsonb_strip_nulls(
                                              jsonb_build_object(
                                                      'attrid',       attrid,
                                                      'attrtype',     attrtype,
                                                      'attrname',     attrname,
                                                      'alias',        alias,
                                                      'unitverfrom',  unitverfrom,
                                                      'unitverto',    unitverto,
                                                      'valueid',      valueid,
                                                      'value',        value
                                              )
                                      )
                              )
                       FROM attrs
                   )
           )
    INTO p_json
    FROM unit_hdr u;
END;
$$;



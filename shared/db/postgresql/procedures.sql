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
-- Function: repo_ingest_attributes_delta
--
-- Ingests a *new unit version* using a "modified" flag per attribute
-- occurrence:
--
--   * modified=true  (default): allocate a new valueid and insert new
--                               vector content (snapshot semantics)
--   * modified=false          : carry forward the previous valueid by
--                               extending unitverto to the new unitver,
--                               and (when inside a modified RECORD)
--                               re-link the new record occurrence to the
--                               carried-forward valueid.
--
-- Notes:
--   * For top-level attributes we locate the "previous" occurrence by
--     selecting the row whose (unitverfrom..unitverto) covers (unitver-1).
--   * For nested RECORD attributes we locate the "previous" child occurrence
--     by looking up repo_record_vector(valueid=<old record>, idx=<child idx>).
--     This makes duplicate child-attrids safe.
--   * If an attribute is omitted from the input for the new version, it is
--     effectively deleted (its previous unitverto stays at the prior version).
---------------------------------------------------------------
CREATE OR REPLACE FUNCTION repo_ingest_attributes_delta(
    p_tenantid                int,
    p_unitid                  bigint,
    p_unitver                 int,
    p_attrs                   jsonb,
    p_new_parent_record_valueid bigint DEFAULT NULL,
    p_old_parent_record_valueid bigint DEFAULT NULL
) RETURNS void
    LANGUAGE plpgsql AS $$
DECLARE
    v_attr        jsonb;
    v_attrid      int;
    v_attrtype    int;
    v_modified    boolean;
    v_value       jsonb;  -- for primitive values
    v_children    jsonb;  -- for nested attributes in records
    v_new_valueid bigint;
    v_old_valueid bigint;
    v_oldver      int := p_unitver - 1;
    v_idx         int := 0; -- index within this record (if any)
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

            -- Default is "modified=true" for backwards compatibility.
            v_modified := COALESCE((v_attr ->> 'modified')::boolean, true);

            -------------------------------------------------------------------
            -- Resolve the "previous" valueid for this attribute occurrence
            -------------------------------------------------------------------
            v_old_valueid := NULL;

            IF v_oldver >= 1 THEN
                IF p_old_parent_record_valueid IS NOT NULL THEN
                    -- Nested: resolve by record+idx (robust even with duplicate attrids)
                    SELECT rv.ref_valueid
                    INTO v_old_valueid
                    FROM repo_record_vector rv
                    WHERE rv.valueid = p_old_parent_record_valueid
                      AND rv.idx     = v_idx
                    LIMIT 1;
                ELSE
                    -- Top-level: resolve by attrid and version range that covers v_oldver
                    SELECT av.valueid
                    INTO v_old_valueid
                    FROM repo_attribute_value av
                    WHERE av.tenantid = p_tenantid
                      AND av.unitid   = p_unitid
                      AND av.attrid   = v_attrid
                      AND v_oldver BETWEEN av.unitverfrom AND av.unitverto
                    ORDER BY av.unitverfrom DESC, av.valueid DESC
                    LIMIT 1;
                END IF;
            END IF;

            -------------------------------------------------------------------
            -- Carry-forward (modified=false) vs new allocation (modified=true)
            -------------------------------------------------------------------
            IF NOT v_modified THEN
                IF v_old_valueid IS NULL THEN
                    RAISE EXCEPTION
                        'Attribute marked modified=false, but no previous occurrence found (tenant=%, unit=%, unitver=% attrid=% idx=%): %',
                        p_tenantid, p_unitid, p_unitver, v_attrid, v_idx, v_attr;
                END IF;

                -- Extend validity of the existing occurrence to the new version.
                UPDATE repo_attribute_value av
                SET unitverto = p_unitver
                WHERE av.valueid   = v_old_valueid
                  AND av.tenantid  = p_tenantid
                  AND av.unitid    = p_unitid
                  AND av.unitverto = v_oldver;

                -- If we are inside a *new* record occurrence, re-link it to the carried-forward valueid.
                IF p_new_parent_record_valueid IS NOT NULL THEN
                    INSERT INTO repo_record_vector(valueid, idx, ref_attrid, ref_valueid)
                    VALUES (p_new_parent_record_valueid, v_idx, v_attrid, v_old_valueid);
                END IF;

                -- Recurse into children (records only) so that their ranges are extended as well.
                IF v_attrtype = 99 THEN
                    v_children := COALESCE(v_attr->'attributes', v_attr->'value');
                    IF v_children IS NOT NULL THEN
                        PERFORM repo_ingest_attributes_delta(
                                p_tenantid,
                                p_unitid,
                                p_unitver,
                                v_children,
                                NULL,           -- no new record occurrence (record valueid is carried forward)
                                v_old_valueid   -- old record valueid (same as carried forward)
                                );
                    END IF;
                END IF;

            ELSE
                -----------------------------------------------------------------------
                -- Insert new attribute_value row for this occurrence, capturing valueid
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
                RETURNING valueid INTO v_new_valueid;

                -- If this attribute is a child of a (new) record, link it.
                IF p_new_parent_record_valueid IS NOT NULL THEN
                    INSERT INTO repo_record_vector(valueid, idx, ref_attrid, ref_valueid)
                    VALUES (p_new_parent_record_valueid, v_idx, v_attrid, v_new_valueid);
                END IF;

                IF v_attrtype = 99 THEN
                    -- RECORD attribute: recurse, and allow children to carry forward by idx from old record (if any)
                    v_children := COALESCE(v_attr->'attributes', v_attr->'value');
                    IF v_children IS NOT NULL THEN
                        PERFORM repo_ingest_attributes_delta(
                                p_tenantid,
                                p_unitid,
                                p_unitver,
                                v_children,
                                v_new_valueid,   -- new record valueid
                                v_old_valueid    -- old record valueid (may be NULL for new record)
                                );
                    END IF;

                ELSE
                    -- PRIMITIVE attribute: write vector based on attrtype
                    v_value := v_attr->'value';

                    IF v_value IS NULL THEN
                        RAISE EXCEPTION
                            'Primitive attribute %.% (attrid=%) missing ''value'' array: %',
                            p_tenantid, p_unitid, v_attrid, v_attr;
                    END IF;

                    CASE v_attrtype
                        WHEN 1 THEN  -- STRING
                        INSERT INTO repo_string_vector(valueid, idx, value)
                        SELECT v_new_valueid, ord - 1, elem
                        FROM jsonb_array_elements_text(v_value)
                                 WITH ORDINALITY AS t(elem, ord);

                        WHEN 2 THEN  -- TIME
                        INSERT INTO repo_time_vector(valueid, idx, value)
                        SELECT v_new_valueid, ord - 1, (elem)::timestamp
                        FROM jsonb_array_elements_text(v_value)
                                 WITH ORDINALITY AS t(elem, ord);

                        WHEN 3 THEN  -- INT
                        INSERT INTO repo_integer_vector(valueid, idx, value)
                        SELECT v_new_valueid, ord - 1, (elem)::int
                        FROM jsonb_array_elements_text(v_value)
                                 WITH ORDINALITY AS t(elem, ord);

                        WHEN 4 THEN  -- LONG
                        INSERT INTO repo_long_vector(valueid, idx, value)
                        SELECT v_new_valueid, ord - 1, (elem)::bigint
                        FROM jsonb_array_elements_text(v_value)
                                 WITH ORDINALITY AS t(elem, ord);

                        WHEN 5 THEN  -- DOUBLE
                        INSERT INTO repo_double_vector(valueid, idx, value)
                        SELECT v_new_valueid, ord - 1, (elem)::double precision
                        FROM jsonb_array_elements_text(v_value)
                                 WITH ORDINALITY AS t(elem, ord);

                        WHEN 6 THEN  -- BOOLEAN
                        INSERT INTO repo_boolean_vector(valueid, idx, value)
                        SELECT v_new_valueid, ord - 1, (elem)::boolean
                        FROM jsonb_array_elements_text(v_value)
                                 WITH ORDINALITY AS t(elem, ord);

                        WHEN 7 THEN  -- DATA / BLOB (base-64)
                        INSERT INTO repo_data_vector(valueid, idx, value)
                        SELECT v_new_valueid, ord - 1, decode(elem, 'base64')
                        FROM jsonb_array_elements_text(v_value)
                                 WITH ORDINALITY AS t(elem, ord);

                        ELSE
                            RAISE EXCEPTION
                                'Unknown attrtype % for attrid % in unit %.%',
                                v_attrtype, v_attrid, p_tenantid, p_unitid
                                USING ERRCODE = '22023';
                        END CASE;
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
    --    When an attribute has "modified": false, we extend the previous
    --    (unitverfrom..unitverto) range to cover the new unitver instead of
    --    allocating a new valueid and duplicating vector content.
    ----------------------------------------------------------------------
    PERFORM repo_ingest_attributes_delta(
            v_tenantid,
            v_unitid,
            p_unitver,
            p_unit -> 'attributes', -- top-level attributes array
            NULL, -- no *new* parent record at the top level
            NULL  -- no *old* parent record at the top level
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
          AND uk.unitid = p_unitid
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
                      JOIN unit_hdr uh
                           ON uh.tenantid = av.tenantid AND uh.unitid = av.unitid
             WHERE av.tenantid = p_tenantid
               AND av.unitid = p_unitid
               AND uh.unitver BETWEEN av.unitverfrom AND av.unitverto
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



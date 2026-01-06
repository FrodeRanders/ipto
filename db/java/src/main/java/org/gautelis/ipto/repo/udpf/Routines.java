/*
 * Copyright (C) 2025-2026 Frode Randers
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
package org.gautelis.ipto.repo.udpf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.logging.*;

/**
 * Java implementations of repo_* routines for DB2 LUW.
 *
 * All entry points are static and use PARAMETER STYLE JAVA.
 *
 * --- !!! OBSERVE !!! ------------------------------------------
 * We are running in DB2 with JAVA_VERSION=1.8.0_331,
 * JAVA_VENDOR=IBM Corporation, and
 * JAVA_HOME=/database/config/db2inst1/sqllib/java/jdk64/jre
 *
 * We need to keep this in mind when writing code,
 * no fancy features! :)
 * --------------------------------------------------------------
 */
public class Routines {
    /*
    private static final Logger LOG = Logger.getLogger("repo.procs");
    static {
        try {
            // Path must exist and be writable by db2inst1
            Handler fileHandler = new FileHandler(
                    "/database/config/db2inst1/sqllib/db2dump/DIAG0000/repo-proc.log",
                    true
            );
            fileHandler.setFormatter(new SimpleFormatter());
            LOG.addHandler(fileHandler);
            LOG.setUseParentHandlers(false);
            LOG.setLevel(Level.INFO);
        } catch (IOException ioe) {
            System.err.println("Warning: could not set up log file handler: " + ioe.getMessage());
        }
    }
    */

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter ISO_LOCAL = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:default:connection");
    }

    /**
     * Reads a CLOB into a String.
     */
    private static String clobToString(Clob clob) throws SQLException {
        if (clob == null) {
            return null;
        }
        long len = clob.length();
        return clob.getSubString(1, (int) len);
    }

    /**
     * Requires field to exist and be an int (numeric or numeric text).
     */
    private static int requireInt(JsonNode obj, String field) throws SQLException {
        JsonNode n = obj.get(field);
        if (n == null || n.isNull()) {
            throw new SQLException("Missing required field '" + field + "': " + obj);
        }
        if (n.isInt() || n.isLong() || n.isShort()) {
            return n.asInt();
        }
        if (n.isNumber()) {
            return n.numberValue().intValue();
        }
        String s = n.asText(null);
        if (s == null) {
            throw new SQLException("Missing required field '" + field + "': " + obj);
        }
        try {
            return Integer.parseInt(s);
        } catch (Exception e) {
            throw new SQLException("Invalid int field '" + field + "': " + s, e);
        }
    }

    /**
     * Requires field to exist and be a long (numeric or numeric text).
     */
    private static long requireLong(JsonNode obj, String field) throws SQLException {
        JsonNode n = obj.get(field);
        if (n == null || n.isNull()) {
            throw new SQLException("Missing required field '" + field + "': " + obj);
        }
        if (n.isLong() || n.isInt() || n.isShort()) {
            return n.asLong();
        }
        if (n.isNumber()) {
            return n.numberValue().longValue();
        }
        String s = n.asText(null);
        if (s == null) {
            throw new SQLException("Missing required field '" + field + "': " + obj);
        }
        try {
            return Long.parseLong(s);
        } catch (Exception e) {
            throw new SQLException("Invalid long field '" + field + "': " + s, e);
        }
    }

    private static String optionalText(JsonNode obj, String field) {
        JsonNode n = obj.get(field);
        if (n == null || n.isNull()) {
            return null;
        }
        return n.asText();
    }

    private static int optionalInt(JsonNode obj, String field, int defaultValue) throws SQLException {
        JsonNode n = obj.get(field);
        if (n == null || n.isNull()) {
            return defaultValue;
        }
        if (n.isInt() || n.isLong() || n.isShort()) {
            return n.asInt();
        }
        if (n.isNumber()) {
            return n.numberValue().intValue();
        }
        String s = n.asText(null);
        if (s == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(s);
        } catch (Exception e) {
            throw new SQLException("Invalid int field '" + field + "': " + s, e);
        }
    }

    /**
     * Parse a timestamp string permissively:
     *  - ISO instant: 2025-11-19T22:44:11.652477Z
     *  - ISO offset:  2025-11-19T22:44:11.652477+01:00
     *  - ISO local:   2025-11-19T22:44:11.652477
     *
     * DB2 stores TIMESTAMP without timezone.
     * When the input carries a timezone/offset, we convert to an Instant and store that moment.
     */
    private static Timestamp parseTimestampFlexible(String text) throws SQLException {
        if (text == null) {
            return null;
        }
        String s = text.trim();
        if (s.isEmpty()) {
            return null;
        }

        // Instant.parse handles only UTC "Z" form (ISO_INSTANT)
        try {
            Instant inst = Instant.parse(s);
            return Timestamp.from(inst);
        } catch (Exception ignore) { /* fall through */ }

        // OffsetDateTime supports explicit offsets
        try {
            OffsetDateTime odt = OffsetDateTime.parse(s);
            return Timestamp.from(odt.toInstant());
        } catch (Exception ignore) { /* fall through */ }

        // LocalDateTime for timezone-less timestamps
        try {
            LocalDateTime ldt = LocalDateTime.parse(s, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            return Timestamp.valueOf(ldt);
        } catch (Exception ignore) { /* fall through */ }

        // Last resort: allow ' ' instead of 'T'
        try {
            String s2 = s.replace('T', ' ');
            return Timestamp.valueOf(s2);
        } catch (Exception e) {
            throw new SQLException("Unknown time format: '" + text + "'", e);
        }
    }

    private static String formatTimestampLocal(Timestamp ts) {
        if (ts == null) {
            return null;
        }
        return ts.toLocalDateTime().format(ISO_LOCAL);
    }

    /**
     * Inserts into repo_attribute_value, return new valueid.
     */
    private static long insertAttributeValue(
            Connection con,
            int tenantId,
            long unitId,
            int attrId,
            int fromVersion,
            int toVersion
    ) throws SQLException {
        String sql =
                "SELECT valueid FROM FINAL TABLE ( " +
                "  INSERT INTO repo_attribute_value " +
                "    (tenantid, unitid, attrid, unitverfrom, unitverto) " +
                "  VALUES (?, ?, ?, ?, ?) " +
                ")";

        try (PreparedStatement pStmt = con.prepareStatement(sql)) {
            pStmt.setInt(1, tenantId);
            pStmt.setLong(2, unitId);
            pStmt.setInt(3, attrId);
            pStmt.setInt(4, fromVersion);
            pStmt.setInt(5, toVersion);

            try (ResultSet rs = pStmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
                throw new SQLException("Insert into repo_attribute_value succeeded but no valueid returned");
            }
        }
    }

    /**
     * Inserts repo_record_vector link: parent(record valueid) to child(attribute occurrence valueid).
     */
    private static void insertRecordVectorLink(
            Connection con,
            long parentRecordValueId,
            int idx,
            int childAttrId,
            long childValueId
    ) throws SQLException {
        String sql = "INSERT INTO repo_record_vector(valueid, idx, ref_attrid, ref_valueid) VALUES (?, ?, ?, ?)";

        try (PreparedStatement pStmt = con.prepareStatement(sql)) {
            pStmt.setLong(1, parentRecordValueId);
            pStmt.setInt(2, idx);
            pStmt.setInt(3, childAttrId);
            pStmt.setLong(4, childValueId);
            pStmt.executeUpdate();
        }
    }

    /**
     * Inserts primitive vector values for a given valueid.
     */
    private static void insertPrimitiveVector(
            Connection con,
            long valueId,
            int attrType,
            ArrayNode values
    ) throws SQLException {

        if (values == null) {
            return;
        }

        switch (attrType) {
            case 1: { // STRING
                String sql = "INSERT INTO repo_string_vector(valueid, idx, value) VALUES (?, ?, ?)";

                try (PreparedStatement pStmt = con.prepareStatement(sql)) {
                    for (int i = 0; i < values.size(); i++) {
                        JsonNode elem = values.get(i);
                        pStmt.setLong(1, valueId);
                        pStmt.setInt(2, i);
                        if (elem == null || elem.isNull()) {
                            pStmt.setNull(3, Types.VARCHAR);
                        } else {
                            pStmt.setString(3, elem.asText());
                        }
                        pStmt.addBatch();
                    }
                    pStmt.executeBatch();
                }
                break;
            }
            case 2: { // TIME (timestamp)
                String sql = "INSERT INTO repo_time_vector(valueid, idx, value) VALUES (?, ?, ?)";

                try (PreparedStatement pStmt = con.prepareStatement(sql)) {
                    for (int i = 0; i < values.size(); i++) {
                        JsonNode elem = values.get(i);
                        pStmt.setLong(1, valueId);
                        pStmt.setInt(2, i);

                        if (elem == null || elem.isNull()) {
                            pStmt.setNull(3, Types.TIMESTAMP);
                        } else {
                            Timestamp ts = parseTimestampFlexible(elem.asText());
                            if (ts == null) {
                                pStmt.setNull(3, Types.TIMESTAMP);
                            } else {
                                pStmt.setTimestamp(3, ts);
                            }
                        }
                        pStmt.addBatch();
                    }
                    pStmt.executeBatch();
                }
                break;
            }
            case 3: { // INT
                String sql = "INSERT INTO repo_integer_vector(valueid, idx, value) VALUES (?, ?, ?)";

                try (PreparedStatement pStmt = con.prepareStatement(sql)) {
                    for (int i = 0; i < values.size(); i++) {
                        JsonNode elem = values.get(i);
                        pStmt.setLong(1, valueId);
                        pStmt.setInt(2, i);
                        if (elem == null || elem.isNull()) {
                            pStmt.setNull(3, Types.INTEGER);
                        } else {
                            pStmt.setInt(3, Integer.parseInt(elem.asText()));
                        }
                        pStmt.addBatch();
                    }
                    pStmt.executeBatch();
                }
                break;
            }
            case 4: { // LONG
                String sql = "INSERT INTO repo_long_vector(valueid, idx, value) VALUES (?, ?, ?)";

                try (PreparedStatement pStmt = con.prepareStatement(sql)) {
                    for (int i = 0; i < values.size(); i++) {
                        JsonNode elem = values.get(i);
                        pStmt.setLong(1, valueId);
                        pStmt.setInt(2, i);
                        if (elem == null || elem.isNull()) {
                            pStmt.setNull(3, Types.BIGINT);
                        } else {
                            pStmt.setLong(3, Long.parseLong(elem.asText()));
                        }
                        pStmt.addBatch();
                    }
                    pStmt.executeBatch();
                }
                break;
            }
            case 5: { // DOUBLE
                String sql = "INSERT INTO repo_double_vector(valueid, idx, value) VALUES (?, ?, ?)";

                try (PreparedStatement pStmt = con.prepareStatement(sql)) {
                    for (int i = 0; i < values.size(); i++) {
                        JsonNode elem = values.get(i);
                        pStmt.setLong(1, valueId);
                        pStmt.setInt(2, i);
                        if (elem == null || elem.isNull()) {
                            pStmt.setNull(3, Types.DOUBLE);
                        } else {
                            pStmt.setDouble(3, Double.parseDouble(elem.asText()));
                        }
                        pStmt.addBatch();
                    }
                    pStmt.executeBatch();
                }
                break;
            }
            case 6: { // BOOLEAN
                String sql = "INSERT INTO repo_boolean_vector(valueid, idx, value) VALUES (?, ?, ?)";

                try (PreparedStatement pStmt = con.prepareStatement(sql)) {
                    for (int i = 0; i < values.size(); i++) {
                        JsonNode elem = values.get(i);
                        pStmt.setLong(1, valueId);
                        pStmt.setInt(2, i);
                        if (elem == null || elem.isNull()) {
                            pStmt.setNull(3, Types.BOOLEAN); // Or maybe SMALLINT? The driver converts
                        } else {
                            boolean b = elem.isBoolean() ? elem.asBoolean() : Boolean.parseBoolean(elem.asText());
                            pStmt.setBoolean(3, b);
                        }
                        pStmt.addBatch();
                    }
                    pStmt.executeBatch();
                }
                break;
            }
            case 7: { // DATA / BLOB (base64)
                String sql = "INSERT INTO repo_data_vector(valueid, idx, value) VALUES (?, ?, ?)";

                try (PreparedStatement pStmt = con.prepareStatement(sql)) {
                    for (int i = 0; i < values.size(); i++) {
                        JsonNode elem = values.get(i);
                        pStmt.setLong(1, valueId);
                        pStmt.setInt(2, i);
                        if (elem == null || elem.isNull()) {
                            pStmt.setNull(3, Types.BLOB);
                        } else {
                            String b64 = elem.asText();
                            byte[] bytes = Base64.getDecoder().decode(b64);
                            pStmt.setBytes(3, bytes);
                        }
                        pStmt.addBatch();
                    }
                    pStmt.executeBatch();
                }
                break;
            }
            default:
                throw new SQLException("Unknown attrtype " + attrType + " for primitive attribute; valueid=" + valueId);
        }
    }

    /**
     * Recursive attribute ingestion, mirroring procedures_pg.sql: repo_ingest_attributes().
     *
     * For each attribute occurrence:
     *  - INSERT repo_attribute_value to obtain valueid
     *  - INSERT vector rows (primitive) or recurse into nested attributes (record)
     *  - If this attribute is a child of a record: INSERT a repo_record_vector link
     *
     * Indexing (idx) is local per call and is used when linking children to a record.
     */
    private static void ingestAttributes(
            Connection con,
            int tenantId,
            long unitId,
            int unitVer,
            JsonNode attrsNode,
            Long parentRecordValueId
    ) throws SQLException {

        if (attrsNode == null || attrsNode.isNull() || !attrsNode.isArray()) {
            return;
        }

        ArrayNode attrs = (ArrayNode) attrsNode;
        int idx = 0;

        for (int i = 0; i < attrs.size(); i++) {
            JsonNode attr = attrs.get(i);
            if (attr == null || attr.isNull() || !attr.isObject()) {
                idx++;
                continue;
            }

            int attrId = requireInt(attr, "attrid");
            int attrType = requireInt(attr, "attrtype");

            long valueId = insertAttributeValue(con, tenantId, unitId, attrId, unitVer, unitVer);

            if (attrType == 99) { // RECORD
                if (parentRecordValueId != null) {
                    insertRecordVectorLink(con, parentRecordValueId, idx, attrId, valueId);
                }

                JsonNode children = attr.get("attributes");
                if (children == null || children.isNull()) {
                    // Some payloads put record children under "value"
                    children = attr.get("value");
                }

                if (children != null && !children.isNull()) {
                    ingestAttributes(con, tenantId, unitId, unitVer, children, valueId);
                }

            } else { // PRIMITIVE
                JsonNode valueNode = attr.get("value");
                if (valueNode == null || valueNode.isNull() || !valueNode.isArray()) {
                    throw new SQLException("Primitive attribute missing 'value' array: " + attr);
                }

                insertPrimitiveVector(con, valueId, attrType, (ArrayNode) valueNode);

                if (parentRecordValueId != null) {
                    insertRecordVectorLink(con, parentRecordValueId, idx, attrId, valueId);
                }
            }

            idx++;
        }
    }

    /**
     * Procedure for ingesting a new unit (snapshot version 1).
     *
     * DB2 wrapper: INGEST_NEW_UNIT_JSON
     *   IN  p_unit     CLOB(2M)
     *   OUT p_unitid   BIGINT
     *   OUT p_unitver  INTEGER
     *   OUT p_created  TIMESTAMP
     *   OUT p_modified TIMESTAMP
     *
     * Parameter-style JAVA: OUT params are single-element arrays.
     */
    public static void ingestNewUnit(
            Clob p_unit,
            long[] p_unitid,
            int[] p_unitver,
            Timestamp[] p_created,
            Timestamp[] p_modified
    ) throws SQLException {
        //LOG.info("Starting INGEST_NEW_UNIT_JSON");

        Connection con = getConnection();

        try {
            String json = clobToString(p_unit);
            JsonNode root = MAPPER.readTree(json);

            int tenantId = requireInt(root, "tenantid");

            String corridStr = optionalText(root, "corrid");
            if (corridStr == null || corridStr.trim().isEmpty()) {
                throw new SQLException("corrid is required in unit JSON: " + json);
            }

            // Validate UUID syntax (PG casts to uuid and fails on invalid input)
            try {
                java.util.UUID.fromString(corridStr);
            } catch (Exception e) {
                throw new SQLException("Invalid corrid UUID: " + corridStr, e);
            }

            int status = optionalInt(root, "status", 30);
            String unitName = optionalText(root, "unitname");

            // Insert unit header row and return (lastver, unitid, created)
            String sqlKernel =
                    "SELECT lastver, unitid, created " +
                    "FROM FINAL TABLE ( " +
                    "  INSERT INTO repo_unit_kernel (tenantid, corrid, status) " +
                    "  VALUES (?, ?, ?) " +
                    ")";

            long unitId;
            int unitVer;
            Timestamp created;

            try (PreparedStatement pStmt = con.prepareStatement(sqlKernel)) {
                pStmt.setInt(1, tenantId);
                pStmt.setString(2, corridStr);
                pStmt.setInt(3, status);

                try (ResultSet rs = pStmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException("Insert into repo_unit_kernel succeeded but no row returned");
                    }
                    unitVer = rs.getInt(1);
                    unitId  = rs.getLong(2);
                    created = rs.getTimestamp(3);
                }
            }

            // Insert unit version row and return modified
            String sqlVer =
                    "SELECT modified " +
                    "FROM FINAL TABLE ( " +
                    "  INSERT INTO repo_unit_version (tenantid, unitid, unitver, unitname) " +
                    "  VALUES (?, ?, ?, ?) " +
                    ")";

            Timestamp modified;
            try (PreparedStatement pStmt = con.prepareStatement(sqlVer)) {
                pStmt.setInt(1, tenantId);
                pStmt.setLong(2, unitId);
                pStmt.setInt(3, unitVer);
                pStmt.setString(4, unitName);

                try (ResultSet rs = pStmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException("Insert into repo_unit_version succeeded but no row returned");
                    }
                    modified = rs.getTimestamp(1);
                }
            }

            // OUT parameters
            p_unitid[0] = unitId;
            p_unitver[0] = unitVer;
            p_created[0] = created;
            p_modified[0] = modified;

            // Recursively ingest attributes (records included)
            ingestAttributes(con, tenantId, unitId, unitVer, root.get("attributes"), null);

        } catch (Exception e) {
            if (e instanceof SQLException) {
                throw (SQLException) e;
            }
            throw new SQLException("Error in ingest_new_unit_json", e);
        }
    }

    /**
     * Procedure for ingesting a new version of an existing unit (full snapshot per version).
     *
     * DB2 wrapper: INGEST_NEW_VERSION_JSON
     *   IN  p_unit     CLOB(2M)
     *   OUT p_unitver  INTEGER
     *   OUT p_modified TIMESTAMP
     */
    public static void ingestNewVersion(
            Clob p_unit,
            int[] p_unitver,
            Timestamp[] p_modified
    ) throws SQLException {
        //LOG.info("Starting INGEST_NEW_VERSION_JSON");

        Connection con = getConnection();

        try {
            String json = clobToString(p_unit);
            JsonNode root = MAPPER.readTree(json);

            int tenantId = requireInt(root, "tenantid");
            long unitId  = requireLong(root, "unitid");
            int status   = optionalInt(root, "status", 30);
            String unitName = optionalText(root, "unitname");

            // UPDATE kernel: status and bump lastver; return lastver
            String sqlKernelUpdate =
                    "SELECT lastver " +
                    "FROM FINAL TABLE ( " +
                    "  UPDATE repo_unit_kernel " +
                    "  SET status = ?, lastver = lastver + 1 " +
                    "  WHERE tenantid = ? AND unitid = ? " +
                    ")";

            int newVer;
            try (PreparedStatement pStmt = con.prepareStatement(sqlKernelUpdate)) {
                pStmt.setInt(1, status);
                pStmt.setInt(2, tenantId);
                pStmt.setLong(3, unitId);

                try (ResultSet rs = pStmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException("No such unit " + tenantId + "." + unitId + " when ingesting new version");
                    }
                    newVer = rs.getInt(1);
                }
            }

            // Insert new unit version row and return modified
            String sqlVer =
                    "SELECT modified " +
                    "FROM FINAL TABLE ( " +
                    "  INSERT INTO repo_unit_version (tenantid, unitid, unitver, unitname) " +
                    "  VALUES (?, ?, ?, ?) " +
                    ")";

            Timestamp modified;
            try (PreparedStatement pStmt = con.prepareStatement(sqlVer)) {
                pStmt.setInt(1, tenantId);
                pStmt.setLong(2, unitId);
                pStmt.setInt(3, newVer);
                pStmt.setString(4, unitName);

                try (ResultSet rs = pStmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException("Insert into repo_unit_version succeeded but no row returned");
                    }
                    modified = rs.getTimestamp(1);
                }
            }

            p_unitver[0] = newVer;
            p_modified[0] = modified;

            // Full snapshot ingest: insert new attribute_value + vectors + record links for this version
            ingestAttributes(con, tenantId, unitId, newVer, root.get("attributes"), null);

        } catch (Exception e) {
            //LOG.warning(e.getMessage());
            if (e instanceof SQLException) {
                throw (SQLException) e;
            }
            throw new SQLException("Error in ingest_new_version_json", e);
        }
    }

    /**
     * DB2 Java routine: OUT parameters are 1-element arrays.
     *
     * Wrapper: EXTRACT_UNIT_JSON
     *   IN  p_tenantid INTEGER
     *   IN  p_unitid   BIGINT
     *   IN  p_unitver  INTEGER   (-1 == latest)
     *   OUT p_json     CLOB(2M)
     *
     * Note: If p_unitver == -1, the routine resolves to the latest version (kernel.lastver).
     * If p_unitver specifies a concrete version, attributes/values are resolved for that version.
     */
    public static void extractUnit(
            int    pTenantId,
            long   pUnitId,
            int    pUnitVer,   // -1 means “latest”
            Clob[] pJsonOut
    ) throws SQLException {

        Connection conn = DriverManager.getConnection("jdbc:default:connection");

        ObjectNode root = MAPPER.createObjectNode();

        int lastVer;
        int unitVer;
        String corrid;
        int status;
        Timestamp created;
        Timestamp modified;
        String unitName;

        // Unit kernel
        String sqlKernel =
                "SELECT corrid, status, lastver, created " +
                "FROM repo_unit_kernel " +
                "WHERE tenantid = ? AND unitid = ?";

        try (PreparedStatement pStmt = conn.prepareStatement(sqlKernel)
        ) {
            pStmt.setInt(1, pTenantId);
            pStmt.setLong(2, pUnitId);

            try (ResultSet rs = pStmt.executeQuery()) {
                if (!rs.next()) {
                    pJsonOut[0] = null;
                    return;
                }
                corrid = rs.getString("corrid");
                status = rs.getInt("status");
                lastVer = rs.getInt("lastver");
                created = rs.getTimestamp("created");
            }
        }

        unitVer = (pUnitVer != -1 ? pUnitVer : lastVer);

        // Unit version row (for unitname + modified)
        String sqlVer =
                "SELECT unitname, modified " +
                "FROM repo_unit_version " +
                "WHERE tenantid = ? AND unitid = ? AND unitver = ?";

        try (PreparedStatement pStmt = conn.prepareStatement(sqlVer)
        ) {
            pStmt.setInt(1, pTenantId);
            pStmt.setLong(2, pUnitId);
            pStmt.setInt(3, unitVer);

            try (ResultSet rs = pStmt.executeQuery()) {
                if (!rs.next()) {
                    pJsonOut[0] = null;
                    return;
                }
                unitName = rs.getString("unitname");
                modified = rs.getTimestamp("modified");
            }
        }

        // Header JSON (mirrors procedures_pg.sql extract_unit_json)
        root.put("@type", "unit");
        root.put("@version", 2);
        root.put("tenantid", pTenantId);
        root.put("unitid", pUnitId);
        root.put("unitver", unitVer);
        root.put("corrid", corrid);
        root.put("status", status);
        root.put("unitname", unitName);

        String createdStr = formatTimestampLocal(created);
        if (createdStr != null) root.put("created", createdStr);

        String modifiedStr = formatTimestampLocal(modified);
        if (modifiedStr != null) root.put("modified", modifiedStr);

        root.put("isreadonly", lastVer > unitVer);

        // Load attributes effective at the requested/resolved version.
        // If pUnitVer == -1, unitVer == lastVer, so this also means “latest”.
        int effectiveVer = unitVer;

        ArrayNode attrsArray = MAPPER.createArrayNode();

        String sqlAttr =
                "SELECT av.attrid, a.attrtype, a.attrname, a.alias, " +
                "       av.unitverfrom, av.unitverto, av.valueid " +
                "FROM   repo_attribute_value av " +
                "JOIN   repo_attribute a " +
                "  ON   a.attrid = av.attrid " +
                "WHERE  av.tenantid = ? " +
                "  AND  av.unitid = ? " +
                "  AND  ? BETWEEN av.unitverfrom AND av.unitverto";

        try (PreparedStatement pStmt = conn.prepareStatement(sqlAttr)) {
            pStmt.setInt(1, pTenantId);
            pStmt.setLong(2, pUnitId);
            pStmt.setInt(3, effectiveVer);

            try (ResultSet rs = pStmt.executeQuery()) {
                while (rs.next()) {
                    int attrId = rs.getInt("attrid");
                    int attrType = rs.getInt("attrtype");
                    String attrName = rs.getString("attrname");
                    String alias = rs.getString("alias");
                    int unitVerFrom = rs.getInt("unitverfrom");
                    int unitVerTo = rs.getInt("unitverto");
                    long valueId = rs.getLong("valueid");

                    ObjectNode attrNode = MAPPER.createObjectNode();
                    attrNode.put("attrid", attrId);
                    attrNode.put("attrtype", attrType);
                    if (attrName != null) {
                        attrNode.put("attrname", attrName);
                    }
                    if (alias != null) {
                        attrNode.put("alias", alias);
                    }
                    attrNode.put("unitverfrom", unitVerFrom);
                    attrNode.put("unitverto", unitVerTo);
                    attrNode.put("valueid", valueId);

                    JsonNode valueNode = loadAttributeValue(conn, attrType, valueId);
                    if (valueNode != null) {
                        attrNode.set("value", valueNode);
                    }

                    attrsArray.add(attrNode);
                }
            }
        }

        root.set("attributes", attrsArray);

        // Serialize JSON into OUT parameter
        try {
            Clob clob = conn.createClob();
            clob.setString(1, MAPPER.writeValueAsString(root));
            pJsonOut[0] = clob;
        } catch (Exception e) {
            throw new SQLException("Failed to serialize JSON", e);
        }
    }

    /**
     * Loads the value vector for a given (attrtype, valueid) and return it as a JSON array
     * or array-of-objects (in case of RECORD).
     */
    private static JsonNode loadAttributeValue(
            Connection conn,
            int attrType,
            long valueId
    ) throws SQLException {

        ArrayNode arr = MAPPER.createArrayNode();

        switch (attrType) {
            case 1: { // STRING
                String sql = "SELECT idx, value FROM repo_string_vector WHERE valueid = ? ORDER BY idx";

                try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                    pStmt.setLong(1, valueId);
                    try (ResultSet rs = pStmt.executeQuery()) {
                        while (rs.next()) {
                            String v = rs.getString("value");
                            if (v == null) arr.addNull(); else arr.add(v);
                        }
                    }
                }
                break;
            }
            case 2: { // TIME (timestamp) -> ISO_LOCAL_DATE_TIME
                String sql = "SELECT idx, value FROM repo_time_vector WHERE valueid = ? ORDER BY idx";

                try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                    pStmt.setLong(1, valueId);
                    try (ResultSet rs = pStmt.executeQuery()) {
                        while (rs.next()) {
                            Timestamp ts = rs.getTimestamp("value");
                            if (ts == null) {
                                arr.addNull();
                            } else {
                                arr.add(formatTimestampLocal(ts));
                            }
                        }
                    }
                }
                break;
            }
            case 3: { // INTEGER
                String sql = "SELECT idx, value FROM repo_integer_vector WHERE valueid = ? ORDER BY idx";

                try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                    pStmt.setLong(1, valueId);
                    try (ResultSet rs = pStmt.executeQuery()) {
                        while (rs.next()) {
                            int v = rs.getInt("value");
                            if (rs.wasNull()) arr.addNull(); else arr.add(v);
                        }
                    }
                }
                break;
            }
            case 4: { // LONG
                String sql = "SELECT idx, value FROM repo_long_vector WHERE valueid = ? ORDER BY idx";

                try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                    pStmt.setLong(1, valueId);
                    try (ResultSet rs = pStmt.executeQuery()) {
                        while (rs.next()) {
                            long v = rs.getLong("value");
                            if (rs.wasNull()) arr.addNull(); else arr.add(v);
                        }
                    }
                }
                break;
            }
            case 5: { // DOUBLE
                String sql = "SELECT idx, value FROM repo_double_vector WHERE valueid = ? ORDER BY idx";

                try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                    pStmt.setLong(1, valueId);
                    try (ResultSet rs = pStmt.executeQuery()) {
                        while (rs.next()) {
                            double v = rs.getDouble("value");
                            if (rs.wasNull()) arr.addNull(); else arr.add(v);
                        }
                    }
                }
                break;
            }
            case 6: { // BOOLEAN
                String sql = "SELECT idx, value FROM repo_boolean_vector WHERE valueid = ? ORDER BY idx";

                try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                    pStmt.setLong(1, valueId);
                    try (ResultSet rs = pStmt.executeQuery()) {
                        while (rs.next()) {
                            boolean v = rs.getBoolean("value");
                            if (rs.wasNull()) {
                                arr.addNull();
                            } else {
                                arr.add(v);
                            }
                        }
                    }
                }
                break;
            }
            case 7: { // DATA / BLOB -> base64
                String sql = "SELECT idx, value FROM repo_data_vector WHERE valueid = ? ORDER BY idx";

                try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                    pStmt.setLong(1, valueId);
                    try (ResultSet rs = pStmt.executeQuery()) {
                        while (rs.next()) {
                            byte[] bytes = rs.getBytes("value");
                            if (bytes == null) {
                                arr.addNull();
                            } else {
                                arr.add(Base64.getEncoder().encodeToString(bytes));
                            }
                        }
                    }
                }
                break;
            }
            case 99: { // RECORD: array of {ref_attrid, ref_valueid}
                String sql = "SELECT idx, ref_attrid, ref_valueid FROM repo_record_vector WHERE valueid = ? ORDER BY idx";

                try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                    pStmt.setLong(1, valueId);
                    try (ResultSet rs = pStmt.executeQuery()) {
                        while (rs.next()) {
                            ObjectNode elem = MAPPER.createObjectNode();
                            int refAttr = rs.getInt("ref_attrid");
                            long refVal = rs.getLong("ref_valueid");
                            elem.put("ref_attrid", refAttr);
                            if (rs.wasNull()) {
                                elem.putNull("ref_valueid");
                            } else {
                                elem.put("ref_valueid", refVal);
                            }
                            arr.add(elem);
                        }
                    }
                }
                break;
            }
            default:
                throw new SQLException("Unknown attrtype " + attrType + " for attribute; valueid=" + valueId);
        }

        return arr;
    }
}

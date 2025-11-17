package org.gautelis.repo.udpf;

import java.sql.*;
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * Java implementations of repo_* routines for DB2 LUW.
 * <p>
 * All entry points are static and use PARAMETER STYLE JAVA.
 * <p>
 * --- !!! OBSERVE !!! ------------------------------------------
 * We are running in DB2 with JAVA_VERSION=1.8.0_331,
 * JAVA_VENDOR=IBM Corporation, and
 * JAVA_HOME=/database/config/db2inst1/sqllib/java/jdk64/jre
 * <p>
 * We need to keep this in mind when writing code,
 * no fancy features! :)
 * --------------------------------------------------------------
 */
public class Routines {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:default:connection");
    }

    /**
     * Reads a Clob into a String.
     */
    private static String clobToString(Clob clob) throws SQLException {
        if (clob == null) return null;
        long len = clob.length();
        return clob.getSubString(1, (int) len);
    }

    /**
     * Inserts into repo_attribute_value, return new valueid
     */
    private static long insertAttributeValue(
            Connection con,
            int tenantId,
            long unitId,
            int attrId,
            int fromVer,
            int toVer
    ) throws SQLException {

        String sql =
                "INSERT INTO repo_attribute_value " +
                        "(tenantid, unitid, attrid, unitverfrom, unitverto) " +
                        "VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement pStmt = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            pStmt.setInt(1, tenantId);
            pStmt.setLong(2, unitId);
            pStmt.setInt(3, attrId);
            pStmt.setInt(4, fromVer);
            pStmt.setInt(5, toVer);
            pStmt.executeUpdate();

            try (ResultSet rs = pStmt.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getLong(1);
                } else {
                    // This is nothing we can recover from
                    String info = "Failed to determine auto-generated value ID";
                    throw new SQLException(info);
                }
            }
        }
    }

    /**
     * Inserts primitive vectors
     */
    private static void insertPrimitiveVector(
            Connection con,
            long valueId,
            int attrType,
            ArrayNode values
    ) throws SQLException {

        if (values == null) return;  // nothing to insert

        switch (attrType) {
            case 1: { // STRING
                String sql = "INSERT INTO repo_string_vector(valueid, idx, value) VALUES (?, ?, ?)";
                try (PreparedStatement pStmt = con.prepareStatement(sql)) {
                    for (int i = 0; i < values.size(); i++) {
                        pStmt.setLong(1, valueId);
                        pStmt.setInt(2, i);
                        pStmt.setString(3, values.get(i).asText());
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
                        pStmt.setLong(1, valueId);
                        pStmt.setInt(2, i);
                        String tsStr = values.get(i).asText();
                        // adapt to your actual timestamp format as needed:
                        Timestamp ts = Timestamp.valueOf(tsStr);
                        pStmt.setTimestamp(3, ts);
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
                        pStmt.setLong(1, valueId);
                        pStmt.setInt(2, i);
                        pStmt.setInt(3, values.get(i).asInt());
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
                        pStmt.setLong(1, valueId);
                        pStmt.setInt(2, i);
                        pStmt.setLong(3, values.get(i).asLong());
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
                        pStmt.setLong(1, valueId);
                        pStmt.setInt(2, i);
                        pStmt.setDouble(3, values.get(i).asDouble());
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
                        pStmt.setLong(1, valueId);
                        pStmt.setInt(2, i);
                        pStmt.setBoolean(3, values.get(i).asBoolean());
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
                        pStmt.setLong(1, valueId);
                        pStmt.setInt(2, i);
                        String b64 = values.get(i).asText();
                        byte[] bytes = Base64.getDecoder().decode(b64);
                        pStmt.setBytes(3, bytes);
                        pStmt.addBatch();
                    }
                    pStmt.executeBatch();
                }
                break;
            }
            default:
                throw new SQLException("Unsupported attrtype " + attrType +
                        " for primitive attribute; valueid=" + valueId);
        }
    }

    /**
     * Inserts record vector (parent row + elements).
     * <p>
     * - Insert parent into repo_attribute_value
     * - Insert record elements into repo_record_vector
     * <p>
     * 'elements' is an array of objects:
     * [{ "idx": int, "ref_attrid": int, "ref_valueid": long }, ...]
     */
    private static void insertRecordValue(
            Connection con,
            int tenantId,
            long unitId,
            int attrId,
            int fromVer,
            int toVer,
            ArrayNode elements
    ) throws SQLException {

        long valueId = insertAttributeValue(con, tenantId, unitId, attrId, fromVer, toVer);

        if (elements == null) return;

        String sql = "INSERT INTO repo_record_vector(valueid, idx, ref_attrid, ref_valueid) VALUES (?, ?, ?, ?)";

        try (PreparedStatement pStmt = con.prepareStatement(sql)) {
            for (int i = 0; i < elements.size(); i++) {
                JsonNode elem = elements.get(i);
                int idx = elem.path("idx").asInt(i); // default to array index if not present
                int refAttrId = elem.path("ref_attrid").asInt();
                JsonNode refValNode = elem.get("ref_valueid");

                pStmt.setLong(1, valueId);
                pStmt.setInt(2, idx);
                pStmt.setInt(3, refAttrId);

                if (refValNode == null || refValNode.isNull()) {
                    pStmt.setNull(4, Types.BIGINT);
                } else {
                    pStmt.setLong(4, refValNode.asLong());
                }
                pStmt.addBatch();
            }
            pStmt.executeBatch();
        }
    }

    /**
     * Procedure for ingesting a new unit.
     * <p>
     * CREATE PROCEDURE ingest_new_unit_json(
     * IN  p_unit     CLOB(2M),
     * OUT p_unitid   BIGINT,
     * OUT p_unitver  INTEGER,
     * OUT p_created  TIMESTAMP,
     * OUT p_modified TIMESTAMP
     * ) LANGUAGE JAVA PARAMETER STYLE JAVA ...
     * <p>
     * Parameter-style JAVA: OUT params are single-element arrays.
     */
    public static void ingestNewUnit(
            Clob p_unit,
            long[] p_unitid,
            int[] p_unitver,
            Timestamp[] p_created,
            Timestamp[] p_modified
    ) throws SQLException {

        Connection con = getConnection();

        try {
            String json = clobToString(p_unit);
            JsonNode root = MAPPER.readTree(json);

            int tenantId = root.path("tenantid").asInt();
            String corridStr = root.path("corrid").asText(null);
            int status = root.path("status").asInt();
            String unitName = root.path("unitname").asText();

            // Insert unit header row (repo_unit_kernel)
            String sqlKernel =
                    "INSERT INTO repo_unit_kernel (tenantid, corrid, status) VALUES (?, ?, ?)";

            long unitId;
            try (PreparedStatement pStmt = con.prepareStatement(sqlKernel)) {
                pStmt.setInt(1, tenantId);
                if (corridStr == null || corridStr.isEmpty()) {
                    pStmt.setNull(2, Types.VARCHAR);
                } else {
                    pStmt.setString(2, corridStr); // store UUID as string
                }
                pStmt.setInt(3, status);
                pStmt.executeUpdate();

                try (ResultSet rs = pStmt.getGeneratedKeys()) {
                    if (rs.next()) {
                        unitId = rs.getLong(1);
                    } else {
                        // This is nothing we can recover from
                        String info = "Failed to determine auto-generated unit ID";
                        throw new SQLException(info);
                    }
                }
            }

            // Determine unit version (from lastver), and get created
            int unitVer;
            Timestamp created;

            String sqlSelectKernel =
                    "SELECT lastver, created " +
                    "FROM repo_unit_kernel " +
                    "WHERE tenantid = ? AND unitid = ?";

            try (PreparedStatement pStmt = con.prepareStatement(sqlSelectKernel)) {
                pStmt.setInt(1, tenantId);
                pStmt.setLong(2, unitId);

                try (ResultSet rs = pStmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException("Inserted repo_unit_kernel not found for unit=" + tenantId + "." + unitId);
                    }
                    unitVer = rs.getInt(1);
                    created = rs.getTimestamp(2);

                }
            }

            // OUT parameters: unitid, unitver, created
            p_unitid[0] = unitId;
            p_unitver[0] = unitVer;
            p_created[0] = created;

            // Insert unit version row
            String sqlVer = "INSERT INTO repo_unit_version (tenantid, unitid, unitver, unitname) VALUES (?, ?, ?, ?)";

            try (PreparedStatement pStmt = con.prepareStatement(sqlVer)) {
                pStmt.setInt(1, tenantId);
                pStmt.setLong(2, unitId);
                pStmt.setInt(3, unitVer);
                pStmt.setString(4, unitName);
                pStmt.executeUpdate();
            }

            Timestamp modified;
            String sqlSelectVer =
                    "SELECT modified " +
                    "FROM repo_unit_version " +
                    "WHERE tenantid = ? AND unitid = ? AND unitver = ?";

            try (PreparedStatement pStmt = con.prepareStatement(sqlSelectVer)) {
                pStmt.setInt(1, tenantId);
                pStmt.setLong(2, unitId);
                pStmt.setInt(3, unitVer);

                try (ResultSet rs = pStmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException(
                                "Inserted repo_unit_version not found for unit=" +
                                        tenantId + "." + unitId + ":" + unitVer
                        );
                    }
                    modified = rs.getTimestamp(1);
                }
            }

            p_modified[0] = modified;

            // Attributes
            ArrayNode attrs = (ArrayNode) root.path("attributes");
            if (attrs == null) {
                // no attributes -> done
                return;
            }

            boolean hasRecordAttrs = false;

            // First pass: insert all attribute_value rows and primitive vectors,
            // and insert record rows with 'ref_valueid = NULL' placeholders.
            for (int i = 0; i < attrs.size(); i++) {
                JsonNode attr = attrs.get(i);
                int attrId = attr.path("attrid").asInt();
                int attrType = attr.path("attrtype").asInt();
                JsonNode valueNode = attr.get("value");

                if (attrType == 99) {
                    hasRecordAttrs = true;

                    long parentValueId = insertAttributeValue(
                            con, tenantId, unitId, attrId, unitVer, unitVer);

                    // value is an array of { ref_attrid, ref_valueid? }, but we ignore any ref_valueid
                    ArrayNode elemArray = valueNode != null && valueNode.isArray()
                            ? (ArrayNode) valueNode
                            : MAPPER.createArrayNode();

                    String sqlRec =
                            "INSERT INTO repo_record_vector (valueid, idx, ref_attrid, ref_valueid) " +
                            "VALUES (?, ?, ?, NULL)";

                    try (PreparedStatement pStmt = con.prepareStatement(sqlRec)) {
                        for (int ord = 0; ord < elemArray.size(); ord++) {
                            JsonNode e = elemArray.get(ord);
                            int refAttrId = e.path("ref_attrid").asInt();

                            pStmt.setLong(1, parentValueId);
                            pStmt.setInt(2, ord); // 0-based
                            pStmt.setInt(3, refAttrId);
                            pStmt.addBatch();
                        }
                        pStmt.executeBatch();
                    }

                } else {
                    // primitive attribute
                    long valueId = insertAttributeValue(
                            con, tenantId, unitId, attrId, unitVer, unitVer);

                    ArrayNode arr = (valueNode != null && valueNode.isArray())
                            ? (ArrayNode) valueNode
                            : MAPPER.createArrayNode();

                    insertPrimitiveVector(con, valueId, attrType, arr);
                }
            }

            // Resolve record placeholders (ref_valueid)
            if (hasRecordAttrs) {
                // Build map: attrid -> valueid effective at this version
                Map<Integer, Long> currentValueIds = new HashMap<Integer, Long>();

                String sqlCurValues =
                        "SELECT attrid, valueid " +
                        "FROM repo_attribute_value " +
                        "WHERE tenantid = ? " +
                        "  AND unitid   = ? " +
                        "  AND unitverfrom <= ? " +
                        "  AND unitverto   >= ?";

                try (PreparedStatement pStmt = con.prepareStatement(sqlCurValues)) {
                    pStmt.setInt(1, tenantId);
                    pStmt.setLong(2, unitId);
                    pStmt.setInt(3, unitVer);
                    pStmt.setInt(4, unitVer);

                    try (ResultSet rs = pStmt.executeQuery()) {
                        while (rs.next()) {
                            int attrId = rs.getInt(1);
                            long valueId = rs.getLong(2);
                            // If more than one, last wins
                            currentValueIds.put(attrId, valueId);
                        }
                    }
                }

                // For each record element with ref_valueid IS NULL, resolve and update
                String sqlSelectRec =
                        "SELECT rv.valueid, rv.idx, rv.ref_attrid " +
                        "FROM repo_record_vector rv " +
                        "JOIN repo_attribute_value parent " +
                        "  ON parent.valueid = rv.valueid " +
                        "WHERE parent.tenantid = ? " +
                        "  AND parent.unitid   = ? " +
                        "  AND rv.ref_valueid IS NULL";

                String sqlUpdateRec =
                        "UPDATE repo_record_vector " +
                        "SET ref_valueid = ? " +
                        "WHERE valueid = ? AND idx = ? AND ref_valueid IS NULL";

                try (PreparedStatement pStmtSel = con.prepareStatement(sqlSelectRec)) {
                    try (PreparedStatement pStmtUpd = con.prepareStatement(sqlUpdateRec)) {
                        pStmtSel.setInt(1, tenantId);
                        pStmtSel.setLong(2, unitId);

                        try (ResultSet rs = pStmtSel.executeQuery()) {
                            while (rs.next()) {
                                long parentValId = rs.getLong(1);
                                int idx = rs.getInt(2);
                                int refAttrId = rs.getInt(3);

                                Long childValId = currentValueIds.get(refAttrId);
                                if (childValId == null) {
                                    throw new SQLException(
                                            "Record references attribute " + refAttrId +
                                            " without effective value at " +
                                            tenantId + "." + unitId + ":" + unitVer
                                    );
                                }

                                pStmtUpd.setLong(1, childValId);
                                pStmtUpd.setLong(2, parentValId);
                                pStmtUpd.setInt(3, idx);
                                pStmtUpd.addBatch();
                            }
                            pStmtUpd.executeBatch();
                        }
                    }
                }
            }

            // No commit -- let DB2 handle commit according to caller / autocommit

        } catch (Exception e) {
            // Any non-SQL exceptions must be wrapped
            if (e instanceof SQLException) throw (SQLException) e;
            throw new SQLException("Error in ingest_new_unit_json", e);
        }
    }
    /* ************************************************************************* */

    private static final class IncomingAttribute {
        final int attrId;
        final int attrType;
        final boolean isModified;
        final JsonNode value;

        IncomingAttribute(int attrId, int attrType, boolean isModified, JsonNode value) {
            this.attrId = attrId;
            this.attrType = attrType;
            this.isModified = isModified;
            this.value = value;
        }
    }

    private static final class RecordElement {
        final int parentAttrId;
        final int idx;
        final int refAttrId;
        Long refValueId;   // resolved later

        RecordElement(int parentAttrId, int idx, int refAttrId) {
            this.parentAttrId = parentAttrId;
            this.idx = idx;
            this.refAttrId = refAttrId;
        }
    }

    private static final class ReferencePair {
        final int refAttrId;
        final long refValueId;

        ReferencePair(int refAttrId, long refValueId) {
            this.refAttrId = refAttrId;
            this.refValueId = refValueId;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof ReferencePair)) return false;
            ReferencePair other = (ReferencePair) o;
            return this.refAttrId == other.refAttrId &&
                    this.refValueId == other.refValueId;
        }

        @Override
        public int hashCode() {
            int result = Integer.hashCode(refAttrId);
            result = 31 * result + Long.hashCode(refValueId);
            return result;
        }
    }

    /**
     * Procedure for ingesting a new version of an existing unit.
     * <p>
     * CREATE OR REPLACE PROCEDURE INGEST_NEW_VERSION_JSON (
     *    IN  P_UNIT     CLOB(2M),
     *    OUT P_UNITVER  INTEGER,
     *    OUT P_MODIFIED TIMESTAMP
     * )
     * LANGUAGE JAVA
     * PARAMETER STYLE JAVA
     * MODIFIES SQL DATA
     * FENCED
     * NO RESULT SETS
     * EXTERNAL NAME 'IPTO_JAR:org.gautelis.repo.db2.RepoRoutines.ingestNewVersion';
     */
    public static void ingestNewVersion(
            Clob p_unit,
            int[] p_unitver,
            Timestamp[] p_modified
    ) throws SQLException {

        Connection con = getConnection();

        try {
            String json = clobToString(p_unit);
            JsonNode root = MAPPER.readTree(json);

            int tenantId = root.path("tenantid").asInt();
            long unitId = root.path("unitid").asLong();
            int status = root.path("status").asInt();
            String unitName = root.path("unitname").asText();

            // Bump unit kernel last version (obtain new version)
            int prevVer;
            int newVer;

            String selectKernel =
                    "SELECT lastver " +
                    "FROM repo_unit_kernel " +
                    "WHERE tenantid = ? AND unitid = ? " +
                    "FOR UPDATE WITH RS";

            try (PreparedStatement pStmt = con.prepareStatement(selectKernel)) {
                pStmt.setInt(1, tenantId);
                pStmt.setLong(2, unitId);

                try (ResultSet rs = pStmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException("No matching repo_unit_kernel for unit=" + tenantId + "." + unitId);
                    }
                    prevVer = rs.getInt(1);
                }
            }

            newVer = prevVer + 1;

            String updateKernel =
                    "UPDATE repo_unit_kernel " +
                    "SET status = ?, lastver = ? " +
                    "WHERE tenantid = ? AND unitid = ?";

            try (PreparedStatement pStmt = con.prepareStatement(updateKernel)) {
                pStmt.setInt(1, status);
                pStmt.setInt(2, newVer);
                pStmt.setInt(3, tenantId);
                pStmt.setLong(4, unitId);
                pStmt.executeUpdate();
            }

            p_unitver[0] = newVer;

            // Insert new unit version
            String insertVersion =
                    "INSERT INTO repo_unit_version (tenantid, unitid, unitver, unitname) " +
                    "VALUES (?, ?, ?, ?)";

            try (PreparedStatement pStmt = con.prepareStatement(insertVersion)) {
                pStmt.setInt(1, tenantId);
                pStmt.setLong(2, unitId);
                pStmt.setInt(3, newVer);
                pStmt.setString(4, unitName);
                pStmt.executeUpdate();
            }

            String selectVersion =
                    "SELECT modified " +
                    "FROM repo_unit_version " +
                    "WHERE tenantid = ? AND unitid = ? AND unitver = ?";

            Timestamp modified;
            try (PreparedStatement pStmt = con.prepareStatement(selectVersion)) {
                pStmt.setInt(1, tenantId);
                pStmt.setLong(2, unitId);
                pStmt.setInt(3, newVer);

                try (ResultSet rs = pStmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException(
                                "New repo_unit_version not found for unit="
                                + tenantId + "." + unitId + ":" + newVer
                        );
                    }
                    modified = rs.getTimestamp(1);
                }
            }
            p_modified[0] = modified;

            // Parse incoming attributes
            ArrayNode attrArray = null;
            JsonNode attrsNode = root.get("attributes");
            if (attrsNode != null && attrsNode.isArray()) {
                attrArray = (ArrayNode) attrsNode;
            }

            List<IncomingAttribute> primitiveAttrs = new ArrayList<>();
            List<IncomingAttribute> recordAttrs = new ArrayList<>();
            Set<Integer> allAttrIds = new HashSet<>();

            if (attrArray != null) {
                for (int i = 0; i < attrArray.size(); i++) {
                    JsonNode a = attrArray.get(i);

                    int attrId = a.path("attrid").asInt();
                    int attrType = a.path("attrtype").asInt();
                    boolean isModified = a.has("ismodified")
                            && a.path("ismodified").asBoolean(false);
                    JsonNode value = a.get("value");

                    IncomingAttribute inAttr = new IncomingAttribute(attrId, attrType, isModified, value);
                    allAttrIds.add(attrId);

                    if (attrType == /* AttributeType.RECORD */ 99) {
                        recordAttrs.add(inAttr);
                    } else {
                        primitiveAttrs.add(inAttr);
                    }
                }
            }

            // Handle existing primitive attributes (modified / unmodified)
            //
            //  - If modified: insert new value and vectors (range [newVer, newVer])
            //  - If unchanged: extend existing value to cover newVer as well
            //
            String updateExtendPrimitive =
                    "UPDATE repo_attribute_value " +
                    "SET unitverto = ? " +
                    "WHERE tenantid = ? AND unitid = ? AND attrid = ? " +
                    "  AND unitverfrom <= ? AND unitverto >= ?";

            for (IncomingAttribute ia : primitiveAttrs) {
                if (ia.isModified) {
                    // new value
                    long valueId = insertAttributeValue(con, tenantId, unitId, ia.attrId, newVer, newVer);

                    ArrayNode valArray;
                    if (ia.value != null && ia.value.isArray()) {
                        valArray = (ArrayNode) ia.value;
                    } else {
                        valArray = MAPPER.createArrayNode();
                    }

                    insertPrimitiveVector(con, valueId, ia.attrType, valArray);

                } else {
                    // attribute was unchanged; extend range to include newVer
                    try (PreparedStatement pStmt = con.prepareStatement(updateExtendPrimitive)) {
                        pStmt.setInt(1, newVer);
                        pStmt.setInt(2, tenantId);
                        pStmt.setLong(3, unitId);
                        pStmt.setInt(4, ia.attrId);
                        pStmt.setInt(5, prevVer);
                        pStmt.setInt(6, prevVer);
                        pStmt.executeUpdate();
                    }
                }
            }

            // If there are attributes removed from new version,
            // then close their range at newVer - 1
            //
            StringBuilder sb = new StringBuilder(
                    "UPDATE repo_attribute_value av " +
                    "SET unitverto = ? " +
                    "WHERE av.tenantid = ? " +
                    "  AND av.unitid   = ? " +
                    "  AND av.unitverfrom <= ? " +
                    "  AND av.unitverto   >= ? "
            );

            List<Integer> attrIdList = new ArrayList<>(allAttrIds);
            if (!attrIdList.isEmpty()) {
                sb.append("AND av.attrid NOT IN (");
                for (int i = 0; i < attrIdList.size(); i++) {
                    if (i > 0) sb.append(", ");
                    sb.append("?");
                }
                sb.append(")");
            }

            try (PreparedStatement pStmt = con.prepareStatement(sb.toString())) {
                int idx = 1;
                pStmt.setInt(idx++, newVer - 1);
                pStmt.setInt(idx++, tenantId);
                pStmt.setLong(idx++, unitId);
                pStmt.setInt(idx++, prevVer);
                pStmt.setInt(idx++, prevVer);

                for (Integer attrId : attrIdList) {
                    pStmt.setInt(idx++, attrId);
                }

                pStmt.executeUpdate();
            }

            // Handle record attributes
            List<RecordElement> recordElems = new ArrayList<>();

            for (IncomingAttribute ia : recordAttrs) {
                JsonNode valueNode = ia.value;
                if (valueNode == null || !valueNode.isArray()) {
                    continue;
                }
                ArrayNode arr = (ArrayNode) valueNode;
                for (int idx = 0; idx < arr.size(); idx++) {
                    JsonNode elem = arr.get(idx);
                    int refAttrId = elem.path("ref_attrid").asInt();
                    RecordElement re = new RecordElement(ia.attrId, idx, refAttrId);
                    recordElems.add(re);
                }
            }

            if (!recordElems.isEmpty()) {
                // Resolve each child ref to its current valueid at newVer
                Set<Integer> refAttrIds = new HashSet<>();
                for (RecordElement re : recordElems) {
                    refAttrIds.add(re.refAttrId);
                }

                Map<Integer, Long> currentChildValueIds = new HashMap<>();

                String sqlChild =
                        "SELECT valueid " +
                        "FROM repo_attribute_value av " +
                        "WHERE av.tenantid = ? AND av.unitid = ? AND av.attrid = ? " +
                        "  AND av.unitverfrom <= ? AND av.unitverto >= ? " +
                        "FETCH FIRST 1 ROW ONLY";

                try (PreparedStatement pStmt = con.prepareStatement(sqlChild)) {
                    for (Integer refAttrId : refAttrIds) {
                        pStmt.setInt(1, tenantId);
                        pStmt.setLong(2, unitId);
                        pStmt.setInt(3, refAttrId);
                        pStmt.setInt(4, newVer);
                        pStmt.setInt(5, newVer);

                        try (ResultSet rs = pStmt.executeQuery()) {
                            if (rs.next()) {
                                long valueId = rs.getLong(1);
                                currentChildValueIds.put(refAttrId, valueId);
                            }
                        }
                    }
                }

                // Assign refValueId per recElem
                for (RecordElement re : recordElems) {
                    re.refValueId = currentChildValueIds.get(re.refAttrId);
                }

                // Ensure no unresolved children remain
                for (RecordElement re : recordElems) {
                    if (re.refValueId == null) {
                        throw new SQLException("Record references attribute " + re.refAttrId
                                + " without an effective value at "
                                + tenantId + "." + unitId + ":" + newVer
                        );
                    }
                }

                // For each parent record_attrid: extend or insert
                Map<Integer, List<RecordElement>> byParent = new HashMap<>();
                for (RecordElement re : recordElems) {
                    byParent.computeIfAbsent(re.parentAttrId,
                            k -> new ArrayList<>()).add(re);
                }

                String sqlPrevParent =
                        "SELECT valueid " +
                        "FROM repo_attribute_value av " +
                        "WHERE av.tenantid = ? AND av.unitid = ? AND av.attrid = ? " +
                        "  AND av.unitverfrom <= ? AND av.unitverto >= ? " +
                        "FETCH FIRST 1 ROW ONLY";

                String sqlInsertRecordVector =
                        "INSERT INTO repo_record_vector " +
                        "  (valueid, idx, ref_attrid, ref_valueid) " +
                        "VALUES (?, ?, ?, ?)";

                String sqlUpdateParentRange =
                        "UPDATE repo_attribute_value " +
                        "SET unitverto = ? " +
                        "WHERE tenantid = ? AND unitid = ? AND attrid = ? " +
                        "  AND unitverfrom <= ? AND unitverto >= ?";

                String sqlSelectOldElems =
                        "SELECT ref_attrid, ref_valueid " +
                        "FROM repo_record_vector " +
                        "WHERE valueid = ? " +
                        "ORDER BY idx";

                for (Map.Entry<Integer, List<RecordElement>> entry : byParent.entrySet()) {
                    int recordAttrId = entry.getKey();
                    List<RecordElement> elems = entry.getValue();
                    elems.sort(Comparator.comparingInt(e -> e.idx));

                    // Find previous parent valueid at prevVer
                    Long prevParentValueId = null;
                    try (PreparedStatement pStmt = con.prepareStatement(sqlPrevParent)) {
                        pStmt.setInt(1, tenantId);
                        pStmt.setLong(2, unitId);
                        pStmt.setInt(3, recordAttrId);
                        pStmt.setInt(4, prevVer);
                        pStmt.setInt(5, prevVer);
                        try (ResultSet rs = pStmt.executeQuery()) {
                            if (rs.next()) {
                                prevParentValueId = rs.getLong(1);
                            }
                        }
                    }

                    if (prevParentValueId == null) {
                        // New record: insert parent row + its elements, range [newVer, newVer]
                        long newParentValueId =
                                insertAttributeValue(con, tenantId, unitId, recordAttrId, newVer, newVer);

                        try (PreparedStatement pStmt = con.prepareStatement(sqlInsertRecordVector)) {
                            for (RecordElement e : elems) {
                                pStmt.setLong(1, newParentValueId);
                                pStmt.setInt(2, e.idx);
                                pStmt.setInt(3, e.refAttrId);
                                pStmt.setLong(4, e.refValueId);
                                pStmt.addBatch();
                            }
                            pStmt.executeBatch();
                        }

                    } else {
                        // Compare ordered pairs (ref_attrid, ref_valueid) old vs new
                        List<ReferencePair> newPairs = new ArrayList<>();
                        for (RecordElement e : elems) {
                            newPairs.add(new ReferencePair(e.refAttrId, e.refValueId));
                        }

                        List<ReferencePair> oldPairs = new ArrayList<>();
                        try (PreparedStatement pStmt = con.prepareStatement(sqlSelectOldElems)) {
                            pStmt.setLong(1, prevParentValueId);

                            try (ResultSet rs = pStmt.executeQuery()) {
                                while (rs.next()) {
                                    int refAttrId = rs.getInt(1);
                                    long refValId = rs.getLong(2);
                                    oldPairs.add(new ReferencePair(refAttrId, refValId));
                                }
                            }
                        }

                        boolean same = newPairs.equals(oldPairs);

                        if (same) {
                            // Extend parent record's attribute value range
                            try (PreparedStatement pStmt = con.prepareStatement(sqlUpdateParentRange)) {
                                pStmt.setInt(1, newVer);
                                pStmt.setInt(2, tenantId);
                                pStmt.setLong(3, unitId);
                                pStmt.setInt(4, recordAttrId);
                                pStmt.setInt(5, prevVer);
                                pStmt.setInt(6, prevVer);
                                pStmt.executeUpdate();
                            }
                        } else {
                            // Insert new parent and elements
                            long newParentValueId =
                                    insertAttributeValue(con, tenantId, unitId, recordAttrId, newVer, newVer);

                            try (PreparedStatement pStmt = con.prepareStatement(sqlInsertRecordVector)) {
                                for (RecordElement e : elems) {
                                    pStmt.setLong(1, newParentValueId);
                                    pStmt.setInt(2, e.idx);
                                    pStmt.setInt(3, e.refAttrId);
                                    pStmt.setLong(4, e.refValueId);
                                    pStmt.addBatch();
                                }
                                pStmt.executeBatch();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            if (e instanceof SQLException) throw (SQLException) e;
            throw new SQLException("Error in ingest_new_version_json", e);
        }
    }
}

package org.gautelis.ipto.api;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Path("/api/attributes")
@Produces(MediaType.APPLICATION_JSON)
public class AttributeResource {
    private static final Logger log = LoggerFactory.getLogger(RecordResource.class);
    private static final String DEFAULT_DESCRIPTION_LANG = "SE";

    @Inject
    Repository repository;

    private static String trimToNull(Object value) {
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? null : text;
    }

    private static Boolean toBoolean(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof Number number) {
            return number.intValue() != 0;
        }
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) {
            return null;
        }
        return Boolean.parseBoolean(text);
    }

    private static void safeRollback(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException sqle) {
            log.warn("Failed to rollback transaction: {}", sqle.getMessage());
        }
    }

    @GET
    public Response list() {
        log.debug("AttributeResource::list()");

        List<Map<String, Object>> attributes = new ArrayList<>();

        String sql = """
            SELECT attrid, alias, attrname, qualname, attrtype, scalar, created
            FROM repo_attribute
            ORDER BY attrid
            """;

        try {
            repository.withConnection(conn -> {
                try (PreparedStatement stmt = conn.prepareStatement(sql);
                     ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        AttributeType type = AttributeType.of(rs.getInt("attrtype"));
                        boolean scalar = rs.getBoolean("scalar");
                        String cardinality = scalar ? "SCALAR" : "VECTOR";
                        boolean searchable = switch (type) {
                            case STRING, TIME, INTEGER, LONG, DOUBLE, BOOLEAN -> true;
                            default -> false;
                        };

                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("_id", rs.getInt("attrid"));
                        row.put("_name", rs.getString("attrname"));
                        row.put("_alias", rs.getString("alias"));
                        row.put("_qual_name", rs.getString("qualname"));
                        row.put("_type", type.name());
                        row.put("_cardinality", cardinality);
                        row.put("_searchable", searchable);
                        row.put("_created", rs.getTimestamp("created"));
                        attributes.add(row);
                    }
                } catch (SQLException sqle) {
                    throw new RuntimeException(sqle);
                }
            });
        } catch (SQLException | RuntimeException ex) {
            return Response.serverError()
                    .entity(Map.of("error", "Failed to load attributes"))
                    .build();
        }

        log.trace("-> attributes: {}", attributes);
        return Response.ok(attributes).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response create(Map<String, Object> payload) {
        log.debug("AttributeResource::create()");

        String alias = trimToNull(payload.get("alias"));
        String attributeName = trimToNull(payload.get("attributeName"));
        String qualifiedName = trimToNull(payload.get("qualifiedName"));
        String typeName = trimToNull(payload.get("type"));
        Boolean isArray = toBoolean(payload.get("isArray"));

        if (attributeName == null || qualifiedName == null || typeName == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "attributeName, qualifiedName, and type are required"))
                    .build();
        }

        final AttributeType type;
        try {
            type = AttributeType.of(typeName);
        } catch (Exception ex) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "Unknown attribute type"))
                    .build();
        }

        try {
            Optional<org.gautelis.ipto.repo.model.KnownAttributes.AttributeInfo> existing =
                    repository.getAttributeInfo(attributeName);
            if (existing.isPresent()) {
                return Response.status(Response.Status.CONFLICT)
                        .entity(Map.of("error", "Attribute name already exists"))
                        .build();
            }

            Optional<org.gautelis.ipto.repo.model.KnownAttributes.AttributeInfo> info =
                    repository.createAttribute(alias, attributeName, qualifiedName, type, isArray != null && isArray);

            if (info.isEmpty()) {
                return Response.status(Response.Status.CONFLICT)
                        .entity(Map.of("error", "Attribute name or qualified name already exists"))
                        .build();
            }

            AttributeType createdType = AttributeType.of(info.get().type);
            boolean scalar = info.get().forcedScalar;
            String cardinality = scalar ? "SCALAR" : "VECTOR";
            boolean searchable = switch (createdType) {
                case STRING, TIME, INTEGER, LONG, DOUBLE, BOOLEAN -> true;
                default -> false;
            };

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("_id", info.get().id);
            row.put("_name", info.get().name);
            row.put("_alias", info.get().alias);
            row.put("_qual_name", info.get().qualName);
            row.put("_type", createdType.name());
            row.put("_cardinality", cardinality);
            row.put("_searchable", searchable);
            row.put("_created", info.get().created);

            log.trace("-> row: {}", row);
            return Response.ok(row).build();

        } catch (Exception ex) {
            return Response.serverError()
                    .entity(Map.of("error", "Failed to create attribute"))
                    .build();
        }
    }

    @GET
    @Path("/{attrId}/descriptions")
    public Response listDescriptions(@PathParam("attrId") int attrId) {
        log.debug("AttributeResource::listDescriptions({})", attrId);

        if (attrId <= 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "attrId must be positive"))
                    .build();
        }

        String sql = """
            SELECT lang, alias, description
            FROM repo_attribute_description
            WHERE attrid = ?
            ORDER BY lang
            """;

        List<Map<String, Object>> rows = new ArrayList<>();

        try {
            repository.withConnection(conn -> {
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setInt(1, attrId);
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            Map<String, Object> row = new LinkedHashMap<>();
                            row.put("lang", rs.getString("lang"));
                            row.put("alias", rs.getString("alias"));
                            row.put("description", rs.getString("description"));
                            rows.add(row);
                        }
                    }
                } catch (SQLException sqle) {
                    throw new RuntimeException(sqle);
                }
            });
        } catch (SQLException | RuntimeException ex) {
            return Response.serverError()
                    .entity(Map.of("error", "Failed to load attribute descriptions"))
                    .build();
        }

        log.trace("-> rows: {}", rows);
        return Response.ok(rows).build();
    }

    @PUT
    @Path("/{attrId}/descriptions")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response upsertDescriptions(@PathParam("attrId") int attrId, Map<String, Object> payload) {
        log.debug("AttributeResource::upsertDescriptions({})", attrId);

        if (attrId <= 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "attrId must be positive"))
                    .build();
        }

        List<Map<String, Object>> items = new ArrayList<>();
        Object rawItems = payload.get("items");
        if (rawItems instanceof List<?> list) {
            for (Object entry : list) {
                if (entry instanceof Map<?, ?> map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> casted = (Map<String, Object>) map;
                    items.add(casted);
                }
            }
        } else {
            items.add(payload);
        }

        if (items.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "No descriptions provided"))
                    .build();
        }

        String lookupSql = """
            SELECT attrname
            FROM repo_attribute
            WHERE attrid = ?
            """;

        String deleteSql = """
            DELETE FROM repo_attribute_description
            WHERE attrid = ? AND lang = ?
            """;

        String insertSql = """
            INSERT INTO repo_attribute_description (attrid, lang, alias, description)
            VALUES (?,?,?,?)
            """;

        List<Map<String, Object>> stored = new ArrayList<>();

        try {
            repository.withConnection(conn -> {
                try {
                    conn.setAutoCommit(false);

                    String attrName = null;
                    try (PreparedStatement stmt = conn.prepareStatement(lookupSql)) {
                        stmt.setInt(1, attrId);
                        try (ResultSet rs = stmt.executeQuery()) {
                            if (rs.next()) {
                                attrName = rs.getString("attrname");
                            }
                        }
                    }

                    if (attrName == null) {
                        throw new IllegalArgumentException("Attribute does not exist");
                    }

                    try (PreparedStatement deleteStmt = conn.prepareStatement(deleteSql);
                         PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
                        for (Map<String, Object> item : items) {
                            String lang = trimToNull(item.get("lang"));
                            String alias = trimToNull(item.get("alias"));
                            String description = trimToNull(item.get("description"));

                            if (lang == null) {
                                lang = DEFAULT_DESCRIPTION_LANG;
                            }
                            if (alias == null) {
                                alias = attrName;
                            }

                            deleteStmt.setInt(1, attrId);
                            deleteStmt.setString(2, lang);
                            deleteStmt.executeUpdate();

                            insertStmt.setInt(1, attrId);
                            insertStmt.setString(2, lang);
                            insertStmt.setString(3, alias);
                            if (description == null) {
                                insertStmt.setNull(4, Types.VARCHAR);
                            } else {
                                insertStmt.setString(4, description);
                            }
                            insertStmt.executeUpdate();

                            Map<String, Object> row = new LinkedHashMap<>();
                            row.put("lang", lang);
                            row.put("alias", alias);
                            row.put("description", description);
                            stored.add(row);
                        }
                    }

                    conn.commit();
                } catch (IllegalArgumentException ex) {
                    safeRollback(conn);
                    throw ex;
                } catch (SQLException sqle) {
                    safeRollback(conn);
                    throw new RuntimeException(sqle);
                }
            });
        } catch (IllegalArgumentException ex) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("error", ex.getMessage()))
                    .build();
        } catch (SQLException | RuntimeException ex) {
            return Response.serverError()
                    .entity(Map.of("error", "Failed to store attribute descriptions"))
                    .build();
        }

        log.trace("-> stored: {}", stored);
        return Response.ok(stored).build();
    }

    @GET
    @Path("/metadata")
    public Response metadata() {
        log.debug("AttributeResource::metadata()");

        List<Map<String, Object>> attributes = new ArrayList<>();

        String namespaceSql = """
            SELECT alias, namespace
            FROM repo_namespace
            """;

        String attributeSql = """
            SELECT attrid, alias, attrname, qualname, attrtype, scalar, created
            FROM repo_attribute
            ORDER BY attrid
            """;

        try {
            repository.withConnection(conn -> {
                Map<String, String> namespaces = new LinkedHashMap<>();
                try {
                    try (PreparedStatement stmt = conn.prepareStatement(namespaceSql);
                         ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            namespaces.put(rs.getString("alias"), rs.getString("namespace"));
                        }
                    }

                    try (PreparedStatement stmt = conn.prepareStatement(attributeSql);
                         ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            AttributeType type = AttributeType.of(rs.getInt("attrtype"));
                            boolean scalar = rs.getBoolean("scalar");
                            String cardinality = scalar ? "SCALAR" : "VECTOR";
                            boolean searchable = switch (type) {
                                case STRING, TIME, INTEGER, LONG, DOUBLE, BOOLEAN -> true;
                                default -> false;
                            };

                            String attrName = rs.getString("attrname");
                            String prefix = null;
                            String localName = null;
                            if (attrName != null) {
                                int sep = attrName.indexOf(':');
                                if (sep >= 0) {
                                    prefix = attrName.substring(0, sep);
                                    localName = attrName.substring(sep + 1);
                                } else {
                                    localName = attrName;
                                }
                            }

                            Map<String, Object> row = new LinkedHashMap<>();
                            row.put("_id", rs.getInt("attrid"));
                            row.put("_name", attrName);
                            row.put("_alias", rs.getString("alias"));
                            row.put("_qual_name", rs.getString("qualname"));
                            row.put("_type", type.name());
                            row.put("_cardinality", cardinality);
                            row.put("_searchable", searchable);
                            row.put("_created", rs.getTimestamp("created"));
                            row.put("_namespace_alias", prefix);
                            row.put("_namespace_uri", prefix == null ? null : namespaces.get(prefix));
                            row.put("_local_name", localName);
                            attributes.add(row);
                        }
                    }
                } catch (SQLException sqle) {
                    throw new RuntimeException(sqle);
                }
            });
        } catch (SQLException | RuntimeException ex) {
            return Response.serverError()
                    .entity(Map.of("error", "Failed to load attribute metadata"))
                    .build();
        }

        log.trace("-> attributes: {}", attributes);
        return Response.ok(attributes).build();
    }
}

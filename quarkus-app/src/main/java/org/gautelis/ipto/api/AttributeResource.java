package org.gautelis.ipto.api;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Path("/api/attributes")
@Produces(MediaType.APPLICATION_JSON)
public class AttributeResource {
    private static final Logger log = LoggerFactory.getLogger(RecordResource.class);

    @Inject
    Repository repository;

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

        log.debug("-> attributes: {}", attributes);
        return Response.ok(attributes).build();
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

        log.debug("-> attributes: {}", attributes);
        return Response.ok(attributes).build();
    }
}

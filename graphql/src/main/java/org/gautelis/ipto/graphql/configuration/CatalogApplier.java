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
package org.gautelis.ipto.graphql.configuration;

import org.gautelis.ipto.graphql.model.CatalogAttribute;
import org.gautelis.ipto.graphql.model.CatalogRecord;
import org.gautelis.ipto.graphql.model.CatalogTemplate;
import org.gautelis.ipto.graphql.model.FieldAliasResolver;
import org.gautelis.ipto.graphql.model.GqlAttributeShape;
import org.gautelis.ipto.graphql.model.GqlFieldShape;
import org.gautelis.ipto.graphql.model.GqlRecordShape;
import org.gautelis.ipto.graphql.model.GqlTemplateShape;
import org.gautelis.ipto.repo.db.Database;
import org.gautelis.ipto.repo.exceptions.BaseException;
import org.gautelis.ipto.repo.exceptions.ConfigurationException;
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.KnownAttributes;
import org.gautelis.ipto.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

final class CatalogApplier {
    private static final Logger log = LoggerFactory.getLogger(CatalogApplier.class);
    private static final String DEFAULT_DESCRIPTION_LANG = "SE";

    private CatalogApplier() {}

    static CatalogAttribute addAttribute(
            Repository repo,
            GqlAttributeShape gqlAttribute,
            PrintStream progress
    ) {
        CatalogAttribute provisional = new CatalogAttribute(
                gqlAttribute.alias(),
                gqlAttribute.name(),
                gqlAttribute.qualName(),
                AttributeType.of(gqlAttribute.typeName()),
                gqlAttribute.isArray()
        );
        CatalogAttribute stored = provisional;

        try {
            Optional<KnownAttributes.AttributeInfo> info = repo.createAttribute(
                    provisional.alias(),
                    provisional.attrName(),
                    provisional.qualifiedName(),
                    provisional.attrType(),
                    provisional.isArray()
            );

            if (info.isPresent()) {
                stored = provisional.withAttrId(info.get().id);
                log.info("↯ Loaded attribute '{}' (attrid={}, name='{}', qual-name='{}')", stored.alias(), stored.attrId(), stored.attrName(), stored.qualifiedName());
                storeAttributeDescription(repo, stored.attrId(), gqlAttribute);

            } else {
                log.error("↯ Failed to store attribute '{}' ({}, '{}')", provisional.alias(), provisional.attrId(), gqlAttribute.name());
            }
        } catch (BaseException ex) {
            log.error("↯ Failed to store attribute '{}' ({}, '{}'): {}", provisional.alias(), provisional.attrId(), gqlAttribute.name(), ex.getMessage(), ex);
        }

        return stored;
    }

    static Optional<CatalogAttribute> updateAttributeIfMutable(
            Repository repo,
            CatalogAttribute existing,
            GqlAttributeShape desired,
            PrintStream progress
    ) {
        if (existing == null || desired == null) {
            return Optional.empty();
        }

        AttributeType desiredType = AttributeType.of(desired.typeName());
        boolean unchanged =
                Objects.equals(existing.alias(), desired.alias())
                        && Objects.equals(existing.attrName(), desired.name())
                        && Objects.equals(existing.qualifiedName(), desired.qualName())
                        && existing.attrType() == desiredType
                        && existing.isArray() == desired.isArray();
        if (unchanged) {
            return Optional.of(existing);
        }

        final int attrId = existing.attrId();
        try {
            if (!repo.canChangeAttribute(attrId)) {
                String message = "Attribute '" + existing.attrName() + "' is in use and cannot be updated";
                log.warn("↯ {}", message);
                if (progress != null) {
                    progress.println(message);
                }
                return Optional.empty();
            }
        } catch (BaseException ex) {
            log.error("↯ Failed to determine mutability for attribute '{}' (attrid={}): {}",
                    existing.attrName(), attrId, ex.getMessage(), ex);
            return Optional.empty();
        }

        String sql = """
                UPDATE repo_attribute
                   SET attrtype = ?,
                       scalar = ?,
                       attrname = ?,
                       qualname = ?,
                       alias = ?
                 WHERE attrid = ?
                """;

        try {
            repo.withConnection(conn ->
                    Database.usePreparedStatement(conn, sql, pStmt -> {
                        int i = 0;
                        pStmt.setInt(++i, desiredType.getType());
                        pStmt.setBoolean(++i, !desired.isArray());
                        pStmt.setString(++i, desired.name());
                        pStmt.setString(++i, desired.qualName());
                        pStmt.setString(++i, desired.alias());
                        pStmt.setInt(++i, attrId);
                        Database.executeUpdate(pStmt);
                    })
            );

            CatalogAttribute updated = new CatalogAttribute(
                    attrId,
                    desired.alias(),
                    desired.name(),
                    desired.qualName(),
                    desiredType,
                    desired.isArray()
            );
            log.info("↯ Updated attribute '{}' (attrid={}) -> alias='{}', name='{}', qname='{}', type={}, isArray={}",
                    existing.attrName(),
                    attrId,
                    updated.alias(),
                    updated.attrName(),
                    updated.qualifiedName(),
                    updated.attrType(),
                    updated.isArray());
            storeAttributeDescription(repo, attrId, desired);
            return Optional.of(updated);

        } catch (SQLException sqle) {
            log.error("↯ Failed to update attribute '{}' (attrid={}): {}",
                    existing.attrName(), attrId, Database.squeeze(sqle));
            return Optional.empty();
        }
    }

    static CatalogRecord addRecord(
            Repository repo,
            GqlRecordShape gqlRecord,
            Map<String, CatalogAttribute> catalogAttributesByAlias,
            PrintStream progress
    ) {
        String recordName = gqlRecord.typeName();
        String recordAttributeName = gqlRecord.attributeEnumName();
        List<GqlFieldShape> fields = gqlRecord.fields();

        CatalogAttribute recordAttribute = catalogAttributesByAlias.get(recordAttributeName);
        if (null == recordAttribute) {
            log.warn("↯ No matching record attribute in catalog: {}", recordAttributeName);
            throw new RuntimeException("No matching record attribute: " + recordAttributeName);
        }

        final int recordAttributeId = recordAttribute.attrId();
        List<CatalogAttribute> storedFields = new java.util.ArrayList<>();
        CatalogRecord catalogRecord = new CatalogRecord(recordAttributeId, recordName, List.of());

        String recordSql = """
                        INSERT INTO repo_record_template (recordid, name)
                        VALUES (?,?)
                        """;

        String elementsSql = """
                        INSERT INTO repo_record_template_elements (recordid, attrid, idx, alias)
                        VALUES (?,?,?,?)
                        """;

        try {
            repo.withConnection(conn -> {
                try {
                    conn.setAutoCommit(false);

                    Database.usePreparedStatement(conn, recordSql, pStmt -> {
                        try {
                            int i = 0;
                            pStmt.setInt(++i, recordAttributeId);
                            pStmt.setString(++i, catalogRecord.recordName());

                            Database.execute(pStmt);

                        } catch (SQLException sqle) {
                            String sqlState = sqle.getSQLState();
                            conn.rollback();

                            if (sqlState.startsWith("23")) {
                                log.info("↯ Record '{}' seems to already have been loaded", recordName);
                            } else {
                                throw sqle;
                            }
                        }
                    });

                    Database.usePreparedStatement(conn, elementsSql, pStmt -> {
                        int idx = 0;
                        for (GqlFieldShape field : fields) {
                            Optional<CatalogAttribute> attribute = FieldAliasResolver.resolveCatalogAttribute(
                                    catalogAttributesByAlias,
                                    field.usedAttributeName()
                            );
                            if (attribute.isEmpty()) {
                                log.warn("↯ No matching catalog attribute: '{}' (name='{}')", field.fieldName(), field.usedAttributeName());
                                continue;
                            }

                            pStmt.clearParameters();

                            int i = 0;
                            pStmt.setInt(++i, catalogRecord.recordAttrId());
                            pStmt.setInt(++i, attribute.get().attrId());
                            pStmt.setInt(++i, ++idx);
                            pStmt.setString(++i, field.fieldName());

                            Database.execute(pStmt);
                            storedFields.add(asFieldAlias(attribute.get(), field.fieldName()));
                        }
                    });

                    conn.commit();
                    log.info("↯ Loaded record '{}'", recordName);

                } catch (Throwable t) {
                    log.error("↯ Failed to store record '{}': {}", recordName, t.getMessage(), t);
                    if (t.getCause() instanceof SQLException sqle) {
                        log.error("  ^--- {}", Database.squeeze(sqle));
                        String sqlState = sqle.getSQLState();

                        try {
                            conn.rollback();
                        } catch (SQLException rbe) {
                            log.error("↯ Failed to rollback transaction: {}", Database.squeeze(rbe), rbe);
                        }

                        if (sqlState.startsWith("23")) {
                            log.info("↯ Record '{}' seems to already have been loaded", recordName);
                        }
                    }
                }
            });
        } catch (SQLException sqle) {
            log.error("↯ Failed to store record: {}", Database.squeeze(sqle));
        }

        return new CatalogRecord(recordAttributeId, recordName, storedFields);
    }

    static CatalogTemplate addUnitTemplate(
            Repository repo,
            GqlTemplateShape gqlUnitTemplate,
            Map<String, CatalogAttribute> catalogAttributesByAlias,
            PrintStream progress
    ) {
        final String templateName = gqlUnitTemplate.typeName();
        List<CatalogAttribute> storedFields = new java.util.ArrayList<>();
        final int[] generatedTemplateId = {-1};

        String templateSql = """
                        INSERT INTO repo_unit_template (name)
                        VALUES (?)
                        """;

        String elementsSql = """
                        INSERT INTO repo_unit_template_elements (templateid, attrid, idx, alias)
                        VALUES (?,?,?,?)
                        """;

        try {
            repo.withConnection(conn -> {
                try {
                    conn.setAutoCommit(false);

                    String[] generatedColumns = { "templateid" };
                    try (PreparedStatement pStmt = conn.prepareStatement(templateSql, generatedColumns)) {
                        int i = 0;
                        pStmt.setString(++i, templateName);

                        Database.executeUpdate(pStmt);

                        try (ResultSet rs = pStmt.getGeneratedKeys()) {
                            if (rs.next()) {
                                generatedTemplateId[0] = rs.getInt(1);
                            } else {
                                String info = "↯ Failed to determine auto-generated template ID";
                                log.error(info);
                                throw new ConfigurationException(info);
                            }
                        }
                    }

                    Database.usePreparedStatement(conn, elementsSql, pStmt -> {
                        int idx = 0;
                        for (GqlFieldShape field : gqlUnitTemplate.fields()) {
                            Optional<CatalogAttribute> attribute = FieldAliasResolver.resolveCatalogAttribute(
                                    catalogAttributesByAlias,
                                    field.usedAttributeName()
                            );
                            if (attribute.isEmpty()) {
                                log.warn("↯ No matching catalog attribute: '{}' ('{}')", field.fieldName(), field.usedAttributeName());
                                continue;
                            }

                            pStmt.clearParameters();

                            int i = 0;
                            pStmt.setInt(++i, generatedTemplateId[0]);
                            pStmt.setInt(++i, attribute.get().attrId());
                            pStmt.setInt(++i, ++idx);
                            pStmt.setString(++i, field.fieldName());

                            Database.execute(pStmt);
                            storedFields.add(asFieldAlias(attribute.get(), field.fieldName()));
                        }
                    });

                    conn.commit();
                    log.info("↯ Loaded template '{}'", templateName);

                } catch (Throwable t) {
                    log.error("↯ Failed to store template '{}': {}", templateName, t.getMessage(), t);
                    if (t.getCause() instanceof SQLException sqle) {
                        log.error("  ^--- {}", Database.squeeze(sqle));
                        String sqlState = sqle.getSQLState();

                        try {
                            conn.rollback();
                        } catch (SQLException rbe) {
                            log.error("↯ Failed to rollback transaction: {}", Database.squeeze(rbe), rbe);
                        }

                        if (sqlState.startsWith("23")) {
                            log.info("↯ Unit template '{}' seems to already have been loaded", templateName);
                        }
                    }
                }
            });
        } catch (SQLException sqle) {
            log.error("↯ Failed to store template: {}", Database.squeeze(sqle));
        }

        return new CatalogTemplate(generatedTemplateId[0], templateName, storedFields);
    }

    private static CatalogAttribute asFieldAlias(CatalogAttribute attribute, String fieldAlias) {
        return new CatalogAttribute(
                attribute.attrId(),
                fieldAlias,
                attribute.attrName(),
                attribute.qualifiedName(),
                attribute.attrType(),
                attribute.isArray()
        );
    }

    private static void storeAttributeDescription(Repository repo, int attrId, GqlAttributeShape gqlAttribute) {
        if (gqlAttribute == null || gqlAttribute.description() == null || gqlAttribute.description().isBlank()) {
            return;
        }

        String alias = gqlAttribute.name() != null ? gqlAttribute.name() : gqlAttribute.alias();
        if (alias == null || alias.isBlank()) {
            return;
        }

        String sql = """
                INSERT INTO repo_attribute_description (attrid, lang, alias, description)
                VALUES (?,?,?,?)
                """;

        try {
            repo.withConnection(conn -> {
                Database.usePreparedStatement(conn, sql, pStmt -> {
                    try {
                        int i = 0;
                        pStmt.setInt(++i, attrId);
                        pStmt.setString(++i, DEFAULT_DESCRIPTION_LANG);
                        pStmt.setString(++i, alias);
                        pStmt.setString(++i, gqlAttribute.description());
                        Database.executeUpdate(pStmt);
                    } catch (SQLException sqle) {
                        String sqlState = sqle.getSQLState();
                        if (sqlState != null && sqlState.startsWith("23")) {
                            log.info("↯ Attribute description already exists for attrId={} lang={}", attrId, DEFAULT_DESCRIPTION_LANG);
                            return;
                        }
                        throw sqle;
                    }
                });
            });
        } catch (SQLException sqle) {
            log.warn("↯ Failed to store attribute description for attrId={}: {}", attrId, Database.squeeze(sqle));
        }
    }
}

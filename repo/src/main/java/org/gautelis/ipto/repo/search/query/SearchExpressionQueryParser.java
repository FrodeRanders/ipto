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
package org.gautelis.ipto.repo.search.query;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.gautelis.ipto.repo.db.Column;
import org.gautelis.ipto.repo.exceptions.InvalidParameterException;
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.AssociationType;
import org.gautelis.ipto.repo.model.RelationType;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.search.model.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class SearchExpressionQueryParser {

    public interface AttributeTypeResolver {
        Optional<ResolvedAttribute> resolve(String name);
    }

    public enum AttributeNameMode {
        NAMES,
        ALIASES,
        NAMES_OR_ALIASES
    }

    public record ResolvedAttribute(String name, AttributeType type) {}

    private static final Map<String, UnitField> UNIT_FIELDS = buildUnitFields();
    private static final Set<String> RELATION_PREFIXES = Set.of("relation", "relations", "rel");
    private static final Set<String> ASSOCIATION_PREFIXES = Set.of("association", "associations", "assoc");

    private SearchExpressionQueryParser() {
    }

    public static SearchExpression parse(String query, AttributeTypeResolver resolver) {
        return parse(query, resolver, AttributeNameMode.NAMES);
    }

    public static SearchExpression parse(String query, AttributeTypeResolver resolver, AttributeNameMode mode) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(resolver, "resolver");
        Objects.requireNonNull(mode, "mode");

        SearchExpressionLexer lexer = new SearchExpressionLexer(CharStreams.fromString(query));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new SearchExpressionErrorListener(query));

        SearchExpressionParser parser = new SearchExpressionParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(new SearchExpressionErrorListener(query));

        SearchExpressionParser.QueryContext tree = parser.query();
        return new Builder(resolver, mode).visit(tree.expr());
    }

    public static SearchExpression parse(String query, Repository repo) {
        Objects.requireNonNull(repo, "repo");
        return parse(query, name -> repo.getAttributeInfo(name)
                .map(info -> new ResolvedAttribute(name, AttributeType.of(info.type))), AttributeNameMode.NAMES);
    }

    public static SearchExpression parse(String query, Repository repo, AttributeNameMode mode) {
        Objects.requireNonNull(repo, "repo");
        return parse(query, name -> repo.getAttributeInfo(name)
                .map(info -> new ResolvedAttribute(name, AttributeType.of(info.type))), mode);
    }

    private static final class Builder extends SearchExpressionBaseVisitor<SearchExpression> {
        private final AttributeTypeResolver resolver;
        private final AttributeNameMode mode;

        private Builder(AttributeTypeResolver resolver, AttributeNameMode mode) {
            this.resolver = resolver;
            this.mode = mode;
        }

        @Override
        public SearchExpression visitOrExpr(SearchExpressionParser.OrExprContext ctx) {
            SearchExpression expr = visit(ctx.andExpr(0));
            for (int i = 1; i < ctx.andExpr().size(); i++) {
                expr = new OrExpression(expr, visit(ctx.andExpr(i)));
            }
            return expr;
        }

        @Override
        public SearchExpression visitAndExpr(SearchExpressionParser.AndExprContext ctx) {
            SearchExpression expr = visit(ctx.notExpr(0));
            for (int i = 1; i < ctx.notExpr().size(); i++) {
                expr = new AndExpression(expr, visit(ctx.notExpr(i)));
            }
            return expr;
        }

        @Override
        public SearchExpression visitNotExpr(SearchExpressionParser.NotExprContext ctx) {
            if (ctx.NOT() != null) {
                return new NotExpression(visit(ctx.notExpr()));
            }
            return visit(ctx.primary());
        }

        @Override
        public SearchExpression visitPrimary(SearchExpressionParser.PrimaryContext ctx) {
            if (ctx.predicate() != null) {
                return visit(ctx.predicate());
            }
            return visit(ctx.expr());
        }

        @Override
        public SearchExpression visitPredicate(SearchExpressionParser.PredicateContext ctx) {
            String field = ctx.field().getText();
            Operator op = toOperator(ctx.operator());

            Optional<RelationSpec> relationSpec = parseRelationSpec(field);
            if (relationSpec.isPresent()) {
                ensureEqOperator(field, op);
                Unit.Id unitId = parseUnitRef(ctx.value());
                RelationSpec spec = relationSpec.get();
                return spec.side == RelationSide.LEFT
                        ? LeftRelationSearchItem.constrainOnLeftRelationEQ(spec.type, unitId)
                        : RightRelationSearchItem.constrainOnRightRelationEQ(spec.type, unitId);
            }

            Optional<AssociationSpec> associationSpec = parseAssociationSpec(field);
            if (associationSpec.isPresent()) {
                ensureEqOperator(field, op);
                String assocString = valueText(ctx.value());
                AssociationSpec spec = associationSpec.get();
                return spec.side == RelationSide.LEFT
                        ? LeftAssociationSearchItem.constrainOnLeftAssociation(spec.type, assocString)
                        : RightAssociationSearchItem.constrainOnRightAssociationEQ(spec.type, assocString);
            }

            Value value = parseValue(ctx.value());

            boolean hasQualifier = field.indexOf(':') >= 0;
            if (!hasQualifier && isAliasEnabled(mode)) {
                Optional<ResolvedAttribute> resolved = resolver.resolve(field);
                if (resolved.isPresent()) {
                    return new LeafExpression<>(toAttributeItem(resolved.get().name(), resolved.get().type(), op, value));
                }
            }

            UnitField unitField = resolveUnitField(field);
            if (unitField != null) {
                return new LeafExpression<>(unitField.toItem(op, value));
            }

            if (hasQualifier || mode != AttributeNameMode.NAMES) {
                Optional<ResolvedAttribute> resolved = resolver.resolve(field);
                if (resolved.isPresent()) {
                    return new LeafExpression<>(toAttributeItem(resolved.get().name(), resolved.get().type(), op, value));
                }
            }

            throw new InvalidParameterException("Unknown field: " + field);
        }

        private UnitField resolveUnitField(String field) {
            return UNIT_FIELDS.get(field.toLowerCase());
        }

        private AttributeSearchItem<?> toAttributeItem(
                String field,
                AttributeType type,
                Operator op,
                Value value
        ) {
            switch (type) {
                case STRING -> {
                    return new StringAttributeSearchItem(field, normalizeStringOperator(op, value), value.asString());
                }
                case TIME -> {
                    return new TimeAttributeSearchItem(field, op, value.asInstant());
                }
                case INTEGER -> {
                    return new IntegerAttributeSearchItem(field, op, value.asInt());
                }
                case LONG -> {
                    return new LongAttributeSearchItem(field, op, value.asLong());
                }
                case DOUBLE -> {
                    return new DoubleAttributeSearchItem(field, op, value.asDouble());
                }
                case BOOLEAN -> {
                    return new BooleanAttributeSearchItem(field, op, value.asBoolean());
                }
                default -> throw new InvalidParameterException("Attribute type " + type.name() + " is not searchable: " + field);
            }
        }
    }

    private static Operator normalizeStringOperator(Operator op, Value value) {
        if (op == Operator.EQ && value.isStringLikePattern()) {
            return Operator.LIKE;
        }
        return op;
    }

    private static boolean isAliasEnabled(AttributeNameMode mode) {
        return mode == AttributeNameMode.ALIASES || mode == AttributeNameMode.NAMES_OR_ALIASES;
    }

    private static Operator toOperator(SearchExpressionParser.OperatorContext ctx) {
        if (ctx.EQ() != null) return Operator.EQ;
        if (ctx.NEQ() != null) return Operator.NEQ;
        if (ctx.GE() != null) return Operator.GEQ;
        if (ctx.GT() != null) return Operator.GT;
        if (ctx.LE() != null) return Operator.LEQ;
        if (ctx.LT() != null) return Operator.LT;
        if (ctx.LIKE() != null) return Operator.LIKE;
        throw new InvalidParameterException("Unsupported operator: " + ctx.getText());
    }

    private static void ensureEqOperator(String field, Operator op) {
        if (op != Operator.EQ) {
            throw new InvalidParameterException("Only '=' is supported for relation/association constraints: " + field);
        }
    }

    private static Optional<RelationSpec> parseRelationSpec(String field) {
        QualifiedField qf = QualifiedField.parse(field);
        if (qf.prefix == null) {
            return Optional.empty();
        }

        String prefixLower = qf.prefix.toLowerCase(Locale.ROOT);
        RelationSide side = extractSideFromPrefixedField(prefixLower, RELATION_PREFIXES);
        if (side == RelationSide.NONE && !RELATION_PREFIXES.contains(prefixLower)) {
            return Optional.empty();
        }

        SideParse localSide = extractSideFromLocal(qf.local);
        RelationSide resolvedSide = resolveSide(side, localSide.side);
        String typeToken = localSide.remaining;
        if (typeToken.isBlank()) {
            throw new InvalidParameterException("Relation type is missing in field: " + field);
        }

        RelationType type = resolveRelationType(typeToken);
        return Optional.of(new RelationSpec(type, resolvedSide));
    }

    private static Optional<AssociationSpec> parseAssociationSpec(String field) {
        QualifiedField qf = QualifiedField.parse(field);
        if (qf.prefix == null) {
            return Optional.empty();
        }

        String prefixLower = qf.prefix.toLowerCase(Locale.ROOT);
        RelationSide side = extractSideFromPrefixedField(prefixLower, ASSOCIATION_PREFIXES);
        if (side == RelationSide.NONE && !ASSOCIATION_PREFIXES.contains(prefixLower)) {
            return Optional.empty();
        }

        SideParse localSide = extractSideFromLocal(qf.local);
        RelationSide resolvedSide = resolveSide(side, localSide.side);
        String typeToken = localSide.remaining;
        if (typeToken.isBlank()) {
            throw new InvalidParameterException("Association type is missing in field: " + field);
        }

        AssociationType type = resolveAssociationType(typeToken);
        return Optional.of(new AssociationSpec(type, resolvedSide));
    }

    private static RelationSide resolveSide(RelationSide prefixSide, RelationSide localSide) {
        if (prefixSide != RelationSide.NONE && localSide != RelationSide.NONE && prefixSide != localSide) {
            throw new InvalidParameterException("Conflicting relation/association side qualifiers");
        }
        RelationSide resolved = prefixSide != RelationSide.NONE ? prefixSide : localSide;
        return resolved != RelationSide.NONE ? resolved : RelationSide.LEFT;
    }

    private static RelationSide extractSideFromPrefixedField(String prefixLower, Set<String> allowedPrefixes) {
        for (String base : allowedPrefixes) {
            RelationSide side = matchSideSuffix(prefixLower, base);
            if (side != RelationSide.NONE) {
                return side;
            }
        }
        return RelationSide.NONE;
    }

    private static RelationSide matchSideSuffix(String prefixLower, String base) {
        if (prefixLower.equals(base + "-left") || prefixLower.equals(base + "_left")) {
            return RelationSide.LEFT;
        }
        if (prefixLower.equals(base + "-right") || prefixLower.equals(base + "_right")) {
            return RelationSide.RIGHT;
        }
        return RelationSide.NONE;
    }

    private static SideParse extractSideFromLocal(String local) {
        String lower = local.toLowerCase(Locale.ROOT);
        if (lower.startsWith("left:")) {
            return new SideParse(RelationSide.LEFT, local.substring("left:".length()));
        }
        if (lower.startsWith("right:")) {
            return new SideParse(RelationSide.RIGHT, local.substring("right:".length()));
        }
        if (lower.startsWith("left-") || lower.startsWith("left_")) {
            return new SideParse(RelationSide.LEFT, local.substring("left-".length()));
        }
        if (lower.startsWith("right-") || lower.startsWith("right_")) {
            return new SideParse(RelationSide.RIGHT, local.substring("right-".length()));
        }
        return new SideParse(RelationSide.NONE, local);
    }

    private static RelationType resolveRelationType(String raw) {
        String normalized = normalizeTypeToken(raw);
        if (!normalized.endsWith("_RELATION")) {
            normalized += "_RELATION";
        }
        try {
            return RelationType.valueOf(normalized);
        } catch (IllegalArgumentException ex) {
            throw new InvalidParameterException("Unknown relation type: " + raw);
        }
    }

    private static AssociationType resolveAssociationType(String raw) {
        String normalized = normalizeTypeToken(raw);
        if (!normalized.endsWith("_ASSOCIATION")) {
            normalized += "_ASSOCIATION";
        }
        try {
            return AssociationType.valueOf(normalized);
        } catch (IllegalArgumentException ex) {
            throw new InvalidParameterException("Unknown association type: " + raw);
        }
    }

    private static String normalizeTypeToken(String raw) {
        return raw.trim()
                .replace('-', '_')
                .replace(' ', '_')
                .toUpperCase(Locale.ROOT);
    }

    private static Value parseValue(SearchExpressionParser.ValueContext ctx) {
        Token token = ctx.getStart();
        switch (token.getType()) {
            case SearchExpressionParser.STRING -> {
                return Value.of(unquote(token.getText()));
            }
            case SearchExpressionParser.NUMBER -> {
                String raw = token.getText();
                if (raw.indexOf('.') >= 0) {
                    return Value.of(Double.parseDouble(raw));
                }
                return Value.of(Long.parseLong(raw));
            }
            case SearchExpressionParser.TRUE -> {
                return Value.of(true);
            }
            case SearchExpressionParser.FALSE -> {
                return Value.of(false);
            }
            case SearchExpressionParser.IDENT -> {
                return Value.of(token.getText());
            }
            default -> throw new InvalidParameterException("Unsupported value: " + token.getText());
        }
    }

    private static Unit.Id parseUnitRef(SearchExpressionParser.ValueContext ctx) {
        String raw = valueText(ctx);
        String trimmed = raw.trim();
        int colon = trimmed.indexOf(':');
        String base = colon >= 0 ? trimmed.substring(0, colon) : trimmed;
        if (colon >= 0) {
            String version = trimmed.substring(colon + 1);
            if (!version.isEmpty() && !version.chars().allMatch(Character::isDigit)) {
                throw new InvalidParameterException("Invalid unit version in reference: " + raw);
            }
        }

        String[] parts = base.split("\\.", -1);
        if (parts.length != 2) {
            throw new InvalidParameterException("Invalid unit reference: " + raw);
        }
        try {
            int tenantId = Integer.parseInt(parts[0]);
            long unitId = Long.parseLong(parts[1]);
            return new Unit.Id(tenantId, unitId);
        } catch (NumberFormatException ex) {
            throw new InvalidParameterException("Invalid unit reference: " + raw);
        }
    }

    private static String valueText(SearchExpressionParser.ValueContext ctx) {
        Token token = ctx.getStart();
        if (token.getType() == SearchExpressionParser.STRING) {
            return unquote(token.getText());
        }
        return token.getText();
    }

    private static String unquote(String raw) {
        if (raw == null || raw.length() < 2) {
            return raw;
        }
        char quote = raw.charAt(0);
        if ((quote == '\'' || quote == '"') && raw.charAt(raw.length() - 1) == quote) {
            String inner = raw.substring(1, raw.length() - 1);
            return inner.replace("\\\"", "\"")
                    .replace("\\'", "'")
                    .replace("\\\\", "\\");
        }
        return raw;
    }

    private record QualifiedField(String prefix, String local) {
        static QualifiedField parse(String field) {
            int idx = field.indexOf(':');
            if (idx < 0) {
                return new QualifiedField(null, field);
            }
            return new QualifiedField(field.substring(0, idx), field.substring(idx + 1));
        }
    }

    private enum RelationSide {
        LEFT,
        RIGHT,
        NONE
    }

    private record RelationSpec(RelationType type, RelationSide side) {
    }

    private record AssociationSpec(AssociationType type, RelationSide side) {
    }

    private record SideParse(RelationSide side, String remaining) {
    }

    private static Map<String, UnitField> buildUnitFields() {
        Map<String, UnitField> fields = new HashMap<>();
        fields.put("tenantid", new UnitField(Column.UNIT_KERNEL_TENANTID, AttributeType.INTEGER));
        fields.put("tenant_id", new UnitField(Column.UNIT_KERNEL_TENANTID, AttributeType.INTEGER));
        fields.put("unitid", new UnitField(Column.UNIT_KERNEL_UNITID, AttributeType.LONG));
        fields.put("unit_id", new UnitField(Column.UNIT_KERNEL_UNITID, AttributeType.LONG));
        fields.put("unitver", new UnitField(Column.UNIT_VERSION_UNITVER, AttributeType.INTEGER));
        fields.put("unit_ver", new UnitField(Column.UNIT_VERSION_UNITVER, AttributeType.INTEGER));
        fields.put("corrid", new UnitField(Column.UNIT_KERNEL_CORRID, AttributeType.STRING));
        fields.put("correlationid", new UnitField(Column.UNIT_KERNEL_CORRID, AttributeType.STRING));
        fields.put("status", new UnitField(Column.UNIT_KERNEL_STATUS, AttributeType.INTEGER));
        fields.put("unitname", new UnitField(Column.UNIT_VERSION_UNITNAME, AttributeType.STRING));
        fields.put("unit_name", new UnitField(Column.UNIT_VERSION_UNITNAME, AttributeType.STRING));
        fields.put("created", new UnitField(Column.UNIT_KERNEL_CREATED, AttributeType.TIME));
        fields.put("modified", new UnitField(Column.UNIT_VERSION_MODIFIED, AttributeType.TIME));
        return fields;
    }

    private record UnitField(Column column, AttributeType type) {
        SearchItem<?> toItem(Operator op, Value value) {
            switch (type) {
                case STRING -> {
                    Operator adjusted = normalizeStringOperator(op, value);
                    return new StringUnitSearchItem(column, adjusted, value.asString());
                }
                case TIME -> {
                    return new TimeUnitSearchItem(column, op, value.asInstant());
                }
                case INTEGER -> {
                    int intValue = "status".equals(column.plain()) ? value.asStatus() : value.asInt();
                    return new IntegerUnitSearchItem(column, op, intValue);
                }
                case LONG -> {
                    return new LongUnitSearchItem(column, op, value.asLong());
                }
                default -> throw new InvalidParameterException("Unsupported unit field type: " + type.name());
            }
        }
    }

    private static final class Value {
        private final Object value;

        private Value(Object value) {
            this.value = value;
        }

        static Value of(Object value) {
            return new Value(value);
        }

        String asString() {
            if (value instanceof String s) {
                return normalizePattern(s);
            }
            return String.valueOf(value);
        }

        int asInt() {
            if (value instanceof Number number) {
                return number.intValue();
            }
            return Integer.parseInt(value.toString());
        }

        long asLong() {
            if (value instanceof Number number) {
                return number.longValue();
            }
            return Long.parseLong(value.toString());
        }

        double asDouble() {
            if (value instanceof Number number) {
                return number.doubleValue();
            }
            return Double.parseDouble(value.toString());
        }

        boolean asBoolean() {
            if (value instanceof Boolean bool) {
                return bool;
            }
            return Boolean.parseBoolean(value.toString());
        }

        Instant asInstant() {
            String raw = value.toString();
            if (raw.toLowerCase().endsWith("z")) {
                return Instant.parse(raw);
            }
            return Instant.parse(raw + "Z");
        }

        int asStatus() {
            if (value instanceof Number number) {
                return number.intValue();
            }
            String raw = value.toString().toUpperCase();
            try {
                return Unit.Status.valueOf(raw).getStatus();
            } catch (IllegalArgumentException ex) {
                return Integer.parseInt(raw);
            }
        }

        boolean isStringLikePattern() {
            return value instanceof String s && (s.indexOf('*') >= 0 || s.indexOf('%') >= 0 || s.indexOf('_') >= 0);
        }

        private String normalizePattern(String raw) {
            if (raw.indexOf('*') >= 0) {
                return raw.replace('*', '%');
            }
            return raw;
        }
    }

    private static final class SearchExpressionErrorListener extends BaseErrorListener {
        private final String input;

        private SearchExpressionErrorListener(String input) {
            this.input = input;
        }

        @Override
        public void syntaxError(
                Recognizer<?, ?> recognizer,
                Object offendingSymbol,
                int line,
                int charPositionInLine,
                String msg,
                RecognitionException e
        ) {
            String symbol = "";
            if (offendingSymbol instanceof Token token) {
                symbol = " near '" + token.getText() + "'";
            }
            throw new InvalidParameterException("Search text syntax error at " + line + ":" + charPositionInLine + symbol + ": " + msg + " in \"" + input + "\"");
        }
    }
}

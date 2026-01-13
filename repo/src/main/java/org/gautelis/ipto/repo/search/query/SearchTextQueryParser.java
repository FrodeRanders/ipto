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
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.search.model.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class SearchTextQueryParser {

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

    private SearchTextQueryParser() {
    }

    public static SearchExpression parse(String query, AttributeTypeResolver resolver) {
        return parse(query, resolver, AttributeNameMode.NAMES);
    }

    public static SearchExpression parse(String query, AttributeTypeResolver resolver, AttributeNameMode mode) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(resolver, "resolver");
        Objects.requireNonNull(mode, "mode");

        SearchTextLexer lexer = new SearchTextLexer(CharStreams.fromString(query));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new SearchTextErrorListener(query));

        SearchTextParser parser = new SearchTextParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(new SearchTextErrorListener(query));

        SearchTextParser.QueryContext tree = parser.query();
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

    private static final class Builder extends SearchTextBaseVisitor<SearchExpression> {
        private final AttributeTypeResolver resolver;
        private final AttributeNameMode mode;

        private Builder(AttributeTypeResolver resolver, AttributeNameMode mode) {
            this.resolver = resolver;
            this.mode = mode;
        }

        @Override
        public SearchExpression visitOrExpr(SearchTextParser.OrExprContext ctx) {
            SearchExpression expr = visit(ctx.andExpr(0));
            for (int i = 1; i < ctx.andExpr().size(); i++) {
                expr = new OrExpression(expr, visit(ctx.andExpr(i)));
            }
            return expr;
        }

        @Override
        public SearchExpression visitAndExpr(SearchTextParser.AndExprContext ctx) {
            SearchExpression expr = visit(ctx.notExpr(0));
            for (int i = 1; i < ctx.notExpr().size(); i++) {
                expr = new AndExpression(expr, visit(ctx.notExpr(i)));
            }
            return expr;
        }

        @Override
        public SearchExpression visitNotExpr(SearchTextParser.NotExprContext ctx) {
            if (ctx.NOT() != null) {
                return new NotExpression(visit(ctx.notExpr()));
            }
            return visit(ctx.primary());
        }

        @Override
        public SearchExpression visitPrimary(SearchTextParser.PrimaryContext ctx) {
            if (ctx.predicate() != null) {
                return visit(ctx.predicate());
            }
            return visit(ctx.expr());
        }

        @Override
        public SearchExpression visitPredicate(SearchTextParser.PredicateContext ctx) {
            String field = ctx.field().getText();
            Operator op = toOperator(ctx.operator());
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

    private static Operator toOperator(SearchTextParser.OperatorContext ctx) {
        if (ctx.EQ() != null) return Operator.EQ;
        if (ctx.NEQ() != null) return Operator.NEQ;
        if (ctx.GE() != null) return Operator.GEQ;
        if (ctx.GT() != null) return Operator.GT;
        if (ctx.LE() != null) return Operator.LEQ;
        if (ctx.LT() != null) return Operator.LT;
        if (ctx.LIKE() != null) return Operator.LIKE;
        throw new InvalidParameterException("Unsupported operator: " + ctx.getText());
    }

    private static Value parseValue(SearchTextParser.ValueContext ctx) {
        Token token = ctx.getStart();
        switch (token.getType()) {
            case SearchTextParser.STRING -> {
                return Value.of(unquote(token.getText()));
            }
            case SearchTextParser.NUMBER -> {
                String raw = token.getText();
                if (raw.indexOf('.') >= 0) {
                    return Value.of(Double.parseDouble(raw));
                }
                return Value.of(Long.parseLong(raw));
            }
            case SearchTextParser.TRUE -> {
                return Value.of(true);
            }
            case SearchTextParser.FALSE -> {
                return Value.of(false);
            }
            case SearchTextParser.IDENT -> {
                return Value.of(token.getText());
            }
            default -> throw new InvalidParameterException("Unsupported value: " + token.getText());
        }
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

    private static final class SearchTextErrorListener extends BaseErrorListener {
        private final String input;

        private SearchTextErrorListener(String input) {
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

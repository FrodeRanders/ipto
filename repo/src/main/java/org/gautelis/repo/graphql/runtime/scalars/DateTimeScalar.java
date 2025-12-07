package org.gautelis.repo.graphql.runtime.scalars;

import graphql.GraphQLContext;
import graphql.execution.CoercedVariables;
import graphql.language.Value;
import graphql.schema.*;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DateTimeException;
import java.time.Instant;
import java.util.Locale;

public final class DateTimeScalar {
    private static final Logger log = LoggerFactory.getLogger(DateTimeScalar.class);

    public static final GraphQLScalarType INSTANCE =
            GraphQLScalarType.newScalar()
                    .name("DateTime")
                    .description("ISO-8601 string parsed to java.time.Instant")
                    .coercing(new Coercing<Instant, String>() {

                        @Override
                        public String serialize(
                                @NonNull Object dataFetcherResult,
                                @NonNull GraphQLContext graphQLContext,
                                @NonNull Locale locale
                        ) throws CoercingSerializeException {
                            log.trace("\u2193 Serializing: {} of type {}", dataFetcherResult, dataFetcherResult.getClass().getName());
                            if (dataFetcherResult instanceof Instant i) {
                                return i.toString();
                            }
                            throw new CoercingSerializeException("Expected Instant");
                        }

                        @Override
                        public Instant parseValue(
                                @NonNull Object input,
                                @NonNull GraphQLContext graphQLContext,
                                @NonNull Locale locale
                        ) throws CoercingParseValueException {
                            log.trace("\u2193 Parsing: {} of type {}", input, input.getClass().getName());
                            try {
                                return Instant.parse(input.toString());
                            } catch (DateTimeException dte) {
                                throw new CoercingParseValueException("Invalid ISO-8601 datetime", dte);
                            }
                        }

                        @Override
                        public Instant parseLiteral(
                                @NonNull Value<?> input,
                                @NonNull CoercedVariables variables,
                                @NonNull GraphQLContext graphQLContext,
                                @NonNull Locale locale
                        ) throws CoercingParseLiteralException {
                            log.trace("\u2193 Parsing literal: {} of type {}", input, input.getClass().getName());
                            if (input instanceof graphql.language.StringValue sv) {
                                return parseValue(sv.getValue(),  graphQLContext, locale);
                            }
                            throw new CoercingParseLiteralException("Expected StringValue");
                        }
                    })
                    .build();

    private DateTimeScalar() {}
}

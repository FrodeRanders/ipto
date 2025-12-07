package org.gautelis.repo.graphql.runtime.scalars;

import graphql.GraphQLContext;
import graphql.execution.CoercedVariables;
import graphql.language.Value;
import graphql.schema.*;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public final class LongScalar {
    private static final Logger log = LoggerFactory.getLogger(LongScalar.class);

    public static final GraphQLScalarType INSTANCE =
            GraphQLScalarType.newScalar()
                    .name("Long")
                    .description("Integer value mapped to Java Long")
                    .coercing(new Coercing<Long, String>() {

                        @Override
                        public String serialize(
                                @NonNull Object dataFetcherResult,
                                @NonNull GraphQLContext graphQLContext,
                                @NonNull Locale locale
                        ) throws CoercingSerializeException {
                            log.trace("\u2193 Serializing: {} of type {}", dataFetcherResult, dataFetcherResult.getClass().getName());
                            if (dataFetcherResult instanceof Number n)
                                return n.toString();
                            throw new CoercingSerializeException("Expected Long but was " + dataFetcherResult);
                        }

                        @Override
                        public Long parseValue(
                                @NonNull Object input,
                                @NonNull GraphQLContext graphQLContext,
                                @NonNull Locale locale
                        ) throws CoercingParseValueException {
                            log.trace("\u2193 Parsing: {} of type {}", input, input.getClass().getName());
                            if (input instanceof String str) {
                                return Long.parseLong(str);
                            } else {
                                return Long.parseLong(input.toString());
                            }
                        }

                        @Override
                        public Long parseLiteral(
                                @NonNull Value<?> input,
                                @NonNull CoercedVariables variables,
                                @NonNull GraphQLContext graphQLContext,
                                @NonNull Locale locale
                        ) throws CoercingParseLiteralException {
                            log.trace("\u2193 Parsing literal: {} of type {}", input, input.getClass().getName());
                            if (input instanceof graphql.language.IntValue iv) {
                                return iv.getValue().longValue();
                            }
                            throw new CoercingParseLiteralException("Expected IntValue");
                        }
                    })
                    .build();

    private LongScalar() {}
}
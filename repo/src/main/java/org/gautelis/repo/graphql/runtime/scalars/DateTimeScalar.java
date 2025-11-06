package org.gautelis.repo.graphql.runtime.scalars;

import graphql.GraphQLContext;
import graphql.execution.CoercedVariables;
import graphql.language.StringValue;
import graphql.language.Value;
import graphql.schema.*;
import org.jspecify.annotations.NonNull;

import java.time.*;
import java.util.Locale;

public final class DateTimeScalar {

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
                            if (input instanceof StringValue sv) {
                                return parseValue(sv.getValue(),  graphQLContext, locale);
                            }
                            throw new CoercingParseLiteralException("Expected StringValue");
                        }
                    })
                    .build();

    private DateTimeScalar() {}
}

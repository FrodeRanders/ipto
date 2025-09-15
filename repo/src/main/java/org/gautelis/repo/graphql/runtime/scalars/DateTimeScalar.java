package org.gautelis.repo.graphql.runtime.scalars;

import graphql.language.StringValue;
import graphql.schema.*;

import java.time.*;

public final class DateTimeScalar {

    public static final GraphQLScalarType INSTANCE =
            GraphQLScalarType.newScalar()
                    .name("DateTime")
                    .description("ISO-8601 string parsed to java.time.Instant")
                    .coercing(new Coercing<Instant, String>() {

                        @Override
                        public String serialize(Object dataFetcherResult) throws CoercingSerializeException {
                            if (dataFetcherResult instanceof Instant i)
                                return i.toString();      // 2025-06-24T12:34:56Z
                            throw new CoercingSerializeException("Expected Instant");
                        }

                        @Override
                        public Instant parseValue(Object input) throws CoercingParseValueException {
                            try {
                                return Instant.parse(input.toString());
                            } catch (DateTimeException dte) {
                                throw new CoercingParseValueException("Invalid ISO-8601 datetime", dte);
                            }
                        }

                        @Override
                        public Instant parseLiteral(Object input) throws CoercingParseLiteralException {
                            if (input instanceof StringValue sv) {
                                return parseValue(sv.getValue());
                            }
                            throw new CoercingParseLiteralException("Expected StringValue");
                        }
                    })
                    .build();

    private DateTimeScalar() {}
}

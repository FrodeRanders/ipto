package org.gautelis.repo.graphql.runtime.scalars;

import graphql.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LongScalar {
    private static final Logger log = LoggerFactory.getLogger(LongScalar.class);

    public static final GraphQLScalarType INSTANCE =
            GraphQLScalarType.newScalar()
                    .name("Long")
                    .description("Java Long ↔ PostgreSQL BIGINT")
                    .coercing(new Coercing<Long, String>() {

                        @Override
                        public String serialize(Object dataFetcherResult) throws CoercingSerializeException {
                            if (dataFetcherResult instanceof Number n)
                                return n.toString();
                            throw new CoercingSerializeException("Expected Long but was " + dataFetcherResult);
                        }

                        @Override
                        public Long parseValue(Object input) throws CoercingParseValueException {
                            log.trace("Parsing: {} of type {}", input, input.getClass().getName());
                            if (input instanceof String str) {
                                return Long.parseLong(str);
                            } else {
                                return Long.parseLong(input.toString());
                            }
                        }

                        @Override
                        public Long parseLiteral(Object input) throws CoercingParseLiteralException {
                            log.trace("Parsing literal: {} of type {}", input, input.getClass().getName());
                            if (input instanceof graphql.language.IntValue iv)
                                return iv.getValue().longValue();
                            throw new CoercingParseLiteralException("Expected IntValue");
                        }

                        /*
                        @Override
                        public Value valueToLiteral(Object input) {
                            log.trace("Value -> literal (Long): {} of type {}", input, input.getClass().getName());

                        }
                        */
                    })
                    .build();

    private LongScalar() {}
}
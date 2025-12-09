package org.gautelis.ipto.graphql.runtime.scalars;

import graphql.GraphQLContext;
import graphql.execution.CoercedVariables;
import graphql.language.Value;
import graphql.schema.*;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Locale;

public final class BytesScalar {
    private static final Logger log = LoggerFactory.getLogger(BytesScalar.class);

    private static final Base64.Encoder ENC = Base64.getEncoder();
    private static final Base64.Decoder DEC = Base64.getDecoder();

    public static final GraphQLScalarType INSTANCE =
            GraphQLScalarType.newScalar()
                    .name("Bytes")
                    .description("Base-64-encoded binary mapped to byte[]")
                    .coercing(new Coercing<byte[], String>() {

                        @Override
                        public String serialize(
                                @NonNull Object dataFetcherResult,
                                @NonNull GraphQLContext graphQLContext,
                                @NonNull Locale locale
                        ) throws CoercingSerializeException {
                            log.trace("\u2193 Serializing: {} of type {}", dataFetcherResult, dataFetcherResult.getClass().getName());
                            if (dataFetcherResult instanceof byte[] b) {
                                return ENC.encodeToString(b);
                            }
                            throw new CoercingSerializeException("Expected byte[]");
                        }

                        @Override
                        public byte[] parseValue(
                                @NonNull Object input,
                                @NonNull GraphQLContext graphQLContext,
                                @NonNull Locale locale
                        ) throws CoercingParseValueException {
                            log.trace("\u2193 Parsing: {} of type {}", input, input.getClass().getName());
                            try {
                                return DEC.decode(input.toString());
                            } catch (IllegalArgumentException iae) {
                                throw new CoercingParseValueException("Invalid base-64", iae);
                            }
                        }

                        @Override
                        public byte[] parseLiteral(
                                @NonNull Value<?> input,
                                @NonNull CoercedVariables variables,
                                @NonNull GraphQLContext graphQLContext,
                                @NonNull Locale locale
                        ) throws CoercingParseLiteralException {
                            log.trace("\u2193 Parsing literal: {} of type {}", input, input.getClass().getName());
                            if (input instanceof graphql.language.StringValue sv) {
                                return parseValue(sv.getValue(), graphQLContext, locale);
                            }
                            throw new CoercingParseLiteralException("Expected StringValue");
                        }
                    })
                    .build();

    private BytesScalar() {}
}

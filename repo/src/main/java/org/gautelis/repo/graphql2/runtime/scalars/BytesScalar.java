package org.gautelis.repo.graphql2.runtime.scalars;

import graphql.language.StringValue;
import graphql.schema.*;

import java.util.Base64;

public final class BytesScalar {

    private static final Base64.Encoder ENC = Base64.getEncoder();
    private static final Base64.Decoder DEC = Base64.getDecoder();

    public static final GraphQLScalarType INSTANCE =
            GraphQLScalarType.newScalar()
                    .name("Bytes")
                    .description("Base-64-encoded binary mapped to PostgreSQL BYTEA")
                    .coercing(new Coercing<byte[], String>() {

                        @Override
                        public String serialize(Object dataFetcherResult) throws CoercingSerializeException {
                            if (dataFetcherResult instanceof byte[] b)
                                return ENC.encodeToString(b);
                            throw new CoercingSerializeException("Expected byte[]");
                        }

                        @Override
                        public byte[] parseValue(Object input) throws CoercingParseValueException {
                            try {
                                return DEC.decode(input.toString());
                            } catch (IllegalArgumentException iae) {
                                throw new CoercingParseValueException("Invalid base-64", iae);
                            }
                        }

                        @Override
                        public byte[] parseLiteral(Object input) throws CoercingParseLiteralException {
                            if (input instanceof StringValue sv)
                                return parseValue(sv.getValue());
                            throw new CoercingParseLiteralException("Expected StringValue");
                        }
                    })
                    .build();

    private BytesScalar() {}
}

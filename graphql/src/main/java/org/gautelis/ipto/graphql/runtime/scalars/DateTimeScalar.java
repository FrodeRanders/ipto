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
package org.gautelis.ipto.graphql.runtime.scalars;

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
                            log.trace("↓ Serializing: {} of type {}", dataFetcherResult, dataFetcherResult.getClass().getName());
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
                            log.trace("↓ Parsing: {} of type {}", input, input.getClass().getName());
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
                            log.trace("↓ Parsing literal: {} of type {}", input, input.getClass().getName());
                            if (input instanceof graphql.language.StringValue sv) {
                                String value = sv.getValue();
                                return parseValue(null != value ? value : "",  graphQLContext, locale);
                            }
                            throw new CoercingParseLiteralException("Expected StringValue");
                        }
                    })
                    .build();

    private DateTimeScalar() {}
}

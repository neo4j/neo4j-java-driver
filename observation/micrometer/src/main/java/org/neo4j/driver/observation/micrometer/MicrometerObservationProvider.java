/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.observation.micrometer;

import io.micrometer.observation.ObservationRegistry;
import java.util.function.Predicate;
import org.neo4j.driver.observation.ObservationProvider;
import org.neo4j.driver.util.Preview;

/**
 * An {@link ObservationProvider} implementation based on Micrometer Observation API.
 *
 * @since 6.0.0
 */
@Preview(name = "Observability")
public sealed interface MicrometerObservationProvider extends ObservationProvider
        permits DriverMicrometerObservationProvider {

    /**
     * Creates a new {@link Builder} instance.
     *
     * @param observationRegistry the Micrometer {@link ObservationRegistry}
     * @return the new {@link Builder} instance
     */
    static Builder builder(ObservationRegistry observationRegistry) {
        return new BuilderImpl(observationRegistry);
    }

    /**
     * A builder for creating a new {@link MicrometerObservationProvider} instance.
     */
    sealed interface Builder permits BuilderImpl {
        /**
         * Sets whether query string would always be included in high cardinality tags.
         * <p>
         * It is {@literal false} by default, meaning only queries that have non-empty parameters are included.
         *
         * @param alwaysIncludeQuery {@literal true} to always include query string and {@literal false} to use the default behaviour
         * @return this builder
         */
        Builder alwaysIncludeQuery(boolean alwaysIncludeQuery);

        /**
         * Sets whether query parameters would be included in high cardinality tags.
         * <p>
         * It is {@literal false} by default.
         *
         * @param includeQueryParameters {@literal true} to include and {@literal false} to exclude
         * @return this builder
         */
        Builder includeQueryParameters(boolean includeQueryParameters);

        /**
         * Sets whether URL scheme would be included in low cardinality tags of HTTP exchange.
         * <p>
         * It is {@literal false} by default.
         *
         * @param includeUrlScheme {@literal true} to include, {@literal false} to exclude
         * @return this builder
         */
        Builder includeUrlScheme(boolean includeUrlScheme);

        /**
         * Sets whether URL template would be included in low cardinality tags of HTTP exchange and contextual name.
         * <p>
         * It is {@literal false} by default.
         *
         * @param includeUrlTemplate {@literal true} to include, {@literal false} to exclude
         * @return this builder
         */
        Builder includeUrlTemplate(boolean includeUrlTemplate);

        /**
         * Sets a whitelist predicate for request headers of HTTP exchange. If it returns {@literal true} for a given
         * HTTP header name, such header would be included in high cardinality tags.
         * <p>
         * It is {@literal null} by default.
         *
         * @param requestHeaderPredicate the header predicate or {@literal null} to exclude all headers
         * @return this builder
         */
        Builder requestHeaderPredicate(Predicate<String> requestHeaderPredicate);

        /**
         * Sets a whitelist predicate for response headers of HTTP exchange. If it returns {@literal true} for a given
         * HTTP header name, such header would be included in high cardinality tags.
         * <p>
         * It is {@literal null} by default.
         *
         * @param responseHeaderPredicate the header predicate or {@literal null} to exclude all headers
         * @return this builder
         */
        Builder responseHeaderPredicate(Predicate<String> responseHeaderPredicate);

        /**
         * Builds a new {@link MicrometerObservationProvider} instance.
         *
         * @return the new {@link MicrometerObservationProvider} instance
         */
        MicrometerObservationProvider build();
    }
}

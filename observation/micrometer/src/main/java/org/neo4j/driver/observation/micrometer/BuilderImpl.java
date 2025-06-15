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
import java.util.Objects;
import java.util.function.Predicate;

final class BuilderImpl implements MicrometerObservationProvider.Builder {
    private final ObservationRegistry observationRegistry;
    private boolean alwaysIncludeQuery;
    private boolean includeQueryParameters;
    private boolean includeUrlScheme;
    private boolean includeUrlTemplate;
    private Predicate<String> requestHeaderPredicate;
    private Predicate<String> responseHeaderPredicate;

    BuilderImpl(ObservationRegistry observationRegistry) {
        this.observationRegistry = Objects.requireNonNull(observationRegistry);
    }

    @Override
    public MicrometerObservationProvider.Builder alwaysIncludeQuery(boolean alwaysIncludeQuery) {
        this.alwaysIncludeQuery = alwaysIncludeQuery;
        return this;
    }

    @Override
    public MicrometerObservationProvider.Builder includeQueryParameters(boolean includeQueryParameters) {
        this.includeQueryParameters = includeQueryParameters;
        return this;
    }

    @Override
    public MicrometerObservationProvider.Builder includeUrlScheme(boolean includeUrlScheme) {
        this.includeUrlScheme = includeUrlScheme;
        return this;
    }

    @Override
    public MicrometerObservationProvider.Builder includeUrlTemplate(boolean includeUrlTemplate) {
        this.includeUrlTemplate = includeUrlTemplate;
        return this;
    }

    @Override
    public MicrometerObservationProvider.Builder requestHeaderPredicate(Predicate<String> requestHeaderPredicate) {
        this.requestHeaderPredicate = requestHeaderPredicate;
        return this;
    }

    @Override
    public MicrometerObservationProvider.Builder responseHeaderPredicate(Predicate<String> responseHeaderPredicate) {
        this.responseHeaderPredicate = responseHeaderPredicate;
        return this;
    }

    @Override
    public MicrometerObservationProvider build() {
        return new DriverMicrometerObservationProvider(
                observationRegistry,
                alwaysIncludeQuery,
                includeQueryParameters,
                includeUrlScheme,
                includeUrlTemplate,
                requestHeaderPredicate,
                responseHeaderPredicate);
    }
}

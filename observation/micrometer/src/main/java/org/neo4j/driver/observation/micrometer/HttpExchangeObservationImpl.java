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

import io.micrometer.observation.Observation;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.neo4j.driver.internal.observation.HttpExchangeObservation;

final class HttpExchangeObservationImpl extends ObservationImpl implements HttpExchangeObservation {
    private final HttpExchangeContext context;

    HttpExchangeObservationImpl(Observation delegate) {
        super(delegate);
        this.context = (HttpExchangeContext) Objects.requireNonNull(delegate.getContext());
    }

    @Override
    public HttpExchangeObservation start() {
        super.start();
        return this;
    }

    @Override
    public HttpExchangeObservation onHeaders(Map<String, List<String>> headers) {
        context.setHeaders(headers);
        return this;
    }

    @Override
    public HttpExchangeObservation onResponse(Response response) {
        context.setResponse(new HttpExchangeContext.Response() {
            @Override
            public int statusCode() {
                return response.statusCode();
            }

            @Override
            public Map<String, List<String>> headers() {
                return response.headers();
            }

            @Override
            public String httpVersion() {
                return response.httpVersion();
            }
        });
        return this;
    }

    @Override
    public HttpExchangeObservation error(Throwable error) {
        context.setErrorType(error.getClass().getSimpleName());
        super.error(error);
        return this;
    }
}

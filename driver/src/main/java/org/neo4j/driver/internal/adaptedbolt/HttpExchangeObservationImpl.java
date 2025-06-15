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
package org.neo4j.driver.internal.adaptedbolt;

import java.util.List;
import java.util.Map;
import org.neo4j.bolt.connection.observation.HttpExchangeObservation;

final class HttpExchangeObservationImpl extends BoltObservation implements HttpExchangeObservation {
    private final org.neo4j.driver.internal.observation.HttpExchangeObservation delegate;

    HttpExchangeObservationImpl(org.neo4j.driver.internal.observation.HttpExchangeObservation delegate) {
        super(delegate);
        this.delegate = delegate;
    }

    @Override
    public HttpExchangeObservation onHeaders(Map<String, List<String>> headers) {
        delegate.onHeaders(headers);
        return this;
    }

    @Override
    public HttpExchangeObservation onResponse(Response response) {
        delegate.onResponse(new org.neo4j.driver.internal.observation.HttpExchangeObservation.Response() {
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
        delegate.error(error);
        return this;
    }
}

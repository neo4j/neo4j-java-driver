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

final class NoopHttpExchangeObservationImpl implements HttpExchangeObservation {
    private static final NoopHttpExchangeObservationImpl INSTANCE = new NoopHttpExchangeObservationImpl();

    static NoopHttpExchangeObservationImpl getInstance() {
        return INSTANCE;
    }

    private NoopHttpExchangeObservationImpl() {}

    @Override
    public HttpExchangeObservation onHeaders(Map<String, List<String>> headers) {
        return this;
    }

    @Override
    public HttpExchangeObservation onResponse(Response response) {
        return this;
    }

    @Override
    public HttpExchangeObservation error(Throwable error) {
        return this;
    }

    @Override
    public void stop() {}
}

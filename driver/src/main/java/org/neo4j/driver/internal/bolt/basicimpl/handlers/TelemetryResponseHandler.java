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
package org.neo4j.driver.internal.bolt.basicimpl.handlers;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.TelemetryMessage;
import org.neo4j.driver.internal.bolt.basicimpl.spi.ResponseHandler;

/**
 * Handles {@link TelemetryMessage} responses.
 *
 */
public class TelemetryResponseHandler implements ResponseHandler {
    private final CompletableFuture<Void> future;

    /**
     * Constructor
     *
     * @param future The future which will be resolved
     */
    public TelemetryResponseHandler(CompletableFuture<Void> future) {
        this.future = requireNonNull(future);
    }

    @Override
    public void onSuccess(Map<String, Value> metadata) {
        future.complete(null);
    }

    @Override
    public void onFailure(Throwable error) {
        future.completeExceptionally(error);
    }

    @Override
    public void onRecord(Value[] fields) {
        throw new UnsupportedOperationException(
                "Telemetry is not expected to receive records: " + Arrays.toString(fields));
    }
}

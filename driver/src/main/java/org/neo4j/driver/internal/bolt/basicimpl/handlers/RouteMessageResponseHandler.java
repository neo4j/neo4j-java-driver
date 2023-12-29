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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.bolt.basicimpl.spi.ResponseHandler;

/**
 * Handles the RouteMessage response getting the success response
 * and return its routing table property as the response.
 */
public class RouteMessageResponseHandler implements ResponseHandler {
    private final CompletableFuture<Map<String, Value>> completableFuture;

    public RouteMessageResponseHandler(final CompletableFuture<Map<String, Value>> completableFuture) {
        this.completableFuture = requireNonNull(completableFuture);
    }

    @Override
    public void onSuccess(Map<String, Value> metadata) {
        try {
            completableFuture.complete(metadata.get("rt").asMap(Values::value));
        } catch (Exception ex) {
            completableFuture.completeExceptionally(ex);
        }
    }

    @Override
    public void onFailure(Throwable error) {
        completableFuture.completeExceptionally(error);
    }

    @Override
    public void onRecord(Value[] fields) {
        completableFuture.completeExceptionally(new UnsupportedOperationException(
                "Route is not expected to receive records: " + Arrays.toString(fields)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (RouteMessageResponseHandler) o;
        return completableFuture.equals(that.completableFuture);
    }

    @Override
    public int hashCode() {
        return Objects.hash(completableFuture);
    }
}

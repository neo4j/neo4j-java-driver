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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.basicimpl.spi.ResponseHandler;

public class ResetResponseHandler implements ResponseHandler {
    private final CompletableFuture<Void> completionFuture;
    private final Throwable throwable;

    public ResetResponseHandler(CompletableFuture<Void> completionFuture) {
        this(completionFuture, null);
    }

    public ResetResponseHandler(CompletableFuture<Void> completionFuture, Throwable throwable) {
        this.completionFuture = completionFuture;
        this.throwable = throwable;
    }

    @Override
    public final void onSuccess(Map<String, Value> metadata) {
        resetCompleted();
    }

    @Override
    public final void onFailure(Throwable error) {
        if (completionFuture != null) {
            completionFuture.completeExceptionally(error);
        }
    }

    @Override
    public final void onRecord(Value[] fields) {
        throw new UnsupportedOperationException();
    }

    public Optional<Throwable> throwable() {
        return Optional.ofNullable(throwable);
    }

    private void resetCompleted() {
        if (completionFuture != null) {
            completionFuture.complete(null);
        }
    }
}

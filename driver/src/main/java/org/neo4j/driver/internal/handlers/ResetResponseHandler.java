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
package org.neo4j.driver.internal.handlers;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.spi.ResponseHandler;

public class ResetResponseHandler implements ResponseHandler {
    private final InboundMessageDispatcher messageDispatcher;
    private final CompletableFuture<Void> completionFuture;
    private final Throwable throwable;

    public ResetResponseHandler(InboundMessageDispatcher messageDispatcher) {
        this(messageDispatcher, null);
    }

    public ResetResponseHandler(InboundMessageDispatcher messageDispatcher, CompletableFuture<Void> completionFuture) {
        this(messageDispatcher, completionFuture, null);
    }

    public ResetResponseHandler(
            InboundMessageDispatcher messageDispatcher, CompletableFuture<Void> completionFuture, Throwable throwable) {
        this.messageDispatcher = messageDispatcher;
        this.completionFuture = completionFuture;
        this.throwable = throwable;
    }

    @Override
    public final void onSuccess(Map<String, Value> metadata) {
        resetCompleted(true);
    }

    @Override
    public final void onFailure(Throwable error) {
        resetCompleted(false);
    }

    @Override
    public final void onRecord(Value[] fields) {
        throw new UnsupportedOperationException();
    }

    public Optional<Throwable> throwable() {
        return Optional.ofNullable(throwable);
    }

    private void resetCompleted(boolean success) {
        messageDispatcher.clearCurrentError();
        if (completionFuture != null) {
            resetCompleted(completionFuture, success);
        }
    }

    protected void resetCompleted(CompletableFuture<Void> completionFuture, boolean success) {
        completionFuture.complete(null);
    }
}

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

import io.netty.channel.Channel;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ProtocolException;
import org.neo4j.driver.internal.bolt.basicimpl.spi.ResponseHandler;

public class LogonResponseHandler implements ResponseHandler {
    private final CompletableFuture<?> future;
    private final Channel channel;
    private final Clock clock;
    private final CompletableFuture<Long> latestAuthMillisFuture;

    public LogonResponseHandler(
            CompletableFuture<?> future, Channel channel, Clock clock, CompletableFuture<Long> latestAuthMillisFuture) {
        this.future = requireNonNull(future, "future must not be null");
        this.channel = channel;
        this.clock = requireNonNull(clock, "clock must not be null");
        this.latestAuthMillisFuture = requireNonNull(latestAuthMillisFuture);
    }

    @Override
    public void onSuccess(Map<String, Value> metadata) {
        latestAuthMillisFuture.complete(clock.millis());
        future.complete(null);
    }

    @Override
    public void onFailure(Throwable error) {
        if (channel != null) {
            channel.close().addListener(future -> this.future.completeExceptionally(error));
        } else {
            future.completeExceptionally(error);
        }
    }

    @Override
    public void onRecord(Value[] fields) {
        future.completeExceptionally(new ProtocolException("Records are not supported on LOGON"));
    }
}
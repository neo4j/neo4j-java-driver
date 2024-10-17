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
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.boltPatchesListeners;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.protocolVersion;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setConnectionId;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setConnectionReadTimeout;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setServerAgent;
import static org.neo4j.driver.internal.bolt.basicimpl.util.MetadataExtractor.extractBoltPatches;
import static org.neo4j.driver.internal.bolt.basicimpl.util.MetadataExtractor.extractServer;

import io.netty.channel.Channel;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v43.BoltProtocolV43;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v44.BoltProtocolV44;
import org.neo4j.driver.internal.bolt.basicimpl.spi.ResponseHandler;

public class HelloResponseHandler implements ResponseHandler {
    private static final String CONNECTION_ID_METADATA_KEY = "connection_id";
    public static final String CONFIGURATION_HINTS_KEY = "hints";
    public static final String CONNECTION_RECEIVE_TIMEOUT_SECONDS_KEY = "connection.recv_timeout_seconds";

    private final CompletableFuture<String> future;
    private final Channel channel;
    private final Clock clock;
    private final CompletableFuture<Long> latestAuthMillisFuture;

    public HelloResponseHandler(
            CompletableFuture<String> future,
            Channel channel,
            Clock clock,
            CompletableFuture<Long> latestAuthMillisFuture) {
        requireNonNull(clock, "clock must not be null");
        this.future = future;
        this.channel = channel;
        this.clock = clock;
        this.latestAuthMillisFuture = Objects.requireNonNull(latestAuthMillisFuture);
    }

    @Override
    public void onSuccess(Map<String, Value> metadata) {
        try {
            var serverAgent = extractServer(metadata).asString();
            setServerAgent(channel, serverAgent);

            var connectionId = extractConnectionId(metadata);
            setConnectionId(channel, connectionId);

            processConfigurationHints(metadata);

            var protocolVersion = protocolVersion(channel);
            if (BoltProtocolV44.VERSION.equals(protocolVersion) || BoltProtocolV43.VERSION.equals(protocolVersion)) {
                var boltPatches = extractBoltPatches(metadata);
                if (!boltPatches.isEmpty()) {
                    boltPatchesListeners(channel).forEach(listener -> listener.handle(boltPatches));
                }
            }

            latestAuthMillisFuture.complete(clock.millis());
            future.complete(serverAgent);
        } catch (Throwable error) {
            onFailure(error);
            throw error;
        }
    }

    @Override
    public void onFailure(Throwable error) {
        channel.close().addListener(future -> this.future.completeExceptionally(error));
    }

    @Override
    public void onRecord(Value[] fields) {
        throw new UnsupportedOperationException();
    }

    private static String extractConnectionId(Map<String, Value> metadata) {
        var value = metadata.get(CONNECTION_ID_METADATA_KEY);
        if (value == null || value.isNull()) {
            throw new IllegalStateException("Unable to extract " + CONNECTION_ID_METADATA_KEY
                    + " from a response to HELLO message. " + "Received metadata: " + metadata);
        }
        return value.asString();
    }

    private void processConfigurationHints(Map<String, Value> metadata) {
        var configurationHints = metadata.get(CONFIGURATION_HINTS_KEY);
        if (configurationHints != null) {
            getFromSupplierOrEmptyOnException(() -> configurationHints
                            .get(CONNECTION_RECEIVE_TIMEOUT_SECONDS_KEY)
                            .asLong())
                    .ifPresent(timeout -> setConnectionReadTimeout(channel, timeout));
        }
    }

    private static <T> Optional<T> getFromSupplierOrEmptyOnException(Supplier<T> supplier) {
        try {
            return Optional.of(supplier.get());
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}

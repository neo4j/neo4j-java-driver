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

import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setConnectionId;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setConnectionReadTimeout;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setServerAgent;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setTelemetryEnabled;
import static org.neo4j.driver.internal.bolt.basicimpl.util.MetadataExtractor.extractServer;

import io.netty.channel.Channel;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.basicimpl.spi.ResponseHandler;

public class HelloV51ResponseHandler implements ResponseHandler {
    private static final String CONNECTION_ID_METADATA_KEY = "connection_id";
    public static final String CONFIGURATION_HINTS_KEY = "hints";
    public static final String CONNECTION_RECEIVE_TIMEOUT_SECONDS_KEY = "connection.recv_timeout_seconds";
    public static final String TELEMETRY_ENABLED_KEY = "telemetry.enabled";

    private final Channel channel;
    private final CompletableFuture<String> helloFuture;

    public HelloV51ResponseHandler(Channel channel, CompletableFuture<String> helloFuture) {
        this.channel = channel;
        this.helloFuture = helloFuture;
    }

    @Override
    public void onSuccess(Map<String, Value> metadata) {
        try {
            var serverAgent = extractServer(metadata).asString();
            setServerAgent(channel, serverAgent);

            var connectionId = extractConnectionId(metadata);
            setConnectionId(channel, connectionId);

            processConfigurationHints(metadata);

            helloFuture.complete(serverAgent);
        } catch (Throwable error) {
            onFailure(error);
            throw error;
        }
    }

    @Override
    public void onFailure(Throwable error) {
        channel.close().addListener(future -> helloFuture.completeExceptionally(error));
    }

    @Override
    public void onRecord(Value[] fields) {
        throw new UnsupportedOperationException();
    }

    private void processConfigurationHints(Map<String, Value> metadata) {
        var configurationHints = metadata.get(CONFIGURATION_HINTS_KEY);
        if (configurationHints != null) {
            getFromSupplierOrEmptyOnException(() -> configurationHints
                            .get(CONNECTION_RECEIVE_TIMEOUT_SECONDS_KEY)
                            .asLong())
                    .ifPresent(timeout -> setConnectionReadTimeout(channel, timeout));

            getFromSupplierOrEmptyOnException(
                            () -> configurationHints.get(TELEMETRY_ENABLED_KEY).asBoolean(false))
                    .ifPresent(telemetryEnabled -> setTelemetryEnabled(channel, telemetryEnabled));
        }
    }

    private static String extractConnectionId(Map<String, Value> metadata) {
        var value = metadata.get(CONNECTION_ID_METADATA_KEY);
        if (value == null || value.isNull()) {
            throw new IllegalStateException("Unable to extract " + CONNECTION_ID_METADATA_KEY
                    + " from a response to HELLO message. " + "Received metadata: " + metadata);
        }
        return value.asString();
    }

    private static <T> Optional<T> getFromSupplierOrEmptyOnException(Supplier<T> supplier) {
        try {
            return Optional.of(supplier.get());
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}

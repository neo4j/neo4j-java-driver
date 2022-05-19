/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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

import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setConnectionId;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setConnectionReadTimeout;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setServerAgent;
import static org.neo4j.driver.internal.util.MetadataExtractor.extractServer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.spi.ResponseHandler;

public class HelloResponseHandler implements ResponseHandler {
    private static final String CONNECTION_ID_METADATA_KEY = "connection_id";
    public static final String CONFIGURATION_HINTS_KEY = "hints";
    public static final String CONNECTION_RECEIVE_TIMEOUT_SECONDS_KEY = "connection.recv_timeout_seconds";

    private final ChannelPromise connectionInitializedPromise;
    private final Channel channel;

    public HelloResponseHandler(ChannelPromise connectionInitializedPromise) {
        this.connectionInitializedPromise = connectionInitializedPromise;
        this.channel = connectionInitializedPromise.channel();
    }

    @Override
    public void onSuccess(Map<String, Value> metadata) {
        try {
            String serverAgent = extractServer(metadata).asString();
            setServerAgent(channel, serverAgent);

            String connectionId = extractConnectionId(metadata);
            setConnectionId(channel, connectionId);

            processConfigurationHints(metadata);

            connectionInitializedPromise.setSuccess();
        } catch (Throwable error) {
            onFailure(error);
            throw error;
        }
    }

    @Override
    public void onFailure(Throwable error) {
        channel.close().addListener(future -> connectionInitializedPromise.setFailure(error));
    }

    @Override
    public void onRecord(Value[] fields) {
        throw new UnsupportedOperationException();
    }

    private static String extractConnectionId(Map<String, Value> metadata) {
        Value value = metadata.get(CONNECTION_ID_METADATA_KEY);
        if (value == null || value.isNull()) {
            throw new IllegalStateException("Unable to extract " + CONNECTION_ID_METADATA_KEY
                    + " from a response to HELLO message. " + "Received metadata: " + metadata);
        }
        return value.asString();
    }

    private void processConfigurationHints(Map<String, Value> metadata) {
        Value configurationHints = metadata.get(CONFIGURATION_HINTS_KEY);
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

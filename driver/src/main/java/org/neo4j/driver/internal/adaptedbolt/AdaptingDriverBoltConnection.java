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
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.AuthInfo;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.message.Message;
import org.neo4j.driver.internal.value.BoltValueFactory;

final class AdaptingDriverBoltConnection implements DriverBoltConnection {
    private final BoltConnection connection;
    private final ErrorMapper errorMapper;
    private final BoltValueFactory boltValueFactory;

    AdaptingDriverBoltConnection(
            BoltConnection connection, ErrorMapper errorMapper, BoltValueFactory boltValueFactory) {
        this.connection = Objects.requireNonNull(connection);
        this.errorMapper = Objects.requireNonNull(errorMapper);
        this.boltValueFactory = Objects.requireNonNull(boltValueFactory);
    }

    @Override
    public CompletionStage<Void> writeAndFlush(DriverResponseHandler handler, List<Message> messages) {
        return connection
                .writeAndFlush(new AdaptingDriverResponseHandler(handler, errorMapper, boltValueFactory), messages)
                .exceptionally(errorMapper::mapAndThrow);
    }

    @Override
    public CompletionStage<Void> write(List<Message> messages) {
        return connection.write(messages).exceptionally(errorMapper::mapAndThrow);
    }

    @Override
    public CompletionStage<Void> forceClose(String reason) {
        return connection.forceClose(reason).exceptionally(errorMapper::mapAndThrow);
    }

    @Override
    public CompletionStage<Void> close() {
        return connection.close().exceptionally(errorMapper::mapAndThrow);
    }

    @Override
    public CompletionStage<AuthInfo> authData() {
        return connection.authInfo().exceptionally(errorMapper::mapAndThrow);
    }

    @Override
    public String serverAgent() {
        return connection.serverAgent();
    }

    @Override
    public BoltServerAddress serverAddress() {
        return connection.serverAddress();
    }

    @Override
    public BoltProtocolVersion protocolVersion() {
        return connection.protocolVersion();
    }

    @Override
    public boolean telemetrySupported() {
        return connection.telemetrySupported();
    }

    @Override
    public boolean serverSideRoutingEnabled() {
        return connection.serverSideRoutingEnabled();
    }

    @Override
    public BoltValueFactory valueFactory() {
        return boltValueFactory;
    }
}

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
package org.neo4j.driver.internal.async;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.AuthInfo;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.message.Message;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnection;
import org.neo4j.driver.internal.adaptedbolt.DriverResponseHandler;
import org.neo4j.driver.internal.value.BoltValueFactory;

public abstract class DelegatingBoltConnection implements DriverBoltConnection {
    protected final DriverBoltConnection delegate;

    protected DelegatingBoltConnection(DriverBoltConnection delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public CompletionStage<Void> writeAndFlush(DriverResponseHandler handler, List<Message> messages) {
        return delegate.writeAndFlush(handler, messages);
    }

    @Override
    public CompletionStage<Void> write(List<Message> messages) {
        return delegate.write(messages);
    }

    @Override
    public CompletionStage<Void> forceClose(String reason) {
        return delegate.forceClose(reason);
    }

    @Override
    public CompletionStage<Void> close() {
        return delegate.close();
    }

    @Override
    public CompletionStage<AuthInfo> authData() {
        return delegate.authData();
    }

    @Override
    public String serverAgent() {
        return delegate.serverAgent();
    }

    @Override
    public BoltServerAddress serverAddress() {
        return delegate.serverAddress();
    }

    @Override
    public BoltProtocolVersion protocolVersion() {
        return delegate.protocolVersion();
    }

    @Override
    public boolean telemetrySupported() {
        return delegate.telemetrySupported();
    }

    @Override
    public boolean serverSideRoutingEnabled() {
        return delegate.serverSideRoutingEnabled();
    }

    @Override
    public BoltValueFactory valueFactory() {
        return delegate.valueFactory();
    }
}

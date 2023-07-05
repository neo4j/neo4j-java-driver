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
package org.neo4j.driver.internal.async.connection;

import java.util.concurrent.CompletionStage;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.DirectConnectionProvider;
import org.neo4j.driver.internal.async.TerminationAwareStateLockingExecutor;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;

/**
 * This is a connection used by {@link DirectConnectionProvider} to connect to a remote database.
 */
public class DirectConnection implements Connection {
    private final Connection delegate;
    private final AccessMode mode;
    private final DatabaseName databaseName;
    private final String impersonatedUser;

    public DirectConnection(Connection delegate, DatabaseName databaseName, AccessMode mode, String impersonatedUser) {
        this.delegate = delegate;
        this.mode = mode;
        this.databaseName = databaseName;
        this.impersonatedUser = impersonatedUser;
    }

    public Connection connection() {
        return delegate;
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public void enableAutoRead() {
        delegate.enableAutoRead();
    }

    @Override
    public void disableAutoRead() {
        delegate.disableAutoRead();
    }

    @Override
    public void write(Message message, ResponseHandler handler) {
        delegate.write(message, handler);
    }

    @Override
    public void writeAndFlush(Message message, ResponseHandler handler) {
        delegate.writeAndFlush(message, handler);
    }

    @Override
    public CompletionStage<Void> reset(Throwable throwable) {
        return delegate.reset(throwable);
    }

    @Override
    public CompletionStage<Void> release() {
        return delegate.release();
    }

    @Override
    public void terminateAndRelease(String reason) {
        delegate.terminateAndRelease(reason);
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
    public BoltProtocol protocol() {
        return delegate.protocol();
    }

    @Override
    public void bindTerminationAwareStateLockingExecutor(TerminationAwareStateLockingExecutor executor) {
        delegate.bindTerminationAwareStateLockingExecutor(executor);
    }

    @Override
    public AccessMode mode() {
        return mode;
    }

    @Override
    public DatabaseName databaseName() {
        return this.databaseName;
    }

    @Override
    public String impersonatedUser() {
        return impersonatedUser;
    }
}

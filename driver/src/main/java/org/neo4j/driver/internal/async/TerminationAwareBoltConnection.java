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

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.AuthData;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionState;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.TelemetryApi;
import org.neo4j.driver.internal.bolt.api.TransactionType;

public class TerminationAwareBoltConnection implements BoltConnection {
    private final BoltConnection delegate;
    private final TerminationAwareStateLockingExecutor executor;

    public TerminationAwareBoltConnection(BoltConnection delegate, TerminationAwareStateLockingExecutor executor) {
        this.delegate = Objects.requireNonNull(delegate);
        this.executor = Objects.requireNonNull(executor);
    }

    public CompletionStage<BoltConnection> clearAndReset() {
        var future = new CompletableFuture<BoltConnection>();
        var thisVal = this;
        delegate.clear()
                .thenCompose(BoltConnection::reset)
                .thenCompose(connection -> connection.flush(new ResponseHandler() {
                    @Override
                    public void onError(Throwable throwable) {
                        future.completeExceptionally(throwable);
                    }

                    @Override
                    public void onComplete() {
                        future.complete(thisVal);
                    }
                }))
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        future.completeExceptionally(throwable);
                    }
                });
        return future;
    }

    @Override
    public boolean telemetrySupported() {
        return delegate.telemetrySupported();
    }

    @Override
    public BoltProtocolVersion protocolVersion() {
        return delegate.protocolVersion();
    }

    @Override
    public BoltServerAddress serverAddress() {
        return delegate.serverAddress();
    }

    @Override
    public String serverAgent() {
        return delegate.serverAgent();
    }

    @Override
    public CompletionStage<AuthData> authData() {
        return delegate.authData();
    }

    @Override
    public BoltConnectionState state() {
        return delegate.state();
    }

    @Override
    public CompletionStage<Void> close() {
        return delegate.close();
    }

    @Override
    public CompletionStage<Void> forceClose(String reason) {
        return delegate.forceClose(reason);
    }

    @Override
    public CompletionStage<Void> flush(ResponseHandler handler) {
        return executor.execute(causeOfTermination -> {
            if (causeOfTermination == null) {
                return delegate.flush(handler);
            } else {
                return CompletableFuture.failedStage(causeOfTermination);
            }
        });
    }

    @Override
    public CompletionStage<BoltConnection> telemetry(TelemetryApi telemetryApi) {
        return delegate.telemetry(telemetryApi);
    }

    @Override
    public CompletionStage<BoltConnection> clear() {
        return delegate.clear();
    }

    @Override
    public CompletionStage<BoltConnection> logon(Map<String, Value> authMap) {
        return delegate.logon(authMap);
    }

    @Override
    public CompletionStage<BoltConnection> logoff() {
        return delegate.logoff();
    }

    @Override
    public CompletionStage<BoltConnection> reset() {
        return delegate.reset();
    }

    @Override
    public CompletionStage<BoltConnection> rollback() {
        return delegate.rollback();
    }

    @Override
    public CompletionStage<BoltConnection> commit() {
        return delegate.commit();
    }

    @Override
    public CompletionStage<BoltConnection> discard(long qid, long number) {
        return delegate.discard(qid, number);
    }

    @Override
    public CompletionStage<BoltConnection> pull(long qid, long request) {
        return delegate.pull(qid, request);
    }

    @Override
    public CompletionStage<BoltConnection> run(String query, Map<String, Value> parameters) {
        return delegate.run(query, parameters);
    }

    @Override
    public CompletionStage<BoltConnection> runInAutoCommitTransaction(
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            String query,
            Map<String, Value> parameters,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            NotificationConfig notificationConfig) {
        return delegate.runInAutoCommitTransaction(
                databaseName,
                accessMode,
                impersonatedUser,
                bookmarks,
                query,
                parameters,
                txTimeout,
                txMetadata,
                notificationConfig);
    }

    @Override
    public CompletionStage<BoltConnection> beginTransaction(
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            TransactionType transactionType,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            String txType,
            NotificationConfig notificationConfig) {
        return delegate.beginTransaction(
                databaseName,
                accessMode,
                impersonatedUser,
                bookmarks,
                transactionType,
                txTimeout,
                txMetadata,
                txType,
                notificationConfig);
    }

    @Override
    public CompletionStage<BoltConnection> route(
            DatabaseName databaseName, String impersonatedUser, Set<String> bookmarks) {
        return delegate.route(databaseName, impersonatedUser, bookmarks);
    }
}

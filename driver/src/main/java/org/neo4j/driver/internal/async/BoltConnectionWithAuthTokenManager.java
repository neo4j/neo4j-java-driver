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
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.SecurityRetryableException;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionInfo;
import org.neo4j.driver.internal.bolt.api.BoltConnectionState;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.TelemetryApi;
import org.neo4j.driver.internal.bolt.api.TransactionType;
import org.neo4j.driver.internal.bolt.api.summary.BeginSummary;
import org.neo4j.driver.internal.bolt.api.summary.CommitSummary;
import org.neo4j.driver.internal.bolt.api.summary.DiscardSummary;
import org.neo4j.driver.internal.bolt.api.summary.LogoffSummary;
import org.neo4j.driver.internal.bolt.api.summary.LogonSummary;
import org.neo4j.driver.internal.bolt.api.summary.PullSummary;
import org.neo4j.driver.internal.bolt.api.summary.ResetSummary;
import org.neo4j.driver.internal.bolt.api.summary.RollbackSummary;
import org.neo4j.driver.internal.bolt.api.summary.RouteSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.bolt.api.summary.TelemetrySummary;
import org.neo4j.driver.internal.security.InternalAuthToken;

public class BoltConnectionWithAuthTokenManager implements BoltConnection {
    private final BoltConnection delegate;
    private final AuthTokenManager authTokenManager;

    public BoltConnectionWithAuthTokenManager(BoltConnection delegate, AuthTokenManager authTokenManager) {
        this.delegate = Objects.requireNonNull(delegate);
        this.authTokenManager = Objects.requireNonNull(authTokenManager);
    }

    @Override
    public CompletionStage<BoltConnection> route(Set<String> bookmarks, String databaseName, String impersonatedUser) {
        return delegate.route(bookmarks, databaseName, impersonatedUser).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> beginTransaction(
            Set<String> bookmarks,
            TransactionType transactionType,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            NotificationConfig notificationConfig) {
        return delegate.beginTransaction(bookmarks, transactionType, txTimeout, txMetadata, notificationConfig)
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> runInAutoCommitTransaction(
            String query,
            Map<String, Value> parameters,
            Set<String> bookmarks,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            NotificationConfig notificationConfig) {
        return delegate.runInAutoCommitTransaction(
                        query, parameters, bookmarks, txTimeout, txMetadata, notificationConfig)
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> run(String query, Map<String, Value> parameters) {
        return delegate.run(query, parameters).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> pull(long qid, long request) {
        return delegate.pull(qid, request).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> discard(long qid, long number) {
        return delegate.discard(qid, number).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> commit() {
        return delegate.commit().thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> rollback() {
        return delegate.rollback().thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> reset() {
        return delegate.reset().thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> logoff() {
        return delegate.logoff().thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> logon(Map<String, Value> authMap) {
        return delegate.logon(authMap).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> telemetry(TelemetryApi telemetryApi) {
        return delegate.telemetry(telemetryApi).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<Void> flush(ResponseHandler handler) {
        return delegate.flush(new ResponseHandler() {
            @Override
            public void onError(Throwable throwable) {
                handler.onError(mapSecurityError(throwable));
            }

            @Override
            public void onBeginSummary(BeginSummary summary) {
                handler.onBeginSummary(summary);
            }

            @Override
            public void onRunSummary(RunSummary summary) {
                handler.onRunSummary(summary);
            }

            @Override
            public void onRecord(Value[] fields) {
                handler.onRecord(fields);
            }

            @Override
            public void onPullSummary(PullSummary summary) {
                handler.onPullSummary(summary);
            }

            @Override
            public void onDiscardSummary(DiscardSummary summary) {
                handler.onDiscardSummary(summary);
            }

            @Override
            public void onCommitSummary(CommitSummary summary) {
                handler.onCommitSummary(summary);
            }

            @Override
            public void onRollbackSummary(RollbackSummary summary) {
                handler.onRollbackSummary(summary);
            }

            @Override
            public void onResetSummary(ResetSummary summary) {
                handler.onResetSummary(summary);
            }

            @Override
            public void onRouteSummary(RouteSummary summary) {
                handler.onRouteSummary(summary);
            }

            @Override
            public void onLogoffSummary(LogoffSummary summary) {
                handler.onLogoffSummary(summary);
            }

            @Override
            public void onLogonSummary(LogonSummary summary) {
                handler.onLogonSummary(summary);
            }

            @Override
            public void onTelemetrySummary(TelemetrySummary summary) {
                handler.onTelemetrySummary(summary);
            }

            @Override
            public void onComplete() {
                handler.onComplete();
            }
        });
    }

    @Override
    public CompletionStage<Void> close() {
        return delegate.close();
    }

    @Override
    public BoltConnectionState state() {
        return delegate.state();
    }

    @Override
    public BoltConnectionInfo connectionInfo() {
        return delegate.connectionInfo();
    }

    @Override
    public Map<String, Value> authMap() {
        return delegate.authMap();
    }

    @Override
    public CompletionStage<Long> latestAuthMillis() {
        return delegate.latestAuthMillis();
    }

    @Override
    public void setDatabase(String database) {
        delegate.setDatabase(database);
    }

    @Override
    public void setAccessMode(AccessMode mode) {
        delegate.setAccessMode(mode);
    }

    @Override
    public void setImpersonatedUser(String impersonatedUser) {
        delegate.setImpersonatedUser(impersonatedUser);
    }

    private Throwable mapSecurityError(Throwable throwable) {
        if (throwable instanceof SecurityException securityException) {
            if (authTokenManager.handleSecurityException(
                    new InternalAuthToken(delegate.authMap()), securityException)) {
                throwable = new SecurityRetryableException(securityException);
            }
        }
        return throwable;
    }
}
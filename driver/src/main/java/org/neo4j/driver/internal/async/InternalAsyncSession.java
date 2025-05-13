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

import static java.util.Collections.emptyMap;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.TelemetryApi;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.AsyncTransactionCallback;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.GqlStatusError;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;
import org.neo4j.driver.internal.util.Futures;

public class InternalAsyncSession extends AsyncAbstractQueryRunner implements AsyncSession {
    private final NetworkSession session;

    public InternalAsyncSession(NetworkSession session) {
        this.session = session;
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(Query query) {
        return runAsync(query, TransactionConfig.empty());
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(String query, TransactionConfig config) {
        return runAsync(query, emptyMap(), config);
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(
            String query, Map<String, Object> parameters, TransactionConfig config) {
        return runAsync(new Query(query, parameters), config);
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(Query query, TransactionConfig config) {
        return session.runAsync(query, config);
    }

    @Override
    public CompletionStage<Void> closeAsync() {
        return session.closeAsync();
    }

    @Override
    public CompletionStage<AsyncTransaction> beginTransactionAsync() {
        return beginTransactionAsync(TransactionConfig.empty());
    }

    @Override
    public CompletionStage<AsyncTransaction> beginTransactionAsync(TransactionConfig config) {
        return session.beginTransactionAsync(config, new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION))
                .thenApply(InternalAsyncTransaction::new);
    }

    @Override
    public <T> CompletionStage<T> executeReadAsync(
            AsyncTransactionCallback<CompletionStage<T>> callback, TransactionConfig config) {
        return transactionAsync(AccessMode.READ, callback, config);
    }

    @Override
    public <T> CompletionStage<T> executeWriteAsync(
            AsyncTransactionCallback<CompletionStage<T>> callback, TransactionConfig config) {
        return transactionAsync(AccessMode.WRITE, callback, config);
    }

    @Override
    public Set<Bookmark> lastBookmarks() {
        return new HashSet<>(session.lastBookmarks());
    }

    private <T> CompletionStage<T> transactionAsync(
            AccessMode mode, AsyncTransactionCallback<CompletionStage<T>> work, TransactionConfig config) {
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.MANAGED_TRANSACTION);
        return session.retryLogic().retryAsync(() -> {
            var resultFuture = new CompletableFuture<T>();
            var txFuture = session.beginTransactionAsync(mode, config, apiTelemetryWork);

            txFuture.whenComplete((tx, completionError) -> {
                var error = Futures.completionExceptionCause(completionError);
                if (error != null) {
                    resultFuture.completeExceptionally(error);
                } else {
                    executeWork(resultFuture, tx, work);
                }
            });

            return resultFuture;
        });
    }

    private <T> void executeWork(
            CompletableFuture<T> resultFuture,
            UnmanagedTransaction tx,
            AsyncTransactionCallback<CompletionStage<T>> work) {
        var workFuture = safeExecuteWork(tx, work);
        workFuture.whenComplete((result, completionError) -> {
            var error = Futures.completionExceptionCause(completionError);
            if (error != null) {
                closeTxAfterFailedTransactionWork(tx, resultFuture, error);
            } else if (result instanceof ResultCursor) {
                var message = String.format(
                        "%s is not a valid return value, it should be consumed before producing a return value",
                        ResultCursor.class.getName());
                error = new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(message),
                        "N/A",
                        message,
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        null);
                closeTxAfterFailedTransactionWork(tx, resultFuture, error);
            } else {
                closeTxAfterSucceededTransactionWork(tx, resultFuture, result);
            }
        });
    }

    private <T> CompletionStage<T> safeExecuteWork(
            UnmanagedTransaction tx, AsyncTransactionCallback<CompletionStage<T>> work) {
        // given work might fail in both async and sync way
        // async failure will result in a failed future being returned
        // sync failure will result in an exception being thrown
        try {
            var result = work.execute(new DelegatingAsyncTransactionContext(new InternalAsyncTransaction(tx)));

            // protect from given transaction function returning null
            return result == null ? completedWithNull() : result;
        } catch (Throwable workError) {
            // work threw an exception, wrap it in a future and proceed
            return CompletableFuture.failedFuture(workError);
        }
    }

    private <T> void closeTxAfterFailedTransactionWork(
            UnmanagedTransaction tx, CompletableFuture<T> resultFuture, Throwable error) {
        tx.closeAsync().whenComplete((ignored, rollbackError) -> {
            if (rollbackError != null) {
                error.addSuppressed(rollbackError);
            }
            resultFuture.completeExceptionally(error);
        });
    }

    private <T> void closeTxAfterSucceededTransactionWork(
            UnmanagedTransaction tx, CompletableFuture<T> resultFuture, T result) {
        tx.closeAsync(true).whenComplete((ignored, completionError) -> {
            var commitError = Futures.completionExceptionCause(completionError);
            if (commitError != null) {
                resultFuture.completeExceptionally(commitError);
            } else {
                resultFuture.complete(result);
            }
        });
    }
}

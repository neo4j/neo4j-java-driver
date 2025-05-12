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
package org.neo4j.driver.internal.reactive;

import static org.neo4j.driver.internal.reactive.RxUtils.createEmptyPublisher;
import static org.neo4j.driver.internal.reactive.RxUtils.createSingleItemPublisher;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.neo4j.bolt.connection.TelemetryApi;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransactionNestingException;
import org.neo4j.driver.internal.GqlStatusError;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactivestreams.ReactiveResult;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public abstract class AbstractReactiveSession<S> {
    protected final NetworkSession session;

    public AbstractReactiveSession(NetworkSession session) {
        // RxSession accept a network session as input.
        // The network session different from async session that it provides ways to both run for Rx and Async
        // Note: Blocking result could just build on top of async result. However, Rx result cannot just build on top of
        // async result.
        this.session = session;
    }

    protected abstract S createTransaction(UnmanagedTransaction unmanagedTransaction);

    protected abstract Publisher<Void> closeTransaction(S transaction, boolean commit);

    Publisher<S> doBeginTransaction(TransactionConfig config, ApiTelemetryWork apiTelemetryWork) {
        return doBeginTransaction(config, null, apiTelemetryWork);
    }

    @SuppressWarnings("DuplicatedCode")
    protected Publisher<S> doBeginTransaction(
            TransactionConfig config, String txType, ApiTelemetryWork apiTelemetryWork) {
        return createSingleItemPublisher(
                () -> {
                    var txFuture = new CompletableFuture<S>();
                    session.beginTransactionAsync(config, txType, apiTelemetryWork)
                            .whenComplete((tx, completionError) -> {
                                if (tx != null) {
                                    txFuture.complete(createTransaction(tx));
                                } else {
                                    releaseConnectionBeforeReturning(txFuture, completionError);
                                }
                            });
                    return txFuture;
                },
                () -> new IllegalStateException(
                        "Unexpected condition, begin transaction call has completed successfully with transaction being null"),
                tx -> Mono.fromDirect(closeTransaction(tx, false)).subscribe());
    }

    @SuppressWarnings("DuplicatedCode")
    private Publisher<S> beginTransaction(
            AccessMode mode, TransactionConfig config, ApiTelemetryWork apiTelemetryWork) {
        return createSingleItemPublisher(
                () -> {
                    var txFuture = new CompletableFuture<S>();
                    session.beginTransactionAsync(mode, config, apiTelemetryWork)
                            .whenComplete((tx, completionError) -> {
                                if (tx != null) {
                                    txFuture.complete(createTransaction(tx));
                                } else {
                                    releaseConnectionBeforeReturning(txFuture, completionError);
                                }
                            });
                    return txFuture;
                },
                () -> new IllegalStateException(
                        "Unexpected condition, begin transaction call has completed successfully with transaction being null"),
                tx -> Mono.fromDirect(closeTransaction(tx, false)).subscribe());
    }

    protected <T> Publisher<T> runTransaction(
            AccessMode mode, Function<S, ? extends Publisher<T>> work, TransactionConfig config) {
        work = work.andThen(publisher -> Flux.from(publisher).handle((value, sink) -> {
            if (value instanceof ReactiveResult) {
                var message = String.format(
                        "%s is not a valid return value, it should be consumed before producing a return value",
                        ReactiveResult.class.getName());
                sink.error(new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(message),
                        "N/A",
                        message,
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        null));
                return;
            } else if (value instanceof org.neo4j.driver.reactive.ReactiveResult) {
                var message = String.format(
                        "%s is not a valid return value, it should be consumed before producing a return value",
                        org.neo4j.driver.reactive.ReactiveResult.class.getName());
                sink.error(new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(message),
                        "N/A",
                        message,
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        null));
                return;
            }
            sink.next(value);
        }));

        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.MANAGED_TRANSACTION);
        var repeatableWork = Flux.usingWhen(
                beginTransaction(mode, config, apiTelemetryWork),
                work,
                tx -> closeTransaction(tx, true),
                (tx, error) -> closeTransaction(tx, false),
                (tx) -> closeTransaction(tx, false));
        return session.retryLogic().retryRx(repeatableWork);
    }

    private <T> void releaseConnectionBeforeReturning(CompletableFuture<T> returnFuture, Throwable completionError) {
        // We failed to create a result cursor, so we cannot rely on result cursor to clean-up resources.
        // Therefore, we will first release the connection that might have been created in the session and then notify
        // the error.
        // The logic here shall be the same as `SessionPullResponseHandler#afterFailure`.
        // The reason we need to release connection in session is that we made `rxSession.close()` optional;
        // Otherwise, session.close shall handle everything for us.
        var error = Futures.completionExceptionCause(completionError);
        if (error instanceof TransactionNestingException) {
            returnFuture.completeExceptionally(error);
        } else {
            session.releaseConnectionAsync()
                    .whenComplete((ignored, closeError) ->
                            returnFuture.completeExceptionally(Futures.combineErrors(error, closeError)));
        }
    }

    public Set<Bookmark> lastBookmarks() {
        return session.lastBookmarks();
    }

    protected <T> Publisher<T> run(Query query, TransactionConfig config, Function<RxResultCursor, T> cursorToResult) {
        var cursorPublishFuture = new CompletableFuture<RxResultCursor>();
        var cursorReference = new AtomicReference<RxResultCursor>();

        return createSingleItemPublisher(
                        () -> runAsStage(query, config, cursorPublishFuture)
                                .thenApply(cursor -> {
                                    cursorReference.set(cursor);
                                    return cursor;
                                })
                                .thenApply(cursorToResult),
                        () -> new IllegalStateException(
                                "Unexpected condition, run call has completed successfully with result being null"),
                        value -> {
                            if (value != null) {
                                cursorReference.get().rollback().whenComplete((unused, throwable) -> {
                                    if (throwable != null) {
                                        cursorPublishFuture.completeExceptionally(throwable);
                                    } else {
                                        cursorPublishFuture.complete(null);
                                    }
                                });
                            }
                        })
                .doOnNext(value -> cursorPublishFuture.complete(cursorReference.get()))
                .doOnError(cursorPublishFuture::completeExceptionally);
    }

    private CompletionStage<RxResultCursor> runAsStage(
            Query query, TransactionConfig config, CompletionStage<RxResultCursor> finalStage) {
        CompletionStage<RxResultCursor> cursorStage;
        try {
            cursorStage = session.runRx(query, config, finalStage);
        } catch (Throwable t) {
            cursorStage = CompletableFuture.failedFuture(t);
        }

        return cursorStage
                .handle((cursor, throwable) -> {
                    if (throwable != null) {
                        return this.<RxResultCursor>releaseConnectionAndRethrow(throwable);
                    } else {
                        var runError = cursor.getRunError();
                        if (runError != null) {
                            return this.<RxResultCursor>releaseConnectionAndRethrow(runError);
                        } else {
                            return CompletableFuture.completedFuture(cursor);
                        }
                    }
                })
                .thenCompose(Function.identity());
    }

    private <T> CompletionStage<T> releaseConnectionAndRethrow(Throwable throwable) {
        return session.releaseConnectionAsync().handle((ignored, releaseThrowable) -> {
            if (releaseThrowable != null) {
                throw Futures.combineErrors(throwable, releaseThrowable);
            } else {
                if (throwable instanceof RuntimeException e) {
                    throw e;
                } else {
                    throw new CompletionException(throwable);
                }
            }
        });
    }

    protected <T> Publisher<T> doClose() {
        return createEmptyPublisher(session::closeAsync);
    }
}

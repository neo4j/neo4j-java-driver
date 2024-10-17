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
package org.neo4j.driver.internal.cursor;

import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransactionNestingException;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.GqlStatusError;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.summary.DiscardSummary;
import org.neo4j.driver.internal.bolt.api.summary.PullSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.summary.ResultSummary;

public class RxResultCursorImpl extends AbstractRecordStateResponseHandler implements RxResultCursor, ResponseHandler {
    public static final MetadataExtractor METADATA_EXTRACTOR = new MetadataExtractor("t_last");
    private final BoltConnection boltConnection;
    private final Query query;
    private final RunSummary runSummary;
    private final Throwable runError;
    private final Consumer<DatabaseBookmark> bookmarkConsumer;
    private final Consumer<Throwable> throwableConsumer;
    private final Supplier<Throwable> termSupplier;
    private final boolean closeOnSummary;
    private final CompletableFuture<ResultSummary> summaryFuture = new CompletableFuture<>();
    private final boolean legacyNotifications;
    private final CompletableFuture<Void> consumedFuture = new CompletableFuture<>();

    private State state;
    private long outstandingDemand;
    private BiConsumer<Record, Throwable> recordConsumer;
    private boolean discardPending;
    private boolean runErrorExposed;
    private boolean summaryExposed;

    private enum State {
        READY,
        STREAMING,
        DISCARDING,
        FAILED,
        SUCCEDED
    }

    public RxResultCursorImpl(
            BoltConnection boltConnection,
            Query query,
            RunSummary runSummary,
            Throwable runError,
            Supplier<Throwable> throwableSupplier,
            Consumer<DatabaseBookmark> bookmarkConsumer,
            Consumer<Throwable> throwableConsumer,
            boolean closeOnSummary,
            Supplier<Throwable> termSupplier) {
        this.boltConnection = boltConnection;
        this.legacyNotifications = new BoltProtocolVersion(5, 5).compareTo(boltConnection.protocolVersion()) > 0;
        this.query = query;
        if (runSummary != null) {
            this.runSummary = runSummary;
            this.state = State.READY;
        } else {
            this.runSummary = new RunSummary() {
                @Override
                public long queryId() {
                    return -1;
                }

                @Override
                public List<String> keys() {
                    return List.of();
                }

                @Override
                public long resultAvailableAfter() {
                    return -1;
                }
            };
            this.state = State.FAILED;
            this.summaryFuture.completeExceptionally(runError);
        }
        this.runError = runError;
        this.bookmarkConsumer = bookmarkConsumer;
        this.closeOnSummary = closeOnSummary;
        this.throwableConsumer = throwableConsumer;
        this.termSupplier = termSupplier;
    }

    @Override
    public void onError(Throwable throwable) {
        Runnable runnable;

        synchronized (this) {
            if (state == State.FAILED) {
                return;
            }
            state = State.FAILED;
            var summary = METADATA_EXTRACTOR.extractSummary(
                    query,
                    boltConnection,
                    runSummary.resultAvailableAfter(),
                    Collections.emptyMap(),
                    legacyNotifications,
                    generateGqlStatusObject(runSummary.keys()));

            if (recordConsumer != null) {
                // records subscriber present
                runnable = () -> {
                    var closeStage = closeOnSummary ? boltConnection.close() : CompletableFuture.completedStage(null);
                    closeStage.whenComplete((ignored, closeThrowable) -> {
                        var error = Futures.completionExceptionCause(closeThrowable);
                        if (error != null) {
                            throwable.addSuppressed(error);
                        }
                        throwableConsumer.accept(throwable);
                        recordConsumer.accept(null, throwable);
                        summaryFuture.complete(summary);
                        dispose();
                    });
                };
            } else {
                runnable = () -> {
                    var closeStage = closeOnSummary ? boltConnection.close() : CompletableFuture.completedStage(null);
                    closeStage.whenComplete((ignored, closeThrowable) -> {
                        var error = Futures.completionExceptionCause(closeThrowable);
                        if (error != null) {
                            throwable.addSuppressed(error);
                        }
                        throwableConsumer.accept(throwable);
                        summaryFuture.completeExceptionally(throwable);
                        dispose();
                    });
                };
            }
        }

        runnable.run();
    }

    @Override
    public void onIgnored() {
        var throwable = termSupplier.get();
        if (throwable == null) {
            var message = "A message has been ignored during result streaming.";
            throwable = new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(message),
                    "N/A",
                    message,
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    null);
        }
        onError(throwable);
    }

    @Override
    public void onRecord(Value[] fields) {
        var record = new InternalRecord(runSummary.keys(), fields);
        synchronized (this) {
            updateRecordState(RecordState.HAD_RECORD);
            decrementDemand();
        }
        recordConsumer.accept(record, null);
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public void onPullSummary(PullSummary summary) {
        var term = termSupplier.get();
        if (term == null) {
            if (summary.hasMore()) {
                synchronized (this) {
                    if (discardPending) {
                        discardPending = false;
                        state = State.DISCARDING;
                        boltConnection
                                .discard(runSummary.queryId(), -1)
                                .thenCompose(conn -> conn.flush(this))
                                .whenComplete((ignored, throwable) -> {
                                    var error = Futures.completionExceptionCause(throwable);
                                    if (error != null) {
                                        onError(error);
                                    }
                                });
                    } else {
                        var demand = getDemand();
                        if (demand != 0) {
                            state = State.STREAMING;
                            boltConnection
                                    .pull(runSummary.queryId(), demand > 0 ? demand : -1)
                                    .thenCompose(conn -> conn.flush(this))
                                    .whenComplete((ignored, throwable) -> {
                                        var error = Futures.completionExceptionCause(throwable);
                                        if (error != null) {
                                            onError(error);
                                        }
                                    });
                        } else {
                            state = State.READY;
                        }
                    }
                }
            } else {
                var resultSummaryRef = new AtomicReference<ResultSummary>();
                CompletableFuture<ResultSummary> resultSummaryFuture;
                Throwable summaryError = null;
                synchronized (this) {
                    resultSummaryFuture = summaryFuture;
                    try {
                        resultSummaryRef.set(METADATA_EXTRACTOR.extractSummary(
                                query,
                                boltConnection,
                                runSummary.resultAvailableAfter(),
                                summary.metadata(),
                                legacyNotifications,
                                generateGqlStatusObject(runSummary.keys())));
                        state = State.SUCCEDED;
                    } catch (Throwable throwable) {
                        summaryError = throwable;
                    }
                }

                if (summaryError == null) {
                    var metadata = summary.metadata();
                    var bookmarkValue = metadata.get("bookmark");
                    if (bookmarkValue != null
                            && !bookmarkValue.isNull()
                            && bookmarkValue.hasType(TYPE_SYSTEM.STRING())) {
                        var bookmarkStr = bookmarkValue.asString();
                        if (!bookmarkStr.isEmpty()) {
                            var databaseBookmark = new DatabaseBookmark(null, Bookmark.from(bookmarkStr));
                            bookmarkConsumer.accept(databaseBookmark);
                        }
                    }

                    recordConsumer.accept(null, null);

                    var closeStage = closeOnSummary ? boltConnection.close() : CompletableFuture.completedStage(null);
                    closeStage.whenComplete((ignored, throwable) -> {
                        var error = Futures.completionExceptionCause(throwable);
                        if (error != null) {
                            resultSummaryFuture.completeExceptionally(error);
                        } else {
                            resultSummaryFuture.complete(resultSummaryRef.get());
                        }
                    });
                    dispose();
                } else {
                    onError(summaryError);
                }
            }
        } else {
            onError(term);
        }
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public void onDiscardSummary(DiscardSummary summary) {
        var resultSummaryRef = new AtomicReference<ResultSummary>();
        CompletableFuture<ResultSummary> resultSummaryFuture;
        Throwable summaryError = null;
        synchronized (this) {
            resultSummaryFuture = summaryFuture;
            try {
                resultSummaryRef.set(METADATA_EXTRACTOR.extractSummary(
                        query,
                        boltConnection,
                        runSummary.resultAvailableAfter(),
                        summary.metadata(),
                        legacyNotifications,
                        generateGqlStatusObject(runSummary.keys())));
                state = State.SUCCEDED;
            } catch (Throwable throwable) {
                summaryError = throwable;
            }
        }

        if (summaryError == null) {
            var metadata = summary.metadata();
            var bookmarkValue = metadata.get("bookmark");
            if (bookmarkValue != null && !bookmarkValue.isNull() && bookmarkValue.hasType(TYPE_SYSTEM.STRING())) {
                var bookmarkStr = bookmarkValue.asString();
                if (!bookmarkStr.isEmpty()) {
                    var databaseBookmark = new DatabaseBookmark(null, Bookmark.from(bookmarkStr));
                    bookmarkConsumer.accept(databaseBookmark);
                }
            }

            var closeStage = closeOnSummary ? boltConnection.close() : CompletableFuture.completedStage(null);
            closeStage.whenComplete((ignored, throwable) -> {
                var error = Futures.completionExceptionCause(throwable);
                if (error != null) {
                    resultSummaryFuture.completeExceptionally(error);
                } else {
                    resultSummaryFuture.complete(resultSummaryRef.get());
                }
            });
            dispose();
        } else {
            onError(summaryError);
        }
    }

    @Override
    public synchronized CompletionStage<Throwable> discardAllFailureAsync() {
        var summaryExposed = this.summaryExposed;
        return summaryAsync()
                .thenApply(ignored -> (Throwable) null)
                .exceptionally(throwable -> runErrorExposed || summaryExposed ? null : throwable);
    }

    @Override
    public CompletionStage<Throwable> pullAllFailureAsync() {
        synchronized (this) {
            if (recordConsumer != null && !isDone()) {
                return CompletableFuture.completedFuture(
                        new TransactionNestingException(
                                "You cannot run another query or begin a new transaction in the same session before you've fully consumed the previous run result."));
            }
        }
        return discardAllFailureAsync();
    }

    @Override
    public CompletionStage<Void> consumed() {
        return consumedFuture;
    }

    @Override
    public List<String> keys() {
        return runSummary.keys();
    }

    @Override
    public void installRecordConsumer(BiConsumer<Record, Throwable> recordConsumer) {
        Objects.requireNonNull(recordConsumer);
        Runnable runnable = () -> {};
        synchronized (this) {
            if (this.recordConsumer == null) {
                this.recordConsumer = recordConsumer;
                if (runError != null) {
                    runErrorExposed = true;
                    runnable = () -> recordConsumer.accept(null, runError);
                }
            }
        }
        runnable.run();
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public CompletionStage<ResultSummary> summaryAsync() {
        synchronized (this) {
            if (summaryExposed) {
                return summaryFuture;
            }
            summaryExposed = true;
            switch (state) {
                case SUCCEDED, FAILED, DISCARDING -> {}
                case READY -> {
                    var term = termSupplier.get();
                    if (term == null) {
                        state = State.DISCARDING;
                        boltConnection
                                .discard(runSummary.queryId(), -1)
                                .thenCompose(conn -> conn.flush(this))
                                .whenComplete((ignored, throwable) -> {
                                    var error = Futures.completionExceptionCause(throwable);
                                    if (error != null) {
                                        onError(error);
                                    }
                                });
                    } else {
                        onError(term);
                    }
                }
                case STREAMING -> discardPending = true;
            }
        }
        var future = new CompletableFuture<ResultSummary>();
        summaryFuture.whenComplete((summary, throwable) -> {
            throwable = Futures.completionExceptionCause(throwable);
            if (throwable != null) {
                consumedFuture.completeExceptionally(throwable);
                future.completeExceptionally(throwable);
            } else {
                consumedFuture.complete(null);
                future.complete(summary);
            }
        });
        return future;
    }

    @Override
    public synchronized boolean isDone() {
        return switch (state) {
            case DISCARDING, STREAMING, READY -> false;
            case FAILED -> runError == null || runErrorExposed;
            case SUCCEDED -> true;
        };
    }

    @Override
    public Throwable getRunError() {
        runErrorExposed = true;
        return runError;
    }

    @Override
    public CompletionStage<Void> rollback() {
        synchronized (this) {
            state = State.SUCCEDED;
        }
        summaryFuture.complete(null);
        var future = new CompletableFuture<Void>();
        boltConnection
                .reset()
                .thenCompose(conn -> conn.flush(new ResponseHandler() {
                    @Override
                    public void onError(Throwable throwable) {
                        future.completeExceptionally(throwable);
                    }

                    @Override
                    public void onComplete() {
                        future.complete(null);
                    }
                }))
                .whenComplete((ignored, throwable) -> {
                    if (throwable != null) {
                        future.completeExceptionally(throwable);
                    }
                });
        return future.thenCompose(ignored -> boltConnection.close()).exceptionally(throwable -> null);
    }

    private synchronized void dispose() {
        recordConsumer = null;
    }

    private synchronized long appendDemand(long n) {
        if (n == Long.MAX_VALUE) {
            outstandingDemand = -1;
        } else {
            try {
                outstandingDemand = Math.addExact(outstandingDemand, n);
            } catch (ArithmeticException ex) {
                outstandingDemand = -1;
            }
        }
        return outstandingDemand;
    }

    private synchronized long getDemand() {
        return outstandingDemand;
    }

    private synchronized void decrementDemand() {
        if (outstandingDemand > 0) {
            outstandingDemand--;
        }
    }

    @Override
    public void request(long n) {
        if (n <= 0) {
            throw new IllegalArgumentException("n must not be 0 or negative");
        }
        synchronized (this) {
            updateRecordState(RecordState.NO_RECORD);
            switch (state) {
                case READY -> {
                    var term = termSupplier.get();
                    if (term == null) {
                        var request = appendDemand(n);
                        state = State.STREAMING;
                        boltConnection
                                .pull(runSummary.queryId(), request)
                                .thenCompose(conn -> conn.flush(this))
                                .whenComplete((ignored, throwable) -> {
                                    var error = Futures.completionExceptionCause(throwable);
                                    if (error != null) {
                                        onError(error);
                                    }
                                });
                    } else {
                        onError(term);
                    }
                }
                case STREAMING -> appendDemand(n);
                case FAILED -> {
                    if (recordConsumer != null && !runErrorExposed) {
                        recordConsumer.accept(null, getRunError());
                    }
                }
                case DISCARDING, SUCCEDED -> {}
            }
        }
    }

    @Override
    public void cancel() {
        synchronized (this) {
            switch (state) {
                case READY -> {
                    state = State.DISCARDING;
                    boltConnection
                            .discard(runSummary.queryId(), -1)
                            .thenCompose(conn -> conn.flush(this))
                            .whenComplete((ignored, throwable) -> {
                                if (throwable != null) {
                                    var error = Futures.completionExceptionCause(throwable);
                                    if (error != null) {
                                        onError(error);
                                    }
                                }
                            });
                }
                case STREAMING -> discardPending = true;
                case DISCARDING, FAILED, SUCCEDED -> {}
            }
        }
    }
}

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
import static org.neo4j.driver.internal.util.ErrorUtil.newResultConsumedError;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.message.Messages;
import org.neo4j.bolt.connection.summary.RunSummary;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.TransactionNestingException;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.GqlStatusError;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnection;
import org.neo4j.driver.internal.adaptedbolt.DriverResponseHandler;
import org.neo4j.driver.internal.adaptedbolt.summary.DiscardSummary;
import org.neo4j.driver.internal.adaptedbolt.summary.PullSummary;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.summary.GqlStatusObject;
import org.neo4j.driver.summary.ResultSummary;

public class RxResultCursorImpl extends AbstractRecordStateResponseHandler
        implements RxResultCursor, DriverResponseHandler {
    private static final MetadataExtractor METADATA_EXTRACTOR = new MetadataExtractor("t_last");
    private static final ClientException IGNORED_ERROR = new ClientException(
            GqlStatusError.UNKNOWN.getStatus(),
            GqlStatusError.UNKNOWN.getStatusDescription("A message has been ignored during result streaming."),
            "N/A",
            "A message has been ignored during result streaming.",
            GqlStatusError.DIAGNOSTIC_RECORD,
            null);
    private static final Runnable NOOP_RUNNABLE = () -> {};
    private static final BiConsumer<Record, Throwable> NOOP_CONSUMER = (record, throwable) -> {};
    private static final RunSummary EMPTY_RUN_SUMMARY = new RunSummary() {
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

        @Override
        public Optional<String> databaseName() {
            return Optional.empty();
        }
    };

    @SuppressWarnings("deprecation")
    private final Logger log;

    private final DriverBoltConnection boltConnection;
    private final Query query;
    private final RunSummary runSummary;
    private final Throwable runError;
    private final Consumer<DatabaseBookmark> bookmarkConsumer;
    private final boolean closeOnSummary;
    private final CompletableFuture<ResultSummary> summaryFuture = new CompletableFuture<>();
    private final CompletableFuture<Void> consumedFuture = new CompletableFuture<>();
    private final boolean legacyNotifications;
    private State state = State.READY;
    private boolean discardPending;
    private boolean runErrorExposed;
    private boolean summaryExposed;
    // subscription
    private BiConsumer<Record, Throwable> recordConsumer;
    private long outstandingDemand;
    private PullSummary pullSummary;
    private DiscardSummary discardSummary;
    private Throwable error;

    private enum State {
        READY,
        STREAMING,
        DISCARDING,
        FAILED,
        SUCCEEDED
    }

    public RxResultCursorImpl(
            DriverBoltConnection boltConnection,
            Query query,
            RunSummary runSummary,
            Throwable runError,
            Consumer<DatabaseBookmark> bookmarkConsumer,
            boolean closeOnSummary,
            @SuppressWarnings("deprecation") Logging logging) {
        this.boltConnection = Objects.requireNonNull(boltConnection);
        this.legacyNotifications = new BoltProtocolVersion(5, 5).compareTo(boltConnection.protocolVersion()) > 0;
        this.query = query;
        this.runSummary = runError == null ? runSummary : EMPTY_RUN_SUMMARY;
        this.runError = runError;
        this.bookmarkConsumer = bookmarkConsumer;
        this.closeOnSummary = closeOnSummary;
        this.log = logging.getLog(getClass());
        log.trace("[%d] New instance (runError=%s)", hashCode(), throwableName(runError));
    }

    @Override
    public synchronized Throwable getRunError() {
        log.trace("[%d] Run error explicitly retrieved (value=%s)", hashCode(), throwableName(runError));
        runErrorExposed = true;
        return runError;
    }

    @Override
    public List<String> keys() {
        return runSummary.keys();
    }

    @Override
    public CompletionStage<Void> consumed() {
        return consumedFuture;
    }

    @Override
    public boolean isDone() {
        return summaryFuture.isDone();
    }

    @Override
    public void installRecordConsumer(BiConsumer<Record, Throwable> recordConsumer) {
        Objects.requireNonNull(recordConsumer);
        if (summaryExposed) {
            throw newResultConsumedError();
        }
        var runnable = NOOP_RUNNABLE;
        synchronized (this) {
            if (this.recordConsumer == null) {
                this.recordConsumer = safeRecordConsumer(recordConsumer);
                log.trace("[%d] Record consumer installed", hashCode());
                if (runError != null) {
                    handleError(runError);
                    runnable = this::onComplete;
                }
            } else {
                log.warn("[%d] Only one record consumer is supported, this request will be ignored", hashCode());
            }
        }
        runnable.run();
    }

    @Override
    public void request(long n) {
        if (n > 0) {
            var runnable = NOOP_RUNNABLE;
            synchronized (this) {
                updateRecordState(RecordState.NO_RECORD);
                log.trace("[%d] %d records requested in %s state", hashCode(), n, state);
                switch (state) {
                    case READY -> {
                        var request = appendDemand(n);
                        state = State.STREAMING;
                        runnable = () -> boltConnection
                                .writeAndFlush(this, Messages.pull(runSummary.queryId(), request))
                                .whenComplete((ignored, throwable) -> {
                                    throwable = Futures.completionExceptionCause(throwable);
                                    if (throwable != null) {
                                        handleError(throwable);
                                        onComplete();
                                    }
                                });
                    }
                    case STREAMING -> appendDemand(n);
                    case FAILED, DISCARDING, SUCCEEDED -> {}
                }
            }
            runnable.run();
        } else {
            log.warn("[%d] %d records requested, negative amounts are ignored", hashCode(), n);
        }
    }

    @Override
    public void cancel() {
        var runnable = NOOP_RUNNABLE;
        synchronized (this) {
            log.trace("[%d] Cancellation requested in %s state", hashCode(), state);
            switch (state) {
                case READY -> runnable = setupDiscardRunnable();
                case STREAMING -> discardPending = true;
                case DISCARDING, FAILED, SUCCEEDED -> {}
            }
        }
        runnable.run();
    }

    @Override
    public CompletionStage<ResultSummary> summaryAsync() {
        var runnable = NOOP_RUNNABLE;
        synchronized (this) {
            log.trace("[%d] Summary requested in %s state", hashCode(), state);
            if (summaryExposed) {
                return summaryFuture;
            }
            summaryExposed = true;
            switch (state) {
                case SUCCEEDED, FAILED, DISCARDING -> {}
                case READY -> {
                    if (runError != null && recordConsumer == null) {
                        handleError(runError);
                        runnable = this::onComplete;
                    } else {
                        runnable = setupDiscardRunnable();
                    }
                }
                case STREAMING -> discardPending = true;
            }
        }
        runnable.run();
        return summaryFuture;
    }

    @Override
    public CompletionStage<Void> rollback() {
        synchronized (this) {
            log.trace("[%d] Rolling back unpublished result %s state", hashCode(), state);
            switch (state) {
                case READY -> state = State.SUCCEEDED;
                case STREAMING, DISCARDING -> {
                    return summaryFuture.thenApply(ignored -> null);
                }
                case FAILED, SUCCEEDED -> {
                    return CompletableFuture.completedFuture(null);
                }
            }
        }
        var resetFuture = new CompletableFuture<Void>();
        boltConnection
                .writeAndFlush(
                        new DriverResponseHandler() {
                            Throwable throwable = null;

                            @Override
                            public void onError(Throwable throwable) {
                                this.throwable = Futures.completionExceptionCause(throwable);
                            }

                            @Override
                            public void onComplete() {
                                if (throwable != null) {
                                    resetFuture.completeExceptionally(throwable);
                                } else {
                                    resetFuture.complete(null);
                                }
                            }
                        },
                        Messages.reset())
                .whenComplete((ignored, throwable) -> {
                    throwable = Futures.completionExceptionCause(throwable);
                    if (throwable != null) {
                        resetFuture.completeExceptionally(throwable);
                    }
                });

        return resetFuture
                .thenCompose(ignored -> boltConnection.close())
                .whenComplete((ignored, throwable) -> completeSummaryFuture(null, null))
                .exceptionally(throwable -> null);
    }

    @Override
    public void onComplete() {
        Runnable runnable;
        synchronized (this) {
            log.trace("[%d] onComplete", hashCode());

            if (error != null) {
                runnable = setupCompletionRunnableWithError(error);
            } else if (pullSummary != null) {
                runnable = setupCompletionRunnableWithPullSummary();
            } else if (discardSummary != null) {
                runnable = setupCompletionRunnableWithSummaryMetadata(discardSummary.metadata());
            } else {
                runnable = () -> log.trace("[%d] onComplete resulted in no action", hashCode());
            }
        }
        runnable.run();
    }

    @Override
    public synchronized void onError(Throwable throwable) {
        log.trace("[%d] onError", hashCode());
        handleError(throwable);
    }

    @Override
    public synchronized void onIgnored() {
        log.trace("[%d] onIgnored", hashCode());
        handleError(IGNORED_ERROR);
    }

    @Override
    public void onRecord(Value[] fields) {
        log.trace("[%d] onRecord", hashCode());
        synchronized (this) {
            updateRecordState(RecordState.HAD_RECORD);
            decrementDemand();
        }
        var record = new InternalRecord(runSummary.keys(), fields);
        recordConsumer.accept(record, null);
    }

    @Override
    public synchronized void onPullSummary(PullSummary summary) {
        log.trace("[%d] onPullSummary", hashCode());
        pullSummary = summary;
    }

    @Override
    public synchronized void onDiscardSummary(DiscardSummary summary) {
        log.trace("[%d] onDiscardSummary", hashCode());
        discardSummary = summary;
    }

    @Override
    public synchronized CompletionStage<Throwable> discardAllFailureAsync() {
        log.trace("[%d] Discard all requested", hashCode());
        var summaryExposed = this.summaryExposed;
        var runErrorExposed = this.runErrorExposed;
        return summaryAsync()
                .thenApply(ignored -> (Throwable) null)
                .exceptionally(throwable -> runErrorExposed || summaryExposed ? null : throwable);
    }

    @Override
    public synchronized CompletionStage<Throwable> pullAllFailureAsync() {
        log.trace("[%d] Pull all failure requested", hashCode());
        var unfinishedState =
                switch (state) {
                    case READY, STREAMING, DISCARDING -> true;
                    case FAILED, SUCCEEDED -> false;
                };
        if (recordConsumer != null && unfinishedState) {
            return CompletableFuture.completedFuture(
                    new TransactionNestingException(
                            "You cannot run another query or begin a new transaction in the same session before you've fully consumed the previous run result."));
        }
        return discardAllFailureAsync();
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
        log.trace("[%d] Appended demand, outstanding is %d", hashCode(), outstandingDemand);
        return outstandingDemand;
    }

    private synchronized long getDemand() {
        log.trace("[%d] Get demand, outstanding is %d", hashCode(), outstandingDemand);
        return outstandingDemand;
    }

    private synchronized void decrementDemand() {
        if (outstandingDemand > 0) {
            outstandingDemand--;
        }
        log.trace("[%d] Decremented demand, outstanding is %d", hashCode(), outstandingDemand);
    }

    private synchronized Runnable setupDiscardRunnable() {
        state = State.DISCARDING;
        return () -> boltConnection
                .writeAndFlush(this, Messages.discard(runSummary.queryId(), -1))
                .whenComplete((ignored, throwable) -> {
                    throwable = Futures.completionExceptionCause(throwable);
                    if (throwable != null) {
                        handleError(throwable);
                        onComplete();
                    }
                });
    }

    private synchronized Runnable setupCompletionRunnableWithPullSummary() {
        log.trace("[%d] Setting up completion with pull summary (hasMore=%b)", hashCode(), pullSummary.hasMore());
        var runnable = NOOP_RUNNABLE;
        if (pullSummary.hasMore()) {
            pullSummary = null;
            if (discardPending) {
                discardPending = false;
                state = State.DISCARDING;
                runnable = () -> boltConnection
                        .writeAndFlush(this, Messages.discard(runSummary.queryId(), -1))
                        .whenComplete((ignored, flushThrowable) -> {
                            var error = Futures.completionExceptionCause(flushThrowable);
                            if (error != null) {
                                handleError(error);
                                onComplete();
                            }
                        });
            } else {
                var demand = getDemand();
                if (demand != 0) {
                    state = State.STREAMING;
                    runnable = () -> boltConnection
                            .writeAndFlush(this, Messages.pull(runSummary.queryId(), demand > 0 ? demand : -1))
                            .whenComplete((ignored, flushThrowable) -> {
                                var error = Futures.completionExceptionCause(flushThrowable);
                                if (error != null) {
                                    handleError(error);
                                    onComplete();
                                }
                            });
                } else {
                    state = State.READY;
                }
            }
        } else {
            runnable = setupCompletionRunnableWithSummaryMetadata(pullSummary.metadata());
        }
        return runnable;
    }

    private synchronized Runnable setupCompletionRunnableWithSummaryMetadata(Map<String, Value> metadata) {
        log.trace("[%d] Setting up completion with summary metadata", hashCode());
        var runnable = NOOP_RUNNABLE;
        ResultSummary resultSummary = null;
        try {
            resultSummary = resultSummary(metadata, generateGqlStatusObject(runSummary.keys()));
            state = State.SUCCEEDED;
        } catch (Throwable summaryThrowable) {
            handleError(summaryThrowable);
        }

        if (resultSummary != null) {
            var bookmarkOpt = databaseBookmark(metadata);
            bookmarkOpt.ifPresent(bookmarkConsumer);
            var completeRunnable = setupSummaryAndRecordCompletionRunnable(resultSummary, null);
            runnable = () -> closeBoltConnection(completeRunnable);
        } else {
            runnable = this::onComplete;
        }
        return runnable;
    }

    private ResultSummary resultSummary(Map<String, Value> metadata, GqlStatusObject gqlStatusObject) {
        return METADATA_EXTRACTOR.extractSummary(
                query,
                boltConnection,
                runSummary.resultAvailableAfter(),
                metadata,
                legacyNotifications,
                gqlStatusObject);
    }

    @SuppressWarnings("DuplicatedCode")
    private static Optional<DatabaseBookmark> databaseBookmark(Map<String, Value> metadata) {
        DatabaseBookmark databaseBookmark = null;
        var bookmarkValue = metadata.get("bookmark");
        if (bookmarkValue != null && !bookmarkValue.isNull() && bookmarkValue.hasType(TYPE_SYSTEM.STRING())) {
            var bookmarkStr = bookmarkValue.asString();
            if (!bookmarkStr.isEmpty()) {
                databaseBookmark = new DatabaseBookmark(null, Bookmark.from(bookmarkStr));
            }
        }
        return Optional.ofNullable(databaseBookmark);
    }

    private synchronized Runnable setupCompletionRunnableWithError(Throwable throwable) {
        log.trace("[%d] Setting up completion with error %s", hashCode(), throwableName(throwable));
        ResultSummary summary = null;
        try {
            summary = resultSummary(Collections.emptyMap(), null);
        } catch (Throwable summaryThrowable) {
            log.error(String.format("[%d] Failed to parse summary", hashCode()), summaryThrowable);
        }
        var completeRunnable = setupSummaryAndRecordCompletionRunnable(summary, throwable);
        return () -> closeBoltConnection(completeRunnable);
    }

    private void closeBoltConnection(Runnable runnable) {
        var closeStage = CompletableFuture.<Void>completedStage(null);
        if (closeOnSummary) {
            closeStage = closeStage.thenCompose(ignored -> boltConnection.close());
        }
        closeStage.whenComplete((ignored, closeThrowable) -> {
            if (log.isTraceEnabled() && closeThrowable != null) {
                log.error(
                        String.format("[%d] Failed to close connection", hashCode()),
                        Futures.completionExceptionCause(closeThrowable));
            }
            runnable.run();
        });
    }

    @SuppressWarnings("DuplicatedCode")
    private synchronized void handleError(Throwable throwable) {
        if (log.isTraceEnabled()) {
            log.error(String.format("[%d] handleError", hashCode()), throwable);
        }
        state = State.FAILED;
        throwable = Futures.completionExceptionCause(throwable);
        if (error == null) {
            error = throwable;
        } else {
            if (throwable == IGNORED_ERROR) {
                return;
            }
            if (error == IGNORED_ERROR || (error instanceof Neo4jException && !(throwable instanceof Neo4jException))) {
                error = throwable;
            }
        }
    }

    private synchronized Runnable setupSummaryAndRecordCompletionRunnable(ResultSummary summary, Throwable throwable) {
        var recordConsumerRef = recordConsumer;
        this.recordConsumer = NOOP_CONSUMER;

        return () -> {
            if (throwable != null) {
                if (recordConsumerRef != null && recordConsumerRef != NOOP_CONSUMER) {
                    completeSummaryFuture(summary, null);
                    recordConsumerRef.accept(null, throwable);
                } else {
                    completeSummaryFuture(null, throwable);
                }
            } else {
                completeSummaryFuture(summary, null);
                if (recordConsumerRef != null) {
                    recordConsumerRef.accept(null, null);
                }
            }
        };
    }

    private void completeSummaryFuture(ResultSummary summary, Throwable throwable) {
        throwable = Futures.completionExceptionCause(throwable);
        log.trace(
                "[%d] Completing summary future (summary=%s, throwable=%s)",
                hashCode(), hash(summary), throwableName(throwable));
        if (throwable != null) {
            consumedFuture.completeExceptionally(throwable);
            summaryFuture.completeExceptionally(throwable);
        } else {
            consumedFuture.complete(null);
            summaryFuture.complete(summary);
        }
    }

    private BiConsumer<Record, Throwable> safeRecordConsumer(BiConsumer<Record, Throwable> recordConsumer) {
        return (record, throwable) -> {
            try {
                recordConsumer.accept(record, throwable);
                log.trace(
                        "[%d] Record consumer notified with (record=%s, throwable=%s)",
                        hashCode(), hash(record), throwableName(throwable));
            } catch (Throwable unexpectedThrowable) {
                log.error(
                        String.format(
                                "[%d] Record consumer threw an error when notified with (record=%s, throwable=%s), this will be ignored",
                                hashCode(), hash(record), throwableName(throwable)),
                        unexpectedThrowable);
            }
        };
    }

    private String hash(Object object) {
        return object == null ? "null" : String.valueOf(object.hashCode());
    }

    private String throwableName(Throwable throwable) {
        return throwable == null ? "null" : throwable.getClass().getCanonicalName();
    }
}

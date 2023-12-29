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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.Futures.combineErrors;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.futureCompletingConsumer;
import static org.neo4j.driver.internal.util.LockUtil.executeWithLock;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransactionTerminatedException;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.TransactionType;
import org.neo4j.driver.internal.bolt.api.summary.BeginSummary;
import org.neo4j.driver.internal.bolt.api.summary.CommitSummary;
import org.neo4j.driver.internal.bolt.api.summary.DiscardSummary;
import org.neo4j.driver.internal.bolt.api.summary.PullSummary;
import org.neo4j.driver.internal.bolt.api.summary.RollbackSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.bolt.api.summary.TelemetrySummary;
import org.neo4j.driver.internal.cursor.DisposableResultCursorImpl;
import org.neo4j.driver.internal.cursor.ResultCursorImpl;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.cursor.RxResultCursorImpl;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;
import org.neo4j.driver.internal.util.ErrorUtil;
import org.neo4j.driver.internal.util.Futures;

public class UnmanagedTransaction {
    private enum State {
        /**
         * The transaction is running with no explicit success or failure marked
         */
        ACTIVE,

        /**
         * This transaction has been terminated either because of a fatal connection error.
         */
        TERMINATED,

        /**
         * This transaction has successfully committed
         */
        COMMITTED,

        /**
         * This transaction has been rolled back
         */
        ROLLED_BACK
    }

    public static final String EXPLICITLY_TERMINATED_MSG =
            "The transaction has been explicitly terminated by the driver";
    protected static final String CANT_COMMIT_COMMITTED_MSG = "Can't commit, transaction has been committed";
    protected static final String CANT_ROLLBACK_COMMITTED_MSG = "Can't rollback, transaction has been committed";
    protected static final String CANT_COMMIT_ROLLED_BACK_MSG = "Can't commit, transaction has been rolled back";
    protected static final String CANT_ROLLBACK_ROLLED_BACK_MSG = "Can't rollback, transaction has been rolled back";
    protected static final String CANT_COMMIT_ROLLING_BACK_MSG =
            "Can't commit, transaction has been requested to be rolled back";
    protected static final String CANT_ROLLBACK_COMMITTING_MSG =
            "Can't rollback, transaction has been requested to be committed";
    private static final EnumSet<State> OPEN_STATES = EnumSet.of(State.ACTIVE, State.TERMINATED);

    private final BoltConnection connection;
    private final Consumer<DatabaseBookmark> bookmarkConsumer;
    private final ResultCursorsHolder resultCursors;
    private final long fetchSize;
    private final Lock lock = new ReentrantLock();
    private State state = State.ACTIVE;
    private CompletableFuture<Void> commitFuture;
    private CompletableFuture<Void> rollbackFuture;
    private Throwable causeOfTermination;
    private CompletionStage<Void> terminationStage;
    private final NotificationConfig notificationConfig;
    private final CompletableFuture<UnmanagedTransaction> beginFuture = new CompletableFuture<>();
    private final DatabaseName databaseName;
    private final AccessMode accessMode;
    private final String impersonatedUser;
    private final Logging logging;

    private final ApiTelemetryWork apiTelemetryWork;

    public UnmanagedTransaction(
            BoltConnection connection,
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Consumer<DatabaseBookmark> bookmarkConsumer,
            long fetchSize,
            NotificationConfig notificationConfig,
            ApiTelemetryWork apiTelemetryWork,
            Logging logging) {
        this(
                connection,
                databaseName,
                accessMode,
                impersonatedUser,
                bookmarkConsumer,
                fetchSize,
                new ResultCursorsHolder(),
                notificationConfig,
                apiTelemetryWork,
                logging);
    }

    protected UnmanagedTransaction(
            BoltConnection connection,
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Consumer<DatabaseBookmark> bookmarkConsumer,
            long fetchSize,
            ResultCursorsHolder resultCursors,
            NotificationConfig notificationConfig,
            ApiTelemetryWork apiTelemetryWork,
            Logging logging) {
        this.connection = connection;
        this.databaseName = databaseName;
        this.accessMode = accessMode;
        this.impersonatedUser = impersonatedUser;
        this.bookmarkConsumer = bookmarkConsumer;
        this.resultCursors = resultCursors;
        this.fetchSize = fetchSize;
        this.notificationConfig = notificationConfig;
        this.logging = logging;
        this.apiTelemetryWork = apiTelemetryWork;
    }

    // flush = false is only supported for async mode with a single subsequent run
    public CompletionStage<UnmanagedTransaction> beginAsync(
            Set<Bookmark> initialBookmarks, TransactionConfig config, String txType, boolean flush) {

        var bookmarks = initialBookmarks.stream().map(Bookmark::value).collect(Collectors.toSet());

        return apiTelemetryWork
                .pipelineTelemetryIfEnabled(connection)
                .thenCompose(connection -> connection.beginTransaction(
                        databaseName,
                        accessMode,
                        impersonatedUser,
                        bookmarks,
                        TransactionType.DEFAULT,
                        config.timeout(),
                        config.metadata(),
                        notificationConfig))
                .thenCompose(connection -> {
                    if (flush) {
                        connection
                                .flush(new ResponseHandler() {
                                    private Throwable error;

                                    @Override
                                    public void onError(Throwable throwable) {
                                        if (error == null) {
                                            error = throwable;
                                            connection.close().whenComplete((ignored, closeThrowable) -> {
                                                if (closeThrowable != null) {
                                                    throwable.addSuppressed(closeThrowable);
                                                }
                                                beginFuture.completeExceptionally(throwable);
                                            });
                                        }
                                    }

                                    @Override
                                    public void onTelemetrySummary(TelemetrySummary summary) {
                                        apiTelemetryWork.acknowledge();
                                    }

                                    @Override
                                    public void onBeginSummary(BeginSummary summary) {
                                        beginFuture.complete(null);
                                    }
                                })
                                .whenComplete((ignored, throwable) -> {
                                    if (throwable != null) {
                                        connection.close().whenComplete((closeResult, closeThrowable) -> {
                                            if (closeThrowable != null) {
                                                throwable.addSuppressed(closeThrowable);
                                            }
                                            beginFuture.completeExceptionally(throwable);
                                        });
                                    }
                                });
                        return beginFuture.thenApply(ignored -> this);
                    } else {
                        return CompletableFuture.completedFuture(this);
                    }
                });
    }

    public CompletionStage<Void> closeAsync() {
        return closeAsync(false);
    }

    public CompletionStage<Void> closeAsync(boolean commit) {
        return closeAsync(commit, true);
    }

    public CompletionStage<Void> commitAsync() {
        return closeAsync(true, false);
    }

    public CompletionStage<Void> rollbackAsync() {
        return closeAsync(false, false);
    }

    public CompletionStage<ResultCursor> runAsync(Query query) {
        ensureCanRunQueries();
        var cursorFuture = new CompletableFuture<DisposableResultCursorImpl>();
        var parameters = query.parameters().asMap(Values::value);
        var transaction = this;
        var st = connection
                .run(query.text(), parameters)
                .thenCompose(ignored2 -> connection.pull(-1, fetchSize))
                .thenCompose(ignored2 -> connection.flush(new ResponseHandler() {
                    private Throwable error;

                    @Override
                    public void onError(Throwable throwable) {
                        if (error == null) {
                            error = Futures.completionExceptionCause(throwable);
                            if (error instanceof IllegalStateException) {
                                error = ErrorUtil.newConnectionTerminatedError();
                            }
                            if (beginFuture.completeExceptionally(error)) {
                                //noinspection ThrowableNotThrown
                                markTerminated(error);
                            } else {
                                if (!cursorFuture.completeExceptionally(error)) {
                                    cursorFuture.getNow(null).delegate().onError(error);
                                } else {
                                    //noinspection ThrowableNotThrown
                                    markTerminated(error);
                                }
                            }
                        }
                    }

                    @Override
                    public void onTelemetrySummary(TelemetrySummary summary) {
                        apiTelemetryWork.acknowledge();
                    }

                    @Override
                    public void onBeginSummary(BeginSummary summary) {
                        beginFuture.complete(transaction);
                    }

                    @Override
                    public void onRunSummary(RunSummary summary) {
                        cursorFuture.complete(new DisposableResultCursorImpl(new ResultCursorImpl(
                                connection,
                                query,
                                fetchSize,
                                transaction::markTerminated,
                                (bookmark) -> {},
                                false,
                                summary,
                                () -> executeWithLock(lock, () -> causeOfTermination))));
                    }

                    @Override
                    public void onRecord(Value[] fields) {
                        cursorFuture.getNow(null).delegate().onRecord(fields);
                    }

                    @Override
                    public void onPullSummary(PullSummary summary) {
                        cursorFuture.getNow(null).delegate().onPullSummary(summary);
                    }

                    @Override
                    public void onDiscardSummary(DiscardSummary summary) {
                        cursorFuture.getNow(null).delegate().onDiscardSummary(summary);
                    }
                }));

        return beginFuture.thenCompose(ignored -> {
            var cursorStage = st.thenCompose(flushResult -> cursorFuture);
            resultCursors.add(cursorStage);
            return cursorStage.thenApply(Function.identity());
        });
    }

    public CompletionStage<RxResultCursor> runRx(Query query) {
        ensureCanRunQueries();
        var cursorFuture = new CompletableFuture<RxResultCursor>();
        var parameters = query.parameters().asMap(Values::value);
        var transaction = this;
        var st = connection
                .run(query.text(), parameters)
                .thenCompose(ignored2 -> connection.flush(new ResponseHandler() {
                    @Override
                    public void onError(Throwable throwable) {
                        throwable = Futures.completionExceptionCause(throwable);
                        if (throwable instanceof IllegalStateException) {
                            throwable = ErrorUtil.newConnectionTerminatedError();
                        }
                        if (beginFuture.completeExceptionally(throwable)) {
                            //noinspection ThrowableNotThrown
                            markTerminated(throwable);
                        } else {
                            cursorFuture.completeExceptionally(throwable);
                            //noinspection ThrowableNotThrown
                            markTerminated(throwable);
                        }
                    }

                    @Override
                    public void onTelemetrySummary(TelemetrySummary summary) {
                        apiTelemetryWork.acknowledge();
                    }

                    @Override
                    public void onRunSummary(RunSummary summary) {
                        cursorFuture.complete(new RxResultCursorImpl(
                                connection,
                                query,
                                summary,
                                bookmark -> {},
                                transaction::markTerminated,
                                false,
                                () -> executeWithLock(lock, () -> causeOfTermination)));
                    }
                }));

        return beginFuture.thenCompose(ignored -> {
            var cursorStage = st.thenCompose(flushResult -> cursorFuture);
            resultCursors.add(cursorStage);
            return cursorStage.thenApply(Function.identity());
        });
    }

    public boolean isOpen() {
        return OPEN_STATES.contains(executeWithLock(lock, () -> state));
    }

    public Throwable markTerminated(Throwable cause) {
        return executeWithLock(lock, () -> {
            if (state == State.TERMINATED) {
                if (cause != null) {
                    addSuppressedWhenNotCaptured(causeOfTermination, cause);
                }
            } else {
                state = State.TERMINATED;
                causeOfTermination =
                        cause != null ? cause : new TransactionTerminatedException(EXPLICITLY_TERMINATED_MSG);
            }
            return causeOfTermination;
        });
    }

    private void addSuppressedWhenNotCaptured(Throwable currentCause, Throwable newCause) {
        if (currentCause != newCause) {
            var noneMatch = Arrays.stream(currentCause.getSuppressed()).noneMatch(suppressed -> suppressed == newCause);
            if (noneMatch) {
                currentCause.addSuppressed(newCause);
            }
        }
    }

    public CompletionStage<Void> terminateAsync() {
        return executeWithLock(lock, () -> {
            if (!isOpen() || commitFuture != null || rollbackFuture != null) {
                return CompletableFuture.failedFuture(
                        new ClientException("Can't terminate closed or closing transaction"));
            } else {
                if (state == State.TERMINATED) {
                    return terminationStage != null ? terminationStage : completedFuture(null);
                } else {
                    var terminationException = markTerminated(null);
                    //                    terminationStage = connection.reset(terminationException);
                    return terminationStage;
                }
            }
        });
    }

    private void ensureCanRunQueries() {
        executeWithLock(lock, () -> {
            if (state == State.COMMITTED) {
                throw new ClientException("Cannot run more queries in this transaction, it has been committed");
            } else if (state == State.ROLLED_BACK) {
                throw new ClientException("Cannot run more queries in this transaction, it has been rolled back");
            } else if (state == State.TERMINATED) {
                if (causeOfTermination instanceof TransactionTerminatedException transactionTerminatedException) {
                    throw transactionTerminatedException;
                } else {
                    throw new TransactionTerminatedException(
                            "Cannot run more queries in this transaction, "
                                    + "it has either experienced an fatal error or was explicitly terminated",
                            causeOfTermination);
                }
            } else if (commitFuture != null) {
                throw new ClientException("Cannot run more queries in this transaction, it is being committed");
            } else if (rollbackFuture != null) {
                throw new ClientException("Cannot run more queries in this transaction, it is being rolled back");
            }
        });
    }

    private CompletionStage<Void> doCommitAsync(Throwable cursorFailure) {
        ClientException exception = executeWithLock(
                lock,
                () -> state == State.TERMINATED
                        ? new TransactionTerminatedException(
                                "Transaction can't be committed. "
                                        + "It has been rolled back either because of an error or explicit termination",
                                cursorFailure != causeOfTermination ? causeOfTermination : null)
                        : null);

        if (exception != null) {
            return CompletableFuture.failedFuture(exception);
        } else {
            var commitSummary = new CompletableFuture<CommitSummary>();
            connection
                    .commit()
                    .thenCompose(connection -> connection.flush(new ResponseHandler() {
                        @Override
                        public void onError(Throwable throwable) {
                            commitSummary.completeExceptionally(throwable);
                        }

                        @Override
                        public void onCommitSummary(CommitSummary summary) {
                            summary.bookmark()
                                    .map(bookmark -> new DatabaseBookmark(null, Bookmark.from(bookmark)))
                                    .ifPresent(bookmarkConsumer);
                            commitSummary.complete(summary);
                        }
                    }))
                    .exceptionally(throwable -> {
                        commitSummary.completeExceptionally(throwable);
                        return null;
                    });
            // todo bookmarkConsumer.accept(summary.getBookmark())
            return commitSummary.thenApply(summary -> null);
        }
    }

    private CompletionStage<Void> doRollbackAsync() {
        if (executeWithLock(lock, () -> state) == State.TERMINATED) {
            return completedWithNull();
        } else {
            var rollbackFuture = new CompletableFuture<Void>();
            connection
                    .rollback()
                    .thenCompose(connection -> connection.flush(new ResponseHandler() {
                        @Override
                        public void onError(Throwable throwable) {
                            rollbackFuture.completeExceptionally(throwable);
                        }

                        @Override
                        public void onRollbackSummary(RollbackSummary summary) {
                            rollbackFuture.complete(null);
                        }
                    }))
                    .exceptionally(throwable -> {
                        rollbackFuture.completeExceptionally(throwable);
                        return null;
                    });

            return rollbackFuture;
        }
    }

    private static BiFunction<Void, Throwable, Void> handleCommitOrRollback(Throwable cursorFailure) {
        return (ignore, commitOrRollbackError) -> {
            commitOrRollbackError = Futures.completionExceptionCause(commitOrRollbackError);
            if (commitOrRollbackError instanceof IllegalStateException) {
                commitOrRollbackError = ErrorUtil.newConnectionTerminatedError();
            }
            var combinedError = combineErrors(cursorFailure, commitOrRollbackError);
            if (combinedError != null) {
                throw combinedError;
            }
            return null;
        };
    }

    private CompletionStage<Void> handleTransactionCompletion(boolean commitAttempt, Throwable throwable) {
        executeWithLock(lock, () -> {
            if (commitAttempt && throwable == null) {
                state = State.COMMITTED;
            } else {
                state = State.ROLLED_BACK;
            }
        });
        return connection
                .close()
                .exceptionally(th -> null)
                .thenCompose(ignored -> throwable != null
                        ? CompletableFuture.failedStage(throwable)
                        : CompletableFuture.completedStage(null));
    }

    private CompletionStage<Void> closeAsync(boolean commit, boolean completeWithNullIfNotOpen) {
        var stage = executeWithLock(lock, () -> {
            CompletionStage<Void> resultStage = null;
            if (completeWithNullIfNotOpen && !isOpen()) {
                resultStage = completedWithNull();
            } else if (state == State.COMMITTED) {
                resultStage = CompletableFuture.failedFuture(
                        new ClientException(commit ? CANT_COMMIT_COMMITTED_MSG : CANT_ROLLBACK_COMMITTED_MSG));
            } else if (state == State.ROLLED_BACK) {
                resultStage = CompletableFuture.failedFuture(
                        new ClientException(commit ? CANT_COMMIT_ROLLED_BACK_MSG : CANT_ROLLBACK_ROLLED_BACK_MSG));
            } else {
                if (commit) {
                    if (rollbackFuture != null) {
                        resultStage = CompletableFuture.failedFuture(new ClientException(CANT_COMMIT_ROLLING_BACK_MSG));
                    } else if (commitFuture != null) {
                        resultStage = commitFuture;
                    } else {
                        commitFuture = new CompletableFuture<>();
                    }
                } else {
                    if (commitFuture != null) {
                        resultStage = CompletableFuture.failedFuture(new ClientException(CANT_ROLLBACK_COMMITTING_MSG));
                    } else if (rollbackFuture != null) {
                        resultStage = rollbackFuture;
                    } else {
                        rollbackFuture = new CompletableFuture<>();
                    }
                }
            }
            return resultStage;
        });

        if (stage == null) {
            CompletableFuture<Void> targetFuture;
            Function<Throwable, CompletionStage<Void>> targetAction;
            if (commit) {
                targetFuture = commitFuture;
                targetAction = throwable -> doCommitAsync(throwable).handle(handleCommitOrRollback(throwable));
            } else {
                targetFuture = rollbackFuture;
                targetAction = throwable -> doRollbackAsync().handle(handleCommitOrRollback(throwable));
            }
            resultCursors
                    .retrieveNotConsumedError()
                    .thenCompose(targetAction)
                    .handle((ignored, throwable) -> handleTransactionCompletion(commit, throwable))
                    .thenCompose(Function.identity())
                    .whenComplete(futureCompletingConsumer(targetFuture));
            stage = targetFuture;
        }

        return stage;
    }
}

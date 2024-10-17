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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.neo4j.driver.internal.util.Futures.combineErrors;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.futureCompletingConsumer;
import static org.neo4j.driver.internal.util.LockUtil.executeWithLock;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Values;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.TransactionTerminatedException;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BasicResponseHandler;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.GqlStatusError;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.TransactionType;
import org.neo4j.driver.internal.bolt.api.summary.BeginSummary;
import org.neo4j.driver.internal.bolt.api.summary.CommitSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.bolt.api.summary.TelemetrySummary;
import org.neo4j.driver.internal.cursor.DisposableResultCursorImpl;
import org.neo4j.driver.internal.cursor.ResultCursorImpl;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.cursor.RxResultCursorImpl;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;
import org.neo4j.driver.internal.util.ErrorUtil;
import org.neo4j.driver.internal.util.Futures;

public class UnmanagedTransaction implements TerminationAwareStateLockingExecutor {
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

    private final TerminationAwareBoltConnection connection;
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
        this.connection = new TerminationAwareBoltConnection(connection, this);
        this.databaseName = databaseName;
        this.accessMode = accessMode;
        this.impersonatedUser = impersonatedUser;
        this.bookmarkConsumer = bookmarkConsumer;
        this.resultCursors = resultCursors;
        this.fetchSize = fetchSize;
        this.notificationConfig = notificationConfig;
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
                        txType,
                        notificationConfig))
                .thenCompose(connection -> {
                    if (flush) {
                        var responseHandler = new BeginResponseHandler(
                                apiTelemetryWork, () -> executeWithLock(lock, () -> causeOfTermination));
                        connection
                                .flush(responseHandler)
                                .thenCompose(ignored -> responseHandler.summaryFuture)
                                .whenComplete((summary, throwable) -> {
                                    if (throwable != null) {
                                        connection.close().whenComplete((ignored, closeThrowable) -> {
                                            if (closeThrowable != null) {
                                                throwable.addSuppressed(closeThrowable);
                                            }
                                            beginFuture.completeExceptionally(throwable);
                                        });
                                    } else {
                                        beginFuture.complete(this);
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
        var parameters = query.parameters().asMap(Values::value);
        var resultCursor = new ResultCursorImpl(
                connection,
                query,
                fetchSize,
                this::markTerminated,
                (bookmark) -> {},
                false,
                () -> executeWithLock(lock, () -> causeOfTermination),
                beginFuture,
                apiTelemetryWork);
        var flushStage = connection
                .run(query.text(), parameters)
                .thenCompose(ignored -> connection.pull(-1, fetchSize))
                .thenCompose(ignored -> connection.flush(resultCursor));
        return beginFuture.thenCompose(ignored -> {
            var cursorStage = flushStage
                    .thenCompose(flushResult -> resultCursor.resultCursor())
                    .thenApply(DisposableResultCursorImpl::new);
            resultCursors.add(cursorStage);
            return cursorStage.thenApply(Function.identity());
        });
    }

    public CompletionStage<RxResultCursor> runRx(Query query) {
        ensureCanRunQueries();
        var parameters = query.parameters().asMap(Values::value);
        var responseHandler = new RunRxResponseHandler(
                apiTelemetryWork,
                () -> executeWithLock(lock, () -> causeOfTermination),
                this::markTerminated,
                beginFuture,
                this,
                connection,
                query);
        var flushStage =
                connection.run(query.text(), parameters).thenCompose(ignored2 -> connection.flush(responseHandler));
        return beginFuture.thenCompose(ignored -> {
            var cursorStage = flushStage.thenCompose(flushResult -> responseHandler.cursorFuture);
            resultCursors.add(cursorStage);
            return cursorStage.thenApply(Function.identity());
        });
    }

    public boolean isOpen() {
        return OPEN_STATES.contains(executeWithLock(lock, () -> state));
    }

    public void markTerminated(Throwable cause) {
        executeWithLock(lock, () -> {
            if (state == State.TERMINATED) {
                if (cause != null) {
                    addSuppressedWhenNotCaptured(causeOfTermination, cause);
                }
            } else {
                state = State.TERMINATED;
                causeOfTermination = cause != null
                        ? cause
                        : new TransactionTerminatedException(
                                GqlStatusError.UNKNOWN.getStatus(),
                                GqlStatusError.UNKNOWN.getStatusDescription(EXPLICITLY_TERMINATED_MSG),
                                "N/A",
                                EXPLICITLY_TERMINATED_MSG,
                                GqlStatusError.DIAGNOSTIC_RECORD,
                                null);
            }
        });
    }

    public BoltConnection connection() {
        return connection;
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
                var message = "Can't terminate closed or closing transaction";
                return failedFuture(new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(message),
                        "N/A",
                        message,
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        null));
            } else {
                if (state == State.TERMINATED) {
                    return terminationStage != null ? terminationStage : completedFuture(null);
                } else {
                    markTerminated(null);
                    terminationStage = connection.clearAndReset().thenApply(ignored -> null);
                    return terminationStage;
                }
            }
        });
    }

    @Override
    public <T> T execute(Function<Throwable, T> causeOfTerminationConsumer) {
        return executeWithLock(lock, () -> causeOfTerminationConsumer.apply(causeOfTermination));
    }

    private void ensureCanRunQueries() {
        executeWithLock(lock, () -> {
            if (state == State.COMMITTED) {
                var message = "Cannot run more queries in this transaction, it has been committed";
                throw new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(message),
                        "N/A",
                        message,
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        null);
            } else if (state == State.ROLLED_BACK) {
                var message = "Cannot run more queries in this transaction, it has been rolled back";
                throw new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(message),
                        "N/A",
                        message,
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        null);
            } else if (state == State.TERMINATED) {
                if (causeOfTermination instanceof TransactionTerminatedException transactionTerminatedException) {
                    throw transactionTerminatedException;
                } else {
                    var message =
                            "Cannot run more queries in this transaction, it has either experienced an fatal error or was explicitly terminated";
                    throw new TransactionTerminatedException(
                            GqlStatusError.UNKNOWN.getStatus(),
                            GqlStatusError.UNKNOWN.getStatusDescription(message),
                            "N/A",
                            message,
                            GqlStatusError.DIAGNOSTIC_RECORD,
                            causeOfTermination);
                }
            } else if (commitFuture != null) {
                var message = "Cannot run more queries in this transaction, it is being committed";
                throw new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(message),
                        "N/A",
                        message,
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        null);
            } else if (rollbackFuture != null) {
                var message = "Cannot run more queries in this transaction, it is being rolled back";
                throw new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(message),
                        "N/A",
                        message,
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        null);
            }
        });
    }

    private CompletionStage<Void> doCommitAsync(Throwable cursorFailure) {
        ClientException exception = executeWithLock(
                lock,
                () -> state == State.TERMINATED
                        ? new TransactionTerminatedException(
                                GqlStatusError.UNKNOWN.getStatus(),
                                GqlStatusError.UNKNOWN.getStatusDescription(
                                        "Transaction can't be committed. It has been rolled back either because of an error or explicit termination"),
                                "N/A",
                                "Transaction can't be committed. It has been rolled back either because of an error or explicit termination",
                                GqlStatusError.DIAGNOSTIC_RECORD,
                                cursorFailure != causeOfTermination ? causeOfTermination : null)
                        : null);

        if (exception != null) {
            return failedFuture(exception);
        } else {
            var commitSummary = new CompletableFuture<CommitSummary>();
            var responseHandler = new BasicResponseHandler();
            connection
                    .commit()
                    .thenCompose(connection -> connection.flush(responseHandler))
                    .thenCompose(ignored -> responseHandler.summaries())
                    .whenComplete((summaries, throwable) -> {
                        if (throwable != null) {
                            commitSummary.completeExceptionally(throwable);
                        } else {
                            var summary = summaries.commitSummary();
                            if (summary != null) {
                                summary.bookmark()
                                        .map(bookmark -> new DatabaseBookmark(null, Bookmark.from(bookmark)))
                                        .ifPresent(bookmarkConsumer);
                                commitSummary.complete(summary);
                            } else {
                                throwable = executeWithLock(lock, () -> causeOfTermination);
                                if (throwable == null) {
                                    var message = summaries.ignored() > 0
                                            ? "Commit exchange contains ignored messages"
                                            : "Unexpected state during commit";
                                    throwable = new ClientException(
                                            GqlStatusError.UNKNOWN.getStatus(),
                                            GqlStatusError.UNKNOWN.getStatusDescription(message),
                                            "N/A",
                                            message,
                                            GqlStatusError.DIAGNOSTIC_RECORD,
                                            null);
                                }
                                commitSummary.completeExceptionally(throwable);
                            }
                        }
                    });
            return commitSummary.thenApply(summary -> null);
        }
    }

    private CompletionStage<Void> doRollbackAsync() {
        if (executeWithLock(lock, () -> state) == State.TERMINATED) {
            return completedWithNull();
        } else {
            var rollbackFuture = new CompletableFuture<Void>();
            var responseHandler = new BasicResponseHandler();
            connection
                    .rollback()
                    .thenCompose(connection -> connection.flush(responseHandler))
                    .thenCompose(ignored -> responseHandler.summaries())
                    .whenComplete((summaries, throwable) -> {
                        if (throwable != null) {
                            rollbackFuture.completeExceptionally(throwable);
                        } else {
                            var summary = summaries.rollbackSummary();
                            if (summary != null) {
                                rollbackFuture.complete(null);
                            } else {
                                throwable = executeWithLock(lock, () -> causeOfTermination);
                                if (throwable == null) {
                                    var message = summaries.ignored() > 0
                                            ? "Rollback exchange contains ignored messages"
                                            : "Unexpected state during rollback";
                                    throwable = new ClientException(
                                            GqlStatusError.UNKNOWN.getStatus(),
                                            GqlStatusError.UNKNOWN.getStatusDescription(message),
                                            "N/A",
                                            message,
                                            GqlStatusError.DIAGNOSTIC_RECORD,
                                            null);
                                }
                                rollbackFuture.completeExceptionally(throwable);
                            }
                        }
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

    @SuppressWarnings("DuplicatedCode")
    private CompletionStage<Void> closeAsync(boolean commit, boolean completeWithNullIfNotOpen) {
        var stage = executeWithLock(lock, () -> {
            CompletionStage<Void> resultStage = null;
            if (completeWithNullIfNotOpen && !isOpen()) {
                resultStage = completedWithNull();
            } else if (state == State.COMMITTED) {
                var message = commit ? CANT_COMMIT_COMMITTED_MSG : CANT_ROLLBACK_COMMITTED_MSG;
                resultStage = failedFuture(new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(message),
                        "N/A",
                        message,
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        null));
            } else if (state == State.ROLLED_BACK) {
                var message = commit ? CANT_COMMIT_ROLLED_BACK_MSG : CANT_ROLLBACK_ROLLED_BACK_MSG;
                resultStage = failedFuture(new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(message),
                        "N/A",
                        message,
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        null));
            } else {
                if (commit) {
                    if (rollbackFuture != null) {
                        resultStage = failedFuture(new ClientException(
                                GqlStatusError.UNKNOWN.getStatus(),
                                GqlStatusError.UNKNOWN.getStatusDescription(CANT_COMMIT_ROLLING_BACK_MSG),
                                "N/A",
                                CANT_COMMIT_ROLLING_BACK_MSG,
                                GqlStatusError.DIAGNOSTIC_RECORD,
                                null));
                    } else if (commitFuture != null) {
                        resultStage = commitFuture;
                    } else {
                        commitFuture = new CompletableFuture<>();
                    }
                } else {
                    if (commitFuture != null) {
                        resultStage = failedFuture(new ClientException(
                                GqlStatusError.UNKNOWN.getStatus(),
                                GqlStatusError.UNKNOWN.getStatusDescription(CANT_ROLLBACK_COMMITTING_MSG),
                                "N/A",
                                CANT_ROLLBACK_COMMITTING_MSG,
                                GqlStatusError.DIAGNOSTIC_RECORD,
                                null));
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

    private static class BeginResponseHandler implements ResponseHandler {
        final CompletableFuture<UnmanagedTransaction> summaryFuture = new CompletableFuture<>();
        private final ApiTelemetryWork apiTelemetryWork;
        private final Supplier<Throwable> termSupplier;
        private Throwable error;
        private BeginSummary beginSummary;
        private int ignoredCount;

        private BeginResponseHandler(ApiTelemetryWork apiTelemetryWork, Supplier<Throwable> termSupplier) {
            this.apiTelemetryWork = apiTelemetryWork;
            this.termSupplier = termSupplier;
        }

        @SuppressWarnings("DuplicatedCode")
        @Override
        public void onError(Throwable throwable) {
            if (throwable instanceof CompletionException) {
                throwable = throwable.getCause();
            }
            if (error == null) {
                error = throwable;
            } else {
                if (error instanceof Neo4jException && !(throwable instanceof Neo4jException)) {
                    // higher order error has occurred
                    throwable.addSuppressed(error);
                    error = throwable;
                } else {
                    error.addSuppressed(throwable);
                }
            }
        }

        @Override
        public void onBeginSummary(BeginSummary summary) {
            beginSummary = summary;
        }

        @Override
        public void onTelemetrySummary(TelemetrySummary summary) {
            apiTelemetryWork.acknowledge();
        }

        @Override
        public void onIgnored() {
            ignoredCount++;
        }

        @Override
        public void onComplete() {
            if (error != null) {
                summaryFuture.completeExceptionally(error);
            } else {
                if (beginSummary != null) {
                    summaryFuture.complete(null);
                } else {
                    var throwable = termSupplier.get();
                    if (throwable == null) {
                        var message = ignoredCount > 0
                                ? "Begin exchange contains ignored messages"
                                : "Unexpected state during begin";
                        throwable = new ClientException(
                                GqlStatusError.UNKNOWN.getStatus(),
                                GqlStatusError.UNKNOWN.getStatusDescription(message),
                                "N/A",
                                message,
                                GqlStatusError.DIAGNOSTIC_RECORD,
                                null);
                    }
                    summaryFuture.completeExceptionally(throwable);
                }
            }
        }
    }

    private static class RunRxResponseHandler implements ResponseHandler {
        final CompletableFuture<RxResultCursor> cursorFuture = new CompletableFuture<>();
        private final ApiTelemetryWork apiTelemetryWork;
        private final Supplier<Throwable> termSupplier;
        private final Consumer<Throwable> markTerminated;
        private final CompletableFuture<UnmanagedTransaction> beginFuture;
        private final UnmanagedTransaction transaction;
        private final BoltConnection connection;
        private final Query query;
        private Throwable error;
        private RunSummary runSummary;
        private int ignoredCount;

        private RunRxResponseHandler(
                ApiTelemetryWork apiTelemetryWork,
                Supplier<Throwable> termSupplier,
                Consumer<Throwable> markTerminated,
                CompletableFuture<UnmanagedTransaction> beginFuture,
                UnmanagedTransaction transaction,
                BoltConnection connection,
                Query query) {
            this.apiTelemetryWork = apiTelemetryWork;
            this.termSupplier = termSupplier;
            this.markTerminated = markTerminated;
            this.beginFuture = beginFuture;
            this.transaction = transaction;
            this.connection = connection;
            this.query = query;
        }

        @SuppressWarnings("DuplicatedCode")
        @Override
        public void onError(Throwable throwable) {
            if (throwable instanceof CompletionException) {
                throwable = throwable.getCause();
            }
            if (error == null) {
                error = throwable;
            } else {
                if (error instanceof Neo4jException && !(throwable instanceof Neo4jException)) {
                    // higher order error has occurred
                    throwable.addSuppressed(error);
                    error = throwable;
                } else {
                    error.addSuppressed(throwable);
                }
            }
        }

        @Override
        public void onTelemetrySummary(TelemetrySummary summary) {
            apiTelemetryWork.acknowledge();
        }

        @Override
        public void onRunSummary(RunSummary summary) {
            runSummary = summary;
        }

        @Override
        public void onIgnored() {
            ignoredCount++;
        }

        @Override
        public void onComplete() {
            if (error != null) {
                if (beginFuture.completeExceptionally(error)) {
                    markTerminated.accept(error);
                } else {
                    markTerminated.accept(error);
                    cursorFuture.complete(new RxResultCursorImpl(
                            connection,
                            query,
                            null,
                            error,
                            termSupplier,
                            bookmark -> {},
                            transaction::markTerminated,
                            false,
                            termSupplier));
                }
            } else {
                if (runSummary != null) {
                    cursorFuture.complete(new RxResultCursorImpl(
                            connection,
                            query,
                            runSummary,
                            null,
                            termSupplier,
                            bookmark -> {},
                            transaction::markTerminated,
                            false,
                            termSupplier));
                } else {
                    var throwable = termSupplier.get();
                    if (throwable == null) {
                        var message = ignoredCount > 0
                                ? "Run exchange contains ignored messages"
                                : "Unexpected state during run";
                        throwable = new ClientException(
                                GqlStatusError.UNKNOWN.getStatus(),
                                GqlStatusError.UNKNOWN.getStatusDescription(message),
                                "N/A",
                                message,
                                GqlStatusError.DIAGNOSTIC_RECORD,
                                null);
                    }
                    if (!beginFuture.completeExceptionally(throwable)) {
                        cursorFuture.completeExceptionally(throwable);
                    }
                }
            }
        }
    }
}

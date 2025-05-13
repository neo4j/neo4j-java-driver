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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.TransactionType;
import org.neo4j.bolt.connection.message.Message;
import org.neo4j.bolt.connection.message.Messages;
import org.neo4j.bolt.connection.summary.BeginSummary;
import org.neo4j.bolt.connection.summary.CommitSummary;
import org.neo4j.bolt.connection.summary.RunSummary;
import org.neo4j.bolt.connection.summary.TelemetrySummary;
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
import org.neo4j.driver.internal.GqlStatusError;
import org.neo4j.driver.internal.adaptedbolt.BasicResponseHandler;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnection;
import org.neo4j.driver.internal.adaptedbolt.DriverResponseHandler;
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

    @SuppressWarnings("deprecation")
    private final Logging logging;

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
    private final Consumer<String> databaseNameConsumer;
    private Message[] beginMessages;

    public UnmanagedTransaction(
            DriverBoltConnection connection,
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Consumer<DatabaseBookmark> bookmarkConsumer,
            long fetchSize,
            NotificationConfig notificationConfig,
            ApiTelemetryWork apiTelemetryWork,
            Consumer<String> databaseNameConsumer,
            @SuppressWarnings("deprecation") Logging logging) {
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
                databaseNameConsumer,
                logging);
    }

    protected UnmanagedTransaction(
            DriverBoltConnection connection,
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Consumer<DatabaseBookmark> bookmarkConsumer,
            long fetchSize,
            ResultCursorsHolder resultCursors,
            NotificationConfig notificationConfig,
            ApiTelemetryWork apiTelemetryWork,
            Consumer<String> databaseNameConsumer,
            @SuppressWarnings("deprecation") Logging logging) {
        this.logging = logging;
        this.connection = new TerminationAwareBoltConnection(logging, connection, this, this::markTerminated);
        this.databaseName = databaseName;
        this.accessMode = accessMode;
        this.impersonatedUser = impersonatedUser;
        this.bookmarkConsumer = bookmarkConsumer;
        this.resultCursors = resultCursors;
        this.fetchSize = fetchSize;
        this.notificationConfig = notificationConfig;
        this.apiTelemetryWork = apiTelemetryWork;
        this.databaseNameConsumer = Objects.requireNonNull(databaseNameConsumer);
    }

    // flush = false is only supported for async mode with a single subsequent run
    public CompletionStage<UnmanagedTransaction> beginAsync(
            Set<Bookmark> initialBookmarks, TransactionConfig config, String txType, boolean flush) {
        var bookmarks = initialBookmarks.stream().map(Bookmark::value).collect(Collectors.toSet());
        return CompletableFuture.completedStage(null)
                .thenApply(ignored -> {
                    var messages = new ArrayList<Message>(2);
                    apiTelemetryWork.getTelemetryMessageIfEnabled(connection).ifPresent(messages::add);
                    messages.add(Messages.beginTransaction(
                            databaseName.databaseName().orElse(null),
                            accessMode,
                            impersonatedUser,
                            bookmarks,
                            mapToTransactionType(txType),
                            config.timeout(),
                            connection.valueFactory().toBoltMap(config.metadata()),
                            notificationConfig));
                    return messages;
                })
                .thenCompose(messages -> {
                    if (flush) {
                        var responseHandler = new BeginResponseHandler(apiTelemetryWork, databaseNameConsumer);
                        connection
                                .writeAndFlush(responseHandler, messages)
                                .thenCompose(ignored -> responseHandler.summaryFuture)
                                .whenComplete((summary, throwable) -> {
                                    if (throwable != null) {
                                        connection.close().whenComplete((ignored, closeThrowable) -> {
                                            if (closeThrowable != null && throwable != closeThrowable) {
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
                        return connection.write(messages).thenApply(ignored -> this);
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
                (bookmark) -> {},
                false,
                beginFuture,
                databaseNameConsumer,
                apiTelemetryWork);
        var flushStage = CompletableFuture.completedStage(null).thenCompose(ignored -> {
            var messages = List.of(
                    Messages.run(query.text(), connection.valueFactory().toBoltMap(parameters)),
                    Messages.pull(-1, fetchSize));
            return connection.writeAndFlush(resultCursor, messages);
        });
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
        var responseHandler = new RunRxResponseHandler(logging, apiTelemetryWork, beginFuture, connection, query);
        var flushStage = CompletableFuture.completedStage(null)
                .thenCompose(runMessage -> connection.writeAndFlush(
                        responseHandler,
                        Messages.run(query.text(), connection.valueFactory().toBoltMap(parameters))));
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
        var throwable = Futures.completionExceptionCause(cause);
        executeWithLock(lock, () -> {
            if (isOpen() && commitFuture == null && rollbackFuture == null) {
                if (state == State.TERMINATED) {
                    if (throwable != null) {
                        addSuppressedWhenNotCaptured(causeOfTermination, throwable);
                    }
                } else {
                    state = State.TERMINATED;
                    causeOfTermination = throwable != null
                            ? throwable
                            : new TransactionTerminatedException(
                                    GqlStatusError.UNKNOWN.getStatus(),
                                    GqlStatusError.UNKNOWN.getStatusDescription(EXPLICITLY_TERMINATED_MSG),
                                    "N/A",
                                    EXPLICITLY_TERMINATED_MSG,
                                    GqlStatusError.DIAGNOSTIC_RECORD,
                                    null);
                }
            }
        });
    }

    public DriverBoltConnection connection() {
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
                    terminationStage = connection.reset();
                    return terminationStage;
                }
            }
        });
    }

    @Override
    public <T> T execute(Function<Throwable, T> causeOfTerminationConsumer) {
        return executeWithLock(lock, () -> {
            var throwable = causeOfTermination == null ? null : failedTxException(causeOfTermination);
            return causeOfTerminationConsumer.apply(throwable);
        });
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
                    throw failedTxException(causeOfTermination);
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
                    .writeAndFlush(responseHandler, Messages.commit())
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
                    .writeAndFlush(responseHandler, Messages.rollback())
                    .thenCompose(ignored -> responseHandler.summaries())
                    .whenComplete((summaries, throwable) -> {
                        if (throwable != null) {
                            rollbackFuture.completeExceptionally(throwable);
                        } else {
                            var summary = summaries.rollbackSummary();
                            if (summary != null) {
                                rollbackFuture.complete(null);
                            } else {
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

    private static TransactionTerminatedException failedTxException(Throwable cause) {
        var message =
                "Cannot run more queries in this transaction, it has either experienced a fatal error or was explicitly terminated";
        return new TransactionTerminatedException(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                "N/A",
                message,
                GqlStatusError.DIAGNOSTIC_RECORD,
                cause);
    }

    private static TransactionType mapToTransactionType(String transactionType) {
        return "IMPLICIT".equals(transactionType) ? TransactionType.UNCONSTRAINED : TransactionType.DEFAULT;
    }

    private static class BeginResponseHandler implements DriverResponseHandler {
        final CompletableFuture<UnmanagedTransaction> summaryFuture = new CompletableFuture<>();
        private final ApiTelemetryWork apiTelemetryWork;
        private final Consumer<String> databaseNameConsumer;
        private Throwable error;
        private BeginSummary beginSummary;
        private int ignoredCount;

        private BeginResponseHandler(ApiTelemetryWork apiTelemetryWork, Consumer<String> databaseNameConsumer) {
            this.apiTelemetryWork = apiTelemetryWork;
            this.databaseNameConsumer = Objects.requireNonNull(databaseNameConsumer);
        }

        @Override
        public void onError(Throwable throwable) {
            throwable = Futures.completionExceptionCause(throwable);
            if (error == null) {
                error = throwable;
            } else {
                if (error instanceof Neo4jException && !(throwable instanceof Neo4jException)) {
                    // higher order error has occurred
                    error = throwable;
                }
            }
        }

        @Override
        public void onBeginSummary(BeginSummary summary) {
            beginSummary = summary;
            summary.databaseName().ifPresent(databaseNameConsumer);
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
                    var message = ignoredCount > 0
                            ? "Begin exchange contains ignored messages"
                            : "Unexpected state during begin";
                    var throwable = new ClientException(
                            GqlStatusError.UNKNOWN.getStatus(),
                            GqlStatusError.UNKNOWN.getStatusDescription(message),
                            "N/A",
                            message,
                            GqlStatusError.DIAGNOSTIC_RECORD,
                            null);
                    summaryFuture.completeExceptionally(throwable);
                }
            }
        }
    }

    private static class RunRxResponseHandler implements DriverResponseHandler {
        final CompletableFuture<RxResultCursor> cursorFuture = new CompletableFuture<>();

        @SuppressWarnings("deprecation")
        private final Logging logging;

        private final ApiTelemetryWork apiTelemetryWork;
        private final CompletableFuture<UnmanagedTransaction> beginFuture;
        private final DriverBoltConnection connection;
        private final Query query;
        private Throwable error;
        private RunSummary runSummary;
        private int ignoredCount;

        private RunRxResponseHandler(
                @SuppressWarnings("deprecation") Logging logging,
                ApiTelemetryWork apiTelemetryWork,
                CompletableFuture<UnmanagedTransaction> beginFuture,
                DriverBoltConnection connection,
                Query query) {
            this.logging = logging;
            this.apiTelemetryWork = apiTelemetryWork;
            this.beginFuture = beginFuture;
            this.connection = connection;
            this.query = query;
        }

        @Override
        public void onError(Throwable throwable) {
            throwable = Futures.completionExceptionCause(throwable);
            if (error == null) {
                error = throwable;
            } else {
                if (error instanceof Neo4jException && !(throwable instanceof Neo4jException)) {
                    // higher order error has occurred
                    error = throwable;
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
                if (!beginFuture.completeExceptionally(error)) {
                    cursorFuture.complete(
                            new RxResultCursorImpl(connection, query, null, error, bookmark -> {}, false, logging));
                }
            } else {
                if (runSummary != null) {
                    cursorFuture.complete(new RxResultCursorImpl(
                            connection, query, runSummary, null, bookmark -> {}, false, logging));
                } else {
                    var message =
                            ignoredCount > 0 ? "Run exchange contains ignored messages" : "Unexpected state during run";
                    var throwable = new ClientException(
                            GqlStatusError.UNKNOWN.getStatus(),
                            GqlStatusError.UNKNOWN.getStatusDescription(message),
                            "N/A",
                            message,
                            GqlStatusError.DIAGNOSTIC_RECORD,
                            null);
                    if (!beginFuture.completeExceptionally(throwable)) {
                        cursorFuture.completeExceptionally(throwable);
                    }
                }
            }
        }
    }
}

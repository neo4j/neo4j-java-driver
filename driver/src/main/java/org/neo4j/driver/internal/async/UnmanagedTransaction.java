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
import static org.neo4j.driver.internal.util.Futures.asCompletionException;
import static org.neo4j.driver.internal.util.Futures.combineErrors;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
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
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ConnectionReadTimeoutException;
import org.neo4j.driver.exceptions.TransactionTerminatedException;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.GqlNotificationConfig;
import org.neo4j.driver.internal.GqlStatusError;
import org.neo4j.driver.internal.cursor.AsyncResultCursor;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;

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

    private final Connection connection;
    private final BoltProtocol protocol;
    private final Consumer<DatabaseBookmark> bookmarkConsumer;
    private final ResultCursorsHolder resultCursors;
    private final long fetchSize;
    private final Lock lock = new ReentrantLock();
    private State state = State.ACTIVE;
    private CompletableFuture<Void> commitFuture;
    private CompletableFuture<Void> rollbackFuture;
    private Throwable causeOfTermination;
    private CompletionStage<Void> terminationStage;
    private final GqlNotificationConfig notificationConfig;
    private final CompletableFuture<UnmanagedTransaction> beginFuture = new CompletableFuture<>();
    private final Logging logging;

    private final ApiTelemetryWork apiTelemetryWork;

    public UnmanagedTransaction(
            Connection connection,
            Consumer<DatabaseBookmark> bookmarkConsumer,
            long fetchSize,
            GqlNotificationConfig notificationConfig,
            ApiTelemetryWork apiTelemetryWork,
            Logging logging) {
        this(
                connection,
                bookmarkConsumer,
                fetchSize,
                new ResultCursorsHolder(),
                notificationConfig,
                apiTelemetryWork,
                logging);
    }

    protected UnmanagedTransaction(
            Connection connection,
            Consumer<DatabaseBookmark> bookmarkConsumer,
            long fetchSize,
            ResultCursorsHolder resultCursors,
            GqlNotificationConfig notificationConfig,
            ApiTelemetryWork apiTelemetryWork,
            Logging logging) {
        this.connection = connection;
        this.protocol = connection.protocol();
        this.bookmarkConsumer = bookmarkConsumer;
        this.resultCursors = resultCursors;
        this.fetchSize = fetchSize;
        this.notificationConfig = notificationConfig;
        this.logging = logging;
        this.apiTelemetryWork = apiTelemetryWork;

        connection.bindTerminationAwareStateLockingExecutor(this);
    }

    // flush = false is only supported for async mode with a single subsequent run
    public CompletionStage<UnmanagedTransaction> beginAsync(
            Set<Bookmark> initialBookmarks, TransactionConfig config, String txType, boolean flush) {

        apiTelemetryWork.execute(connection, protocol).whenComplete((unused, throwable) -> {
            if (throwable != null) {
                beginFuture.completeExceptionally(throwable);
            }
        });

        protocol.beginTransaction(connection, initialBookmarks, config, txType, notificationConfig, logging, flush)
                .handle((ignore, beginError) -> {
                    if (beginError != null) {
                        if (beginError instanceof AuthorizationExpiredException) {
                            connection.terminateAndRelease(AuthorizationExpiredException.DESCRIPTION);
                        } else if (beginError instanceof ConnectionReadTimeoutException) {
                            connection.terminateAndRelease(beginError.getMessage());
                        } else {
                            connection.release();
                        }
                        throw asCompletionException(beginError);
                    }
                    return this;
                })
                .whenComplete(futureCompletingConsumer(beginFuture));
        return flush ? beginFuture : CompletableFuture.completedFuture(this);
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
        var cursorStage = protocol.runInUnmanagedTransaction(connection, query, this, fetchSize)
                .asyncResult();
        resultCursors.add(cursorStage);
        return beginFuture.thenCompose(ignored -> cursorStage
                .thenCompose(AsyncResultCursor::mapSuccessfulRunCompletionAsync)
                .thenApply(Function.identity()));
    }

    public CompletionStage<RxResultCursor> runRx(Query query) {
        ensureCanRunQueries();
        var cursorStage = protocol.runInUnmanagedTransaction(connection, query, this, fetchSize)
                .rxResult();
        resultCursors.add(cursorStage);
        return cursorStage;
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

    public Connection connection() {
        return connection;
    }

    @Override
    public void execute(Consumer<Throwable> causeOfTerminationConsumer) {
        executeWithLock(lock, () -> causeOfTerminationConsumer.accept(causeOfTermination));
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
                    var terminationException = markTerminated(null);
                    terminationStage = connection.reset(terminationException);
                    return terminationStage;
                }
            }
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
        return exception != null
                ? failedFuture(exception)
                : protocol.commitTransaction(connection).thenAccept(bookmarkConsumer);
    }

    private CompletionStage<Void> doRollbackAsync() {
        return executeWithLock(lock, () -> state) == State.TERMINATED
                ? completedWithNull()
                : protocol.rollbackTransaction(connection);
    }

    private static BiFunction<Void, Throwable, Void> handleCommitOrRollback(Throwable cursorFailure) {
        return (ignore, commitOrRollbackError) -> {
            var combinedError = combineErrors(cursorFailure, commitOrRollbackError);
            if (combinedError != null) {
                throw combinedError;
            }
            return null;
        };
    }

    private void handleTransactionCompletion(boolean commitAttempt, Throwable throwable) {
        executeWithLock(lock, () -> {
            if (commitAttempt && throwable == null) {
                state = State.COMMITTED;
            } else {
                state = State.ROLLED_BACK;
            }
        });
        if (throwable instanceof AuthorizationExpiredException) {
            connection.terminateAndRelease(AuthorizationExpiredException.DESCRIPTION);
        } else if (throwable instanceof ConnectionReadTimeoutException) {
            connection.terminateAndRelease(throwable.getMessage());
        } else {
            connection.release(); // release in background
        }
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
                    .whenComplete((ignored, throwable) -> handleTransactionCompletion(commit, throwable))
                    .whenComplete(futureCompletingConsumer(targetFuture));
            stage = targetFuture;
        }

        return stage;
    }
}

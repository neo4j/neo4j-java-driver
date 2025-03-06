/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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

import static org.neo4j.driver.internal.util.Futures.asCompletionException;
import static org.neo4j.driver.internal.util.Futures.combineErrors;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.internal.util.Futures.futureCompletingConsumer;
import static org.neo4j.driver.internal.util.LockUtil.executeWithLock;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ConnectionReadTimeoutException;
import org.neo4j.driver.internal.BookmarkHolder;
import org.neo4j.driver.internal.cursor.AsyncResultCursor;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.logging.PrefixedLogger;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.spi.Connection;

public class UnmanagedTransaction {
    private enum State {
        /**
         * The transaction is running with no explicit success or failure marked
         */
        ACTIVE,

        /**
         * This transaction has been terminated either because of explicit {@link Session#reset()} or because of a fatal connection error.
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
    private final BookmarkHolder bookmarkHolder;
    private final ResultCursorsHolder resultCursors;
    private final long fetchSize;
    private final Lock lock = new ReentrantLock();
    private State state = State.ACTIVE;
    private CompletableFuture<Void> commitFuture;
    private CompletableFuture<Void> rollbackFuture;
    private Throwable causeOfTermination;
    private final Logging logging;
    private final Logger log;

    public UnmanagedTransaction(Connection connection, BookmarkHolder bookmarkHolder, long fetchSize, Logging logging) {
        this(connection, bookmarkHolder, fetchSize, new ResultCursorsHolder(), logging);
    }

    protected UnmanagedTransaction(
            Connection connection,
            BookmarkHolder bookmarkHolder,
            long fetchSize,
            ResultCursorsHolder resultCursors,
            Logging logging) {
        this.connection = connection;
        this.protocol = connection.protocol();
        this.bookmarkHolder = bookmarkHolder;
        this.resultCursors = resultCursors;
        this.fetchSize = fetchSize;
        this.logging = logging;
        this.log = new PrefixedLogger(
                "[" + Thread.currentThread().getName() + "][" + hashCode() + "]", logging.getLog(getClass()));
        ;
    }

    public CompletionStage<UnmanagedTransaction> beginAsync(Bookmark initialBookmark, TransactionConfig config) {
        log.trace("beginAsync");
        return protocol.beginTransaction(connection, initialBookmark, config, logging)
                .handle((ignore, beginError) -> {
                    log.trace("beginAsync protocol.beginTransaction finished");
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
        CompletionStage<AsyncResultCursor> cursorStage = protocol.runInUnmanagedTransaction(
                        connection, query, this, fetchSize)
                .asyncResult();
        resultCursors.add(cursorStage);
        return cursorStage
                .thenCompose(AsyncResultCursor::mapSuccessfulRunCompletionAsync)
                .thenApply(cursor -> cursor);
    }

    public CompletionStage<RxResultCursor> runRx(Query query) {
        ensureCanRunQueries();
        CompletionStage<RxResultCursor> cursorStage = protocol.runInUnmanagedTransaction(
                        connection, query, this, fetchSize)
                .rxResult();
        resultCursors.add(cursorStage);
        return cursorStage;
    }

    public boolean isOpen() {
        return OPEN_STATES.contains(executeWithLock(lock, () -> state));
    }

    public void markTerminated(Throwable cause) {
        executeWithLock(lock, () -> {
            if (state == State.TERMINATED) {
                if (causeOfTermination != null) {
                    addSuppressedWhenNotCaptured(causeOfTermination, cause);
                }
            } else {
                state = State.TERMINATED;
                causeOfTermination = cause;
            }
        });
    }

    private void addSuppressedWhenNotCaptured(Throwable currentCause, Throwable newCause) {
        if (currentCause != newCause) {
            boolean noneMatch =
                    Arrays.stream(currentCause.getSuppressed()).noneMatch(suppressed -> suppressed == newCause);
            if (noneMatch) {
                currentCause.addSuppressed(newCause);
            }
        }
    }

    public Connection connection() {
        return connection;
    }

    private void ensureCanRunQueries() {
        executeWithLock(lock, () -> {
            if (state == State.COMMITTED) {
                throw new ClientException("Cannot run more queries in this transaction, it has been committed");
            } else if (state == State.ROLLED_BACK) {
                throw new ClientException("Cannot run more queries in this transaction, it has been rolled back");
            } else if (state == State.TERMINATED) {
                throw new ClientException(
                        "Cannot run more queries in this transaction, "
                                + "it has either experienced an fatal error or was explicitly terminated",
                        causeOfTermination);
            }
        });
    }

    private CompletionStage<Void> doCommitAsync(Throwable cursorFailure) {
        ClientException exception = executeWithLock(
                lock,
                () -> state == State.TERMINATED
                        ? new ClientException(
                                "Transaction can't be committed. "
                                        + "It has been rolled back either because of an error or explicit termination",
                                cursorFailure != causeOfTermination ? causeOfTermination : null)
                        : null);
        return exception != null
                ? failedFuture(exception)
                : protocol.commitTransaction(connection).thenAccept(bookmarkHolder::setBookmark);
    }

    private CompletionStage<Void> doRollbackAsync() {
        return executeWithLock(lock, () -> state) == State.TERMINATED
                ? completedWithNull()
                : protocol.rollbackTransaction(connection);
    }

    private static BiFunction<Void, Throwable, Void> handleCommitOrRollback(Throwable cursorFailure) {
        return (ignore, commitOrRollbackError) -> {
            CompletionException combinedError = combineErrors(cursorFailure, commitOrRollbackError);
            if (combinedError != null) {
                throw combinedError;
            }
            return null;
        };
    }

    private void handleTransactionCompletion(boolean commitAttempt, Throwable throwable) {
        log.trace(
                "handleTransactionCompletion(commitAttempt=%b, throwable is null=%b)",
                commitAttempt, throwable == null);
        executeWithLock(lock, () -> {
            log.trace("handleTransactionCompletion lock acquired");
            if (commitAttempt && throwable == null) {
                state = State.COMMITTED;
            } else {
                state = State.ROLLED_BACK;
            }
        });
        log.trace("handleTransactionCompletion lock released");
        if (throwable instanceof AuthorizationExpiredException) {
            connection.terminateAndRelease(AuthorizationExpiredException.DESCRIPTION);
        } else if (throwable instanceof ConnectionReadTimeoutException) {
            connection.terminateAndRelease(throwable.getMessage());
        } else {
            connection.release(); // release in background
        }
        log.trace("handleTransactionCompletion finished");
    }

    private CompletionStage<Void> closeAsync(boolean commit, boolean completeWithNullIfNotOpen) {
        log.trace("closeAsync(commit=%b, completeWithNullIfNotOpen=%b) before lock", commit, completeWithNullIfNotOpen);
        CompletionStage<Void> stage = executeWithLock(lock, () -> {
            log.trace("closeAsync lock acquired");
            CompletionStage<Void> resultStage = null;
            if (completeWithNullIfNotOpen && !isOpen()) {
                log.trace("closeAsync will complete with null");
                resultStage = completedWithNull();
            } else if (state == State.COMMITTED) {
                log.trace("closeAsync state=%s", state);
                resultStage = failedFuture(
                        new ClientException(commit ? CANT_COMMIT_COMMITTED_MSG : CANT_ROLLBACK_COMMITTED_MSG));
            } else if (state == State.ROLLED_BACK) {
                log.trace("closeAsync state=%s", state);
                resultStage = failedFuture(
                        new ClientException(commit ? CANT_COMMIT_ROLLED_BACK_MSG : CANT_ROLLBACK_ROLLED_BACK_MSG));
            } else {
                log.trace("closeAsync state=%s", state);
                if (commit) {
                    if (rollbackFuture != null) {
                        log.trace("closeAsync rollbackFuture not null");
                        resultStage = failedFuture(new ClientException(CANT_COMMIT_ROLLING_BACK_MSG));
                    } else if (commitFuture != null) {
                        log.trace("closeAsync commitFuture not null");
                        resultStage = commitFuture;
                    } else {
                        log.trace("closeAsync initializing commitFuture");
                        commitFuture = new CompletableFuture<>();
                    }
                } else {
                    if (commitFuture != null) {
                        log.trace("closeAsync commitFuture not null");
                        resultStage = failedFuture(new ClientException(CANT_ROLLBACK_COMMITTING_MSG));
                    } else if (rollbackFuture != null) {
                        log.trace("closeAsync rollbackFuture not null");
                        resultStage = rollbackFuture;
                    } else {
                        log.trace("closeAsync initializing rollbackFuture");
                        rollbackFuture = new CompletableFuture<>();
                    }
                }
            }
            return resultStage;
        });
        log.trace("closeAsync lock released");

        if (stage == null) {
            log.trace("closeAsync stage is null");
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

        log.trace("closeAsync finished");
        return stage;
    }
}

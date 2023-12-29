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
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.completionExceptionCause;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.BookmarkManager;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.TransactionNestingException;
import org.neo4j.driver.exceptions.UnsupportedFeatureException;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.FailableCursor;
import org.neo4j.driver.internal.NotificationConfigMapper;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.DatabaseNameUtil;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.TelemetryApi;
import org.neo4j.driver.internal.bolt.api.exception.MinVersionAcquisitionException;
import org.neo4j.driver.internal.bolt.api.summary.DiscardSummary;
import org.neo4j.driver.internal.bolt.api.summary.PullSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.cursor.DisposableResultCursorImpl;
import org.neo4j.driver.internal.cursor.ResultCursorImpl;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.cursor.RxResultCursorImpl;
import org.neo4j.driver.internal.logging.PrefixedLogger;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;
import org.neo4j.driver.internal.util.ErrorUtil;
import org.neo4j.driver.internal.util.Futures;

public class NetworkSession {
    private final BoltConnectionProvider boltConnectionProvider;
    private final NetworkSessionConnectionContext connectionContext;
    private final AccessMode mode;
    private final RetryLogic retryLogic;
    private final Logging logging;
    protected final Logger log;

    private final long fetchSize;
    private volatile CompletionStage<UnmanagedTransaction> transactionStage = completedWithNull();
    private volatile CompletionStage<BoltConnection> connectionStage = completedWithNull();
    private volatile CompletionStage<? extends FailableCursor> resultCursorStage = completedWithNull();

    private final AtomicBoolean open = new AtomicBoolean(true);
    private final BookmarkManager bookmarkManager;
    private volatile Set<Bookmark> lastUsedBookmarks = Collections.emptySet();
    private volatile Set<Bookmark> lastReceivedBookmarks;
    private final NotificationConfig driverNotificationConfig;
    private final NotificationConfig notificationConfig;
    private final boolean telemetryDisabled;
    private final AuthTokenManager authTokenManager;

    public NetworkSession(
            BoltConnectionProvider boltConnectionProvider,
            RetryLogic retryLogic,
            DatabaseName databaseName,
            AccessMode mode,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            long fetchSize,
            Logging logging,
            BookmarkManager bookmarkManager,
            org.neo4j.driver.NotificationConfig driverNotificationConfig,
            org.neo4j.driver.NotificationConfig notificationConfig,
            AuthToken overrideAuthToken,
            boolean telemetryDisabled,
            AuthTokenManager authTokenManager) {
        Objects.requireNonNull(bookmarks, "bookmarks may not be null");
        Objects.requireNonNull(bookmarkManager, "bookmarkManager may not be null");
        this.boltConnectionProvider = Objects.requireNonNull(boltConnectionProvider);
        this.mode = mode;
        this.retryLogic = retryLogic;
        this.logging = logging;
        this.log = new PrefixedLogger("[" + hashCode() + "]", logging.getLog(getClass()));
        var databaseNameFuture = databaseName
                .databaseName()
                .map(ignored -> CompletableFuture.completedFuture(databaseName))
                .orElse(new CompletableFuture<>());
        this.bookmarkManager = bookmarkManager;
        this.lastReceivedBookmarks = bookmarks;
        this.connectionContext = new NetworkSessionConnectionContext(
                databaseNameFuture, determineBookmarks(false), impersonatedUser, overrideAuthToken);
        this.fetchSize = fetchSize;
        this.driverNotificationConfig = NotificationConfigMapper.map(driverNotificationConfig);
        this.notificationConfig = NotificationConfigMapper.map(notificationConfig);
        this.telemetryDisabled = telemetryDisabled;
        this.authTokenManager = authTokenManager;
    }

    public CompletionStage<ResultCursor> runAsync(Query query, TransactionConfig config) {
        return ensureNoOpenTxBeforeRunningQuery()
                .thenCompose(ignore -> acquireConnection(mode))
                .thenCompose(connection -> {
                    var cursorFuture = new CompletableFuture<DisposableResultCursorImpl>();
                    var parameters = query.parameters().asMap(Values::value);
                    var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.AUTO_COMMIT_TRANSACTION);
                    apiTelemetryWork.setEnabled(!telemetryDisabled);
                    var session = this;
                    var cursorStage = apiTelemetryWork
                            .pipelineTelemetryIfEnabled(connection)
                            .thenCompose(conn -> conn.runInAutoCommitTransaction(
                                    connectionContext.databaseNameFuture.getNow(null),
                                    asBoltAccessMode(mode),
                                    connectionContext.impersonatedUser,
                                    determineBookmarks(true).stream()
                                            .map(Bookmark::value)
                                            .collect(Collectors.toSet()),
                                    query.text(),
                                    parameters,
                                    config.timeout(),
                                    config.metadata(),
                                    notificationConfig))
                            .thenCompose(conn -> conn.pull(-1, fetchSize))
                            .thenCompose(conn -> conn.flush(new ResponseHandler() {
                                @Override
                                public void onError(Throwable throwable) {
                                    throwable = Futures.completionExceptionCause(throwable);
                                    if (throwable instanceof IllegalStateException) {
                                        throwable = ErrorUtil.newConnectionTerminatedError();
                                    }
                                    if (!cursorFuture.completeExceptionally(throwable)) {
                                        cursorFuture.getNow(null).delegate().onError(throwable);
                                    }
                                }

                                @Override
                                public void onRunSummary(RunSummary summary) {
                                    cursorFuture.complete(new DisposableResultCursorImpl(new ResultCursorImpl(
                                            connection,
                                            query,
                                            fetchSize,
                                            null,
                                            session::handleNewBookmark,
                                            true,
                                            summary,
                                            () -> null)));
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
                            }))
                            .thenCompose(flushResult -> cursorFuture)
                            .handle((resultCursor, throwable) -> {
                                var error = completionExceptionCause(throwable);
                                if (error != null) {
                                    return connection
                                            .close()
                                            .<DisposableResultCursorImpl>handle((ignored, closeError) -> {
                                                if (closeError != null) {
                                                    error.addSuppressed(closeError);
                                                }
                                                if (error instanceof RuntimeException runtimeException) {
                                                    throw runtimeException;
                                                } else {
                                                    throw new CompletionException(error);
                                                }
                                            });
                                } else {
                                    return CompletableFuture.completedStage(resultCursor);
                                }
                            })
                            .thenCompose(Function.identity());
                    resultCursorStage = cursorStage.exceptionally(error -> null);
                    return cursorStage.thenApply(Function.identity());
                });
    }

    public CompletionStage<RxResultCursor> runRx(
            Query query, TransactionConfig config, CompletionStage<RxResultCursor> cursorPublishStage) {

        return ensureNoOpenTxBeforeRunningQuery()
                .thenCompose(ignore -> acquireConnection(mode))
                .thenCompose(connection -> {
                    var cursorFuture = new CompletableFuture<RxResultCursor>();
                    var parameters = query.parameters().asMap(Values::value);
                    var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.AUTO_COMMIT_TRANSACTION);
                    apiTelemetryWork.setEnabled(!telemetryDisabled);
                    var session = this;
                    var cursorStage = apiTelemetryWork
                            .pipelineTelemetryIfEnabled(connection)
                            .thenCompose(conn -> conn.runInAutoCommitTransaction(
                                    connectionContext.databaseNameFuture.getNow(null),
                                    asBoltAccessMode(mode),
                                    connectionContext.impersonatedUser,
                                    determineBookmarks(true).stream()
                                            .map(Bookmark::value)
                                            .collect(Collectors.toSet()),
                                    query.text(),
                                    parameters,
                                    config.timeout(),
                                    config.metadata(),
                                    notificationConfig))
                            .thenCompose(conn -> conn.flush(new ResponseHandler() {
                                @Override
                                public void onError(Throwable throwable) {
                                    throwable = Futures.completionExceptionCause(throwable);
                                    if (throwable instanceof IllegalStateException) {
                                        throwable = ErrorUtil.newConnectionTerminatedError();
                                    }
                                    cursorFuture.completeExceptionally(throwable);
                                }

                                @Override
                                public void onRunSummary(RunSummary summary) {
                                    cursorFuture.complete(new RxResultCursorImpl(
                                            connection,
                                            query,
                                            summary,
                                            session::handleNewBookmark,
                                            (ignored) -> {},
                                            true,
                                            () -> null));
                                }
                            }))
                            .thenCompose(flushResult -> cursorFuture)
                            .handle((resultCursor, throwable) -> {
                                var error = completionExceptionCause(throwable);
                                if (error != null) {
                                    return connection.close().<RxResultCursor>handle((ignored, closeError) -> {
                                        if (closeError != null) {
                                            error.addSuppressed(closeError);
                                        }
                                        if (error instanceof RuntimeException runtimeException) {
                                            throw runtimeException;
                                        } else {
                                            throw new CompletionException(error);
                                        }
                                    });
                                } else {
                                    return CompletableFuture.completedStage(resultCursor);
                                }
                            })
                            .thenCompose(Function.identity());
                    resultCursorStage = cursorStage.exceptionally(error -> null);
                    return cursorStage.thenApply(Function.identity());
                });
    }

    public CompletionStage<UnmanagedTransaction> beginTransactionAsync(
            TransactionConfig config, ApiTelemetryWork apiTelemetryWork) {
        return beginTransactionAsync(mode, config, null, apiTelemetryWork, true);
    }

    public CompletionStage<UnmanagedTransaction> beginTransactionAsync(
            TransactionConfig config, String txType, ApiTelemetryWork apiTelemetryWork) {
        return this.beginTransactionAsync(mode, config, txType, apiTelemetryWork, true);
    }

    public CompletionStage<UnmanagedTransaction> beginTransactionAsync(
            org.neo4j.driver.AccessMode mode, TransactionConfig config, ApiTelemetryWork apiTelemetryWork) {
        return beginTransactionAsync(mode, config, null, apiTelemetryWork, true);
    }

    public CompletionStage<UnmanagedTransaction> beginTransactionAsync(
            org.neo4j.driver.AccessMode mode,
            TransactionConfig config,
            String txType,
            ApiTelemetryWork apiTelemetryWork,
            boolean flush) {
        ensureSessionIsOpen();

        apiTelemetryWork.setEnabled(!telemetryDisabled);

        // create a chain that acquires connection and starts a transaction
        var newTransactionStage = ensureNoOpenTxBeforeStartingTx()
                .thenCompose(ignore -> acquireConnection(mode))
                //                .thenApply(connection -> ImpersonationUtil.ensureImpersonationSupport(connection,
                // connection.impersonatedUser()))
                .thenCompose(connection -> {
                    var tx = new UnmanagedTransaction(
                            connection,
                            connectionContext.databaseNameFuture.getNow(null),
                            asBoltAccessMode(mode),
                            connectionContext.impersonatedUser,
                            this::handleNewBookmark,
                            fetchSize,
                            notificationConfig,
                            apiTelemetryWork,
                            logging);
                    return tx.beginAsync(determineBookmarks(true), config, txType, flush);
                });

        // update the reference to the only known transaction
        var currentTransactionStage = transactionStage;

        transactionStage = newTransactionStage
                .exceptionally(error -> null) // ignore errors from starting new transaction
                .thenCompose(tx -> {
                    if (tx == null) {
                        // failed to begin new transaction, keep reference to the existing one
                        return currentTransactionStage;
                    }
                    // new transaction started, keep reference to it
                    return completedFuture(tx);
                });

        return newTransactionStage;
    }

    public CompletionStage<Void> resetAsync() {
        var terminationException = new AtomicReference<Throwable>();
        return existingTransactionOrNull()
                .thenAccept(tx -> {
                    if (tx != null) {
                        terminationException.set(tx.markTerminated(null));
                    }
                })
                .thenCompose(ignore -> connectionStage)
                .thenCompose(connection -> {
                    if (connection != null) {
                        // there exists an active connection, send a RESET message over it
                        //                        return connection.reset(terminationException.get());
                    }
                    return completedWithNull();
                });
    }

    public RetryLogic retryLogic() {
        return retryLogic;
    }

    public Set<Bookmark> lastBookmarks() {
        return lastReceivedBookmarks;
    }

    public CompletionStage<Void> releaseConnectionAsync() {
        return connectionStage.thenCompose(connection -> {
            if (connection != null) {
                // there exists connection, try to release it back to the pool
                return connection.close();
            }
            // no connection so return null
            return completedWithNull();
        });
    }

    public CompletionStage<BoltConnection> connectionAsync() {
        return connectionStage;
    }

    public boolean isOpen() {
        return open.get();
    }

    public CompletionStage<Void> closeAsync() {
        if (open.compareAndSet(true, false)) {
            return resultCursorStage
                    .thenCompose(cursor -> {
                        if (cursor != null) {
                            // there exists a cursor with potentially unconsumed error, try to extract and propagate it
                            return cursor.discardAllFailureAsync();
                        }
                        // no result cursor exists so no error exists
                        return completedWithNull();
                    })
                    .thenCompose(cursorError -> closeTransactionAndReleaseConnection()
                            .thenApply(txCloseError -> {
                                // now we have cursor error, active transaction has been closed and connection has been
                                // released
                                // back to the pool; try to propagate cursor and transaction close errors, if any
                                var combinedError = Futures.combineErrors(cursorError, txCloseError);
                                if (combinedError != null) {
                                    throw combinedError;
                                }
                                return null;
                            }));
        }
        return completedWithNull();
    }

    //    protected CompletionStage<Boolean> currentConnectionIsOpen() {
    //        return connectionStage.handle((connection, error) -> error == null
    //                && // no acquisition error
    //                connection != null
    //                && // some connection has actually been acquired
    //                connection.isOpen()); // and it's still open
    //    }

    private org.neo4j.driver.internal.bolt.api.AccessMode asBoltAccessMode(AccessMode mode) {
        return switch (mode) {
            case WRITE -> org.neo4j.driver.internal.bolt.api.AccessMode.WRITE;
            case READ -> org.neo4j.driver.internal.bolt.api.AccessMode.READ;
        };
    }

    private CompletionStage<BoltConnection> acquireConnection(AccessMode mode) {
        var currentConnectionStage = connectionStage;

        var newConnectionStage = resultCursorStage
                .thenCompose(cursor -> {
                    if (cursor == null) {
                        return completedWithNull();
                    }
                    // make sure previous result is fully consumed and connection is released back to the pool
                    return cursor.pullAllFailureAsync();
                })
                .thenCompose(error -> {
                    if (error == null) {
                        // there is no unconsumed error, so one of the following is true:
                        //   1) this is first time connection is acquired in this session
                        //   2) previous result has been successful and is fully consumed
                        //   3) previous result failed and error has been consumed

                        // return existing connection, which should've been released back to the pool by now
                        return currentConnectionStage.exceptionally(ignore -> null);
                    } else {
                        // there exists unconsumed error, re-throw it
                        throw new CompletionException(error);
                    }
                })
                .thenCompose(existingConnection -> {
                    //                    if (existingConnection != null && existingConnection.isOpen()) {
                    //                        // there somehow is an existing open connection, this should not happen,
                    // just a precondition
                    //                        throw new IllegalStateException("Existing open connection detected");
                    //                    }
                    var databaseName = connectionContext.databaseNameFuture.getNow(null);

                    Supplier<CompletionStage<Map<String, Value>>> tokenStageSupplier;
                    BoltProtocolVersion minVersion = null;
                    if (connectionContext.impersonatedUser() != null) {
                        minVersion = new BoltProtocolVersion(4, 4);
                    }
                    var overrideAuthToken = connectionContext.overrideAuthToken();
                    if (overrideAuthToken != null) {
                        tokenStageSupplier = () -> CompletableFuture.completedStage(connectionContext.authToken)
                                .thenApply(token -> ((InternalAuthToken) token).toMap());
                        minVersion = new BoltProtocolVersion(5, 1);
                    } else {
                        tokenStageSupplier = () ->
                                authTokenManager.getToken().thenApply(token -> ((InternalAuthToken) token).toMap());
                    }
                    return boltConnectionProvider
                            .connect(
                                    databaseName,
                                    tokenStageSupplier,
                                    switch (mode) {
                                        case WRITE -> org.neo4j.driver.internal.bolt.api.AccessMode.WRITE;
                                        case READ -> org.neo4j.driver.internal.bolt.api.AccessMode.READ;
                                    },
                                    connectionContext.rediscoveryBookmarks().stream()
                                            .map(Bookmark::value)
                                            .collect(Collectors.toSet()),
                                    connectionContext.impersonatedUser(),
                                    minVersion,
                                    driverNotificationConfig,
                                    (name) -> connectionContext
                                            .databaseNameFuture()
                                            .complete(name == null ? DatabaseNameUtil.defaultDatabase() : name))
                            .thenApply(connection -> (BoltConnection) new BoltConnectionWithAuthTokenManager(
                                    connection,
                                    overrideAuthToken != null
                                            ? new AuthTokenManager() {
                                                @Override
                                                public CompletionStage<AuthToken> getToken() {
                                                    return null;
                                                }

                                                @Override
                                                public boolean handleSecurityException(
                                                        AuthToken authToken, SecurityException exception) {
                                                    return false;
                                                }
                                            }
                                            : authTokenManager))
                            .exceptionally(throwable -> {
                                throwable = Futures.completionExceptionCause(throwable);
                                if (throwable instanceof TimeoutException) {
                                    throw new ClientException(throwable.getMessage(), throwable);
                                }
                                if (throwable
                                        instanceof MinVersionAcquisitionException minVersionAcquisitionException) {
                                    if (overrideAuthToken == null && connectionContext.impersonatedUser() != null) {
                                        throw new ClientException(
                                                "Detected connection that does not support impersonation, please make sure to have all servers running 4.4 version or above and communicating"
                                                        + " over Bolt version 4.4 or above when using impersonation feature");
                                    } else {
                                        throw new CompletionException(new UnsupportedFeatureException(String.format(
                                                "Detected Bolt %s connection that does not support the auth token override feature, please make sure to have all servers communicating over Bolt 5.1 or above to use the feature",
                                                minVersionAcquisitionException.version())));
                                    }
                                } else {
                                    throw new CompletionException(throwable);
                                }
                            });
                });

        connectionStage = newConnectionStage.exceptionally(error -> null);

        return newConnectionStage;
    }

    private CompletionStage<Throwable> closeTransactionAndReleaseConnection() {
        return existingTransactionOrNull()
                .thenCompose(tx -> {
                    if (tx != null) {
                        // there exists an open transaction, let's close it and propagate the error, if any
                        return tx.closeAsync()
                                .thenApply(ignore -> (Throwable) null)
                                .exceptionally(Function.identity());
                    }
                    // no open transaction so nothing to close
                    return completedWithNull();
                })
                .thenCompose(txCloseError ->
                        // then release the connection and propagate transaction close error, if any
                        releaseConnectionAsync().thenApply(ignore -> txCloseError));
    }

    private CompletionStage<Void> ensureNoOpenTxBeforeRunningQuery() {
        return ensureNoOpenTx("Queries cannot be run directly on a session with an open transaction; "
                + "either run from within the transaction or use a different session.");
    }

    private CompletionStage<Void> ensureNoOpenTxBeforeStartingTx() {
        return ensureNoOpenTx("You cannot begin a transaction on a session with an open transaction; "
                + "either run from within the transaction or use a different session.");
    }

    private CompletionStage<Void> ensureNoOpenTx(String errorMessage) {
        return existingTransactionOrNull().thenAccept(tx -> {
            if (tx != null) {
                throw new TransactionNestingException(errorMessage);
            }
        });
    }

    private CompletionStage<UnmanagedTransaction> existingTransactionOrNull() {
        return transactionStage
                .exceptionally(error -> null) // handle previous connection acquisition and tx begin failures
                .thenApply(tx -> tx != null && tx.isOpen() ? tx : null);
    }

    private void ensureSessionIsOpen() {
        if (!open.get()) {
            throw new ClientException(
                    "No more interaction with this session are allowed as the current session is already closed. ");
        }
    }

    private void handleNewBookmark(DatabaseBookmark databaseBookmark) {
        assertDatabaseNameFutureIsDone();
        var bookmark = databaseBookmark.bookmark();
        if (bookmark != null) {
            var bookmarks = Set.of(bookmark);
            lastReceivedBookmarks = bookmarks;
            bookmarkManager.updateBookmarks(lastUsedBookmarks, bookmarks);
        }
    }

    private Set<Bookmark> determineBookmarks(boolean updateLastUsed) {
        var bookmarks = new HashSet<>(bookmarkManager.getBookmarks());
        if (updateLastUsed) {
            lastUsedBookmarks = Collections.unmodifiableSet(bookmarks);
        }
        bookmarks.addAll(lastReceivedBookmarks);
        return bookmarks;
    }

    private void assertDatabaseNameFutureIsDone() {
        if (!connectionContext.databaseNameFuture().isDone()) {
            throw new IllegalStateException("Illegal internal state encountered, database name future is not done.");
        }
    }

    /**
     * The {@link NetworkSessionConnectionContext#mode} can be mutable for a session connection context
     */
    private static class NetworkSessionConnectionContext implements ConnectionContext {
        private final CompletableFuture<DatabaseName> databaseNameFuture;
        private AccessMode mode;

        // These bookmarks are only used for rediscovery.
        // They have to be the initial bookmarks given at the creation of the session.
        // As only those bookmarks could carry extra system bookmarks
        private final Set<Bookmark> rediscoveryBookmarks;
        private final String impersonatedUser;
        private final AuthToken authToken;

        private NetworkSessionConnectionContext(
                CompletableFuture<DatabaseName> databaseNameFuture,
                Set<Bookmark> bookmarks,
                String impersonatedUser,
                AuthToken authToken) {
            this.databaseNameFuture = databaseNameFuture;
            this.rediscoveryBookmarks = bookmarks;
            this.impersonatedUser = impersonatedUser;
            this.authToken = authToken;
        }

        private ConnectionContext contextWithMode(AccessMode mode) {
            this.mode = mode;
            return this;
        }

        @Override
        public CompletableFuture<DatabaseName> databaseNameFuture() {
            return databaseNameFuture;
        }

        @Override
        public AccessMode mode() {
            return mode;
        }

        @Override
        public Set<Bookmark> rediscoveryBookmarks() {
            return rediscoveryBookmarks;
        }

        @Override
        public String impersonatedUser() {
            return impersonatedUser;
        }

        @Override
        public AuthToken overrideAuthToken() {
            return authToken;
        }
    }
}

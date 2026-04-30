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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.neo4j.bolt.connection.AuthTokens;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.RoutedBoltConnectionParameters;
import org.neo4j.bolt.connection.TelemetryApi;
import org.neo4j.bolt.connection.exception.MinVersionAcquisitionException;
import org.neo4j.bolt.connection.message.Message;
import org.neo4j.bolt.connection.message.Messages;
import org.neo4j.bolt.connection.message.RunMessage;
import org.neo4j.bolt.connection.summary.RunSummary;
import org.neo4j.bolt.connection.summary.TelemetrySummary;
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
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.TransactionNestingException;
import org.neo4j.driver.exceptions.UnsupportedFeatureException;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.FailableCursor;
import org.neo4j.driver.internal.GqlStatusError;
import org.neo4j.driver.internal.NotificationConfigMapper;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnection;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnectionSource;
import org.neo4j.driver.internal.adaptedbolt.DriverResponseHandler;
import org.neo4j.driver.internal.cursor.DisposableResultCursorImpl;
import org.neo4j.driver.internal.cursor.ResultCursorImpl;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.cursor.RxResultCursorImpl;
import org.neo4j.driver.internal.homedb.HomeDatabaseCache;
import org.neo4j.driver.internal.homedb.HomeDatabaseCacheKey;
import org.neo4j.driver.internal.logging.PrefixedLogger;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.internal.observation.NoopObservation;
import org.neo4j.driver.internal.observation.Observation;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.value.BoltValueFactory;
import org.neo4j.driver.types.TypeSystem;

public class NetworkSession {
    private final DriverBoltConnectionSource boltConnectionProvider;
    private final NetworkSessionConnectionContext connectionContext;
    private final AccessMode mode;
    private final RetryLogic retryLogic;

    @SuppressWarnings("deprecation")
    private final Logging logging;

    @SuppressWarnings("deprecation")
    protected final Logger log;

    private final long fetchSize;
    private volatile CompletionStage<UnmanagedTransaction> transactionStage = completedWithNull();
    private volatile CompletionStage<BoltConnectionWithCloseTracking> connectionStage = completedWithNull();
    private volatile CompletionStage<? extends FailableCursor> resultCursorStage = completedWithNull();

    private final AtomicBoolean open = new AtomicBoolean(true);
    private final BookmarkManager bookmarkManager;
    private volatile Set<Bookmark> lastUsedBookmarks = Collections.emptySet();
    private volatile Set<Bookmark> lastReceivedBookmarks;
    private final NotificationConfig notificationConfig;
    private final boolean telemetryDisabled;
    private final AuthTokenManager authTokenManager;
    private final HomeDatabaseCache homeDatabaseCache;
    private final HomeDatabaseCacheKey homeDatabaseKey;
    private final boolean autoCommitRetriesDisabled;
    private final DriverObservationProvider observationProvider;

    public NetworkSession(
            DriverBoltConnectionSource boltConnectionProvider,
            RetryLogic retryLogic,
            DatabaseName databaseName,
            AccessMode mode,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            long fetchSize,
            @SuppressWarnings("deprecation") Logging logging,
            BookmarkManager bookmarkManager,
            @SuppressWarnings("deprecation") org.neo4j.driver.NotificationConfig notificationConfig,
            AuthToken overrideAuthToken,
            boolean telemetryDisabled,
            AuthTokenManager authTokenManager,
            HomeDatabaseCache homeDatabaseCache,
            boolean autoCommitRetriesDisabled,
            DriverObservationProvider observationProvider) {
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
        this.notificationConfig = NotificationConfigMapper.map(notificationConfig);
        this.telemetryDisabled = telemetryDisabled;
        this.authTokenManager = authTokenManager;
        this.homeDatabaseCache = Objects.requireNonNull(homeDatabaseCache);
        this.homeDatabaseKey = HomeDatabaseCacheKey.of(overrideAuthToken, impersonatedUser);
        this.autoCommitRetriesDisabled = autoCommitRetriesDisabled;
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    public CompletionStage<ResultCursor> runAsync(
            Query query, TransactionConfig config, Observation parentObservation, Class<?> resultType) {
        ensureSessionIsOpen();
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.AUTO_COMMIT_TRANSACTION);
        apiTelemetryWork.setEnabled(!telemetryDisabled);
        var disposable = ensureNoOpenTxBeforeRunningQuery()
                .thenCompose(ignored -> autoCommitRun(
                        query,
                        config,
                        apiTelemetryWork,
                        autoCommitRetriesDisabled,
                        false,
                        parentObservation,
                        resultType))
                .thenApply(DisposableResultCursorImpl::new);
        resultCursorStage = disposable.exceptionally(error -> null);
        return disposable.thenApply(Function.identity());
    }

    private CompletionStage<ResultCursorImpl> autoCommitRun(
            Query query,
            TransactionConfig config,
            ApiTelemetryWork apiTelemetryWork,
            boolean autoCommitRetriesDisabled,
            boolean skipPull,
            Observation parentObservation,
            Class<?> resultType) {
        return acquireConnection(mode, parentObservation, skipPull).thenCompose(connection -> {
            var parameters = query.parameters().asMap(Values::value);
            var resultCursor = new ResultCursorImpl(
                    connection,
                    query,
                    fetchSize,
                    this::handleNewBookmark,
                    true,
                    null,
                    this::handleDatabaseName,
                    apiTelemetryWork,
                    observationProvider,
                    resultType);
            var telemetryEnabled = apiTelemetryWork.getTelemetryMessageIfEnabled(connection);
            var mayRetry = new AtomicBoolean();
            return CompletableFuture.completedStage(null)
                    .thenCompose(ignored -> {
                        var messages = new ArrayList<Message>(3);
                        telemetryEnabled.ifPresent(messages::add);
                        messages.add(newRunMessage(connection, query, parameters, config));
                        messages.add(Messages.pull(-1, fetchSize));
                        return connection.writeAndFlush(resultCursor, messages, parentObservation);
                    })
                    .thenCompose(ignored -> resultCursor.resultCursor().exceptionally(resultCursorThrowable -> {
                        if (telemetryEnabled.isPresent()) {
                            if (apiTelemetryWork.acknowledged().get()) {
                                mayRetry.set(isIdempotent(completionExceptionCause(resultCursorThrowable)));
                            }
                        } else {
                            mayRetry.set(isIdempotent(completionExceptionCause(resultCursorThrowable)));
                        }
                        if (resultCursorThrowable instanceof RuntimeException runtimeException) {
                            throw runtimeException;
                        } else {
                            throw new CompletionException(resultCursorThrowable);
                        }
                    }))
                    .handle((resultCursorImpl, throwable) -> {
                        var error = completionExceptionCause(throwable);
                        if (error != null) {
                            return connection
                                    .close()
                                    .handle((ignored, closeError) -> {
                                        if (closeError != null) {
                                            error.addSuppressed(closeError);
                                        }
                                        if (!autoCommitRetriesDisabled && mayRetry.get()) {
                                            return autoCommitRun(
                                                    query,
                                                    config,
                                                    apiTelemetryWork,
                                                    true,
                                                    true,
                                                    parentObservation,
                                                    resultType);
                                        }
                                        if (error instanceof RuntimeException runtimeException) {
                                            throw runtimeException;
                                        } else {
                                            throw new CompletionException(error);
                                        }
                                    })
                                    .thenCompose(Function.identity());
                        } else {
                            return CompletableFuture.completedStage(resultCursorImpl);
                        }
                    })
                    .thenCompose(Function.identity());
        });
    }

    public CompletionStage<RxResultCursor> runRx(
            Query query,
            TransactionConfig config,
            CompletionStage<RxResultCursor> cursorPublishStage,
            Observation parentObservation) {
        ensureSessionIsOpen();
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.AUTO_COMMIT_TRANSACTION);
        apiTelemetryWork.setEnabled(!telemetryDisabled);
        var newResultCursorStage = ensureNoOpenTxBeforeRunningQuery()
                .thenCompose(ignore -> autoCommitRunRx(
                        query, config, apiTelemetryWork, autoCommitRetriesDisabled, false, parentObservation));
        resultCursorStage = newResultCursorStage
                .thenCompose(cursor -> cursor == null ? CompletableFuture.completedFuture(null) : cursorPublishStage)
                .exceptionally(throwable -> null);
        return newResultCursorStage;
    }

    private CompletionStage<RxResultCursor> autoCommitRunRx(
            Query query,
            TransactionConfig config,
            ApiTelemetryWork apiTelemetryWork,
            boolean autoCommitRetriesDisabled,
            boolean skipPull,
            Observation parentObservation) {
        return acquireConnection(mode, parentObservation, skipPull).thenCompose(connection -> {
            var parameters = query.parameters().asMap(Values::value);
            var runFailed = new AtomicBoolean(false);
            var telemetryEnabled = apiTelemetryWork.getTelemetryMessageIfEnabled(connection);
            var responseHandler = new RunRxResponseHandler(
                    logging,
                    connection,
                    query,
                    this::handleNewBookmark,
                    runFailed,
                    this::handleDatabaseName,
                    apiTelemetryWork);
            return CompletableFuture.completedStage(null)
                    .thenCompose(ignored -> {
                        var messages = new ArrayList<Message>(2);
                        telemetryEnabled.ifPresent(messages::add);
                        messages.add(newRunMessage(connection, query, parameters, config));
                        return connection.writeAndFlush(responseHandler, messages, parentObservation);
                    })
                    .thenCompose(ignored -> responseHandler.cursorFuture)
                    .handle((resultCursor, throwable) -> {
                        if (resultCursor != null) {
                            throwable = responseHandler.error;
                        }
                        var error = completionExceptionCause(throwable);
                        if (error != null) {
                            return connection
                                    .close()
                                    .handle((ignored, closeError) -> {
                                        if (closeError != null) {
                                            error.addSuppressed(closeError);
                                        }
                                        var mayRetry = false;
                                        if (telemetryEnabled.isPresent()) {
                                            if (apiTelemetryWork.acknowledged().get()) {
                                                mayRetry = isIdempotent(error);
                                            }
                                        } else {
                                            mayRetry = isIdempotent(error);
                                        }
                                        if (!autoCommitRetriesDisabled && mayRetry) {
                                            return autoCommitRunRx(
                                                    query, config, apiTelemetryWork, true, true, parentObservation);
                                        }
                                        if (error instanceof RuntimeException runtimeException) {
                                            throw runtimeException;
                                        } else {
                                            throw new CompletionException(error);
                                        }
                                    })
                                    .thenCompose(Function.identity());
                        } else if (runFailed.get()) {
                            return connection.close().handle((ignored1, ignored2) -> resultCursor);
                        } else {
                            return CompletableFuture.completedStage(resultCursor);
                        }
                    })
                    .thenCompose(Function.identity());
        });
    }

    public CompletionStage<UnmanagedTransaction> beginTransactionAsync(
            TransactionConfig config, ApiTelemetryWork apiTelemetryWork, Observation parentObservation) {
        return beginTransactionAsync(mode, config, null, apiTelemetryWork, true, parentObservation);
    }

    public CompletionStage<UnmanagedTransaction> beginTransactionAsync(
            TransactionConfig config, String txType, ApiTelemetryWork apiTelemetryWork, Observation parentObservation) {
        return this.beginTransactionAsync(mode, config, txType, apiTelemetryWork, true, parentObservation);
    }

    public CompletionStage<UnmanagedTransaction> beginTransactionAsync(
            org.neo4j.driver.AccessMode mode,
            TransactionConfig config,
            ApiTelemetryWork apiTelemetryWork,
            Observation parentObservation) {
        return beginTransactionAsync(mode, config, null, apiTelemetryWork, true, parentObservation);
    }

    public CompletionStage<UnmanagedTransaction> beginTransactionAsync(
            org.neo4j.driver.AccessMode mode,
            TransactionConfig config,
            String txType,
            ApiTelemetryWork apiTelemetryWork,
            boolean flush,
            Observation parentObservation) {
        ensureSessionIsOpen();

        apiTelemetryWork.setEnabled(!telemetryDisabled);

        // create a chain that acquires connection and starts a transaction
        var newTransactionStage = ensureNoOpenTxBeforeStartingTx()
                .thenCompose(ignore -> acquireConnection(mode, parentObservation, false))
                .thenCompose(connection -> {
                    var tx = new UnmanagedTransaction(
                            connection,
                            connectionContext.databaseNameFuture.getNow(DatabaseName.defaultDatabase()),
                            asBoltAccessMode(mode),
                            connectionContext.impersonatedUser,
                            this::handleNewBookmark,
                            fetchSize,
                            notificationConfig,
                            apiTelemetryWork,
                            this::handleDatabaseName,
                            logging,
                            observationProvider);
                    return tx.beginAsync(determineBookmarks(true), config, txType, flush, parentObservation);
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
        return existingTransactionOrNull()
                .thenAccept(tx -> {
                    if (tx != null) {
                        tx.markTerminated(null);
                    }
                })
                .thenCompose(ignore -> connectionStage)
                .thenCompose(connection -> {
                    if (connection != null && connection.isOpen()) {
                        var future = new CompletableFuture<Void>();
                        return connection
                                .writeAndFlush(
                                        new DriverResponseHandler() {
                                            @Override
                                            public void onError(Throwable throwable) {
                                                future.completeExceptionally(throwable);
                                            }

                                            @Override
                                            public void onComplete() {
                                                future.complete(null);
                                            }
                                        },
                                        Messages.reset(),
                                        NoopObservation.getInstance())
                                .thenCompose(ignored -> future);
                    } else {
                        return completedWithNull();
                    }
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

    public CompletionStage<DriverBoltConnection> connectionAsync() {
        return connectionStage.thenApply(Function.identity());
    }

    public boolean isOpen() {
        return open.get();
    }

    public CompletionStage<Void> closeAsync(Observation parentObservation) {
        if (open.compareAndSet(true, false)) {
            return resultCursorStage
                    .thenCompose(cursor -> {
                        if (cursor != null) {
                            // there exists a cursor with potentially unconsumed error, try to extract and propagate it
                            return cursor.discardAllFailureAsync(parentObservation);
                        }
                        // no result cursor exists so no error exists
                        return completedWithNull();
                    })
                    .thenCompose(cursorError -> closeTransactionAndReleaseConnection(parentObservation)
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

    protected CompletionStage<Boolean> currentConnectionIsOpen() {
        return connectionStage.handle((connection, error) -> error == null
                && // no acquisition error
                connection != null
                && // some connection has actually been acquired
                connection.isOpen()); // and it's still open
    }

    private void handleDatabaseName(String name) {
        connectionContext.databaseNameFuture.complete(DatabaseName.database(name));
        homeDatabaseCache.put(homeDatabaseKey, name);
    }

    private CompletionStage<BoltConnectionWithCloseTracking> acquireConnection(
            AccessMode mode, Observation parentObservation, boolean skipPull) {
        var overrideAuthToken = connectionContext.overrideAuthToken();
        var authTokenManager = overrideAuthToken != null ? NoopAuthTokenManager.INSTANCE : this.authTokenManager;
        var newConnectionStage = (skipPull
                        ? CompletableFuture.<BoltConnectionWithCloseTracking>completedStage(null)
                        : pulledResultCursorStage(connectionStage, parentObservation))
                .thenCompose(ignored -> acquireAdaptedConnection(mode, parentObservation))
                .thenApply(connection ->
                        (DriverBoltConnection) new BoltConnectionWithAuthTokenManager(connection, authTokenManager))
                .thenApply(BoltConnectionWithCloseTracking::new)
                .exceptionally(this::mapAcquisitionError);
        connectionStage = newConnectionStage.exceptionally(error -> null);
        return newConnectionStage;
    }

    private BoltConnectionWithCloseTracking mapAcquisitionError(Throwable throwable) {
        throwable = Futures.completionExceptionCause(throwable);
        if (throwable instanceof TimeoutException) {
            throw new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(throwable.getMessage()),
                    "N/A",
                    throwable.getMessage(),
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    throwable);
        }
        if (throwable instanceof MinVersionAcquisitionException minVersionAcquisitionException) {
            if (connectionContext.overrideAuthToken() == null && connectionContext.impersonatedUser() != null) {
                var message =
                        "Detected connection that does not support impersonation, please make sure to have all servers running 4.4 version or above and communicating"
                                + " over Bolt version 4.4 or above when using impersonation feature";
                throw new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(message),
                        "N/A",
                        message,
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        null);
            } else {
                throw new CompletionException(new UnsupportedFeatureException(String.format(
                        "Detected Bolt %s connection that does not support the auth token override feature, please make sure to have all servers communicating over Bolt 5.1 or above to use the feature",
                        minVersionAcquisitionException.version())));
            }
        } else {
            throw new CompletionException(throwable);
        }
    }

    private CompletionStage<DriverBoltConnection> acquireAdaptedConnection(
            AccessMode mode, Observation parentObservation) {
        var databaseName = connectionContext.databaseNameFuture().getNow(null);
        var impersonatedUser = connectionContext.impersonatedUser();
        var minVersion = minBoltVersion(connectionContext);
        var overrideAuthToken = connectionContext.overrideAuthToken() != null
                ? AuthTokens.custom(BoltValueFactory.getInstance()
                        .toBoltMap(((InternalAuthToken) connectionContext.overrideAuthToken()).toMap()))
                : null;
        var accessMode = asBoltAccessMode(mode);
        var bookmarks = connectionContext.rediscoveryBookmarks().stream()
                .map(Bookmark::value)
                .collect(Collectors.toSet());
        Consumer<DatabaseName> databaseNameListener = (name) -> {
            if (name != null) {
                if (databaseName == null) {
                    name.databaseName().ifPresent(n -> homeDatabaseCache.put(homeDatabaseKey, n));
                }
            } else {
                name = DatabaseName.defaultDatabase();
            }
            connectionContext.databaseNameFuture().complete(name);
        };
        var homeDatabaseHint = homeDatabaseCache.get(homeDatabaseKey).orElse(null);
        var parameters = RoutedBoltConnectionParameters.builder()
                .withAuthToken(overrideAuthToken)
                .withMinVersion(minVersion)
                .withAccessMode(accessMode)
                .withDatabaseName(databaseName)
                .withDatabaseNameListener(databaseNameListener)
                .withHomeDatabaseHint(homeDatabaseHint)
                .withBookmarks(bookmarks)
                .withImpersonatedUser(impersonatedUser)
                .build();
        return boltConnectionProvider.getConnection(parameters, parentObservation);
    }

    private CompletionStage<Void> pulledResultCursorStage(
            CompletionStage<BoltConnectionWithCloseTracking> connectionStage, Observation parentObservation) {
        return resultCursorStage
                .thenCompose(cursor -> {
                    if (cursor == null) {
                        return completedWithNull();
                    }
                    // make sure previous result is fully consumed and connection is released back to the pool
                    return cursor.pullAllFailureAsync(parentObservation);
                })
                .thenCompose(error -> {
                    if (error == null) {
                        // there is no unconsumed error, so one of the following is true:
                        //   1) this is first time connection is acquired in this session
                        //   2) previous result has been successful and is fully consumed
                        //   3) previous result failed and error has been consumed

                        // the existing connection should've been released back to the pool by now
                        return connectionStage.handle((ignored, throwable) -> null);
                    } else {
                        // there exists unconsumed error, re-throw it
                        throw new CompletionException(error);
                    }
                });
    }

    private CompletionStage<Throwable> closeTransactionAndReleaseConnection(Observation parentObservation) {
        return existingTransactionOrNull()
                .thenCompose(tx -> {
                    if (tx != null) {
                        // there exists an open transaction, let's close it and propagate the error, if any
                        return tx.closeAsync(parentObservation)
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
            var message =
                    "No more interaction with this session are allowed as the current session is already closed. ";
            throw new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(message),
                    "N/A",
                    message,
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    null);
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

    private RunMessage newRunMessage(
            DriverBoltConnection connection, Query query, Map<String, Value> parameters, TransactionConfig config) {
        return Messages.run(
                connectionContext
                        .databaseNameFuture
                        .getNow(DatabaseName.defaultDatabase())
                        .databaseName()
                        .orElse(null),
                asBoltAccessMode(mode),
                connectionContext.impersonatedUser,
                determineBookmarks(true).stream().map(Bookmark::value).collect(Collectors.toSet()),
                query.text(),
                connection.valueFactory().toBoltMap(parameters),
                config.timeout(),
                connection.valueFactory().toBoltMap(config.metadata()),
                notificationConfig);
    }

    private static BoltProtocolVersion minBoltVersion(NetworkSessionConnectionContext connectionContext) {
        BoltProtocolVersion minBoltVersion = null;
        if (connectionContext.overrideAuthToken() != null) {
            minBoltVersion = new BoltProtocolVersion(5, 1);
        } else if (connectionContext.impersonatedUser() != null) {
            minBoltVersion = new BoltProtocolVersion(4, 4);
        }
        return minBoltVersion;
    }

    private static Supplier<CompletionStage<Map<String, Value>>> tokenStageSupplier(
            AuthToken overrideAuthToken, AuthTokenManager authTokenManager) {
        return overrideAuthToken != null
                ? () -> CompletableFuture.completedStage(overrideAuthToken)
                        .thenApply(token -> ((InternalAuthToken) token).toMap())
                : () -> authTokenManager.getToken().thenApply(token -> ((InternalAuthToken) token).toMap());
    }

    private static org.neo4j.bolt.connection.AccessMode asBoltAccessMode(AccessMode mode) {
        return switch (mode) {
            case WRITE -> org.neo4j.bolt.connection.AccessMode.WRITE;
            case READ -> org.neo4j.bolt.connection.AccessMode.READ;
        };
    }

    private static boolean isIdempotent(Throwable throwable) {
        if (throwable instanceof Neo4jException neo4jException) {
            var val = neo4jException.diagnosticRecord().get("_idempotent");
            if (val != null && val.hasType(TypeSystem.getDefault().BOOLEAN())) {
                return val.asBoolean();
            }
        }
        return false;
    }

    /**
     * The {@link NetworkSessionConnectionContext#mode} can be mutable for a session connection context
     */
    private static class NetworkSessionConnectionContext implements ConnectionContext {
        private final CompletableFuture<DatabaseName> databaseNameFuture;

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

        @Override
        public CompletableFuture<DatabaseName> databaseNameFuture() {
            return databaseNameFuture;
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

    public static class RunRxResponseHandler implements DriverResponseHandler {
        private static final Lock NOOP_LOCK = new NoopLock();
        final CompletableFuture<RxResultCursor> cursorFuture = new CompletableFuture<>();

        @SuppressWarnings("deprecation")
        private final Logging logging;

        private final DriverBoltConnection connection;
        private final Query query;
        private final Consumer<DatabaseBookmark> bookmarkConsumer;
        private final AtomicBoolean runFailed;
        private final Consumer<String> databaseNameConsumer;
        private final ApiTelemetryWork apiTelemetryWork;
        private RunSummary runSummary;
        private Throwable error;
        private int ignoredCount;

        public RunRxResponseHandler(
                @SuppressWarnings("deprecation") Logging logging,
                DriverBoltConnection connection,
                Query query,
                Consumer<DatabaseBookmark> bookmarkConsumer,
                AtomicBoolean runFailed,
                Consumer<String> databaseNameConsumer,
                ApiTelemetryWork apiTelemetryWork) {
            this.logging = logging;
            this.connection = connection;
            this.query = query;
            this.bookmarkConsumer = bookmarkConsumer;
            this.runFailed = runFailed;
            this.databaseNameConsumer = Objects.requireNonNull(databaseNameConsumer);
            this.apiTelemetryWork = apiTelemetryWork;
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
            if (apiTelemetryWork != null) {
                apiTelemetryWork.acknowledge();
            }
        }

        @Override
        public void onRunSummary(RunSummary summary) {
            runSummary = summary;
            summary.databaseName().ifPresent(databaseNameConsumer);
        }

        @Override
        public void onIgnored() {
            ignoredCount++;
        }

        @Override
        public void onComplete() {
            if (runSummary != null || error != null) {
                if (error != null) {
                    runFailed.set(true);
                }
                cursorFuture.complete(
                        new RxResultCursorImpl(connection, query, runSummary, error, bookmarkConsumer, true, logging));
            } else {
                var message = ignoredCount > 0
                        ? "Run exchange contains ignored messages."
                        : "Unexpected state during session run.";
                cursorFuture.completeExceptionally(new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(message),
                        "N/A",
                        message,
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        null));
            }
        }
    }

    private static final class NoopAuthTokenManager implements AuthTokenManager {
        static final NoopAuthTokenManager INSTANCE = new NoopAuthTokenManager();

        @Override
        public CompletionStage<AuthToken> getToken() {
            return null;
        }

        @Override
        public boolean handleSecurityException(AuthToken authToken, SecurityException exception) {
            return false;
        }
    }

    private static class NoopLock implements Lock {
        @Override
        public void lock() {}

        @Override
        public void lockInterruptibly() {}

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) {
            return true;
        }

        @Override
        public void unlock() {}

        @Override
        public Condition newCondition() {
            return null;
        }
    }
}

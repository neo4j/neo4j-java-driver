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
package org.neo4j.driver.internal.bolt.pooledimpl;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltConnectionState;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.MetricsListener;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.bolt.api.exception.MinVersionAcquisitionException;
import org.neo4j.driver.internal.bolt.api.summary.ResetSummary;
import org.neo4j.driver.internal.bolt.pooledimpl.util.FutureUtil;

public class PooledBoltConnectionProvider implements BoltConnectionProvider {
    private final LoggingProvider logging;
    private final System.Logger log;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final BoltConnectionProvider boltConnectionProvider;
    private final List<ConnectionEntry> pooledConnectionEntries;
    private final Queue<CompletableFuture<PooledBoltConnection>> pendingAcquisitions;
    private final int maxSize;
    private final long acquisitionTimeout;
    private final long maxLifetime;
    private final long idleBeforeTest;
    private final Clock clock;
    private MetricsListener metricsListener;
    private CompletionStage<Void> closeStage;
    private BoltServerAddress address;

    private long minAuthTimestamp;

    public PooledBoltConnectionProvider(
            BoltConnectionProvider boltConnectionProvider,
            int maxSize,
            long acquisitionTimeout,
            long maxLifetime,
            long idleBeforeTest,
            Clock clock,
            LoggingProvider logging) {
        this.boltConnectionProvider = boltConnectionProvider;
        this.pooledConnectionEntries = new ArrayList<>(maxSize);
        this.pendingAcquisitions = new ArrayDeque<>(100);
        this.maxSize = maxSize;
        this.acquisitionTimeout = acquisitionTimeout;
        this.maxLifetime = maxLifetime;
        this.idleBeforeTest = idleBeforeTest;
        this.clock = Objects.requireNonNull(clock);
        this.logging = Objects.requireNonNull(logging);
        this.log = logging.getLog(getClass());
    }

    @Override
    public CompletionStage<Void> init(
            BoltServerAddress address,
            SecurityPlan securityPlan,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            MetricsListener metricsListener) {
        this.address = Objects.requireNonNull(address);
        this.metricsListener = Objects.requireNonNull(metricsListener);
        metricsListener.registerPoolMetrics(
                String.valueOf(hashCode()),
                address,
                () -> {
                    synchronized (this) {
                        return (int) pooledConnectionEntries.stream()
                                .filter(entry -> !entry.available)
                                .count();
                    }
                },
                () -> {
                    synchronized (this) {
                        return (int) pooledConnectionEntries.stream()
                                .filter(entry -> entry.available)
                                .count();
                    }
                });
        return boltConnectionProvider.init(
                address, securityPlan, routingContext, boltAgent, userAgent, connectTimeoutMillis, metricsListener);
    }

    @SuppressWarnings({"ReassignedVariable", "ConstantValue"})
    @Override
    public CompletionStage<BoltConnection> connect(
            DatabaseName databaseName,
            Supplier<CompletionStage<Map<String, Value>>> authMapStageSupplier,
            AccessMode mode,
            Set<String> bookmarks,
            String impersonatedUser,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig,
            Consumer<DatabaseName> databaseNameConsumer) {
        synchronized (this) {
            if (closeStage != null) {
                return CompletableFuture.failedFuture(new IllegalStateException("Connection provider is closed."));
            }
        }

        var acquisitionFuture = new CompletableFuture<PooledBoltConnection>();

        // schedule timeout
        executorService.schedule(
                () -> {
                    synchronized (this) {
                        pendingAcquisitions.remove(acquisitionFuture);
                    }
                    try {
                        acquisitionFuture.completeExceptionally(new TimeoutException(
                                "Unable to acquire connection from the pool within configured maximum time of "
                                        + acquisitionTimeout + "ms"));
                    } catch (Throwable throwable) {
                        log.log(System.Logger.Level.WARNING, "Unexpected error occured.", throwable);
                    }
                },
                acquisitionTimeout,
                TimeUnit.MILLISECONDS);

        authMapStageSupplier.get().whenComplete((authMap, authThrowable) -> {
            if (authThrowable != null) {
                acquisitionFuture.completeExceptionally(authThrowable);
                return;
            }

            connect(
                    acquisitionFuture,
                    databaseName,
                    authMap,
                    authMapStageSupplier,
                    mode,
                    bookmarks,
                    impersonatedUser,
                    minVersion,
                    notificationConfig);
        });

        return acquisitionFuture
                .whenComplete((ignored, throwable) -> {
                    if (throwable == null) {
                        databaseNameConsumer.accept(databaseName);
                    }
                })
                .thenApply(Function.identity());
    }

    public void connect(
            CompletableFuture<PooledBoltConnection> acquisitionFuture,
            DatabaseName databaseName,
            Map<String, Value> authMap,
            Supplier<CompletionStage<Map<String, Value>>> authMapStageSupplier,
            AccessMode mode,
            Set<String> bookmarks,
            String impersonatedUser,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig) {

        ConnectionEntryWithMetadata connectionEntryWithMetadata = null;
        Throwable pendingAcquisitionsFull = null;
        var empty = new AtomicBoolean();
        synchronized (this) {
            try {
                empty.set(pooledConnectionEntries.isEmpty());
                try {
                    // go over existing entries first
                    connectionEntryWithMetadata = acquireExistingEntry(authMap, mode, impersonatedUser, minVersion);
                } catch (MinVersionAcquisitionException e) {
                    acquisitionFuture.completeExceptionally(e);
                    return;
                }

                if (connectionEntryWithMetadata == null) {
                    // no entry found
                    if (pooledConnectionEntries.size() < maxSize) {
                        // space is available, reserve
                        var acquiredEntry = new ConnectionEntry(null);
                        pooledConnectionEntries.add(acquiredEntry);
                        connectionEntryWithMetadata = new ConnectionEntryWithMetadata(acquiredEntry, false);
                    } else {
                        // fallback to queue
                        if (pendingAcquisitions.size() < 100 && !acquisitionFuture.isDone()) {
                            pendingAcquisitions.add(acquisitionFuture);
                        } else {
                            pendingAcquisitionsFull =
                                    new TransientException("N/A", "Connection pool pending acquisition queue is full.");
                        }
                    }
                }

            } catch (Throwable throwable) {
                if (connectionEntryWithMetadata != null) {
                    if (connectionEntryWithMetadata.connectionEntry.connection != null) {
                        // not new entry, make it available
                        connectionEntryWithMetadata.connectionEntry.available = true;
                    } else {
                        // new empty entry
                        pooledConnectionEntries.remove(connectionEntryWithMetadata.connectionEntry);
                    }
                }
                pendingAcquisitions.remove(acquisitionFuture);
                acquisitionFuture.completeExceptionally(throwable);
            }
        }

        if (pendingAcquisitionsFull != null) {
            // no space in queue was available
            acquisitionFuture.completeExceptionally(pendingAcquisitionsFull);
        } else if (connectionEntryWithMetadata != null) {
            if (connectionEntryWithMetadata.connectionEntry.connection != null) {
                // entry with connection
                var entryWithMetadata = connectionEntryWithMetadata;
                var entry = entryWithMetadata.connectionEntry;

                livenessCheckStage(entry).whenComplete((ignored, throwable) -> {
                    if (throwable != null) {
                        // liveness check failed
                        purge(entry);
                        connect(
                                acquisitionFuture,
                                databaseName,
                                authMap,
                                authMapStageSupplier,
                                mode,
                                bookmarks,
                                impersonatedUser,
                                minVersion,
                                notificationConfig);
                    } else {
                        // liveness check green or not needed
                        var pooledConnection = new PooledBoltConnection(
                                entry.connection, this, () -> release(entry), () -> purge(entry), logging);
                        reauthStage(entryWithMetadata, authMap).whenComplete((ignored2, throwable2) -> {
                            if (!acquisitionFuture.complete(pooledConnection)) {
                                // acquisition timed out
                                CompletableFuture<PooledBoltConnection> pendingAcquisition;
                                synchronized (this) {
                                    pendingAcquisition = pendingAcquisitions.poll();
                                    if (pendingAcquisition == null) {
                                        // nothing pending, just make the entry available
                                        entry.available = true;
                                    }
                                }
                                if (pendingAcquisition != null) {
                                    pendingAcquisition.complete(pooledConnection);
                                }
                            }
                        });
                    }
                });
            } else {
                // get reserved entry
                var entry = connectionEntryWithMetadata.connectionEntry;
                boltConnectionProvider
                        .connect(
                                databaseName,
                                empty.get() ? () -> CompletableFuture.completedStage(authMap) : authMapStageSupplier,
                                mode,
                                bookmarks,
                                impersonatedUser,
                                minVersion,
                                notificationConfig,
                                (ignored) -> {})
                        .whenComplete((boltConnection, throwable) -> {
                            var error = FutureUtil.completionExceptionCause(throwable);
                            if (error != null) {
                                // todo decide if retry can be done
                                synchronized (this) {
                                    pooledConnectionEntries.remove(entry);
                                }
                                acquisitionFuture.completeExceptionally(error);
                            } else {
                                synchronized (this) {
                                    entry.connection = boltConnection;
                                }
                                var pooledConnection = new PooledBoltConnection(
                                        boltConnection, this, () -> release(entry), () -> purge(entry), logging);
                                if (!acquisitionFuture.complete(pooledConnection)) {
                                    // acquisition timed out
                                    CompletableFuture<PooledBoltConnection> pendingAcquisition;
                                    synchronized (this) {
                                        pendingAcquisition = pendingAcquisitions.poll();
                                        if (pendingAcquisition == null) {
                                            // nothing pending, just make the entry available
                                            entry.available = true;
                                        }
                                    }
                                    if (pendingAcquisition != null) {
                                        pendingAcquisition.complete(pooledConnection);
                                    }
                                }
                            }
                        });
            }
        }
    }

    private synchronized ConnectionEntryWithMetadata acquireExistingEntry(
            Map<String, Value> authMap, AccessMode mode, String impersonatedUser, BoltProtocolVersion minVersion) {
        ConnectionEntryWithMetadata connectionEntryWithMetadata = null;
        var iterator = pooledConnectionEntries.iterator();
        while (iterator.hasNext()) {
            var connectionEntry = iterator.next();

            // unavailable
            if (!connectionEntry.available) {
                continue;
            }

            var connection = connectionEntry.connection;
            // unusable
            if (connection.state() != BoltConnectionState.OPEN) {
                iterator.remove();
                continue;
            }

            // lower version is present
            if (minVersion != null && minVersion.compareTo(connection.protocolVersion()) > 0) {
                throw new MinVersionAcquisitionException("lower version", connection.protocolVersion());
            }

            // the pool must not have unauthenticated connections
            var authData = connection.authData().toCompletableFuture().getNow(null);

            var expiredByError = minAuthTimestamp > 0 && authData.authAckMillis() <= minAuthTimestamp;
            var authMatches = authMap.equals(authData.authMap());
            var reauthNeeded = expiredByError || !authMatches;

            if (reauthNeeded) {
                if (new BoltProtocolVersion(5, 1).compareTo(connectionEntry.connection.protocolVersion()) > 0) {
                    log.log(System.Logger.Level.DEBUG, "reauth is not supported, the connection is voided");
                    iterator.remove();
                    connectionEntry.connection.close().whenComplete((ignored, throwable) -> {
                        if (throwable != null) {
                            log.log(
                                    System.Logger.Level.WARNING,
                                    "Connection close has failed with %s.",
                                    throwable.getClass().getCanonicalName());
                        }
                    });
                    continue;
                }
            }
            log.log(System.Logger.Level.DEBUG, "Connection acquired from the pool. " + address);
            connectionEntry.available = false;
            connectionEntryWithMetadata = new ConnectionEntryWithMetadata(connectionEntry, reauthNeeded);
            break;
        }
        return connectionEntryWithMetadata;
    }

    private CompletionStage<Void> reauthStage(
            ConnectionEntryWithMetadata connectionEntryWithMetadata, Map<String, Value> authMap) {
        CompletionStage<Void> stage;
        if (connectionEntryWithMetadata.reauthNeeded) {
            stage = connectionEntryWithMetadata
                    .connectionEntry
                    .connection
                    .logoff()
                    .thenCompose(conn -> conn.logon(authMap))
                    .handle((ignored, throwable) -> {
                        if (throwable != null) {
                            connectionEntryWithMetadata.connectionEntry.connection.close();
                            synchronized (this) {
                                pooledConnectionEntries.remove(connectionEntryWithMetadata.connectionEntry);
                            }
                        }
                        return null;
                    });
        } else {
            stage = CompletableFuture.completedStage(null);
        }
        return stage;
    }

    private CompletionStage<Void> livenessCheckStage(ConnectionEntry entry) {
        CompletionStage<Void> stage;
        if (idleBeforeTest >= 0 && entry.lastUsedTimestamp + idleBeforeTest < clock.millis()) {
            var future = new CompletableFuture<Void>();
            entry.connection
                    .reset()
                    .thenCompose(conn -> conn.flush(new ResponseHandler() {
                        @Override
                        public void onError(Throwable throwable) {
                            future.completeExceptionally(throwable);
                        }

                        @Override
                        public void onResetSummary(ResetSummary summary) {
                            future.complete(null);
                        }
                    }));
            stage = future;
        } else {
            stage = CompletableFuture.completedStage(null);
        }
        return stage;
    }

    @Override
    public CompletionStage<Void> verifyConnectivity(Map<String, Value> authMap) {
        return connect(
                        null,
                        () -> CompletableFuture.completedStage(authMap),
                        AccessMode.WRITE,
                        Collections.emptySet(),
                        null,
                        null,
                        null,
                        (ignored) -> {})
                .thenCompose(BoltConnection::close);
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb(Map<String, Value> authMap) {
        return connect(
                        null,
                        () -> CompletableFuture.completedStage(authMap),
                        AccessMode.WRITE,
                        Collections.emptySet(),
                        null,
                        null,
                        null,
                        (ignored) -> {})
                .thenCompose(boltConnection -> {
                    var supports = boltConnection.protocolVersion().compareTo(new BoltProtocolVersion(4, 0)) >= 0;
                    return boltConnection.close().thenApply(ignored -> supports);
                });
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth(Map<String, Value> authMap) {
        return connect(
                        null,
                        () -> CompletableFuture.completedStage(authMap),
                        AccessMode.WRITE,
                        Collections.emptySet(),
                        null,
                        null,
                        null,
                        (ignored) -> {})
                .thenCompose(boltConnection -> {
                    var supports = new BoltProtocolVersion(5, 1).compareTo(boltConnection.protocolVersion()) <= 0;
                    return boltConnection.close().thenApply(ignored -> supports);
                });
    }

    @Override
    public CompletionStage<Void> close() {
        CompletionStage<Void> closeStage;
        synchronized (this) {
            if (this.closeStage == null) {
                this.closeStage = CompletableFuture.completedStage(null);
                var iterator = pooledConnectionEntries.iterator();
                while (iterator.hasNext()) {
                    var entry = iterator.next();
                    if (entry.connection != null && entry.connection.state() == BoltConnectionState.OPEN) {
                        this.closeStage = this.closeStage.thenCompose(
                                ignored -> entry.connection.close().exceptionally(throwable -> null));
                    }
                    iterator.remove();
                }
                metricsListener.removePoolMetrics(String.valueOf(hashCode()));
                this.closeStage = this.closeStage
                        .thenCompose(ignored -> boltConnectionProvider.close())
                        .exceptionally(throwable -> null)
                        .whenComplete((ignored, throwable) -> executorService.shutdown());
            }
            closeStage = this.closeStage;
        }
        return closeStage;
    }

    private void release(ConnectionEntry entry) {
        CompletableFuture<PooledBoltConnection> pendingAcquisition;
        synchronized (this) {
            entry.lastUsedTimestamp = clock.millis();
            pendingAcquisition = pendingAcquisitions.poll();
            if (pendingAcquisition == null) {
                // nothing pending, just make the entry available
                entry.available = true;
            }
        }
        if (pendingAcquisition != null) {
            pendingAcquisition.complete(new PooledBoltConnection(
                    entry.connection, this, () -> release(entry), () -> purge(entry), logging));
        }
        log.log(System.Logger.Level.DEBUG, "Connection released to the pool.");
    }

    private void purge(ConnectionEntry entry) {
        synchronized (this) {
            pooledConnectionEntries.remove(entry);
        }
        entry.connection.close();
        log.log(System.Logger.Level.DEBUG, "Connection purged from the pool.");
    }

    synchronized void onExpired() {
        var now = clock.millis();
        minAuthTimestamp = Math.max(minAuthTimestamp, now);
    }

    private static class ConnectionEntry {
        private BoltConnection connection;
        private boolean available;
        private long lastUsedTimestamp;

        private ConnectionEntry(BoltConnection connection) {
            this.connection = connection;
        }
    }

    private static class ConnectionEntryWithMetadata {
        private final ConnectionEntry connectionEntry;
        private final boolean reauthNeeded;

        private ConnectionEntryWithMetadata(ConnectionEntry connectionEntry, boolean reauthNeeded) {
            this.connectionEntry = connectionEntry;
            this.reauthNeeded = reauthNeeded;
        }
    }
}

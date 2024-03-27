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

import static org.neo4j.driver.internal.util.Futures.completionExceptionCause;

import java.net.URI;
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
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltConnectionState;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.exception.MinVersionAcquisitionException;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v51.BoltProtocolV51;
import org.neo4j.driver.internal.bolt.routedimpl.cluster.RoutingContext;
import org.neo4j.driver.internal.metrics.MetricsListener;
import org.neo4j.driver.internal.security.SecurityPlan;

public class PooledBoltConnectionProvider implements BoltConnectionProvider {
    private final Logging logging;
    private final Logger log;
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
            Logging logging) {
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
    public CompletionStage<Boolean> supports(URI uri) {
        return boltConnectionProvider.supports(uri);
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
                        log.warn("Unexpected error occured.", throwable);
                    }
                },
                acquisitionTimeout,
                TimeUnit.MILLISECONDS);

        authMapStageSupplier.get().whenComplete((authMap, authThrowable) -> {
            if (authThrowable != null) {
                acquisitionFuture.completeExceptionally(authThrowable);
                return;
            }

            ConnectionEntryWithMetadata connectionEntryWithMetadata = null;
            Throwable pendingAcquisitionsFull = null;
            var empty = new AtomicBoolean();
            synchronized (this) {
                try {
                    empty.set(pooledConnectionEntries.isEmpty());
                    try {
                        // go over existing entries first
                        connectionEntryWithMetadata =
                                acquireExistingEntry(databaseName, authMap, mode, impersonatedUser, minVersion);
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
                                pendingAcquisitionsFull = new TransientException(
                                        "N/A", "Connection pool pending acquisition queue is full.");
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
                    var entry = connectionEntryWithMetadata.connectionEntry;
                    var pooledConnection = new PooledBoltConnection(
                            entry.connection, this, () -> release(entry), () -> purge(entry), logging);
                    reauthStage(connectionEntryWithMetadata, authMap).whenComplete((ignored, throwable) -> {
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
                } else {
                    // get reserved entry
                    var entry = connectionEntryWithMetadata.connectionEntry;
                    boltConnectionProvider
                            .connect(
                                    databaseName,
                                    empty.get()
                                            ? () -> CompletableFuture.completedStage(authMap)
                                            : authMapStageSupplier,
                                    mode,
                                    bookmarks,
                                    impersonatedUser,
                                    minVersion,
                                    notificationConfig,
                                    (ignored) -> {})
                            .whenComplete((boltConnection, throwable) -> {
                                var error = completionExceptionCause(throwable);
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
        });

        return acquisitionFuture
                .whenComplete((ignored, throwable) -> {
                    if (throwable == null) {
                        databaseNameConsumer.accept(databaseName);
                    }
                })
                .thenApply(Function.identity());
    }

    private synchronized ConnectionEntryWithMetadata acquireExistingEntry(
            DatabaseName databaseName,
            Map<String, Value> authMap,
            AccessMode mode,
            String impersonatedUser,
            BoltProtocolVersion minVersion) {
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
            if (minVersion != null
                    && minVersion.compareTo(connection.connectionInfo().protocolVersion()) > 0) {
                throw new MinVersionAcquisitionException(
                        "lower version", connection.connectionInfo().protocolVersion());
            }

            // the pool must not have unauthenticated connections
            var lastestAuthMillis =
                    connection.latestAuthMillis().toCompletableFuture().join();

            var expiredByError = minAuthTimestamp > 0 && lastestAuthMillis <= minAuthTimestamp;
            var authMatches = authMap.equals(connectionEntry.connection.authMap());
            var reauthNeeded = expiredByError || !authMatches;

            if (reauthNeeded) {
                if (new BoltProtocolVersion(5, 1)
                                .compareTo(connectionEntry
                                        .connection
                                        .connectionInfo()
                                        .protocolVersion())
                        > 0) {
                    log.debug("reauth is not supported, the connection is voided");
                    iterator.remove();
                    connectionEntry.connection.close().whenComplete((ignored, throwable) -> {
                        if (throwable != null) {
                            log.warn(
                                    "Connection close has failed with %s.",
                                    throwable.getClass().getCanonicalName());
                        }
                    });
                    continue;
                }
            }
            log.debug("Connection acquired from the pool. " + address);
            connectionEntry.available = false;
            connection.setDatabase(
                    databaseName != null ? databaseName.databaseName().orElse(null) : null);
            connection.setAccessMode(mode);
            connection.setImpersonatedUser(impersonatedUser);
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
                    var supports =
                            boltConnection.connectionInfo().protocolVersion().compareTo(BoltProtocolV4.VERSION) >= 0;
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
                    var supports = BoltProtocolV51.VERSION.compareTo(
                                    boltConnection.connectionInfo().protocolVersion())
                            <= 0;
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
        log.debug("Connection released to the pool.");
    }

    private void purge(ConnectionEntry entry) {
        synchronized (this) {
            pooledConnectionEntries.remove(entry);
        }
        entry.connection.close();
        log.debug("Connection purged from the pool.");
    }

    synchronized void onExpired() {
        var now = clock.millis();
        minAuthTimestamp = Math.max(minAuthTimestamp, now);
    }

    private static class ConnectionEntry {
        private BoltConnection connection;
        private boolean available;

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

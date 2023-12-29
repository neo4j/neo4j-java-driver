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
            int connectTimeoutMillis) {
        this.address = Objects.requireNonNull(address);
        return boltConnectionProvider.init(
                address, securityPlan, routingContext, boltAgent, userAgent, connectTimeoutMillis);
    }

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

            ConnectionEntry acquiredEntry = null;
            Throwable pendingAcquisitionsFull = null;
            var empty = new AtomicBoolean();
            var reauthStage = CompletableFuture.<Void>completedStage(null);
            synchronized (this) {
                try {
                    empty.set(pooledConnectionEntries.isEmpty());
                    // go over existing entries first
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
                                && minVersion.compareTo(
                                                connection.connectionInfo().protocolVersion())
                                        > 0) {
                            acquisitionFuture.completeExceptionally(new MinVersionAcquisitionException(
                                    "lower version", connection.connectionInfo().protocolVersion()));
                            return;
                        }

                        // the pool must not have unauthenticated connections
                        var lastestAuthMillis = connection
                                .latestAuthMillis()
                                .toCompletableFuture()
                                .join();

                        var expiredByError = minAuthTimestamp > 0 && lastestAuthMillis <= minAuthTimestamp;
                        var authMatches = authMap.equals(connectionEntry.connection.authMap());

                        if (expiredByError || !authMatches) {
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
                            reauthStage = connectionEntry
                                    .connection
                                    .logoff()
                                    .thenCompose(ignored -> connection.logon(authMap))
                                    .handle((ignored, throwable) -> {
                                        if (throwable != null) {
                                            connection.close();
                                            synchronized (this) {
                                                pooledConnectionEntries.remove(connectionEntry);
                                            }
                                        }
                                        return null;
                                    });
                        }
                        log.debug("Connection acquired from the pool. " + address);
                        connectionEntry.available = false;
                        connection.setDatabase(
                                databaseName != null
                                        ? databaseName.databaseName().orElse(null)
                                        : null);
                        connection.setAccessMode(mode);
                        connection.setImpersonatedUser(impersonatedUser);
                        acquiredEntry = connectionEntry;
                        break;
                    }

                    if (acquiredEntry == null) {
                        // no entry found
                        if (pooledConnectionEntries.size() < maxSize) {
                            // space is available, reserve
                            acquiredEntry = new ConnectionEntry(null);
                            pooledConnectionEntries.add(acquiredEntry);
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
                    if (acquiredEntry != null) {
                        acquiredEntry.available = true;
                    }
                    pendingAcquisitions.remove(acquisitionFuture);
                    acquisitionFuture.completeExceptionally(throwable);
                }
            }

            if (pendingAcquisitionsFull != null) {
                // no space in queue was available
                acquisitionFuture.completeExceptionally(pendingAcquisitionsFull);
            } else if (acquiredEntry != null) {
                if (acquiredEntry.connection != null) {
                    // entry with connection
                    var entry = acquiredEntry;
                    var pooledConnection = new PooledBoltConnection(
                            entry.connection, this, () -> release(entry), () -> purge(entry), logging);
                    reauthStage.whenComplete((ignored, throwable) -> {
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
                    var entry = acquiredEntry;
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
}

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
package org.neo4j.driver.internal.bolt.routedimpl.cluster;

import static java.util.Objects.requireNonNull;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.DatabaseNameUtil;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.bolt.routedimpl.util.FutureUtil;

public class RoutingTableRegistryImpl implements RoutingTableRegistry {
    private static final Supplier<IllegalStateException> PENDING_DATABASE_NAME_EXCEPTION_SUPPLIER =
            () -> new IllegalStateException("Pending database name encountered.");
    private final ConcurrentMap<DatabaseName, RoutingTableHandler> routingTableHandlers;
    private final Map<Principal, CompletionStage<DatabaseName>> principalToDatabaseNameStage;
    private final RoutingTableHandlerFactory factory;
    private final System.Logger log;
    private final Clock clock;
    private final Function<BoltServerAddress, BoltConnectionProvider> connectionProviderGetter;
    private final Rediscovery rediscovery;

    public RoutingTableRegistryImpl(
            Function<BoltServerAddress, BoltConnectionProvider> connectionProviderGetter,
            Rediscovery rediscovery,
            Clock clock,
            LoggingProvider logging,
            long routingTablePurgeDelayMs,
            Consumer<Set<BoltServerAddress>> addressesToRetainConsumer) {
        this(
                new ConcurrentHashMap<>(),
                new RoutingTableHandlerFactory(
                        connectionProviderGetter,
                        rediscovery,
                        clock,
                        logging,
                        routingTablePurgeDelayMs,
                        addressesToRetainConsumer),
                clock,
                connectionProviderGetter,
                rediscovery,
                logging);
    }

    RoutingTableRegistryImpl(
            ConcurrentMap<DatabaseName, RoutingTableHandler> routingTableHandlers,
            RoutingTableHandlerFactory factory,
            Clock clock,
            Function<BoltServerAddress, BoltConnectionProvider> connectionProviderGetter,
            Rediscovery rediscovery,
            LoggingProvider logging) {
        requireNonNull(rediscovery, "rediscovery must not be null");
        this.factory = factory;
        this.routingTableHandlers = routingTableHandlers;
        this.principalToDatabaseNameStage = new HashMap<>();
        this.clock = clock;
        this.connectionProviderGetter = connectionProviderGetter;
        this.rediscovery = rediscovery;
        this.log = logging.getLog(getClass());
    }

    @Override
    public CompletionStage<RoutingTableHandler> ensureRoutingTable(
            SecurityPlan securityPlan,
            CompletableFuture<DatabaseName> databaseNameFuture,
            AccessMode mode,
            Set<String> rediscoveryBookmarks,
            String impersonatedUser,
            Supplier<CompletionStage<Map<String, Value>>> authMapStageSupplier,
            BoltProtocolVersion minVersion) {
        return ensureDatabaseNameIsCompleted(
                        securityPlan,
                        databaseNameFuture,
                        mode,
                        rediscoveryBookmarks,
                        impersonatedUser,
                        authMapStageSupplier,
                        minVersion)
                .thenCompose(ctxAndHandler -> {
                    var handler = ctxAndHandler.handler() != null
                            ? ctxAndHandler.handler()
                            : getOrCreate(FutureUtil.joinNowOrElseThrow(
                                    ctxAndHandler.databaseNameFuture(), PENDING_DATABASE_NAME_EXCEPTION_SUPPLIER));
                    return handler.ensureRoutingTable(
                                    securityPlan, mode, rediscoveryBookmarks, authMapStageSupplier, minVersion)
                            .thenApply(ignored -> handler);
                });
    }

    private CompletionStage<ConnectionContextAndHandler> ensureDatabaseNameIsCompleted(
            SecurityPlan securityPlan,
            CompletableFuture<DatabaseName> databaseNameFutureS,
            AccessMode mode,
            Set<String> rediscoveryBookmarks,
            String impersonatedUser,
            Supplier<CompletionStage<Map<String, Value>>> authMapStageSupplier,
            BoltProtocolVersion minVersion) {
        CompletionStage<ConnectionContextAndHandler> contextAndHandlerStage;

        if (databaseNameFutureS.isDone()) {
            contextAndHandlerStage = CompletableFuture.completedFuture(
                    new ConnectionContextAndHandler(databaseNameFutureS, mode, rediscoveryBookmarks, null));
        } else {
            synchronized (this) {
                if (databaseNameFutureS.isDone()) {
                    contextAndHandlerStage = CompletableFuture.completedFuture(
                            new ConnectionContextAndHandler(databaseNameFutureS, mode, rediscoveryBookmarks, null));
                } else {
                    var principal = new Principal(impersonatedUser);
                    var databaseNameStage = principalToDatabaseNameStage.get(principal);
                    var handlerRef = new AtomicReference<RoutingTableHandler>();

                    if (databaseNameStage == null) {
                        var databaseNameFuture = new CompletableFuture<DatabaseName>();
                        principalToDatabaseNameStage.put(principal, databaseNameFuture);
                        databaseNameStage = databaseNameFuture;

                        var routingTable = new ClusterRoutingTable(DatabaseNameUtil.defaultDatabase(), clock);
                        rediscovery
                                .lookupClusterComposition(
                                        securityPlan,
                                        routingTable,
                                        connectionProviderGetter,
                                        rediscoveryBookmarks,
                                        impersonatedUser,
                                        authMapStageSupplier,
                                        minVersion)
                                .thenCompose(compositionLookupResult -> {
                                    var databaseName = DatabaseNameUtil.database(compositionLookupResult
                                            .getClusterComposition()
                                            .databaseName());
                                    var handler = getOrCreate(databaseName);
                                    handlerRef.set(handler);
                                    return handler.updateRoutingTable(compositionLookupResult)
                                            .thenApply(ignored -> databaseName);
                                })
                                .whenComplete((databaseName, throwable) -> {
                                    synchronized (this) {
                                        principalToDatabaseNameStage.remove(principal);
                                    }
                                })
                                .whenComplete((databaseName, throwable) -> {
                                    if (throwable != null) {
                                        databaseNameFuture.completeExceptionally(throwable);
                                    } else {
                                        databaseNameFuture.complete(databaseName);
                                    }
                                });
                    }

                    contextAndHandlerStage = databaseNameStage.thenApply(databaseName -> {
                        synchronized (this) {
                            databaseNameFutureS.complete(databaseName);
                        }
                        return new ConnectionContextAndHandler(
                                databaseNameFutureS, mode, rediscoveryBookmarks, handlerRef.get());
                    });
                }
            }
        }

        return contextAndHandlerStage;
    }

    @Override
    public Set<BoltServerAddress> allServers() {
        // obviously we just had a snapshot of all servers in all routing tables
        // after we read it, the set could already be changed.
        return routingTableHandlers.values().stream()
                .flatMap(tableHandler -> tableHandler.servers().stream())
                .collect(Collectors.toSet());
    }

    @Override
    public void remove(DatabaseName databaseName) {
        routingTableHandlers.remove(databaseName);
        log.log(
                System.Logger.Level.DEBUG,
                "Routing table handler for database '%s' is removed.",
                databaseName.description());
    }

    @Override
    public void removeAged() {
        routingTableHandlers.forEach((databaseName, handler) -> {
            if (handler.isRoutingTableAged()) {
                log.log(
                        System.Logger.Level.INFO,
                        "Routing table handler for database '%s' is removed because it has not been used for a long time. Routing table: %s",
                        databaseName.description(),
                        handler.routingTable());
                routingTableHandlers.remove(databaseName);
            }
        });
    }

    @Override
    public Optional<RoutingTableHandler> getRoutingTableHandler(DatabaseName databaseName) {
        return Optional.ofNullable(routingTableHandlers.get(databaseName));
    }

    // For tests
    public boolean contains(DatabaseName databaseName) {
        return routingTableHandlers.containsKey(databaseName);
    }

    private RoutingTableHandler getOrCreate(DatabaseName databaseName) {
        return routingTableHandlers.computeIfAbsent(databaseName, name -> {
            var handler = factory.newInstance(name, this);
            log.log(
                    System.Logger.Level.DEBUG,
                    "Routing table handler for database '%s' is added.",
                    databaseName.description());
            return handler;
        });
    }

    static class RoutingTableHandlerFactory {
        private final Function<BoltServerAddress, BoltConnectionProvider> connectionProviderGetter;
        private final Rediscovery rediscovery;
        private final LoggingProvider logging;
        private final Clock clock;
        private final long routingTablePurgeDelayMs;
        private final Consumer<Set<BoltServerAddress>> addressesToRetainConsumer;

        RoutingTableHandlerFactory(
                Function<BoltServerAddress, BoltConnectionProvider> connectionProviderGetter,
                Rediscovery rediscovery,
                Clock clock,
                LoggingProvider logging,
                long routingTablePurgeDelayMs,
                Consumer<Set<BoltServerAddress>> addressesToRetainConsumer) {
            this.connectionProviderGetter = connectionProviderGetter;
            this.rediscovery = rediscovery;
            this.clock = clock;
            this.logging = logging;
            this.routingTablePurgeDelayMs = routingTablePurgeDelayMs;
            this.addressesToRetainConsumer = addressesToRetainConsumer;
        }

        RoutingTableHandler newInstance(DatabaseName databaseName, RoutingTableRegistry allTables) {
            var routingTable = new ClusterRoutingTable(databaseName, clock);
            return new RoutingTableHandlerImpl(
                    routingTable,
                    rediscovery,
                    connectionProviderGetter,
                    allTables,
                    logging,
                    routingTablePurgeDelayMs,
                    addressesToRetainConsumer);
        }
    }

    private record Principal(String id) {

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            var principal = (Principal) o;
            return Objects.equals(id, principal.id);
        }
    }

    private record ConnectionContextAndHandler(
            CompletableFuture<DatabaseName> databaseNameFuture,
            AccessMode mode,
            Set<String> rediscoveryBookmarks,
            RoutingTableHandler handler) {}
}

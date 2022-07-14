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
package org.neo4j.driver.internal.cluster;

import static org.neo4j.driver.internal.async.ConnectionContext.PENDING_DATABASE_NAME_EXCEPTION_SUPPLIER;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.DatabaseNameUtil;
import org.neo4j.driver.internal.async.ConnectionContext;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Futures;

public class RoutingTableRegistryImpl implements RoutingTableRegistry {
    static final String TABLE_ACQUISITION_TIMEOUT_MESSAGE = "Failed to acquire routing table in configured timeout.";
    private final ConcurrentMap<DatabaseName, RoutingTableHandler> routingTableHandlers;
    private final Map<Principal, CompletionStage<DatabaseName>> principalToDatabaseNameStage;
    private final RoutingTableHandlerFactory factory;
    private final Logger log;
    private final long updateRoutingTableTimeoutMillis;
    private final Clock clock;
    private final ConnectionPool connectionPool;
    private final Rediscovery rediscovery;

    public RoutingTableRegistryImpl(
            ConnectionPool connectionPool,
            Rediscovery rediscovery,
            long updateRoutingTableTimeoutMillis,
            Clock clock,
            Logging logging,
            long routingTablePurgeDelayMs) {
        this(
                new ConcurrentHashMap<>(),
                new RoutingTableHandlerFactory(connectionPool, rediscovery, clock, logging, routingTablePurgeDelayMs),
                updateRoutingTableTimeoutMillis,
                clock,
                connectionPool,
                rediscovery,
                logging);
    }

    RoutingTableRegistryImpl(
            ConcurrentMap<DatabaseName, RoutingTableHandler> routingTableHandlers,
            RoutingTableHandlerFactory factory,
            long updateRoutingTableTimeoutMillis,
            Clock clock,
            ConnectionPool connectionPool,
            Rediscovery rediscovery,
            Logging logging) {
        this.factory = factory;
        this.routingTableHandlers = routingTableHandlers;
        this.principalToDatabaseNameStage = new HashMap<>();
        this.updateRoutingTableTimeoutMillis = updateRoutingTableTimeoutMillis;
        this.clock = clock;
        this.connectionPool = connectionPool;
        this.rediscovery = rediscovery;
        this.log = logging.getLog(getClass());
    }

    @Override
    public CompletionStage<RoutingTableHandler> ensureRoutingTable(ConnectionContext context) {
        return ensureDatabaseNameIsCompleted(context)
                .thenCompose(ctxAndHandler -> {
                    ConnectionContext completedContext = ctxAndHandler.getContext();
                    RoutingTableHandler handler = ctxAndHandler.getHandler() != null
                            ? ctxAndHandler.getHandler()
                            : getOrCreate(Futures.joinNowOrElseThrow(
                                    completedContext.databaseNameFuture(), PENDING_DATABASE_NAME_EXCEPTION_SUPPLIER));
                    return handler.ensureRoutingTable(completedContext).thenApply(ignored -> handler);
                })
                .toCompletableFuture()
                .orTimeout(updateRoutingTableTimeoutMillis, TimeUnit.MILLISECONDS)
                .handle(this::handleTimeoutException);
    }

    private CompletionStage<ConnectionContextAndHandler> ensureDatabaseNameIsCompleted(ConnectionContext context) {
        CompletionStage<ConnectionContextAndHandler> contextAndHandlerStage;
        CompletableFuture<DatabaseName> contextDatabaseNameFuture = context.databaseNameFuture();

        if (contextDatabaseNameFuture.isDone()) {
            contextAndHandlerStage = CompletableFuture.completedFuture(new ConnectionContextAndHandler(context, null));
        } else {
            synchronized (this) {
                if (contextDatabaseNameFuture.isDone()) {
                    contextAndHandlerStage =
                            CompletableFuture.completedFuture(new ConnectionContextAndHandler(context, null));
                } else {
                    String impersonatedUser = context.impersonatedUser();
                    Principal principal = new Principal(impersonatedUser);
                    CompletionStage<DatabaseName> databaseNameStage = principalToDatabaseNameStage.get(principal);
                    AtomicReference<RoutingTableHandler> handlerRef = new AtomicReference<>();

                    if (databaseNameStage == null) {
                        CompletableFuture<DatabaseName> databaseNameFuture = new CompletableFuture<>();
                        principalToDatabaseNameStage.put(principal, databaseNameFuture);
                        databaseNameStage = databaseNameFuture;

                        ClusterRoutingTable routingTable =
                                new ClusterRoutingTable(DatabaseNameUtil.defaultDatabase(), clock);
                        rediscovery
                                .lookupClusterComposition(
                                        routingTable, connectionPool, context.rediscoveryBookmarks(), impersonatedUser)
                                .thenCompose(compositionLookupResult -> {
                                    DatabaseName databaseName = DatabaseNameUtil.database(compositionLookupResult
                                            .getClusterComposition()
                                            .databaseName());
                                    RoutingTableHandler handler = getOrCreate(databaseName);
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
                            contextDatabaseNameFuture.complete(databaseName);
                        }
                        return new ConnectionContextAndHandler(context, handlerRef.get());
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
        Set<BoltServerAddress> servers = new HashSet<>();
        for (RoutingTableHandler tableHandler : routingTableHandlers.values()) {
            servers.addAll(tableHandler.servers());
        }
        return servers;
    }

    @Override
    public void remove(DatabaseName databaseName) {
        routingTableHandlers.remove(databaseName);
        log.debug("Routing table handler for database '%s' is removed.", databaseName.description());
    }

    @Override
    public void removeAged() {
        routingTableHandlers.forEach((databaseName, handler) -> {
            if (handler.isRoutingTableAged()) {
                log.info(
                        "Routing table handler for database '%s' is removed because it has not been used for a long time. Routing table: %s",
                        databaseName.description(), handler.routingTable());
                routingTableHandlers.remove(databaseName);
            }
        });
    }

    @Override
    public Optional<RoutingTableHandler> getRoutingTableHandler(DatabaseName databaseName) {
        return Optional.ofNullable(routingTableHandlers.get(databaseName));
    }

    private RoutingTableHandler handleTimeoutException(RoutingTableHandler handler, Throwable throwable) {
        if (throwable != null) {
            if (throwable instanceof TimeoutException) {
                throw new ServiceUnavailableException(TABLE_ACQUISITION_TIMEOUT_MESSAGE, throwable);
            } else if (throwable instanceof RuntimeException runtimeException) {
                throw runtimeException;
            } else {
                throw new CompletionException(throwable);
            }
        }
        return handler;
    }

    // For tests
    public boolean contains(DatabaseName databaseName) {
        return routingTableHandlers.containsKey(databaseName);
    }

    private RoutingTableHandler getOrCreate(DatabaseName databaseName) {
        return routingTableHandlers.computeIfAbsent(databaseName, name -> {
            RoutingTableHandler handler = factory.newInstance(name, this);
            log.debug("Routing table handler for database '%s' is added.", databaseName.description());
            return handler;
        });
    }

    static class RoutingTableHandlerFactory {
        private final ConnectionPool connectionPool;
        private final Rediscovery rediscovery;
        private final Logging logging;
        private final Clock clock;
        private final long routingTablePurgeDelayMs;

        RoutingTableHandlerFactory(
                ConnectionPool connectionPool,
                Rediscovery rediscovery,
                Clock clock,
                Logging logging,
                long routingTablePurgeDelayMs) {
            this.connectionPool = connectionPool;
            this.rediscovery = rediscovery;
            this.clock = clock;
            this.logging = logging;
            this.routingTablePurgeDelayMs = routingTablePurgeDelayMs;
        }

        RoutingTableHandler newInstance(DatabaseName databaseName, RoutingTableRegistry allTables) {
            ClusterRoutingTable routingTable = new ClusterRoutingTable(databaseName, clock);
            return new RoutingTableHandlerImpl(
                    routingTable, rediscovery, connectionPool, allTables, logging, routingTablePurgeDelayMs);
        }
    }

    private static class Principal {
        private final String id;

        private Principal(String id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Principal principal = (Principal) o;
            return Objects.equals(id, principal.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    private static class ConnectionContextAndHandler {
        private final ConnectionContext context;
        private final RoutingTableHandler handler;

        private ConnectionContextAndHandler(ConnectionContext context, RoutingTableHandler handler) {
            this.context = context;
            this.handler = handler;
        }

        public ConnectionContext getContext() {
            return context;
        }

        public RoutingTableHandler getHandler() {
            return handler;
        }
    }
}

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

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.bolt.routedimpl.util.FutureUtil;

public class RoutingTableHandlerImpl implements RoutingTableHandler {
    private final RoutingTable routingTable;
    private final DatabaseName databaseName;
    private final RoutingTableRegistry routingTableRegistry;
    private volatile CompletableFuture<RoutingTable> refreshRoutingTableFuture;
    private final Function<BoltServerAddress, BoltConnectionProvider> connectionProviderGetter;
    private final Rediscovery rediscovery;
    private final System.Logger log;
    private final long routingTablePurgeDelayMs;
    private final Set<BoltServerAddress> resolvedInitialRouters = new HashSet<>();
    private final Consumer<Set<BoltServerAddress>> addressesToRetainConsumer;

    public RoutingTableHandlerImpl(
            RoutingTable routingTable,
            Rediscovery rediscovery,
            Function<BoltServerAddress, BoltConnectionProvider> connectionProviderGetter,
            RoutingTableRegistry routingTableRegistry,
            LoggingProvider logging,
            long routingTablePurgeDelayMs,
            Consumer<Set<BoltServerAddress>> addressesToRetainConsumer) {
        this.routingTable = routingTable;
        this.databaseName = routingTable.database();
        this.rediscovery = rediscovery;
        this.connectionProviderGetter = connectionProviderGetter;
        this.routingTableRegistry = routingTableRegistry;
        this.log = logging.getLog(getClass());
        this.routingTablePurgeDelayMs = routingTablePurgeDelayMs;
        this.addressesToRetainConsumer = addressesToRetainConsumer;
    }

    @Override
    public void onConnectionFailure(BoltServerAddress address) {
        // remove server from the routing table, to prevent concurrent threads from making connections to this address
        routingTable.forget(address);
    }

    @Override
    public void onWriteFailure(BoltServerAddress address) {
        routingTable.forgetWriter(address);
    }

    @Override
    public synchronized CompletionStage<RoutingTable> ensureRoutingTable(
            SecurityPlan securityPlan,
            AccessMode mode,
            Set<String> rediscoveryBookmarks,
            Supplier<CompletionStage<Map<String, Value>>> authMapStageSupplier,
            BoltProtocolVersion minVersion) {
        if (refreshRoutingTableFuture != null) {
            // refresh is already happening concurrently, just use it's result
            return refreshRoutingTableFuture;
        } else if (routingTable.isStaleFor(mode)) {
            // existing routing table is not fresh and should be updated
            log.log(
                    System.Logger.Level.DEBUG,
                    "Routing table for database '%s' is stale. %s",
                    databaseName.description(),
                    routingTable);

            var resultFuture = new CompletableFuture<RoutingTable>();
            refreshRoutingTableFuture = resultFuture;

            rediscovery
                    .lookupClusterComposition(
                            securityPlan,
                            routingTable,
                            connectionProviderGetter,
                            rediscoveryBookmarks,
                            null,
                            authMapStageSupplier,
                            minVersion)
                    .whenComplete((composition, completionError) -> {
                        var error = FutureUtil.completionExceptionCause(completionError);
                        if (error != null) {
                            clusterCompositionLookupFailed(error);
                        } else {
                            freshClusterCompositionFetched(composition);
                        }
                    });

            return resultFuture;
        } else {
            // existing routing table is fresh, use it
            return completedFuture(routingTable);
        }
    }

    @Override
    public synchronized CompletionStage<RoutingTable> updateRoutingTable(
            ClusterCompositionLookupResult compositionLookupResult) {
        if (refreshRoutingTableFuture != null) {
            // refresh is already happening concurrently, just use its result
            return refreshRoutingTableFuture;
        } else {
            if (compositionLookupResult.getClusterComposition().expirationTimestamp()
                    < routingTable.expirationTimestamp()) {
                return completedFuture(routingTable);
            }
            var resultFuture = new CompletableFuture<RoutingTable>();
            refreshRoutingTableFuture = resultFuture;
            freshClusterCompositionFetched(compositionLookupResult);
            return resultFuture;
        }
    }

    private synchronized void freshClusterCompositionFetched(ClusterCompositionLookupResult compositionLookupResult) {
        try {
            log.log(
                    System.Logger.Level.DEBUG,
                    "Fetched cluster composition for database '%s'. %s",
                    databaseName.description(),
                    compositionLookupResult.getClusterComposition());
            routingTable.update(compositionLookupResult.getClusterComposition());
            routingTableRegistry.removeAged();

            Set<BoltServerAddress> addressesToRetain = new LinkedHashSet<>();
            routingTableRegistry.allServers().stream()
                    .flatMap(BoltServerAddress::unicastStream)
                    .forEach(addressesToRetain::add);
            compositionLookupResult.getResolvedInitialRouters().ifPresent(addresses -> {
                resolvedInitialRouters.clear();
                resolvedInitialRouters.addAll(addresses);
            });
            addressesToRetain.addAll(resolvedInitialRouters);
            addressesToRetainConsumer.accept(addressesToRetain);

            log.log(
                    System.Logger.Level.DEBUG,
                    "Updated routing table for database '%s'. %s",
                    databaseName.description(),
                    routingTable);

            var routingTableFuture = refreshRoutingTableFuture;
            refreshRoutingTableFuture = null;
            routingTableFuture.complete(routingTable);
        } catch (Throwable error) {
            clusterCompositionLookupFailed(error);
        }
    }

    private synchronized void clusterCompositionLookupFailed(Throwable error) {
        log.log(
                System.Logger.Level.ERROR,
                String.format(
                        "Failed to update routing table for database '%s'. Current routing table: %s.",
                        databaseName.description(), routingTable),
                error);
        routingTableRegistry.remove(databaseName);
        var routingTableFuture = refreshRoutingTableFuture;
        refreshRoutingTableFuture = null;
        routingTableFuture.completeExceptionally(error);
    }

    // This method cannot be synchronized as it will be visited by all routing table handler's threads concurrently
    @Override
    public Set<BoltServerAddress> servers() {
        return routingTable.servers();
    }

    // This method cannot be synchronized as it will be visited by all routing table handler's threads concurrently
    @Override
    public boolean isRoutingTableAged() {
        return refreshRoutingTableFuture == null && routingTable.hasBeenStaleFor(routingTablePurgeDelayMs);
    }

    public RoutingTable routingTable() {
        return routingTable;
    }
}

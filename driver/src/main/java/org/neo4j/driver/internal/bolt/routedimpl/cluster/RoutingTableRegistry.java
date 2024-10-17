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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;

/**
 * A generic interface to access all routing tables as a whole.
 * It also provides methods to obtain a routing table or manage a routing table for a specified database.
 */
public interface RoutingTableRegistry {
    /**
     * Ensures the routing table for the database with given access mode.
     * For server version lower than 4.0, the database name will be ignored while refreshing routing table.
     * @return The future of a new routing table handler.
     */
    CompletionStage<RoutingTableHandler> ensureRoutingTable(
            SecurityPlan securityPlan,
            CompletableFuture<DatabaseName> databaseNameFuture,
            AccessMode mode,
            Set<String> rediscoveryBookmarks,
            String impersonatedUser,
            Supplier<CompletionStage<Map<String, Value>>> authMapStageSupplier,
            BoltProtocolVersion minVersion);

    /**
     * @return all servers in the registry
     */
    Set<BoltServerAddress> allServers();

    /**
     * Removes a routing table of the given database from registry.
     */
    void remove(DatabaseName databaseName);

    /**
     * Removes all routing tables that has been not used for a long time.
     */
    void removeAged();

    /**
     * Returns routing table handler for the given database name if it exists in the registry.
     *
     * @param databaseName the database name
     * @return the routing table handler for the requested database name
     */
    Optional<RoutingTableHandler> getRoutingTableHandler(DatabaseName databaseName);
}

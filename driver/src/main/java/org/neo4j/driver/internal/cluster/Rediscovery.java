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

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;

/**
 * Provides cluster composition lookup capabilities and initial router address resolution.
 */
public interface Rediscovery {
    /**
     * Fetches cluster composition using the provided routing table.
     * <p>
     * Implementation must be thread safe to be called with distinct routing tables concurrently. The routing table instance may be modified.
     *
     * @param routingTable     the routing table for cluster composition lookup
     * @param connectionPool   the connection pool for connection acquisition
     * @param bookmarks        the bookmarks that are presented to the server
     * @param impersonatedUser the impersonated user for cluster composition lookup, should be {@code null} for non-impersonated requests
     * @param overrideAuthToken the override auth token
     * @return cluster composition lookup result
     */
    CompletionStage<ClusterCompositionLookupResult> lookupClusterComposition(
            RoutingTable routingTable,
            ConnectionPool<InetSocketAddress> connectionPool,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            AuthToken overrideAuthToken);

    List<BoltServerAddress> resolve() throws UnknownHostException;
}

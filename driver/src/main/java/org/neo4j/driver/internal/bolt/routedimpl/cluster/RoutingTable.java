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

import java.util.List;
import java.util.Set;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.ClusterComposition;
import org.neo4j.driver.internal.bolt.api.DatabaseName;

public interface RoutingTable {
    boolean isStaleFor(AccessMode mode);

    boolean hasBeenStaleFor(long staleRoutingTableTimeout);

    void update(ClusterComposition cluster);

    void forget(BoltServerAddress address);

    /**
     * Returns an immutable list of reader addresses.
     *
     * @return the immutable list of reader addresses.
     */
    List<BoltServerAddress> readers();

    /**
     * Returns an immutable list of writer addresses.
     *
     * @return the immutable list of write addresses.
     */
    List<BoltServerAddress> writers();

    /**
     * Returns an immutable list of router addresses.
     *
     * @return the immutable list of router addresses.
     */
    List<BoltServerAddress> routers();

    /**
     * Returns an immutable unordered set of all addresses known by this routing table. This includes all router, reader, writer and disused addresses.
     *
     * @return the immutable set of all addresses.
     */
    Set<BoltServerAddress> servers();

    DatabaseName database();

    void forgetWriter(BoltServerAddress toRemove);

    void replaceRouterIfPresent(BoltServerAddress oldRouter, BoltServerAddress newRouter);

    boolean preferInitialRouter();

    long expirationTimestamp();
}

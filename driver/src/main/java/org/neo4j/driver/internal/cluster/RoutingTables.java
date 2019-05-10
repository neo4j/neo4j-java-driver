/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.internal.BoltServerAddress;

/**
 * A generic interface to access all routing tables as a whole.
 * It also provides methods to obtain a routing table or manage a routing table for a specified database.
 */
public interface RoutingTables
{
    /**
     * Fresh the routing table for the database and given access mode.
     * For server version lower than 4.0, the database name will be ignored while refresh routing table.
     * @return The future of a new routing table handler.
     */
    CompletionStage<RoutingTableHandler> refreshRoutingTable( String databaseName, AccessMode mode );

    /**
     * @return all servers in all routing tables
     */
    Set<BoltServerAddress> allServers();

    /**
     * Removes a routing table of the given database from all tables.
     */
    void remove( String databaseName );

    /**
     * Removes all stale routing tables.
     */
    void removeStale();
}

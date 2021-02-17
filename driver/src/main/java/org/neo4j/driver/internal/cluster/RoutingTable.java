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

import java.util.Set;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.internal.DatabaseName;

public interface RoutingTable
{
    boolean isStaleFor( AccessMode mode );

    boolean hasBeenStaleFor( long staleRoutingTableTimeout );

    void update( ClusterComposition cluster );

    void forget( BoltServerAddress address );

    AddressSet readers();

    AddressSet writers();

    AddressSet routers();

    Set<BoltServerAddress> servers();

    DatabaseName database();

    void forgetWriter( BoltServerAddress toRemove );

    boolean preferInitialRouter();
}

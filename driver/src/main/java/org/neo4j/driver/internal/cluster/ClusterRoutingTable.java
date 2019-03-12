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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.AccessMode;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class ClusterRoutingTable implements RoutingTable
{
    private static final int MIN_ROUTERS = 1;

    private final Clock clock;
    private volatile long expirationTimeout;
    private final AddressSet readers;
    private final AddressSet writers;
    private final AddressSet routers;

    public ClusterRoutingTable( Clock clock, BoltServerAddress... routingAddresses )
    {
        this( clock );
        routers.update( new LinkedHashSet<>( asList( routingAddresses ) ) );
    }

    private ClusterRoutingTable( Clock clock )
    {
        this.clock = clock;
        this.expirationTimeout = clock.millis() - 1;

        this.readers = new AddressSet();
        this.writers = new AddressSet();
        this.routers = new AddressSet();
    }

    @Override
    public boolean isStaleFor( AccessMode mode )
    {
        return expirationTimeout < clock.millis() ||
               routers.size() < MIN_ROUTERS ||
               mode == AccessMode.READ && readers.size() == 0 ||
               mode == AccessMode.WRITE && writers.size() == 0;
    }

    @Override
    public synchronized void update( ClusterComposition cluster )
    {
        expirationTimeout = cluster.expirationTimestamp();
        readers.update( cluster.readers() );
        writers.update( cluster.writers() );
        routers.update( cluster.routers() );
    }

    @Override
    public synchronized void forget( BoltServerAddress address )
    {
        routers.remove( address );
        readers.remove( address );
        writers.remove( address );
    }

    @Override
    public AddressSet readers()
    {
        return readers;
    }

    @Override
    public AddressSet writers()
    {
        return writers;
    }

    @Override
    public AddressSet routers()
    {
        return routers;
    }

    @Override
    public Set<BoltServerAddress> servers()
    {
        Set<BoltServerAddress> servers = new HashSet<>();
        Collections.addAll( servers, readers.toArray() );
        Collections.addAll( servers, writers.toArray() );
        Collections.addAll( servers, routers.toArray() );
        return servers;
    }

    @Override
    public void removeWriter( BoltServerAddress toRemove )
    {
        writers.remove( toRemove );
    }


    @Override
    public synchronized String toString()
    {
        return format( "Ttl %s, currentTime %s, routers %s, writers %s, readers %s",
                expirationTimeout, clock.millis(), routers, writers, readers );
    }
}

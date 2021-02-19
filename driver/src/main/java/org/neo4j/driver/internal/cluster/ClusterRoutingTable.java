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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.util.Clock;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class ClusterRoutingTable implements RoutingTable
{
    private static final int MIN_ROUTERS = 1;

    private final Clock clock;
    private volatile long expirationTimestamp;
    private final AddressSet readers;
    private final AddressSet writers;
    private final AddressSet routers;

    private final DatabaseName databaseName; // specifies the database this routing table is acquired for
    private boolean preferInitialRouter;

    public ClusterRoutingTable( DatabaseName ofDatabase, Clock clock, BoltServerAddress... routingAddresses )
    {
        this( ofDatabase, clock );
        routers.update( new LinkedHashSet<>( asList( routingAddresses ) ) );
    }

    private ClusterRoutingTable( DatabaseName ofDatabase, Clock clock )
    {
        this.databaseName = ofDatabase;
        this.clock = clock;
        this.expirationTimestamp = clock.millis() - 1;
        this.preferInitialRouter = true;

        this.readers = new AddressSet();
        this.writers = new AddressSet();
        this.routers = new AddressSet();
    }

    @Override
    public boolean isStaleFor( AccessMode mode )
    {
        return expirationTimestamp < clock.millis() ||
               routers.size() < MIN_ROUTERS ||
               mode == AccessMode.READ && readers.size() == 0 ||
               mode == AccessMode.WRITE && writers.size() == 0;
    }

    @Override
    public boolean hasBeenStaleFor( long extraTime )
    {
        long totalTime = expirationTimestamp + extraTime;
        if ( totalTime < 0 )
        {
            totalTime = Long.MAX_VALUE;
        }
        return  totalTime < clock.millis();
    }

    @Override
    public synchronized void update( ClusterComposition cluster )
    {
        expirationTimestamp = cluster.expirationTimestamp();
        readers.update( cluster.readers() );
        writers.update( cluster.writers() );
        routers.update( cluster.routers() );
        preferInitialRouter = !cluster.hasWriters();
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
    public DatabaseName database()
    {
        return databaseName;
    }

    @Override
    public void forgetWriter( BoltServerAddress toRemove )
    {
        writers.remove( toRemove );
    }

    @Override
    public boolean preferInitialRouter()
    {
        return preferInitialRouter;
    }

    @Override
    public synchronized String toString()
    {
        return format( "Ttl %s, currentTime %s, routers %s, writers %s, readers %s, database '%s'",
                expirationTimestamp, clock.millis(), routers, writers, readers, databaseName.description() );
    }
}

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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.util.Clock;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.neo4j.driver.internal.util.LockUtil.executeWithLock;

public class ClusterRoutingTable implements RoutingTable
{
    private static final int MIN_ROUTERS = 1;

    private final ReadWriteLock tableLock = new ReentrantReadWriteLock();
    private final DatabaseName databaseName;
    private final Clock clock;
    private final Set<BoltServerAddress> disused = new LinkedHashSet<>();

    private long expirationTimestamp;
    private boolean preferInitialRouter = true;
    private Set<BoltServerAddress> readers = Collections.emptySet();
    private Set<BoltServerAddress> writers = Collections.emptySet();
    private Set<BoltServerAddress> routers = Collections.emptySet();

    public ClusterRoutingTable( DatabaseName ofDatabase, Clock clock, BoltServerAddress... routingAddresses )
    {
        this( ofDatabase, clock );
        routers = Collections.unmodifiableSet( new LinkedHashSet<>( asList( routingAddresses ) ) );
    }

    private ClusterRoutingTable( DatabaseName ofDatabase, Clock clock )
    {
        this.databaseName = ofDatabase;
        this.clock = clock;
        this.expirationTimestamp = clock.millis() - 1;
    }

    @Override
    public boolean isStaleFor( AccessMode mode )
    {
        return executeWithLock( tableLock.readLock(), () ->
                expirationTimestamp < clock.millis() ||
                routers.size() < MIN_ROUTERS ||
                mode == AccessMode.READ && readers.size() == 0 ||
                mode == AccessMode.WRITE && writers.size() == 0 );
    }

    @Override
    public boolean hasBeenStaleFor( long extraTime )
    {
        long totalTime = executeWithLock( tableLock.readLock(), () -> expirationTimestamp ) + extraTime;
        if ( totalTime < 0 )
        {
            totalTime = Long.MAX_VALUE;
        }
        return totalTime < clock.millis();
    }

    @Override
    public void update( ClusterComposition cluster )
    {
        executeWithLock( tableLock.writeLock(), () ->
        {
            expirationTimestamp = cluster.expirationTimestamp();
            readers = newWithReusedAddresses( readers, disused, cluster.readers() );
            writers = newWithReusedAddresses( writers, disused, cluster.writers() );
            routers = newWithReusedAddresses( routers, disused, cluster.routers() );
            disused.clear();
            preferInitialRouter = !cluster.hasWriters();
        } );
    }

    @Override
    public void forget( BoltServerAddress address )
    {
        executeWithLock( tableLock.writeLock(), () ->
        {
            routers = newWithoutAddressIfPresent( routers, address );
            readers = newWithoutAddressIfPresent( readers, address );
            writers = newWithoutAddressIfPresent( writers, address );
            disused.add( address );
        } );
    }

    @Override
    public Set<BoltServerAddress> readers()
    {
        return executeWithLock( tableLock.readLock(), () -> readers );
    }

    @Override
    public Set<BoltServerAddress> writers()
    {
        return executeWithLock( tableLock.readLock(), () -> writers );
    }

    @Override
    public Set<BoltServerAddress> routers()
    {
        return executeWithLock( tableLock.readLock(), () -> routers );
    }

    @Override
    public Set<BoltServerAddress> servers()
    {
        return executeWithLock( tableLock.readLock(), () ->
        {
            Set<BoltServerAddress> servers = new HashSet<>();
            servers.addAll( readers );
            servers.addAll( writers );
            servers.addAll( routers );
            servers.addAll( disused );
            return servers;
        } );
    }

    @Override
    public DatabaseName database()
    {
        return databaseName;
    }

    @Override
    public void forgetWriter( BoltServerAddress toRemove )
    {
        executeWithLock( tableLock.writeLock(), () ->
        {
            writers = newWithoutAddressIfPresent( writers, toRemove );
            disused.add( toRemove );
        } );
    }

    @Override
    public void replaceRouterIfPresent( BoltServerAddress oldRouter, BoltServerAddress newRouter )
    {
        executeWithLock( tableLock.writeLock(), () -> routers = newWithAddressReplacedIfPresent( routers, oldRouter, newRouter ) );
    }

    @Override
    public boolean preferInitialRouter()
    {
        return executeWithLock( tableLock.readLock(), () -> preferInitialRouter );
    }

    @Override
    public long expirationTimestamp()
    {
        return executeWithLock( tableLock.readLock(), () -> expirationTimestamp );
    }

    @Override
    public String toString()
    {
        return executeWithLock( tableLock.readLock(), () ->
                format( "Ttl %s, currentTime %s, routers %s, writers %s, readers %s, database '%s'",
                        expirationTimestamp, clock.millis(), routers, writers, readers, databaseName.description() ) );
    }

    private Set<BoltServerAddress> newWithoutAddressIfPresent( Set<BoltServerAddress> addresses, BoltServerAddress addressToSkip )
    {
        return newWithAddressReplacedIfPresent( addresses, addressToSkip, null );
    }

    private Set<BoltServerAddress> newWithAddressReplacedIfPresent( Set<BoltServerAddress> addresses, BoltServerAddress oldAddress,
                                                                    BoltServerAddress newAddress )
    {
        if ( !addresses.contains( oldAddress ) )
        {
            return addresses;
        }
        Stream<BoltServerAddress> addressStream = addresses.stream();
        addressStream = newAddress != null
                        ? addressStream.map( address -> address.equals( oldAddress ) ? newAddress : address )
                        : addressStream.filter( address -> !address.equals( oldAddress ) );
        return Collections.unmodifiableSet( (Set<? extends BoltServerAddress>) addressStream.collect( Collectors.toCollection( LinkedHashSet::new ) ) );
    }

    private Set<BoltServerAddress> newWithReusedAddresses( Set<BoltServerAddress> currentAddresses, Set<BoltServerAddress> disusedAddresses,
                                                           Set<BoltServerAddress> newAddresses )
    {
        Set<BoltServerAddress> result = Stream.concat( currentAddresses.stream(), disusedAddresses.stream() )
                                              .filter( address -> newAddresses.remove( toBoltServerAddress( address ) ) )
                                              .collect( Collectors.toCollection( LinkedHashSet::new ) );
        result.addAll( newAddresses );
        return Collections.unmodifiableSet( result );
    }

    private BoltServerAddress toBoltServerAddress( BoltServerAddress address )
    {
        return BoltServerAddress.class.equals( address.getClass() ) ? address : new BoltServerAddress( address.host(), address.port() );
    }
}

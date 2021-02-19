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
package org.neo4j.driver.internal.cluster.loadbalancing;

import io.netty.bootstrap.Bootstrap;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.FatalDiscoveryException;
import org.neo4j.driver.exceptions.ProtocolException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.connection.BootstrapFactory;
import org.neo4j.driver.internal.async.pool.NettyChannelTracker;
import org.neo4j.driver.internal.async.pool.PoolSettings;
import org.neo4j.driver.internal.async.pool.TestConnectionPool;
import org.neo4j.driver.internal.cluster.ClusterComposition;
import org.neo4j.driver.internal.cluster.Rediscovery;
import org.neo4j.driver.internal.cluster.RoutingTable;
import org.neo4j.driver.internal.cluster.RoutingTableRegistry;
import org.neo4j.driver.internal.cluster.RoutingTableRegistryImpl;
import org.neo4j.driver.internal.metrics.InternalAbstractMetrics;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Futures;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Logging.none;
import static org.neo4j.driver.internal.DatabaseNameUtil.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.internal.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.cluster.RediscoveryUtil.contextWithDatabase;
import static org.neo4j.driver.internal.cluster.RoutingSettings.STALE_ROUTING_TABLE_PURGE_DELAY_MS;
import static org.neo4j.driver.internal.metrics.InternalAbstractMetrics.DEV_NULL_METRICS;
import static org.neo4j.driver.util.TestUtil.await;

class RoutingTableAndConnectionPoolTest
{
    private static final BoltServerAddress A = new BoltServerAddress( "localhost:30000" );
    private static final BoltServerAddress B = new BoltServerAddress( "localhost:30001" );
    private static final BoltServerAddress C = new BoltServerAddress( "localhost:30002" );
    private static final BoltServerAddress D = new BoltServerAddress( "localhost:30003" );
    private static final BoltServerAddress E = new BoltServerAddress( "localhost:30004" );
    private static final BoltServerAddress F = new BoltServerAddress( "localhost:30005" );
    private static final List<BoltServerAddress> SERVERS = new LinkedList<>( Arrays.asList( null, A, B, C, D, E, F ) );

    private static final String[] DATABASES = new String[]{"", SYSTEM_DATABASE_NAME, "my database"};

    private final Random random = new Random();
    private final Clock clock = Clock.SYSTEM;
    private final Logging logging = none();

    @Test
    void shouldAddServerToRoutingTableAndConnectionPool() throws Throwable
    {
        // Given
        ConnectionPool connectionPool = newConnectionPool();
        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.lookupClusterComposition( any(), any(), any() ) ).thenReturn( clusterComposition( A ) );
        RoutingTableRegistryImpl routingTables = newRoutingTables( connectionPool, rediscovery );
        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTables );

        // When
        await( loadBalancer.acquireConnection( contextWithDatabase( "neo4j" ) ) );

        // Then
        assertThat( routingTables.allServers().size(), equalTo( 1 ) );
        assertTrue( routingTables.allServers().contains( A ) );
        assertTrue( routingTables.contains( database( "neo4j" ) ) );
        assertTrue( connectionPool.isOpen( A ) );
    }

    @Test
    void shouldNotAddToRoutingTableWhenFailedWithRoutingError() throws Throwable
    {
        // Given
        ConnectionPool connectionPool = newConnectionPool();
        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.lookupClusterComposition( any(), any(), any() ) ).thenReturn( Futures.failedFuture( new FatalDiscoveryException( "No database found" ) ) );
        RoutingTableRegistryImpl routingTables = newRoutingTables( connectionPool, rediscovery );
        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTables );

        // When
        assertThrows( FatalDiscoveryException.class, () -> await( loadBalancer.acquireConnection( contextWithDatabase( "neo4j" ) ) ) );

        // Then
        assertTrue( routingTables.allServers().isEmpty() );
        assertFalse( routingTables.contains( database( "neo4j" ) ) );
        assertFalse( connectionPool.isOpen( A ) );
    }

    @Test
    void shouldNotAddToRoutingTableWhenFailedWithProtocolError() throws Throwable
    {
        // Given
        ConnectionPool connectionPool = newConnectionPool();
        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.lookupClusterComposition( any(), any(), any() ) ).thenReturn( Futures.failedFuture( new ProtocolException( "No database found" ) ) );
        RoutingTableRegistryImpl routingTables = newRoutingTables( connectionPool, rediscovery );
        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTables );

        // When
        assertThrows( ProtocolException.class, () -> await( loadBalancer.acquireConnection( contextWithDatabase( "neo4j" ) ) ) );

        // Then
        assertTrue( routingTables.allServers().isEmpty() );
        assertFalse( routingTables.contains( database( "neo4j" ) ) );
        assertFalse( connectionPool.isOpen( A ) );
    }

    @Test
    void shouldNotAddToRoutingTableWhenFailedWithSecurityError() throws Throwable
    {
        // Given
        ConnectionPool connectionPool = newConnectionPool();
        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.lookupClusterComposition( any(), any(), any() ) ).thenReturn( Futures.failedFuture( new SecurityException( "No database found" ) ) );
        RoutingTableRegistryImpl routingTables = newRoutingTables( connectionPool, rediscovery );
        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTables );

        // When
        assertThrows( SecurityException.class, () -> await( loadBalancer.acquireConnection( contextWithDatabase( "neo4j" ) ) ) );

        // Then
        assertTrue( routingTables.allServers().isEmpty() );
        assertFalse( routingTables.contains( database( "neo4j" ) ) );
        assertFalse( connectionPool.isOpen( A ) );
    }

    @Test
    void shouldNotRemoveNewlyAddedRoutingTableEvenIfItIsExpired() throws Throwable
    {
        // Given
        ConnectionPool connectionPool = newConnectionPool();
        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.lookupClusterComposition( any(), any(), any() ) ).thenReturn( expiredClusterComposition( A ) );
        RoutingTableRegistryImpl routingTables = newRoutingTables( connectionPool, rediscovery );
        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTables );

        // When
        Connection connection = await( loadBalancer.acquireConnection( contextWithDatabase( "neo4j" ) ) );
        await( connection.release() );

        // Then
        assertTrue( routingTables.contains( database( "neo4j" ) ) );

        assertThat( routingTables.allServers().size(), equalTo( 1 ) );
        assertTrue( routingTables.allServers().contains( A ) );

        assertTrue( connectionPool.isOpen( A ) );
    }

    @Test
    void shouldRemoveExpiredRoutingTableAndServers() throws Throwable
    {
        // Given
        ConnectionPool connectionPool = newConnectionPool();
        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.lookupClusterComposition( any(), any(), any() ) ).thenReturn( expiredClusterComposition( A ) ).thenReturn( clusterComposition( B ) );
        RoutingTableRegistryImpl routingTables = newRoutingTables( connectionPool, rediscovery );
        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTables );

        // When
        Connection connection = await( loadBalancer.acquireConnection( contextWithDatabase( "neo4j" ) ) );
        await( connection.release() );
        await( loadBalancer.acquireConnection( contextWithDatabase( "foo"  ) ) );

        // Then
        assertFalse( routingTables.contains( database( "neo4j" ) ) );
        assertTrue( routingTables.contains( database( "foo" ) ) );

        assertThat( routingTables.allServers().size(), equalTo( 1 ) );
        assertTrue( routingTables.allServers().contains( B ) );

        assertTrue( connectionPool.isOpen( B ) );
    }

    @Test
    void shouldRemoveExpiredRoutingTableButNotServer() throws Throwable
    {
        // Given
        ConnectionPool connectionPool = newConnectionPool();
        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.lookupClusterComposition( any(), any(), any() ) ).thenReturn( expiredClusterComposition( A ) ).thenReturn( clusterComposition( B ) );
        RoutingTableRegistryImpl routingTables = newRoutingTables( connectionPool, rediscovery );
        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTables );

        // When
        await( loadBalancer.acquireConnection( contextWithDatabase("neo4j" ) ) );
        await( loadBalancer.acquireConnection( contextWithDatabase( "foo" ) ) );

        // Then
        assertThat( routingTables.allServers().size(), equalTo( 1 ) );
        assertTrue( routingTables.allServers().contains( B ) );
        assertTrue( connectionPool.isOpen( B ) );
        assertFalse( routingTables.contains( database( "neo4j" ) ) );
        assertTrue( routingTables.contains( database( "foo" ) ) );

        // I still have A as A's connection is in use
        assertTrue( connectionPool.isOpen( A ) );
    }

    @Test
    void shouldHandleAddAndRemoveFromRoutingTableAndConnectionPool() throws Throwable
    {
        // Given
        ConnectionPool connectionPool = newConnectionPool();
        Rediscovery rediscovery = new RandomizedRediscovery();
        RoutingTableRegistry routingTables = newRoutingTables( connectionPool, rediscovery );
        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTables );

        // When
        acquireAndReleaseConnections( loadBalancer );
        Set<BoltServerAddress> servers = routingTables.allServers();
        BoltServerAddress openServer = null;
        for( BoltServerAddress server: servers )
        {
            if ( connectionPool.isOpen( server ) )
            {
                openServer = server;
                break;
            }
        }
        assertNotNull( servers );

        // if we remove the open server from servers, then the connection pool should remove the server from the pool.
        SERVERS.remove( openServer );
        acquireAndReleaseConnections( loadBalancer );

        assertFalse( connectionPool.isOpen( openServer ) );
    }

    private void acquireAndReleaseConnections( LoadBalancer loadBalancer ) throws InterruptedException
    {
        ExecutorService executorService = Executors.newFixedThreadPool( 4 );
        int count = 100;
        Future<?>[] futures = new Future<?>[count];

        for ( int i = 0; i < count; i++ )
        {
            Future<?> future = executorService.submit( () -> {
                int index = random.nextInt( DATABASES.length );
                CompletionStage<Void> task = loadBalancer.acquireConnection( contextWithDatabase( DATABASES[index] ) ).thenCompose( Connection::release );
                await( task );
            } );
            futures[i] = future;
        }

        executorService.shutdown();
        executorService.awaitTermination( 10, TimeUnit.SECONDS );

        List<Throwable> errors = new ArrayList<>();
        for ( Future<?> f : futures )
        {
            try
            {
                f.get();
            }
            catch ( ExecutionException e )
            {
                errors.add( e.getCause() );
            }
        }

        // Then
        assertThat( errors.size(), equalTo( 0 ) );
    }

    private ConnectionPool newConnectionPool()
    {
        InternalAbstractMetrics metrics = DEV_NULL_METRICS;
        PoolSettings poolSettings = new PoolSettings( 10, 5000, -1, -1 );
        Bootstrap bootstrap = BootstrapFactory.newBootstrap( 1 );
        NettyChannelTracker channelTracker = new NettyChannelTracker( metrics, bootstrap.config().group().next(), logging );

        return new TestConnectionPool( bootstrap, channelTracker, poolSettings, metrics, logging, clock, true );
    }

    private RoutingTableRegistryImpl newRoutingTables( ConnectionPool connectionPool, Rediscovery rediscovery )
    {
        return new RoutingTableRegistryImpl( connectionPool, rediscovery, clock, logging.getLog( "RT" ), STALE_ROUTING_TABLE_PURGE_DELAY_MS );
    }

    private LoadBalancer newLoadBalancer( ConnectionPool connectionPool, RoutingTableRegistry routingTables )
    {
        Rediscovery rediscovery = mock( Rediscovery.class );
        return new LoadBalancer( connectionPool, routingTables, rediscovery, new LeastConnectedLoadBalancingStrategy( connectionPool, logging ),
                GlobalEventExecutor.INSTANCE, logging.getLog( "LB" ) );
    }

    private CompletableFuture<ClusterComposition> clusterComposition( BoltServerAddress... addresses )
    {
        return clusterComposition( Duration.ofSeconds( 30 ).toMillis(), addresses );
    }

    private CompletableFuture<ClusterComposition> expiredClusterComposition( BoltServerAddress... addresses )
    {
        return clusterComposition( -STALE_ROUTING_TABLE_PURGE_DELAY_MS - 1, addresses );
    }

    private CompletableFuture<ClusterComposition> clusterComposition( long expireAfterMs, BoltServerAddress... addresses )
    {
        HashSet<BoltServerAddress> servers = new HashSet<>( Arrays.asList( addresses ) );
        ClusterComposition composition = new ClusterComposition( clock.millis() + expireAfterMs, servers, servers, servers );
        return CompletableFuture.completedFuture( composition );
    }

    private class RandomizedRediscovery implements Rediscovery
    {
        @Override
        public CompletionStage<ClusterComposition> lookupClusterComposition( RoutingTable routingTable, ConnectionPool connectionPool, Bookmark bookmark )
        {
            // when looking up a new routing table, we return a valid random routing table back
            Set<BoltServerAddress> servers = new HashSet<>();
            for ( int i = 0; i < 3; i++ )
            {
                int index = random.nextInt( SERVERS.size() );
                BoltServerAddress server = SERVERS.get( index );
                if ( server != null )
                {
                    servers.add( server );
                }
            }
            if ( servers.size() == 0 )
            {
                servers.add( A );
            }
            ClusterComposition composition = new ClusterComposition( clock.millis() + 1, servers, servers, servers );
            return CompletableFuture.completedFuture( composition );
        }

        @Override
        public List<BoltServerAddress> resolve()
        {
            throw new UnsupportedOperationException( "Not implemented" );
        }
    }
}

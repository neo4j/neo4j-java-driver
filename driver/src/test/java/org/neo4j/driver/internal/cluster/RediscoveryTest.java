/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.pooling.PooledConnection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.exceptions.ProtocolException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( Enclosed.class )
public class RediscoveryTest
{
    private static final long NEVER_EXPIRE = System.currentTimeMillis() + TimeUnit.HOURS.toMillis( 1 );
    private static final List<BoltServerAddress> EMPTY = new ArrayList<>();

    private static final BoltServerAddress A = new BoltServerAddress( "1111:11" );
    private static final BoltServerAddress B = new BoltServerAddress( "2222:22" );
    private static final BoltServerAddress C = new BoltServerAddress( "3333:33" );
    private static final BoltServerAddress D = new BoltServerAddress( "4444:44" );
    private static final BoltServerAddress E = new BoltServerAddress( "5555:55" );
    private static final BoltServerAddress F = new BoltServerAddress( "6666:66" );

    private static final ClusterComposition VALID_CLUSTER_COMPOSITION = createClusterComposition(
            NEVER_EXPIRE, asList( A, B ), asList( C ), asList( D, E ) );

    public static class FailedToConnectTest
    {
        @Ignore("This driver allow the user to retry several times before totally failed.")
        public void shouldForgetRouterWhenFailedToConnect() throws Throwable
        {
        }

        @Test
        public void shouldTryNextRouterWhenFailedToConnect() throws Throwable
        {
            // Given
            ListBasedRoutingTable routingTable = new ListBasedRoutingTable( asList( A, B ) );

            PooledConnection healthyConn = mock( PooledConnection.class );
            ConnectionPool mockedConnections = mock( ConnectionPool.class );
            when( mockedConnections.acquire( A ) ).thenThrow( new ServiceUnavailableException( "failed to connect" ) );
            when( mockedConnections.acquire( B ) ).thenReturn( healthyConn );

            ClusterComposition.Provider mockedProvider = mock( ClusterComposition.Provider.class );
            when( mockedProvider.getClusterComposition( healthyConn ) ).thenReturn( VALID_CLUSTER_COMPOSITION );

            // When
            ClusterComposition clusterComposition = rediscover( mockedConnections, routingTable, mockedProvider );

            // Then
            assertThat( routingTable.removedRouters.size(), equalTo( 1 ) );
            assertThat( routingTable.removedRouters.get( 0 ), equalTo( A ) );
            assertThat( clusterComposition, equalTo( VALID_CLUSTER_COMPOSITION ) );
        }
    }

    public static class ProcedureNotFoundTest
    {
        @Test
        public void shouldThrowServiceUnavailableWhenNoProcedureFound() throws Throwable
        {
            // Given
            RoutingTable routingTable = new ListBasedRoutingTable( asList( A ) );

            PooledConnection healthyConn = mock( PooledConnection.class );
            ConnectionPool mockedConnections = mock( ConnectionPool.class );
            when( mockedConnections.acquire( A ) ).thenReturn( healthyConn );


            ClusterComposition.Provider mockedProvider = mock( ClusterComposition.Provider.class );
            when( mockedProvider.getClusterComposition( healthyConn ) )
                    .thenThrow( new ClusterComposition.Provider.ProcedureNotFoundException( "No such procedure" ) );

            // When & When
            try
            {
                ClusterComposition clusterComposition = rediscover( mockedConnections, routingTable, mockedProvider );
                fail( "Expecting a failure but not triggered." );
            }
            catch( Exception e )
            {
                assertThat( e, instanceOf( ServiceUnavailableException.class ) );
                assertThat( e.getMessage(), startsWith( "No such procedure" ) );
            }
        }
    }

    @RunWith( Parameterized.class )
    public static class NoWritersTest
    {

        @Parameters(name = "Rediscovery result: {0}")
        public static Collection<Object[]> data() {
            return asList(new Object[][] {
                    { "([A], [C], [])", createClusterComposition( asList( A ), EMPTY, asList( C ) ) },
                    { "([A], [CD], [])", createClusterComposition( asList( A ), EMPTY, asList( C, D ) ) },
                    { "([AB], [C], [])", createClusterComposition( asList( A, B ), EMPTY, asList( C ) ) },
                    { "([AB], [CD], [])", createClusterComposition( asList( A, B ), EMPTY, asList( C, D ) )}
            });
        }

        private ClusterComposition noWriters;

        public NoWritersTest( String testName, ClusterComposition noWriters )
        {
            this.noWriters = noWriters;
        }

        @Test
        public void shouldTryNextRouterWhenNoWriters() throws Throwable
        {
            // Given
            RoutingTable routingTable = new ListBasedRoutingTable( asList( A, B ) );

            PooledConnection noWriterConn = mock( PooledConnection.class );
            PooledConnection healthyConn = mock( PooledConnection.class );
            ConnectionPool mockedConnections = mock( ConnectionPool.class );
            when( mockedConnections.acquire( A ) ).thenReturn( noWriterConn );
            when( mockedConnections.acquire( B ) ).thenReturn( healthyConn );

            ClusterComposition.Provider mockedProvider = mock( ClusterComposition.Provider.class );
            when( mockedProvider.getClusterComposition( noWriterConn ) ).thenReturn( noWriters );
            when( mockedProvider.getClusterComposition( healthyConn ) ).thenReturn( VALID_CLUSTER_COMPOSITION );

            // When
            ClusterComposition clusterComposition = rediscover( mockedConnections, routingTable, mockedProvider );

            // Then
            assertThat( clusterComposition, equalTo( VALID_CLUSTER_COMPOSITION ) );
        }

        @Test
        public void shouldThrowServiceUnavailableWhenNoNextRouter() throws Throwable
        {
            // Given
            RoutingTable routingTable = new ListBasedRoutingTable( asList( A ) );

            PooledConnection noWriterConn = mock( PooledConnection.class );
            ConnectionPool mockedConnections = mock( ConnectionPool.class );
            when( mockedConnections.acquire( A ) ).thenReturn( noWriterConn );

            ClusterComposition.Provider mockedProvider = mock( ClusterComposition.Provider.class );
            when( mockedProvider.getClusterComposition( noWriterConn ) ).thenReturn( noWriters );

            // When & THen
            try
            {
                ClusterComposition clusterComposition = rediscover( mockedConnections, routingTable, mockedProvider );
                fail( "Expecting a failure but not triggered." );
            }
            catch( Exception e )
            {
                assertThat( e, instanceOf( ServiceUnavailableException.class ) );
                assertThat( e.getMessage(), startsWith( "Could not perform discovery. No routing servers available." ) );
            }
        }
    }

    @RunWith( Parameterized.class )
    public static class AtLeastOneOfEachTest
    {
        @Parameters(name = "Rediscovery result: {0}")
        public static Collection<Object[]> data() {
            return asList(new Object[][] {
                    { "([A], [C], [E])", createClusterComposition( asList( A ), asList( C ), asList( E ) ) },
                    { "([AB], [C], [E])", createClusterComposition( asList( A, B ), asList( C ), asList( E ) ) },
                    { "([A], [CD], [E])", createClusterComposition( asList( A ), asList( C, D ), asList( E ) ) },
                    { "([AB], [CD], [E])", createClusterComposition( asList( A, B ), asList( C, D ), asList( E ) ) },
                    { "([A], [C], [EF])", createClusterComposition( asList( A ), asList( C ), asList( E, F ) ) },
                    { "([AB], [C], [EF])", createClusterComposition( asList( A, B ), asList( C ), asList( E, F ) ) },
                    { "([A], [CD], [EF])", createClusterComposition( asList( A ), asList( C, D ), asList( E, F ) ) },
                    { "([AB], [CD], [EF])", createClusterComposition( asList( A, B ), asList( C, D ), asList( E, F ) )}
            });
        }

        private ClusterComposition atLeastOneOfEach;

        public AtLeastOneOfEachTest( String testName, ClusterComposition atLeastOneOfEach )
        {
            this.atLeastOneOfEach = atLeastOneOfEach;
        }

        @Test
        public void shouldUpdateRoutingTableWithTheNewOne() throws Throwable
        {
            // Given
            RoutingTable routingTable = new ListBasedRoutingTable( asList( A ) );

            PooledConnection healthyConn = mock( PooledConnection.class );
            ConnectionPool mockedConnections = mock( ConnectionPool.class );
            when( mockedConnections.acquire( A ) ).thenReturn( healthyConn );

            ClusterComposition.Provider mockedProvider = mock( ClusterComposition.Provider.class );
            when( mockedProvider.getClusterComposition( healthyConn ) ).thenReturn( atLeastOneOfEach );

            // When
            ClusterComposition clusterComposition = rediscover( mockedConnections, routingTable, mockedProvider );

            // Then
            assertThat( clusterComposition, equalTo( atLeastOneOfEach ) );
        }
    }

    // TODO refactoing the Provider code and move this code to it so that we could test it prope
    public static class IllegalResponseTest
    {
        @Test
        public void shouldProtocolErrorWhenNotOneRecord() throws Throwable
        {
            // Given
            RoutingTable routingTable = new ListBasedRoutingTable( asList( A ) );

            PooledConnection healthyConn = mock( PooledConnection.class );
            ConnectionPool mockedConnections = mock( ConnectionPool.class );
            when( mockedConnections.acquire( A ) ).thenReturn( healthyConn );


            ClusterComposition.Provider mockedProvider = mock( ClusterComposition.Provider.class );
            ProtocolException exception = new ProtocolException( "No record found" );
            when( mockedProvider.getClusterComposition( healthyConn ) )
                    .thenThrow( exception );

            // When & When
            try
            {
                ClusterComposition clusterComposition = rediscover( mockedConnections, routingTable, mockedProvider );
                fail( "Expecting a failure but not triggered." );
            }
            catch( Exception e )
            {
                assertThat( e, instanceOf( ProtocolException.class ) );
                assertThat( e, equalTo( (Exception) exception ) );
            }
        }

        @Test
        public void shouldProtocolErrorWhenUnparsableRecord() throws Throwable
        {
            // Given
            RoutingTable routingTable = new ListBasedRoutingTable( asList( A ) );

            PooledConnection healthyConn = mock( PooledConnection.class );
            ConnectionPool mockedConnections = mock( ConnectionPool.class );
            when( mockedConnections.acquire( A ) ).thenReturn( healthyConn );


            ClusterComposition.Provider mockedProvider = mock( ClusterComposition.Provider.class );
            ProtocolException exception = new ProtocolException( "Unparsable record received" );
            when( mockedProvider.getClusterComposition( healthyConn ) )
                    .thenThrow( exception );

            // When & When
            try
            {
                ClusterComposition clusterComposition = rediscover( mockedConnections, routingTable, mockedProvider );
                fail( "Expecting a failure but not triggered." );
            }
            catch( Exception e )
            {
                assertThat( e, instanceOf( ProtocolException.class ) );
                assertThat( e, equalTo( (Exception) exception ) );
            }
        }

        @Test
        public void shouldProtocolErrorWhenNoRouters() throws Throwable
        {
            // Given
            RoutingTable routingTable = new ListBasedRoutingTable( asList( A ) );

            PooledConnection healthyConn = mock( PooledConnection.class );
            ConnectionPool mockedConnections = mock( ConnectionPool.class );
            when( mockedConnections.acquire( A ) ).thenReturn( healthyConn );

            ClusterComposition.Provider mockedProvider = mock( ClusterComposition.Provider.class );
            ClusterComposition noRouters = createClusterComposition( EMPTY, asList( A ), asList( C ) );
            when( mockedProvider.getClusterComposition( healthyConn ) ).thenReturn( noRouters );

            // When & When
            try
            {
                rediscover( mockedConnections, routingTable, mockedProvider );
                fail( "Expecting a failure but not triggered." );
            }
            catch( Exception e )
            {
                assertThat( e, instanceOf( ProtocolException.class ) ); // TODO this protocol error is not that
                // protocol error
                assertThat( e.getMessage(), startsWith( "Error to receive a routing table with no router in it" ) );
            }
        }

        @Test
        public void shouldProtocolErrorWhenNoReaders() throws Throwable
        {
            // Given
            RoutingTable routingTable = new ListBasedRoutingTable( asList( A ) );

            PooledConnection healthyConn = mock( PooledConnection.class );
            ConnectionPool mockedConnections = mock( ConnectionPool.class );
            when( mockedConnections.acquire( A ) ).thenReturn( healthyConn );

            ClusterComposition.Provider mockedProvider = mock( ClusterComposition.Provider.class );
            ClusterComposition noReaders = createClusterComposition( asList( A ), asList( C ) );
            when( mockedProvider.getClusterComposition( healthyConn ) ).thenReturn( noReaders );

            // When & When
            try
            {
                rediscover( mockedConnections, routingTable, mockedProvider );
                fail( "Expecting a failure but not triggered." );
            }
            catch( Exception e )
            {
                assertThat( e, instanceOf( ProtocolException.class ) ); // TODO protocol error
                assertThat( e.getMessage(), startsWith( "Error to receive a routing table with no readers in it" ) );
            }
        }
    }

    private static ClusterComposition rediscover( ConnectionPool connections, RoutingTable routingTable,
            ClusterComposition.Provider provider ) throws InterruptedException
    {

        RoutingSettings defaultRoutingSettings = new RoutingSettings( 1, 0 );
        Clock mockedClock = mock( Clock.class );
        Logger mockedLogger = mock( Logger.class );

        Rediscovery rediscovery = new Rediscovery( defaultRoutingSettings, mockedClock, mockedLogger, provider );
        return rediscovery.lookupRoutingTable( connections, routingTable );
    }


    @SafeVarargs
    private static ClusterComposition createClusterComposition( List<BoltServerAddress>... servers )
    {
        return createClusterComposition( NEVER_EXPIRE, servers );
    }

    @SafeVarargs
    private static ClusterComposition createClusterComposition( long expirationTimestamp, List<BoltServerAddress>...
            servers )
    {
        Set<BoltServerAddress> routers = new HashSet<>();
        Set<BoltServerAddress> writers = new HashSet<>();
        Set<BoltServerAddress> readers = new HashSet<>();

        switch( servers.length )
        {
        case 3:
            readers.addAll( servers[2] );
            // no break on purpose
        case 2:
            writers.addAll( servers[1] );
            // no break on purpose
        case 1:
            routers.addAll( servers[0] );
        }
        return new ClusterComposition( expirationTimestamp, readers, writers, routers );
    }

    private static class ListBasedRoutingTable implements RoutingTable
    {
        private final List<BoltServerAddress> routers;
        private int index;
        private final List<BoltServerAddress> removedRouters = new ArrayList<>();

        public ListBasedRoutingTable( List<BoltServerAddress> routers )
        {
            this.routers = routers;
            this.index = 0;
        }

        @Override
        public boolean isStale()
        {
            throw new NotImplementedException();
        }

        @Override
        public HashSet<BoltServerAddress> update( ClusterComposition cluster )
        {
            throw new NotImplementedException();
        }

        @Override
        public void forget( BoltServerAddress address )
        {
            throw new NotImplementedException();
        }

        @Override
        public RoundRobinAddressSet readers()
        {
            throw new NotImplementedException();
        }

        @Override
        public RoundRobinAddressSet writers()
        {
            throw new NotImplementedException();
        }

        @Override
        public BoltServerAddress nextRouter()
        {
            return routers.get( index ++ );
        }

        @Override
        public int routerSize()
        {
            return routers.size();
        }

        @Override
        public void removeWriter( BoltServerAddress toRemove )
        {
            throw new NotImplementedException();
        }

        @Override
        public void removeRouter( BoltServerAddress toRemove )
        {
            removedRouters.add( toRemove );
//            throw new UnsupportedOperationException( "Should never remove any router from routing table" );
        }
    }
}

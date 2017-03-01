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

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.exceptions.ProtocolException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.B;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.C;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.D;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.E;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.F;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.INVALID_CLUSTER_COMPOSITION;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.createClusterComposition;
import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;

@RunWith( Enclosed.class )
public class RediscoveryTest
{

    private static ClusterCompositionResponse.Success success( ClusterComposition cluster )
    {
        return new ClusterCompositionResponse.Success( cluster );
    }

    private static ClusterCompositionResponse.Failure failure( RuntimeException e )
    {
        return new ClusterCompositionResponse.Failure( e );
    }

    public static class RoutingSettingsTest
    {
        @Test
        public void shouldTryConfiguredMaxRoutingFailures() throws Exception
        {
            // given
            int maxRoutingFailures = 7;
            RoutingSettings settings = new RoutingSettings( maxRoutingFailures, 10 );

            RoutingTable routingTable = new TestRoutingTable( A );

            ClusterCompositionProvider mockedProvider = mock( ClusterCompositionProvider.class );
            when( mockedProvider.getClusterComposition( any( Connection.class ) ) )
                    .thenReturn( success( INVALID_CLUSTER_COMPOSITION ) );

            Rediscovery rediscovery = new Rediscovery( settings, mock( Clock.class ), DEV_NULL_LOGGER, mockedProvider );

            // when
            try
            {
                rediscovery.lookupClusterComposition( mock( ConnectionPool.class ), routingTable );
                fail("Should fail as failed to discovery");
            }
            catch( ServiceUnavailableException e )
            {
                assertThat( e.getMessage(), containsString( "No routing servers available" ) );
            }
            // then

            verify( mockedProvider, times( maxRoutingFailures ) ).getClusterComposition( any( Connection.class ) );

        }
    }

    public static class FailedToConnectTest
    {
        @Test
        public void shouldForgetRouterAndTryNextRouterWhenFailedToConnect() throws Throwable
        {
            // Given
            TestRoutingTable routingTable = new TestRoutingTable( A, B );

            PooledConnection healthyConn = mock( PooledConnection.class );
            ConnectionPool mockedConnections = mock( ConnectionPool.class );
            when( mockedConnections.acquire( A ) ).thenThrow( new ServiceUnavailableException( "failed to connect" ) );
            when( mockedConnections.acquire( B ) ).thenReturn( healthyConn );

            ClusterCompositionProvider
                    mockedProvider = mock( ClusterCompositionProvider.class );
            when( mockedProvider.getClusterComposition( healthyConn ) )
                    .thenReturn( success( ClusterCompositionUtil.VALID_CLUSTER_COMPOSITION ) );

            // When
            ClusterComposition clusterComposition = rediscover( mockedConnections, routingTable, mockedProvider );

            // Then
            assertThat( routingTable.removedRouters.size(), equalTo( 1 ) );
            assertThat( routingTable.removedRouters.get( 0 ), equalTo( A ) );
            assertThat( clusterComposition, equalTo( ClusterCompositionUtil.VALID_CLUSTER_COMPOSITION ) );
        }
    }

    public static class ProcedureNotFoundTest
    {
        @Test
        public void shouldThrowServiceUnavailableWhenNoProcedureFound() throws Throwable
        {
            // Given
            RoutingTable routingTable = new TestRoutingTable( A );

            PooledConnection healthyConn = mock( PooledConnection.class );
            ConnectionPool mockedConnections = mock( ConnectionPool.class );
            when( mockedConnections.acquire( A ) ).thenReturn( healthyConn );


            ClusterCompositionProvider
                    mockedProvider = mock( ClusterCompositionProvider.class );
            when( mockedProvider.getClusterComposition( healthyConn ) )
                    .thenReturn( failure( new ServiceUnavailableException( "No such procedure" ) ) );

            // When & When
            try
            {
                rediscover( mockedConnections, routingTable, mockedProvider );
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
                    { "([A], [C], [])", createClusterComposition( asList( A ), ClusterCompositionUtil.EMPTY, asList( C ) ) },
                    { "([A], [CD], [])", createClusterComposition( asList( A ), ClusterCompositionUtil.EMPTY, asList( C, D ) ) },
                    { "([AB], [C], [])", createClusterComposition( asList( A, B ), ClusterCompositionUtil.EMPTY, asList( C ) ) },
                    { "([AB], [CD], [])", createClusterComposition( asList( A, B ), ClusterCompositionUtil.EMPTY, asList( C, D ) )}
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
            RoutingTable routingTable = new TestRoutingTable( A, B );

            PooledConnection noWriterConn = mock( PooledConnection.class );
            PooledConnection healthyConn = mock( PooledConnection.class );
            ConnectionPool mockedConnections = mock( ConnectionPool.class );
            when( mockedConnections.acquire( A ) ).thenReturn( noWriterConn );
            when( mockedConnections.acquire( B ) ).thenReturn( healthyConn );

            ClusterCompositionProvider
                    mockedProvider = mock( ClusterCompositionProvider.class );
            when( mockedProvider.getClusterComposition( noWriterConn ) ).thenReturn( success( noWriters ) );
            when( mockedProvider.getClusterComposition( healthyConn ) )
                    .thenReturn( success( ClusterCompositionUtil.VALID_CLUSTER_COMPOSITION ) );

            // When
            ClusterComposition clusterComposition = rediscover( mockedConnections, routingTable, mockedProvider );

            // Then
            assertThat( clusterComposition, equalTo( ClusterCompositionUtil.VALID_CLUSTER_COMPOSITION ) );
        }

        @Test
        public void shouldThrowServiceUnavailableWhenNoNextRouter() throws Throwable
        {
            // Given
            RoutingTable routingTable = new TestRoutingTable( A );

            PooledConnection noWriterConn = mock( PooledConnection.class );
            ConnectionPool mockedConnections = mock( ConnectionPool.class );
            when( mockedConnections.acquire( A ) ).thenReturn( noWriterConn );

            ClusterCompositionProvider
                    mockedProvider = mock( ClusterCompositionProvider.class );
            when( mockedProvider.getClusterComposition( noWriterConn ) ).thenReturn( success( noWriters ) );

            // When & THen
            try
            {
                rediscover( mockedConnections, routingTable, mockedProvider );
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
            RoutingTable routingTable = new TestRoutingTable( A );

            PooledConnection healthyConn = mock( PooledConnection.class );
            ConnectionPool mockedConnections = mock( ConnectionPool.class );
            when( mockedConnections.acquire( A ) ).thenReturn( healthyConn );

            ClusterCompositionProvider
                    mockedProvider = mock( ClusterCompositionProvider.class );
            when( mockedProvider.getClusterComposition( healthyConn ) ).thenReturn( success( atLeastOneOfEach ) );

            // When
            ClusterComposition clusterComposition = rediscover( mockedConnections, routingTable, mockedProvider );

            // Then
            assertThat( clusterComposition, equalTo( atLeastOneOfEach ) );
        }
    }

    public static class IllegalResponseTest
    {
        @Test
        public void shouldProtocolErrorWhenFailedToPaseClusterCompositin() throws Throwable
        {
            // Given
            RoutingTable routingTable = new TestRoutingTable( A );

            PooledConnection healthyConn = mock( PooledConnection.class );
            ConnectionPool mockedConnections = mock( ConnectionPool.class );
            when( mockedConnections.acquire( A ) ).thenReturn( healthyConn );


            ClusterCompositionProvider mockedProvider = mock( ClusterCompositionProvider.class );
            ProtocolException exception = new ProtocolException( "Failed to parse result" );
            when( mockedProvider.getClusterComposition( healthyConn ) ).thenReturn( failure( exception ) );

            // When & When
            try
            {
                rediscover( mockedConnections, routingTable, mockedProvider );
                fail( "Expecting a failure but not triggered." );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( ProtocolException.class ) );
                assertThat( e, equalTo( (Exception) exception ) );
            }
        }
    }

    private static ClusterComposition rediscover( ConnectionPool connections, RoutingTable routingTable,
            ClusterCompositionProvider provider ) throws InterruptedException
    {

        RoutingSettings defaultRoutingSettings = new RoutingSettings( 1, 0 );
        Clock mockedClock = mock( Clock.class );
        Logger mockedLogger = mock( Logger.class );

        Rediscovery rediscovery = new Rediscovery( defaultRoutingSettings, mockedClock, mockedLogger, provider );
        return rediscovery.lookupClusterComposition( connections, routingTable );
    }

    private static class TestRoutingTable extends ClusterRoutingTable
    {
        final List<BoltServerAddress> removedRouters = new ArrayList<>();

        TestRoutingTable( BoltServerAddress... routers )
        {
            super( Clock.SYSTEM, routers );
        }

        @Override
        public void removeRouter( BoltServerAddress router )
        {
            super.removeRouter( router );
            removedRouters.add( router );
        }
    }
}

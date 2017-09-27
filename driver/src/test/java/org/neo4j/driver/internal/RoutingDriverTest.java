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
package org.neo4j.driver.internal;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.internal.stubbing.answers.ThrowsException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.cluster.loadbalancing.LeastConnectedLoadBalancingStrategy;
import org.neo4j.driver.internal.cluster.loadbalancing.LoadBalancer;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.retry.FixedRetryLogic;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.summary.InternalServerInfo;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ProtocolException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.fail;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.cluster.ClusterCompositionProviderTest.serverInfo;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.security.SecurityPlan.insecure;
import static org.neo4j.driver.v1.Values.value;

public class RoutingDriverTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();
    private static final BoltServerAddress SEED = new BoltServerAddress( "localhost", 7687 );
    private static final String GET_SERVERS = "CALL dbms.cluster.routing.getServers";
    private final FakeClock clock = new FakeClock();

    @Test
    public void shouldDiscoveryOnInitialization()
    {
        // Given
        ConnectionPool pool = poolWithServers(
                10,
                serverInfo( "ROUTE", "localhost:1111" ),
                serverInfo( "READ", "localhost:2222" ),
                serverInfo( "WRITE", "localhost:3333" ) );

        // When
        driverWithPool( pool );

        // Then
        verify( pool ).acquire( SEED );
    }

    @Test
    public void shouldRediscoveryIfNoWritersProvided()
    {
        // Given
        Driver driver = driverWithPool( pool(
                withServers( 10, serverInfo( "ROUTE", "localhost:1111" ),
                        serverInfo( "WRITE" ),
                        serverInfo( "READ", "localhost:5555" ) ),
                withServers( 10, serverInfo( "ROUTE", "localhost:1112" ),
                        serverInfo( "READ", "localhost:2222" ),
                        serverInfo( "WRITE", "localhost:3333" ) ) ) );

        // When
        NetworkSessionWithAddress writing = (NetworkSessionWithAddress) driver.session( AccessMode.WRITE );

        // Then
        assertEquals( boltAddress( "localhost", 3333 ), writing.address );
    }

    @Test
    public void shouldNotRediscoveryOnSessionAcquisitionIfNotNecessary()
    {
        // Given
        Driver driver = driverWithPool( pool(
                withServers( 10, serverInfo( "ROUTE", "localhost:1111", "localhost:1112", "localhost:1113" ),
                        serverInfo( "READ", "localhost:2222" ),
                        serverInfo( "WRITE", "localhost:3333" ) ),
                withServers( 10, serverInfo( "ROUTE", "localhost:5555" ),
                        serverInfo( "READ", "localhost:5555" ),
                        serverInfo( "WRITE", "localhost:5555" ) ) ) );

        // When
        NetworkSessionWithAddress writing = (NetworkSessionWithAddress) driver.session( AccessMode.WRITE );
        NetworkSessionWithAddress reading = (NetworkSessionWithAddress) driver.session( AccessMode.READ );

        // Then
        assertEquals( boltAddress( "localhost", 3333 ), writing.address );
        assertEquals( boltAddress( "localhost", 2222 ), reading.address );
    }

    @Test
    public void shouldFailIfNoRouting()
    {
        // Given
        ConnectionPool pool = pool( new ThrowsException( new ClientException(
                "Neo.ClientError.Procedure.ProcedureNotFound", "Procedure not found" ) ) );

        // When
        try
        {
            driverWithPool( pool );
        }
        // Then
        catch ( ServiceUnavailableException e )
        {
            assertThat( e.getMessage(),
                    containsString( "Failed to run 'CALL dbms.cluster.routing.getServers {}' on server." ) );
        }
    }

    @Test
    public void shouldFailIfNoRoutersProvided()
    {
        // Given
        ConnectionPool pool = poolWithServers(
                10,
                serverInfo( "ROUTE" ),
                serverInfo( "READ", "localhost:1111" ),
                serverInfo( "WRITE", "localhost:1111" ) );

        // When
        try
        {
            driverWithPool( pool );
        }
        // Then
        catch ( ProtocolException e )
        {
            assertThat( e.getMessage(), containsString( "no router or reader found in response" ) );
        }
    }

    @Test
    public void shouldFailIfNoReaderProvided()
    {
        // Given
        ConnectionPool pool = poolWithServers(
                10,
                serverInfo( "READ" ),
                serverInfo( "ROUTE", "localhost:1111" ),
                serverInfo( "WRITE", "localhost:1111" ) );

        // When
        try
        {
            driverWithPool( pool );
        }
        // Then
        catch ( ProtocolException e )
        {
            assertThat( e.getMessage(), containsString( "no router or reader found in response" ) );
        }
    }

    @Test
    public void shouldForgetServersOnRediscovery()
    {
        // Given
        ConnectionPool pool = pool(
                withServers( 10, serverInfo( "ROUTE", "localhost:1111" ),
                        serverInfo( "READ", "localhost:5555" ),
                        serverInfo( "WRITE" ) ),
                withServers( 10, serverInfo( "ROUTE", "localhost:1112" ),
                        serverInfo( "READ", "localhost:2222" ),
                        serverInfo( "WRITE", "localhost:3333" ) ) );

        Driver driver = driverWithPool( pool );

        // When
        NetworkSessionWithAddress write1 = (NetworkSessionWithAddress) driver.session( AccessMode.WRITE );
        NetworkSessionWithAddress write2 = (NetworkSessionWithAddress) driver.session( AccessMode.WRITE );

        // Then
        assertEquals( boltAddress( "localhost", 3333 ), write1.address );
        assertEquals( boltAddress( "localhost", 3333 ), write2.address );
    }

    @Test
    public void shouldRediscoverOnTimeout()
    {
        // Given
        Driver driver = driverWithPool( pool(
                withServers( 10, serverInfo( "ROUTE", "localhost:1111", "localhost:1112", "localhost:1113" ),
                        serverInfo( "READ", "localhost:2222" ),
                        serverInfo( "WRITE", "localhost:3333" ) ),
                withServers( 60, serverInfo( "ROUTE", "localhost:5555", "localhost:6666" ),
                        serverInfo( "READ", "localhost:7777" ),
                        serverInfo( "WRITE", "localhost:8888" ) ) ) );

        clock.progress( 11_000 );

        // When
        NetworkSessionWithAddress writing = (NetworkSessionWithAddress) driver.session( AccessMode.WRITE );
        NetworkSessionWithAddress reading = (NetworkSessionWithAddress) driver.session( AccessMode.READ );

        // Then
        assertEquals( boltAddress( "localhost", 8888 ), writing.address );
        assertEquals( boltAddress( "localhost", 7777 ), reading.address );
    }

    @Test
    public void shouldNotRediscoverWhenNoTimeout()
    {
        // Given
        Driver driver = driverWithPool( pool(
                withServers( 10, serverInfo( "ROUTE", "localhost:1111", "localhost:1112", "localhost:1113" ),
                        serverInfo( "READ", "localhost:2222" ),
                        serverInfo( "WRITE", "localhost:3333" ) ),
                withServers( 10, serverInfo( "ROUTE", "localhost:5555" ),
                        serverInfo( "READ", "localhost:5555" ),
                        serverInfo( "WRITE", "localhost:5555" ) ) ) );
        clock.progress( 9900 );

        // When
        NetworkSessionWithAddress writer = (NetworkSessionWithAddress) driver.session( AccessMode.WRITE );
        NetworkSessionWithAddress reader = (NetworkSessionWithAddress) driver.session( AccessMode.READ );

        // Then
        assertEquals( boltAddress( "localhost", 2222 ), reader.address );
        assertEquals( boltAddress( "localhost", 3333 ), writer.address );
    }

    @Test
    public void shouldRoundRobinAmongReadServers()
    {
        // Given
        Driver driver = driverWithServers( 60,
                serverInfo( "ROUTE", "localhost:1111", "localhost:1112" ),
                serverInfo( "READ", "localhost:2222", "localhost:2223", "localhost:2224" ),
                serverInfo( "WRITE", "localhost:3333" ) );

        // When
        NetworkSessionWithAddress read1 = (NetworkSessionWithAddress) driver.session( AccessMode.READ );
        NetworkSessionWithAddress read2 = (NetworkSessionWithAddress) driver.session( AccessMode.READ );
        NetworkSessionWithAddress read3 = (NetworkSessionWithAddress) driver.session( AccessMode.READ );
        NetworkSessionWithAddress read4 = (NetworkSessionWithAddress) driver.session( AccessMode.READ );
        NetworkSessionWithAddress read5 = (NetworkSessionWithAddress) driver.session( AccessMode.READ );
        NetworkSessionWithAddress read6 = (NetworkSessionWithAddress) driver.session( AccessMode.READ );

        // Then
        assertEquals( read1.address, read4.address );
        assertEquals( read2.address, read5.address );
        assertEquals( read3.address, read6.address );
        assertNotEquals( read1.address, read2.address );
        assertNotEquals( read2.address, read3.address );
        assertNotEquals( read3.address, read1.address );
    }

    @Test
    public void shouldRoundRobinAmongWriteServers()
    {
        // Given
        Driver driver = driverWithServers( 60, serverInfo( "ROUTE", "localhost:1111", "localhost:1112" ),
                serverInfo( "READ", "localhost:3333" ),
                serverInfo( "WRITE", "localhost:2222", "localhost:2223", "localhost:2224" ) );

        // When
        NetworkSessionWithAddress write1 = (NetworkSessionWithAddress) driver.session( AccessMode.WRITE );
        NetworkSessionWithAddress write2 = (NetworkSessionWithAddress) driver.session( AccessMode.WRITE );
        NetworkSessionWithAddress write3 = (NetworkSessionWithAddress) driver.session( AccessMode.WRITE );
        NetworkSessionWithAddress write4 = (NetworkSessionWithAddress) driver.session( AccessMode.WRITE );
        NetworkSessionWithAddress write5 = (NetworkSessionWithAddress) driver.session( AccessMode.WRITE );
        NetworkSessionWithAddress write6 = (NetworkSessionWithAddress) driver.session( AccessMode.WRITE );

        // Then
        assertEquals( write1.address, write4.address );
        assertEquals( write2.address, write5.address );
        assertEquals( write3.address, write6.address );
        assertNotEquals( write1.address, write2.address );
        assertNotEquals( write2.address, write3.address );
        assertNotEquals( write3.address, write1.address );
    }

    @SuppressWarnings( "deprecation" )
    @Test
    public void testTrustOnFirstUseNotCompatibleWithRoutingDriver()
    {
        // Given
        final Config tofuConfig = Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.REQUIRED )
                .withTrustStrategy( Config.TrustStrategy.trustOnFirstUse( new File( "foo" ) ) ).toConfig();

        try
        {
            // When
            GraphDatabase.driver( "bolt+routing://127.0.0.1:7687", tofuConfig );
            fail();
        }
        catch ( IllegalArgumentException e )
        {
            // Then we should end up here
        }
    }

    @SafeVarargs
    private final Driver driverWithServers( long ttl, Map<String,Object>... serverInfo )
    {
        return driverWithPool( poolWithServers( ttl, serverInfo ) );
    }

    private Driver driverWithPool( ConnectionPool pool )
    {
        Logging logging = DEV_NULL_LOGGING;
        RoutingSettings settings = new RoutingSettings( 10, 5_000, null );
        ConnectionProvider connectionProvider = new LoadBalancer( SEED, settings, pool, clock, logging,
                new LeastConnectedLoadBalancingStrategy( pool, logging ) );
        Config config = Config.build().withLogging( logging ).toConfig();
        SessionFactory sessionFactory = new NetworkSessionWithAddressFactory( connectionProvider, config );
        return new InternalDriver( insecure(), sessionFactory, logging );
    }

    @SafeVarargs
    private final ConnectionPool poolWithServers( long ttl, Map<String,Object>... serverInfo )
    {
        return pool( withServers( ttl, serverInfo ) );
    }

    @SafeVarargs
    private static Answer withServers( long ttl, Map<String,Object>... serverInfo )
    {
        return withServerList( new Value[]{value( ttl ), value( asList( serverInfo ) )} );
    }

    private BoltServerAddress boltAddress( String host, int port )
    {
        return new BoltServerAddress( host, port );
    }

    private ConnectionPool pool( final Answer toGetServers, final Answer... furtherGetServers )
    {
        ConnectionPool pool = mock( ConnectionPool.class );

        when( pool.acquire( any( BoltServerAddress.class ) ) ).thenAnswer( new Answer<PooledConnection>()
        {
            int answer;

            @Override
            public PooledConnection answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                BoltServerAddress address = invocationOnMock.getArgumentAt( 0, BoltServerAddress.class );
                PooledConnection connection = mock( PooledConnection.class );
                when( connection.isOpen() ).thenReturn( true );
                when( connection.boltServerAddress() ).thenReturn( address );
                when( connection.server() ).thenReturn( new InternalServerInfo( address, "Neo4j/3.1.0" ) );
                doAnswer( withKeys( "ttl", "servers" ) ).when( connection ).run(
                        eq( GET_SERVERS ),
                        eq( Collections.<String,Value>emptyMap() ),
                        any( ResponseHandler.class ) );
                if ( answer > furtherGetServers.length )
                {
                    answer = furtherGetServers.length;
                }
                int offset = answer++;
                doAnswer( offset == 0 ? toGetServers : furtherGetServers[offset - 1] )
                        .when( connection ).pullAll( any( ResponseHandler.class ) );

                return connection;
            }
        } );

        return pool;
    }

    private static ResponseHandlerAnswer withKeys( final String... keys )
    {
        return new ResponseHandlerAnswer()
        {
            @Override
            void setUp( ResponseHandler handler )
            {
                handler.onSuccess( Collections.singletonMap( "fields", value( Arrays.asList( keys ) ) ) );
            }
        };
    }

    private static ResponseHandlerAnswer withServerList( final Value[]... records )
    {
        return new ResponseHandlerAnswer()
        {
            @Override
            void setUp( ResponseHandler handler )
            {
                for ( Value[] fields : records )
                {
                    handler.onRecord( fields );
                }
                handler.onSuccess( Collections.<String,Value>emptyMap() );
            }
        };
    }

    private static class NetworkSessionWithAddressFactory extends SessionFactoryImpl
    {
        NetworkSessionWithAddressFactory( ConnectionProvider connectionProvider, Config config )
        {
            super( connectionProvider, new FixedRetryLogic( 0 ), config );
        }

        @Override
        protected NetworkSession createSession( ConnectionProvider connectionProvider, RetryLogic retryLogic,
                AccessMode mode, Logging logging )
        {
            return new NetworkSessionWithAddress( connectionProvider, mode, logging );
        }
    }

    private static class NetworkSessionWithAddress extends NetworkSession
    {
        final BoltServerAddress address;

        NetworkSessionWithAddress( ConnectionProvider connectionProvider, AccessMode mode, Logging logging )
        {
            super( connectionProvider, mode, new FixedRetryLogic( 0 ), logging );
            try ( PooledConnection connection = connectionProvider.acquireConnection( mode ) )
            {
                this.address = connection.boltServerAddress();
            }
        }
    }

    private static abstract class ResponseHandlerAnswer implements Answer<Void>
    {
        abstract void setUp( ResponseHandler handler );

        @Override
        public Void answer( InvocationOnMock invocation ) throws Throwable
        {
            ResponseHandler handler = handlerFrom( invocation );
            setUp( handler );
            return null;
        }

        private ResponseHandler handlerFrom( InvocationOnMock invocation )
        {
            switch ( invocation.getMethod().getName() )
            {
            case "pullAll":
                return invocation.getArgumentAt( 0, ResponseHandler.class );
            case "run":
                return invocation.getArgumentAt( 2, ResponseHandler.class );
            default:
                throw new UnsupportedOperationException( invocation.getMethod().getName() );
            }
        }
    }
}

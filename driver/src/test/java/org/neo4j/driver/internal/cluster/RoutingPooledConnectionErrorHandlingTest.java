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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.neo4j.driver.internal.cluster.loadbalancing.LoadBalancer;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.internal.net.pooling.SocketConnectionPool;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.exceptions.TransientException;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.net.pooling.PoolSettings.DEFAULT_MAX_IDLE_CONNECTION_POOL_SIZE;
import static org.neo4j.driver.internal.net.pooling.PoolSettings.INFINITE_CONNECTION_LIFETIME;
import static org.neo4j.driver.internal.net.pooling.PoolSettings.NO_IDLE_CONNECTION_TEST;
import static org.neo4j.driver.internal.util.Matchers.containsReader;
import static org.neo4j.driver.internal.util.Matchers.containsRouter;
import static org.neo4j.driver.internal.util.Matchers.containsWriter;
import static org.neo4j.driver.v1.AccessMode.READ;
import static org.neo4j.driver.v1.AccessMode.WRITE;
import static org.neo4j.driver.v1.Values.value;

@RunWith( Parameterized.class )
public class RoutingPooledConnectionErrorHandlingTest
{
    private static final BoltServerAddress ADDRESS1 = new BoltServerAddress( "server-1", 26000 );
    private static final BoltServerAddress ADDRESS2 = new BoltServerAddress( "server-2", 27000 );
    private static final BoltServerAddress ADDRESS3 = new BoltServerAddress( "server-3", 28000 );

    @Parameter
    public ConnectionMethod method;

    @Parameters( name = "{0}" )
    public static List<ConnectionMethod> methods()
    {
        return asList(
                new Init(),
                new Run(),
                new DiscardAll(),
                new PullAll(),
                new Reset(),
                new ResetAsync(),
                new AckFailure(),
                new Sync(),
                new Flush(),
                new ReceiveOne() );
    }

    @Test
    public void shouldHandleServiceUnavailableException()
    {
        ServiceUnavailableException serviceUnavailable = new ServiceUnavailableException( "Oh!" );
        Connector connector = newConnectorWithThrowingConnections( serviceUnavailable );
        ClusterComposition clusterComposition = newClusterComposition( ADDRESS1, ADDRESS2, ADDRESS3 );
        RoutingTable routingTable = newRoutingTable( clusterComposition );
        ConnectionPool connectionPool = newConnectionPool( connector, ADDRESS1, ADDRESS2, ADDRESS3 );
        LoadBalancer loadBalancer = newLoadBalancer( clusterComposition, routingTable, connectionPool );

        Connection readConnection = loadBalancer.acquireConnection( READ );
        verifyServiceUnavailableHandling( readConnection, routingTable, connectionPool );

        Connection writeConnection = loadBalancer.acquireConnection( WRITE );
        verifyServiceUnavailableHandling( writeConnection, routingTable, connectionPool );

        assertThat( routingTable, containsRouter( ADDRESS3 ) );
        assertTrue( connectionPool.hasAddress( ADDRESS3 ) );
    }

    @Test
    public void shouldHandleFailureToWriteWithWriteConnection()
    {
        testHandleFailureToWriteWithWriteConnection( new ClientException( "Neo.ClientError.Cluster.NotALeader", "" ) );
        testHandleFailureToWriteWithWriteConnection(
                new ClientException( "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase", "" ) );
    }

    @Test
    public void shouldHandleFailureToWrite()
    {
        testHandleFailureToWrite( new ClientException( "Neo.ClientError.Cluster.NotALeader", "" ) );
        testHandleFailureToWrite( new ClientException( "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase", "" ) );
    }

    @Test
    public void shouldPropagateThrowable()
    {
        testThrowablePropagation( new RuntimeException( "Random error" ) );
    }

    @Test
    public void shouldPropagateClientExceptionWithoutErrorCode()
    {
        testThrowablePropagation( new ClientException( null, "Message" ) );
    }

    @Test
    public void shouldHandleTransientException()
    {
        TransientException error = new TransientException( "Neo.TransientError.Transaction.DeadlockDetected", "" );
        testTransientErrorHandling( error, false );
    }

    @Test
    public void shouldHandleTransientDatabaseUnavailableException()
    {
        TransientException error = new TransientException( "Neo.TransientError.General.DatabaseUnavailable", "" );
        testTransientErrorHandling( error, true );
    }

    private void testHandleFailureToWriteWithWriteConnection( ClientException error )
    {
        Connector connector = newConnectorWithThrowingConnections( error );
        ClusterComposition clusterComposition = newClusterComposition( ADDRESS1, ADDRESS2, ADDRESS3 );
        RoutingTable routingTable = newRoutingTable( clusterComposition );
        ConnectionPool connectionPool = newConnectionPool( connector, ADDRESS1, ADDRESS2, ADDRESS3 );
        LoadBalancer loadBalancer = newLoadBalancer( clusterComposition, routingTable, connectionPool );

        Connection readConnection = loadBalancer.acquireConnection( READ );
        try
        {
            method.invoke( readConnection );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );

            BoltServerAddress address = readConnection.boltServerAddress();
            assertThat( routingTable, containsRouter( address ) );
            assertThat( routingTable, containsReader( address ) );
            assertThat( routingTable, containsWriter( address ) );
            assertTrue( connectionPool.hasAddress( address ) );
        }

        assertThat( routingTable, containsRouter( ADDRESS3 ) );
        assertTrue( connectionPool.hasAddress( ADDRESS3 ) );
    }

    private void testHandleFailureToWrite( ClientException error )
    {
        Connector connector = newConnectorWithThrowingConnections( error );
        ClusterComposition clusterComposition = newClusterComposition( ADDRESS1, ADDRESS2, ADDRESS3 );
        RoutingTable routingTable = newRoutingTable( clusterComposition );
        ConnectionPool connectionPool = newConnectionPool( connector, ADDRESS1, ADDRESS2, ADDRESS3 );
        LoadBalancer loadBalancer = newLoadBalancer( clusterComposition, routingTable, connectionPool );

        Connection readConnection = loadBalancer.acquireConnection( WRITE );
        try
        {
            method.invoke( readConnection );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( SessionExpiredException.class ) );

            BoltServerAddress address = readConnection.boltServerAddress();
            assertThat( routingTable, containsRouter( address ) );
            assertThat( routingTable, containsReader( address ) );
            assertThat( routingTable, not( containsWriter( address ) ) );
            assertTrue( connectionPool.hasAddress( address ) );
        }

        assertThat( routingTable, containsRouter( ADDRESS3 ) );
        assertTrue( connectionPool.hasAddress( ADDRESS3 ) );
    }

    private void testThrowablePropagation( Throwable error )
    {
        Connector connector = newConnectorWithThrowingConnections( error );
        ClusterComposition clusterComposition = newClusterComposition( ADDRESS1, ADDRESS2, ADDRESS3 );
        RoutingTable routingTable = newRoutingTable( clusterComposition );
        ConnectionPool connectionPool = newConnectionPool( connector, ADDRESS1, ADDRESS2, ADDRESS3 );
        LoadBalancer loadBalancer = newLoadBalancer( clusterComposition, routingTable, connectionPool );

        Connection readConnection = loadBalancer.acquireConnection( READ );
        verifyThrowablePropagation( readConnection, routingTable, connectionPool, error.getClass() );

        Connection writeConnection = loadBalancer.acquireConnection( WRITE );
        verifyThrowablePropagation( writeConnection, routingTable, connectionPool, error.getClass() );

        assertThat( routingTable, containsRouter( ADDRESS3 ) );
        assertTrue( connectionPool.hasAddress( ADDRESS3 ) );
    }

    private void testTransientErrorHandling( TransientException error, boolean shouldRemoveFromRoutingTable )
    {
        Connector connector = newConnectorWithThrowingConnections( error );
        ClusterComposition clusterComposition = newClusterComposition( ADDRESS1, ADDRESS2, ADDRESS3 );
        RoutingTable routingTable = newRoutingTable( clusterComposition );
        ConnectionPool connectionPool = newConnectionPool( connector, ADDRESS1, ADDRESS2, ADDRESS3 );
        LoadBalancer loadBalancer = newLoadBalancer( clusterComposition, routingTable, connectionPool );

        Connection connection = loadBalancer.acquireConnection( READ );
        try
        {
            method.invoke( connection );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );

            BoltServerAddress address = connection.boltServerAddress();
            if ( shouldRemoveFromRoutingTable )
            {
                assertThat( routingTable, not( containsRouter( address ) ) );
                assertThat( routingTable, not( containsReader( address ) ) );
                assertThat( routingTable, not( containsWriter( address ) ) );
                assertFalse( connectionPool.hasAddress( address ) );
            }
            else
            {
                assertThat( routingTable, containsRouter( address ) );
                assertThat( routingTable, containsReader( address ) );
                assertThat( routingTable, containsWriter( address ) );
                assertTrue( connectionPool.hasAddress( address ) );
            }
        }
    }

    private void verifyServiceUnavailableHandling( Connection connection, RoutingTable routingTable,
            ConnectionPool connectionPool )
    {
        try
        {
            method.invoke( connection );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( SessionExpiredException.class ) );
            assertThat( e.getCause(), instanceOf( ServiceUnavailableException.class ) );

            BoltServerAddress address = connection.boltServerAddress();
            assertThat( routingTable, not( containsRouter( address ) ) );
            assertThat( routingTable, not( containsReader( address ) ) );
            assertThat( routingTable, not( containsWriter( address ) ) );
            assertFalse( connectionPool.hasAddress( address ) );
        }
    }

    private <T extends Throwable> void verifyThrowablePropagation( Connection connection, RoutingTable routingTable,
            ConnectionPool connectionPool, Class<T> expectedClass )
    {
        try
        {
            method.invoke( connection );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( expectedClass ) );

            BoltServerAddress address = connection.boltServerAddress();
            assertThat( routingTable, containsRouter( address ) );
            assertThat( routingTable, containsReader( address ) );
            assertThat( routingTable, containsWriter( address ) );
            assertTrue( connectionPool.hasAddress( address ) );
        }
    }

    private Connector newConnectorWithThrowingConnections( final Throwable error )
    {
        Connector connector = mock( Connector.class );
        when( connector.connect( any( BoltServerAddress.class ) ) ).thenAnswer( new Answer<Connection>()
        {
            @Override
            public Connection answer( InvocationOnMock invocation ) throws Throwable
            {
                BoltServerAddress address = invocation.getArgumentAt( 0, BoltServerAddress.class );
                Connection connection = newConnectionMock( address );
                method.invoke( doThrow( error ).doNothing().when( connection ) );
                return connection;
            }
        } );
        return connector;
    }

    private static Connection newConnectionMock( BoltServerAddress address )
    {
        Connection connection = mock( Connection.class );
        when( connection.boltServerAddress() ).thenReturn( address );
        return connection;
    }

    private static ClusterComposition newClusterComposition( BoltServerAddress... addresses )
    {
        return new ClusterComposition(
                Long.MAX_VALUE,
                new HashSet<>( asList( addresses ) ),
                new HashSet<>( asList( addresses ) ),
                new HashSet<>( asList( addresses ) ) );
    }

    private static RoutingTable newRoutingTable( ClusterComposition clusterComposition )
    {
        RoutingTable routingTable = new ClusterRoutingTable( Clock.SYSTEM );
        routingTable.update( clusterComposition );
        return routingTable;
    }

    private static ConnectionPool newConnectionPool( Connector connector, BoltServerAddress... addresses )
    {
        int maxIdleConnections = DEFAULT_MAX_IDLE_CONNECTION_POOL_SIZE;
        PoolSettings settings = new PoolSettings( maxIdleConnections, NO_IDLE_CONNECTION_TEST,
                INFINITE_CONNECTION_LIFETIME );
        SocketConnectionPool pool = new SocketConnectionPool( settings, connector, Clock.SYSTEM, DEV_NULL_LOGGING );

        // force pool to create and memorize some connections
        for ( BoltServerAddress address : addresses )
        {
            List<PooledConnection> connections = new ArrayList<>();
            for ( int i = 0; i < maxIdleConnections; i++ )
            {
                connections.add( pool.acquire( address ) );
            }
            for ( PooledConnection connection : connections )
            {
                connection.close();
            }
        }

        return pool;
    }

    private static LoadBalancer newLoadBalancer( ClusterComposition clusterComposition, RoutingTable routingTable,
            ConnectionPool connectionPool )
    {
        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.lookupClusterComposition( routingTable, connectionPool ) ).thenReturn( clusterComposition );
        return new LoadBalancer( connectionPool, routingTable, rediscovery, DEV_NULL_LOGGING );
    }

    private interface ConnectionMethod
    {
        void invoke( Connection connection );
    }

    private static class Init implements ConnectionMethod
    {
        @Override
        public void invoke( Connection connection )
        {
            connection.init( "JavaDriver", singletonMap( "Key", value( "Value" ) ) );
        }
    }

    private static class Run implements ConnectionMethod
    {
        @Override
        public void invoke( Connection connection )
        {
            connection.run( "CREATE (n:Node {name: {value}})", singletonMap( "value", value( "A" ) ),
                    NoOpResponseHandler.INSTANCE );
        }
    }

    private static class DiscardAll implements ConnectionMethod
    {
        @Override
        public void invoke( Connection connection )
        {
            connection.discardAll( NoOpResponseHandler.INSTANCE );
        }
    }

    private static class PullAll implements ConnectionMethod
    {
        @Override
        public void invoke( Connection connection )
        {
            connection.pullAll( NoOpResponseHandler.INSTANCE );
        }
    }

    private static class Reset implements ConnectionMethod
    {
        @Override
        public void invoke( Connection connection )
        {
            connection.reset();
        }
    }

    private static class ResetAsync implements ConnectionMethod
    {
        @Override
        public void invoke( Connection connection )
        {
            connection.resetAsync();
        }
    }

    private static class AckFailure implements ConnectionMethod
    {
        @Override
        public void invoke( Connection connection )
        {
            connection.ackFailure();
        }
    }

    private static class Sync implements ConnectionMethod
    {
        @Override
        public void invoke( Connection connection )
        {
            connection.sync();
        }
    }

    private static class Flush implements ConnectionMethod
    {
        @Override
        public void invoke( Connection connection )
        {
            connection.flush();
        }
    }

    private static class ReceiveOne implements ConnectionMethod
    {
        @Override
        public void invoke( Connection connection )
        {
            connection.receiveOne();
        }
    }
}

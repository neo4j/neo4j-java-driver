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
package org.neo4j.driver.internal.net.pooling;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.v1.Logging;

import static java.util.Collections.newSetFromMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.net.BoltServerAddress.DEFAULT_PORT;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;

public class SocketConnectionPoolTest
{
    private static final BoltServerAddress ADDRESS_1 = LOCAL_DEFAULT;
    private static final BoltServerAddress ADDRESS_2 = new BoltServerAddress( "localhost", DEFAULT_PORT + 42 );
    private static final BoltServerAddress ADDRESS_3 = new BoltServerAddress( "localhost", DEFAULT_PORT + 4242 );

    @Test
    public void acquireCreatesNewConnectionWhenPoolIsEmpty()
    {
        Connector connector = newMockConnector();
        SocketConnectionPool pool = newPool( connector );

        Connection connection = pool.acquire( ADDRESS_1 );

        assertThat( connection, instanceOf( PooledConnection.class ) );
        verify( connector ).connect( ADDRESS_1 );
    }

    @Test
    public void acquireUsesExistingConnectionIfPresent()
    {
        Connection connection = newConnectionMock( ADDRESS_1 );
        Connector connector = newMockConnector( connection );

        SocketConnectionPool pool = newPool( connector );

        Connection acquiredConnection1 = pool.acquire( ADDRESS_1 );
        assertThat( acquiredConnection1, instanceOf( PooledConnection.class ) );
        acquiredConnection1.close(); // return connection to the pool

        Connection acquiredConnection2 = pool.acquire( ADDRESS_1 );
        assertThat( acquiredConnection2, instanceOf( PooledConnection.class ) );

        verify( connector ).connect( ADDRESS_1 );
    }

    @Test
    public void purgeDoesNothingForNonExistingAddress()
    {
        Connection connection = newConnectionMock( ADDRESS_1 );
        SocketConnectionPool pool = newPool( newMockConnector( connection ) );

        pool.acquire( ADDRESS_1 ).close();

        assertTrue( pool.hasAddress( ADDRESS_1 ) );
        pool.purge( ADDRESS_2 );
        assertTrue( pool.hasAddress( ADDRESS_1 ) );
    }

    @Test
    public void purgeRemovesAddress()
    {
        Connection connection = newConnectionMock( ADDRESS_1 );
        SocketConnectionPool pool = newPool( newMockConnector( connection ) );

        pool.acquire( ADDRESS_1 ).close();

        assertTrue( pool.hasAddress( ADDRESS_1 ) );
        pool.purge( ADDRESS_1 );
        assertFalse( pool.hasAddress( ADDRESS_1 ) );
    }

    @Test
    public void purgeTerminatesPoolCorrespondingToTheAddress()
    {
        Connection connection1 = newConnectionMock( ADDRESS_1 );
        Connection connection2 = newConnectionMock( ADDRESS_1 );
        Connection connection3 = newConnectionMock( ADDRESS_1 );
        SocketConnectionPool pool = newPool( newMockConnector( connection1, connection2, connection3 ) );

        Connection pooledConnection1 = pool.acquire( ADDRESS_1 );
        Connection pooledConnection2 = pool.acquire( ADDRESS_1 );
        pool.acquire( ADDRESS_1 );

        // return two connections to the pool
        pooledConnection1.close();
        pooledConnection2.close();

        pool.purge( ADDRESS_1 );

        verify( connection1 ).close();
        verify( connection2 ).close();
        verify( connection3 ).close();
    }

    @Test
    public void hasAddressReturnsFalseWhenPoolIsEmpty()
    {
        SocketConnectionPool pool = newPool( newMockConnector() );

        assertFalse( pool.hasAddress( ADDRESS_1 ) );
        assertFalse( pool.hasAddress( ADDRESS_2 ) );
    }

    @Test
    public void hasAddressReturnsFalseForUnknownAddress()
    {
        SocketConnectionPool pool = newPool( newMockConnector() );

        assertNotNull( pool.acquire( ADDRESS_1 ) );

        assertFalse( pool.hasAddress( ADDRESS_2 ) );
    }

    @Test
    public void hasAddressReturnsTrueForKnownAddress()
    {
        SocketConnectionPool pool = newPool( newMockConnector() );

        assertNotNull( pool.acquire( ADDRESS_1 ) );

        assertTrue( pool.hasAddress( ADDRESS_1 ) );
    }

    @Test
    public void closeTerminatesAllPools()
    {
        Connection connection1 = newConnectionMock( ADDRESS_1 );
        Connection connection2 = newConnectionMock( ADDRESS_1 );
        Connection connection3 = newConnectionMock( ADDRESS_2 );
        Connection connection4 = newConnectionMock( ADDRESS_2 );

        Connector connector = newMockConnector( connection1, connection2, connection3, connection4 );

        SocketConnectionPool pool = newPool( connector );

        assertNotNull( pool.acquire( ADDRESS_1 ) );
        pool.acquire( ADDRESS_1 ).close(); // return to the pool
        assertNotNull( pool.acquire( ADDRESS_2 ) );
        pool.acquire( ADDRESS_2 ).close(); // return to the pool

        assertTrue( pool.hasAddress( ADDRESS_1 ) );
        assertTrue( pool.hasAddress( ADDRESS_2 ) );

        pool.close();

        verify( connection1 ).close();
        verify( connection2 ).close();
        verify( connection3 ).close();
        verify( connection4 ).close();
    }

    @Test
    public void closeRemovesAllPools()
    {
        Connection connection1 = newConnectionMock( ADDRESS_1 );
        Connection connection2 = newConnectionMock( ADDRESS_2 );
        Connection connection3 = newConnectionMock( ADDRESS_3 );

        Connector connector = newMockConnector( connection1, connection2, connection3 );

        SocketConnectionPool pool = newPool( connector );

        assertNotNull( pool.acquire( ADDRESS_1 ) );
        assertNotNull( pool.acquire( ADDRESS_2 ) );
        assertNotNull( pool.acquire( ADDRESS_3 ) );

        assertTrue( pool.hasAddress( ADDRESS_1 ) );
        assertTrue( pool.hasAddress( ADDRESS_2 ) );
        assertTrue( pool.hasAddress( ADDRESS_3 ) );

        pool.close();

        assertFalse( pool.hasAddress( ADDRESS_1 ) );
        assertFalse( pool.hasAddress( ADDRESS_2 ) );
        assertFalse( pool.hasAddress( ADDRESS_3 ) );
    }

    @Test
    public void closeWithConcurrentAcquisitionsEmptiesThePool() throws InterruptedException
    {
        Connector connector = mock( Connector.class );
        Set<Connection> createdConnections = newSetFromMap( new ConcurrentHashMap<Connection,Boolean>() );
        when( connector.connect( any( BoltServerAddress.class ) ) )
                .then( createConnectionAnswer( createdConnections ) );

        SocketConnectionPool pool = newPool( connector );

        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<Void>> results = new ArrayList<>();

        AtomicInteger port = new AtomicInteger();
        for ( int i = 0; i < 5; i++ )
        {
            Future<Void> result = executor.submit( acquireConnection( pool, port ) );
            results.add( result );
        }

        Thread.sleep( 500 ); // allow workers to do something

        pool.close();

        for ( Future<Void> result : results )
        {
            try
            {
                result.get( 20, TimeUnit.SECONDS );
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( ExecutionException.class ) );
                assertThat( e.getCause(), instanceOf( IllegalStateException.class ) );
            }
        }
        executor.shutdownNow();
        executor.awaitTermination( 10, TimeUnit.SECONDS );

        for ( int i = 0; i < port.intValue(); i++ )
        {
            boolean hasAddress = pool.hasAddress( new BoltServerAddress( "localhost", i ) );
            assertFalse( "Pool still has connection queues" + pool, hasAddress );
        }
        for ( Connection connection : createdConnections )
        {
            verify( connection ).close();
        }
    }

    @Test
    public void recentlyUsedConnectionNotValidatedDuringAcquisition() throws Throwable
    {
        long idleTimeBeforeConnectionTest = 100;
        long creationTimestamp = 42;
        long closedAfterMs = 10;
        long acquiredAfterMs = 20;

        Connection connection = newConnectionMock( ADDRESS_1 );

        FakeClock clock = new FakeClock();
        SocketConnectionPool pool = newPool( newMockConnector( connection ), clock, idleTimeBeforeConnectionTest );

        clock.progress( creationTimestamp );
        Connection acquiredConnection1 = pool.acquire( ADDRESS_1 );
        verify( connection, never() ).reset();
        verify( connection, never() ).sync();

        // return to the pool
        clock.progress( closedAfterMs );
        acquiredConnection1.close();
        verify( connection ).reset();
        verify( connection ).sync();

        clock.progress( acquiredAfterMs );
        Connection acquiredConnection2 = pool.acquire( ADDRESS_1 );
        assertSame( acquiredConnection1, acquiredConnection2 );

        // reset & sync were called only when pooled connection was closed previously
        verify( connection ).reset();
        verify( connection ).sync();
    }

    @Test
    public void connectionThatWasIdleForALongTimeIsValidatedDuringAcquisition() throws Throwable
    {
        Connection connection = newConnectionMock( ADDRESS_1 );
        long idleTimeBeforeConnectionTest = 100;
        FakeClock clock = new FakeClock();

        SocketConnectionPool pool = newPool( newMockConnector( connection ), clock, idleTimeBeforeConnectionTest );

        Connection acquiredConnection1 = pool.acquire( ADDRESS_1 );
        verify( connection, never() ).reset();
        verify( connection, never() ).sync();

        // return to the pool
        acquiredConnection1.close();
        verify( connection ).reset();
        verify( connection ).sync();

        clock.progress( idleTimeBeforeConnectionTest + 42 );

        Connection acquiredConnection2 = pool.acquire( ADDRESS_1 );
        assertSame( acquiredConnection1, acquiredConnection2 );

        // reset & sync were called only when pooled connection was closed previously
        verify( connection, times( 2 ) ).reset();
        verify( connection, times( 2 ) ).sync();
    }

    @Test
    public void connectionThatWasIdleForALongTimeIsNotValidatedDuringAcquisitionWhenTimeoutNotConfigured() throws Throwable
    {
        Connection connection = newConnectionMock( ADDRESS_1 );
        long idleTimeBeforeConnectionTest = PoolSettings.NO_IDLE_CONNECTION_TEST;
        FakeClock clock = new FakeClock();

        SocketConnectionPool pool = newPool( newMockConnector( connection ), clock, idleTimeBeforeConnectionTest );

        Connection acquiredConnection1 = pool.acquire( ADDRESS_1 );
        verify( connection, never() ).reset();
        verify( connection, never() ).sync();

        // return to the pool
        acquiredConnection1.close();
        verify( connection ).reset();
        verify( connection ).sync();

        clock.progress( 1000 );

        Connection acquiredConnection2 = pool.acquire( ADDRESS_1 );
        assertSame( acquiredConnection1, acquiredConnection2 );
        verify( connection ).reset();
        verify( connection ).sync();
    }

    @Test
    public void brokenConnectionsSkippedDuringAcquisition() throws Throwable
    {
        Connection connection1 = newConnectionMock( ADDRESS_1 );
        Connection connection2 = newConnectionMock( ADDRESS_1 );
        Connection connection3 = newConnectionMock( ADDRESS_1 );

        doNothing().doThrow( new RuntimeException( "failed to reset" ) ).when( connection1 ).reset();
        doNothing().doThrow( new RuntimeException( "failed to sync" ) ).when( connection2 ).sync();


        int idleTimeBeforeConnectionTest = 10;
        FakeClock clock = new FakeClock();
        Connector connector = newMockConnector( connection1, connection2, connection3 );
        SocketConnectionPool pool = newPool( connector, clock, idleTimeBeforeConnectionTest );

        Connection acquiredConnection1 = pool.acquire( ADDRESS_1 );
        Connection acquiredConnection2 = pool.acquire( ADDRESS_1 );
        Connection acquiredConnection3 = pool.acquire( ADDRESS_1 );

        // return acquired connections to the pool
        acquiredConnection1.close();
        acquiredConnection2.close();
        acquiredConnection3.close();

        clock.progress( idleTimeBeforeConnectionTest + 1 );

        Connection acquiredConnection = pool.acquire( ADDRESS_1 );
        acquiredConnection.reset();
        acquiredConnection.sync();
        assertSame( acquiredConnection3, acquiredConnection );
    }

    @Test
    public void limitedNumberOfBrokenConnectionsIsSkippedDuringAcquisition() throws Throwable
    {
        Connection connection1 = newConnectionMock( ADDRESS_1 );
        Connection connection2 = newConnectionMock( ADDRESS_1 );
        Connection connection3 = newConnectionMock( ADDRESS_1 );
        Connection connection4 = newConnectionMock( ADDRESS_1 );

        doNothing().doThrow( new RuntimeException( "failed to reset 1" ) ).when( connection1 ).reset();
        doNothing().doThrow( new RuntimeException( "failed to sync 2" ) ).when( connection2 ).sync();
        doNothing().doThrow( new RuntimeException( "failed to reset 3" ) ).when( connection3 ).reset();
        RuntimeException recentlyUsedConnectionFailure = new RuntimeException( "failed to sync 4" );
        doNothing().doThrow( recentlyUsedConnectionFailure ).when( connection4 ).sync();

        int idleTimeBeforeConnectionTest = 10;
        FakeClock clock = new FakeClock();
        Connector connector = newMockConnector( connection1, connection2, connection3, connection4 );
        SocketConnectionPool pool = newPool( connector, clock, idleTimeBeforeConnectionTest );

        Connection acquiredConnection1 = pool.acquire( ADDRESS_1 );
        Connection acquiredConnection2 = pool.acquire( ADDRESS_1 );
        Connection acquiredConnection3 = pool.acquire( ADDRESS_1 );
        Connection acquiredConnection4 = pool.acquire( ADDRESS_1 );

        acquiredConnection1.close();
        acquiredConnection2.close();
        acquiredConnection3.close();
        clock.progress( idleTimeBeforeConnectionTest + 1 );
        acquiredConnection4.close();

        Connection acquiredConnection = pool.acquire( ADDRESS_1 );
        acquiredConnection.reset();
        try
        {
            acquiredConnection.sync();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertSame( recentlyUsedConnectionFailure, e );
        }
        assertSame( acquiredConnection4, acquiredConnection );
    }

    @Test
    public void acquireRetriesUntilAConnectionIsCreated() throws Throwable
    {
        Connection connection1 = newConnectionMock( ADDRESS_1 );
        Connection connection2 = newConnectionMock( ADDRESS_1 );
        Connection connection3 = newConnectionMock( ADDRESS_1 );
        Connection connection4 = newConnectionMock( ADDRESS_1 );

        doNothing().doThrow( new RuntimeException() ).when( connection1 ).reset();
        doNothing().doThrow( new RuntimeException() ).when( connection2 ).reset();
        doNothing().doThrow( new RuntimeException() ).when( connection3 ).reset();

        int idleTimeBeforeConnectionTest = 10;
        FakeClock clock = new FakeClock();
        Connector connector = newMockConnector( connection1, connection2, connection3, connection4 );
        SocketConnectionPool pool = newPool( connector, clock, idleTimeBeforeConnectionTest );

        Connection acquiredConnection1 = pool.acquire( ADDRESS_1 );
        Connection acquiredConnection2 = pool.acquire( ADDRESS_1 );
        Connection acquiredConnection3 = pool.acquire( ADDRESS_1 );

        acquiredConnection1.close();
        acquiredConnection2.close();
        acquiredConnection3.close();

        // make all connections seem idle for too long
        clock.progress( idleTimeBeforeConnectionTest + 10 );

        Connection acquiredConnection = pool.acquire( ADDRESS_1 );
        assertThat( acquiredConnection,
                not( isOneOf( acquiredConnection1, acquiredConnection2, acquiredConnection3 ) ) );

        // all connections were tested and appeared to be broken
        InOrder inOrder = inOrder( connection1, connection2, connection3, connection4 );
        inOrder.verify( connection1 ).reset();
        inOrder.verify( connection2 ).reset();
        inOrder.verify( connection3 ).reset();
        inOrder.verify( connection4, never() ).reset();
        inOrder.verify( connection4, never() ).sync();
    }

    private static Answer<Connection> createConnectionAnswer( final Set<Connection> createdConnections )
    {
        return new Answer<Connection>()
        {
            @Override
            public Connection answer( InvocationOnMock invocation )
            {
                BoltServerAddress address = invocation.getArgumentAt( 0, BoltServerAddress.class );
                Connection connection = newConnectionMock( address );
                createdConnections.add( connection );
                return connection;
            }
        };
    }

    private static Callable<Void> acquireConnection( final SocketConnectionPool pool, final AtomicInteger port )
    {
        return new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                while ( true )
                {
                    pool.acquire( new BoltServerAddress( "localhost", port.incrementAndGet() ) );
                }
            }
        };
    }

    private static Connector newMockConnector()
    {
        Connection connection = mock( Connection.class );
        return newMockConnector( connection );
    }

    private static Connector newMockConnector( Connection connection, Connection... otherConnections )
    {
        Connector connector = mock( Connector.class );
        when( connector.connect( any( BoltServerAddress.class ) ) ).thenReturn( connection, otherConnections );
        return connector;
    }

    private static SocketConnectionPool newPool( Connector connector )
    {
        return newPool( connector, Clock.SYSTEM, 0 );
    }

    private static SocketConnectionPool newPool( Connector connector, Clock clock, long idleTimeBeforeConnectionTest )
    {
        PoolSettings poolSettings = new PoolSettings( 42, idleTimeBeforeConnectionTest );
        Logging logging = mock( Logging.class, RETURNS_MOCKS );
        return new SocketConnectionPool( poolSettings, connector, clock, logging );
    }

    private static Connection newConnectionMock( BoltServerAddress address )
    {
        Connection connection = mock( Connection.class );
        if ( address != null )
        {
            when( connection.boltServerAddress() ).thenReturn( address );
        }
        return connection;
    }
}

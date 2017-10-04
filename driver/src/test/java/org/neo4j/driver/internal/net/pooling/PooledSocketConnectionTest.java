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

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionValidator;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.exceptions.ClientException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.async.BoltServerAddress.LOCAL_DEFAULT;

public class PooledSocketConnectionTest
{

    private static final ConnectionValidator<PooledConnection> VALID_CONNECTION = newFixedValidator( true, true );
    private static final ConnectionValidator<PooledConnection> INVALID_CONNECTION = newFixedValidator( false, false );

    @Test
    public void shouldDisposeConnectionIfNotValidConnection() throws Throwable
    {
        // Given
        final BlockingPooledConnectionQueue pool = newConnectionQueue(1);

        final boolean[] flags = {false};

        Connection conn = mock( Connection.class );
        PooledConnectionReleaseConsumer releaseConsumer = new PooledConnectionReleaseConsumer( pool, INVALID_CONNECTION );


        PooledConnection pooledConnection = new PooledSocketConnection( conn, releaseConsumer, Clock.SYSTEM )
        {
            @Override
            public void dispose()
            {
                flags[0] = true;
            }
        };

        // When
        pooledConnection.close();

        // Then
        assertThat( pool.size(), equalTo( 0 ) );
        assertThat( flags[0], equalTo( true ) );
    }

    @Test
    public void shouldReturnToThePoolIfIsValidConnectionAndIdlePoolIsNotFull() throws Throwable
    {
        // Given
        final BlockingPooledConnectionQueue pool = newConnectionQueue(1);

        final boolean[] flags = {false};

        Connection conn = mock( Connection.class );
        PooledConnectionReleaseConsumer releaseConsumer = new PooledConnectionReleaseConsumer( pool, VALID_CONNECTION );

                PooledConnection pooledConnection = new PooledSocketConnection( conn, releaseConsumer, Clock.SYSTEM )
        {
            @Override
            public void dispose()
            {
                flags[0] = true;
            }
        };

        // When
        pooledConnection.close();

        // Then
        assertTrue( pool.contains(pooledConnection));
        assertThat( pool.size(), equalTo( 1 ) );
        assertThat( flags[0], equalTo( false ) );
    }


    @Test
    public void shouldDisposeConnectionIfValidConnectionAndIdlePoolIsFull() throws Throwable
    {
        // Given
        final BlockingPooledConnectionQueue pool = newConnectionQueue(1);

        final boolean[] flags = {false};

        Connection conn = mock( Connection.class );
        PooledConnectionReleaseConsumer releaseConsumer = new PooledConnectionReleaseConsumer( pool, VALID_CONNECTION);

        PooledConnection pooledConnection = new PooledSocketConnection( conn, releaseConsumer, Clock.SYSTEM );
        PooledConnection shouldBeClosedConnection = new PooledSocketConnection( conn, releaseConsumer, Clock.SYSTEM )
        {
            @Override
            public void dispose()
            {
                flags[0] = true;
            }
        };

        // When
        pooledConnection.close();
        shouldBeClosedConnection.close();

        // Then
        assertTrue( pool.contains(pooledConnection) );
        assertThat( pool.size(), equalTo( 1 ) );
        assertThat( flags[0], equalTo( true ) );
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldDisposeAcquiredConnectionsWhenPoolIsClosed()
    {
        PooledConnection connection = mock( PooledConnection.class );

        BlockingPooledConnectionQueue pool = newConnectionQueue( 5 );

        Supplier<PooledConnection> pooledConnectionFactory = mock( Supplier.class );
        when( pooledConnectionFactory.get() ).thenReturn( connection );

        PooledConnection acquiredConnection = pool.acquire( pooledConnectionFactory );
        assertSame( acquiredConnection, connection );

        pool.terminate();
        verify( connection ).dispose();
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldDisposeAcquiredAndIdleConnectionsWhenPoolIsClosed()
    {
        PooledConnection connection1 = mock( PooledConnection.class );
        PooledConnection connection2 = mock( PooledConnection.class );
        PooledConnection connection3 = mock( PooledConnection.class );

        BlockingPooledConnectionQueue pool = newConnectionQueue( 5 );

        Supplier<PooledConnection> pooledConnectionFactory = mock( Supplier.class );
        when( pooledConnectionFactory.get() )
                .thenReturn( connection1 )
                .thenReturn( connection2 )
                .thenReturn( connection3 );

        PooledConnection acquiredConnection1 = pool.acquire( pooledConnectionFactory );
        PooledConnection acquiredConnection2 = pool.acquire( pooledConnectionFactory );
        PooledConnection acquiredConnection3 = pool.acquire( pooledConnectionFactory );
        assertSame( acquiredConnection1, connection1 );
        assertSame( acquiredConnection2, connection2 );
        assertSame( acquiredConnection3, connection3 );

        pool.offer( acquiredConnection2 );

        pool.terminate();

        verify( connection1 ).dispose();
        verify( connection2 ).dispose();
        verify( connection3 ).dispose();
    }

    @Test
    public void shouldDisposeConnectionIfPoolAlreadyClosed() throws Throwable
    {
        // driver = GraphDatabase.driver();
        // session = driver.session();
        // ...
        // driver.close() -> clear the pools
        // session.close() -> well, close the connection directly without putting back to the pool

        // Given
        final BlockingPooledConnectionQueue pool = newConnectionQueue(1);
        pool.terminate();
        final boolean[] flags = {false};

        Connection conn = mock( Connection.class );
        PooledConnectionReleaseConsumer releaseConsumer = new PooledConnectionReleaseConsumer( pool, VALID_CONNECTION);

        PooledConnection pooledConnection = new PooledSocketConnection( conn, releaseConsumer, Clock.SYSTEM )
        {
            @Override
            public void dispose()
            {
                flags[0] = true;
            }
        };

        // When
        pooledConnection.close();

        // Then
        assertThat( pool.size(), equalTo( 0 ) );
        assertThat( flags[0], equalTo( true ) ); // make sure that the dispose is called
    }

    @Test
    public void shouldDisposeConnectionIfPoolStoppedAfterPuttingConnectionBackToPool() throws Throwable
    {
        // Given
        final BlockingPooledConnectionQueue pool = newConnectionQueue(1);
        pool.terminate();
        final boolean[] flags = {false};

        Connection conn = mock( Connection.class );

        PooledConnectionReleaseConsumer releaseConsumer = new PooledConnectionReleaseConsumer( pool, VALID_CONNECTION);

        PooledConnection pooledConnection = new PooledSocketConnection( conn, releaseConsumer, Clock.SYSTEM )
        {
            @Override
            public void dispose()
            {
                flags[0] = true;
            }
        };

        // When
        pooledConnection.close();

        // Then
        assertThat( pool.size(), equalTo( 0 ) );
        assertThat( flags[0], equalTo( true ) ); // make sure that the dispose is called
    }

    @Test
    public void shouldAckFailureOnRecoverableFailure() throws Throwable
    {
        // Given
        Connection conn = mock( Connection.class );
        ClientException error = new ClientException( "Neo.ClientError", "a recoverable error" );
        doThrow( error ).when( conn ).sync();
        PooledConnection pooledConnection = new PooledSocketConnection(
                conn,
                mock( PooledConnectionReleaseConsumer.class ),
                mock( Clock.class ) );

        // When
        try
        {
            pooledConnection.sync();
            fail( "Should have thrown a recoverable error" );
        }
        // Then
        catch( ClientException e )
        {
            assertThat( e, equalTo( error ) );
        }
        verify( conn, times( 1 ) ).ackFailure();
        assertThat( pooledConnection.hasUnrecoverableErrors(), equalTo( false ) );
    }

    @Test
    public void shouldNotAckFailureOnUnRecoverableFailure()
    {
        // Given
        Connection conn = mock( Connection.class );
        ClientException error = new ClientException( "an unrecoverable error" );
        doThrow( error ).when( conn ).sync();
        PooledConnection pooledConnection = new PooledSocketConnection(
                conn,
                mock( PooledConnectionReleaseConsumer.class ),
                mock( Clock.class ) );

        // When
        try
        {
            pooledConnection.sync();
            fail( "Should have thrown an unrecoverable error" );
        }
        //Then
        catch( ClientException e )
        {
            assertThat( e, equalTo( error ) );
        }
        verify( conn, times( 0 ) ).ackFailure();
        assertThat( pooledConnection.hasUnrecoverableErrors(), equalTo( true ) );
    }

    @Test
    public void shouldThrowExceptionIfFailureReceivedForAckFailure()
    {
        // Given
        Connection conn = mock( Connection.class );
        ClientException error = new ClientException( "Neo.ClientError", "a recoverable error" );

        ClientException failedToAckFailError = new ClientException(
                "Invalid server response message `FAILURE` received for client message `ACK_FAILURE`." );
        doThrow( error ).doThrow( failedToAckFailError ).when( conn ).sync();

        PooledConnection pooledConnection = new PooledSocketConnection(
                conn,
                mock( PooledConnectionReleaseConsumer.class ),
                mock( Clock.class ) );

        // When & Then
        try
        {
            pooledConnection.sync();
            fail( "Should have thrown a recoverable error" );
        }
        catch( ClientException e )
        {
            assertThat( e, equalTo( error ) );
        }
        assertThat( pooledConnection.hasUnrecoverableErrors(), equalTo( false ) );

        try
        {
            // sync ackFailure
            pooledConnection.sync();
            fail( "Should have thrown an unrecoverable error" );
        }
        catch( ClientException e )
        {
            assertThat( e, equalTo( failedToAckFailError ) );
        }

        verify( conn, times( 1 ) ).ackFailure();
        assertThat( pooledConnection.hasUnrecoverableErrors(), equalTo( true ) );
    }

    @Test
    public void hasNewLastUsedTimestampWhenCreated()
    {
        PooledConnectionReleaseConsumer releaseConsumer = mock( PooledConnectionReleaseConsumer.class );
        Clock clock = when( mock( Clock.class ).millis() ).thenReturn( 42L ).getMock();

        PooledConnection connection = new PooledSocketConnection( mock( Connection.class ), releaseConsumer, clock );

        assertEquals( 42L, connection.lastUsedTimestamp() );
    }

    @Test
    public void lastUsedTimestampUpdatedWhenConnectionClosed()
    {
        PooledConnectionReleaseConsumer releaseConsumer = mock( PooledConnectionReleaseConsumer.class );
        Clock clock = when( mock( Clock.class ).millis() )
                .thenReturn( 42L ).thenReturn( 42L ).thenReturn( 4242L ).thenReturn( 424242L ).getMock();

        PooledConnection connection = new PooledSocketConnection( mock( Connection.class ), releaseConsumer, clock );

        assertEquals( 42, connection.lastUsedTimestamp() );

        connection.close();
        assertEquals( 4242, connection.lastUsedTimestamp() );

        connection.close();
        assertEquals( 424242, connection.lastUsedTimestamp() );
    }

    @Test
    public void shouldHaveCreationTimestampAfterConstruction()
    {
        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 424242L ).thenReturn( -1L );

        PooledSocketConnection connection = new PooledSocketConnection( mock( Connection.class ),
                mock( PooledConnectionReleaseConsumer.class ), clock );

        long timestamp = connection.creationTimestamp();

        assertEquals( 424242L, timestamp );
    }

    @Test
    public void shouldNotChangeCreationTimestampAfterClose()
    {
        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 424242L ).thenReturn( -1L );

        PooledSocketConnection connection = new PooledSocketConnection( mock( Connection.class ),
                mock( PooledConnectionReleaseConsumer.class ), clock );

        long timestamp1 = connection.creationTimestamp();

        connection.close();
        long timestamp2 = connection.creationTimestamp();

        connection.close();
        long timestamp3 = connection.creationTimestamp();

        assertEquals( 424242L, timestamp1 );
        assertEquals( timestamp1, timestamp2 );
        assertEquals( timestamp1, timestamp3 );
    }

    private static BlockingPooledConnectionQueue newConnectionQueue( int capacity )
    {
        return new BlockingPooledConnectionQueue( LOCAL_DEFAULT, capacity, mock( Logging.class, RETURNS_MOCKS ) );
    }

    private static ConnectionValidator<PooledConnection> newFixedValidator( final boolean reusable,
            final boolean connected )
    {
        return new ConnectionValidator<PooledConnection>()
        {

            @Override
            public boolean isReusable( PooledConnection connection )
            {
                return reusable;
            }

            @Override
            public boolean isConnected( PooledConnection connection )
            {
                return connected;
            }
        };
    }
}

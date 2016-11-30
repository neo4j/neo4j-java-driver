/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import org.neo4j.driver.internal.exceptions.InvalidOperationException;
import org.neo4j.driver.internal.exceptions.ServerNeo4jException;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.v1.util.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;

public class PooledConnectionTest
{

    private static final Function<PooledConnection,Boolean>
            VALID_CONNECTION = new Function<PooledConnection,Boolean>()
    {
        @Override
        public Boolean apply( PooledConnection pooledConnection )
        {
            return true;
        }
    };
    private static final Function<PooledConnection,Boolean>
            INVALID_CONNECTION = new Function<PooledConnection,Boolean>()
    {
        @Override
        public Boolean apply( PooledConnection pooledConnection )
        {
            return false;
        }
    };

    @Test
    public void shouldDisposeConnectionIfNotValidConnection() throws Throwable
    {
        // Given
        final BlockingPooledConnectionQueue pool = newConnectionQueue(1);

        final boolean[] flags = {false};

        Connection conn = mock( Connection.class );
        PooledConnectionReleaseManager releaseConsumer = new PooledConnectionReleaseManager( pool, INVALID_CONNECTION );


        PooledConnection pooledConnection = new PooledSocketConnection( conn, releaseConsumer )
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
        PooledConnectionReleaseManager releaseConsumer = new PooledConnectionReleaseManager( pool, VALID_CONNECTION );

        PooledConnection pooledConnection = new PooledSocketConnection( conn, releaseConsumer )
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
        PooledConnectionReleaseManager releaseConsumer = new PooledConnectionReleaseManager( pool, VALID_CONNECTION);

        PooledConnection pooledConnection = new PooledSocketConnection( conn, releaseConsumer );
        PooledConnection shouldBeClosedConnection = new PooledSocketConnection( conn, releaseConsumer )
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
    public void shouldDisposeAcquiredConnectionsWhenPoolIsClosed() throws Throwable
    {
        PooledConnection connection = mock( PooledSocketConnection.class );

        BlockingPooledConnectionQueue pool = newConnectionQueue( 5 );

        PooledConnectionFactory pooledConnectionFactory = mock( PooledConnectionFactory.class );
        when( pooledConnectionFactory.newInstance() ).thenReturn( connection );

        PooledConnection acquiredConnection = pool.acquire( pooledConnectionFactory );
        assertSame( acquiredConnection, connection );

        pool.terminate();
        verify( connection ).dispose();
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldDisposeAcquiredAndIdleConnectionsWhenPoolIsClosed() throws Throwable
    {
        PooledConnection connection1 = mock( PooledSocketConnection.class );
        PooledConnection connection2 = mock( PooledSocketConnection.class );
        PooledConnection connection3 = mock( PooledSocketConnection.class );

        BlockingPooledConnectionQueue pool = newConnectionQueue( 5 );

        PooledConnectionFactory pooledConnectionFactory = mock( PooledConnectionFactory.class );
        when( pooledConnectionFactory.newInstance() )
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
        PooledConnectionReleaseManager releaseConsumer = new PooledConnectionReleaseManager( pool, VALID_CONNECTION);

        PooledConnection pooledConnection = new PooledSocketConnection( conn, releaseConsumer )
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

        PooledConnectionReleaseManager releaseConsumer = new PooledConnectionReleaseManager( pool, VALID_CONNECTION);

        PooledConnection pooledConnection = new PooledSocketConnection( conn, releaseConsumer )
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
        ServerNeo4jException error = new ServerNeo4jException( "Neo.ClientError", "a recoverable error" );
        doThrow( error ).when( conn ).sync();
        PooledConnection pooledConnection = new PooledSocketConnection(
                conn,
                mock( PooledConnectionReleaseManager.class ) );

        // When
        try
        {
            pooledConnection.sync();
            fail( "Should have thrown a recoverable error" );
        }
        // Then
        catch( Throwable e )
        {
            assertThat( e, CoreMatchers.<Throwable>equalTo( error ) );
        }
        verify( conn, times( 1 ) ).ackFailure();
        assertThat( pooledConnection.hasUnrecoverableErrors(), equalTo( false ) );
    }

    @Test
    public void shouldNotAckFailureOnUnRecoverableFailure() throws Throwable
    {
        // Given
        Connection conn = mock( Connection.class );
        InvalidOperationException error = new InvalidOperationException( "an unrecoverable error" );
        doThrow( error ).when( conn ).sync();
        PooledConnection pooledConnection = new PooledSocketConnection(
                conn,
                mock( PooledConnectionReleaseManager.class ) );

        // When
        try
        {
            pooledConnection.sync();
            fail( "Should have thrown an unrecoverable error" );
        }
        //Then
        catch( Throwable e )
        {
            assertThat( e, CoreMatchers.<Throwable>equalTo( error ) );
        }
        verify( conn, times( 0 ) ).ackFailure();
        assertThat( pooledConnection.hasUnrecoverableErrors(), equalTo( true ) );
    }

    @Test
    public void shouldThrowExceptionIfFailureReceivedForAckFailure() throws Throwable
    {
        // Given
        Connection conn = mock( Connection.class );
        ServerNeo4jException error = new ServerNeo4jException( "Neo.ClientError", "a recoverable error" );

        InvalidOperationException failedToAckFailError = new InvalidOperationException(
                "Invalid server response message `FAILURE` received for client message `ACK_FAILURE`." );
        doThrow( error ).doThrow( failedToAckFailError ).when( conn ).sync();

        PooledConnection pooledConnection = new PooledSocketConnection(
                conn,
                mock( PooledConnectionReleaseManager.class ) );

        // When & Then
        try
        {
            pooledConnection.sync();
            fail( "Should have thrown a recoverable error" );
        }
        catch( Throwable e )
        {
            assertThat( e, CoreMatchers.<Throwable>equalTo( error ) );
        }
        assertThat( pooledConnection.hasUnrecoverableErrors(), equalTo( false ) );

        try
        {
            // sync ackFailure
            pooledConnection.sync();
            fail( "Should have thrown an unrecoverable error" );
        }
        catch( Throwable e )
        {
            assertThat( e, CoreMatchers.<Throwable>equalTo( failedToAckFailError ) );
        }

        verify( conn, times( 1 ) ).ackFailure();
        assertThat( pooledConnection.hasUnrecoverableErrors(), equalTo( true ) );
    }

    private static BlockingPooledConnectionQueue newConnectionQueue( int capacity )
    {
        return new BlockingPooledConnectionQueue( LOCAL_DEFAULT, capacity, mock( Logging.class, RETURNS_MOCKS ) );
    }
}

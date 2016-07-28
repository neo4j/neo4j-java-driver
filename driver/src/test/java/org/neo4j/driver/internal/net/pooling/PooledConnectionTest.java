/**
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

import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.exceptions.ClientException;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PooledConnectionTest
{
    @Test
    public void shouldDisposeConnectionIfNotValidConnection() throws Throwable
    {
        // Given
        final BlockingQueue<PooledConnection> pool = new LinkedBlockingQueue<>(1);

        final boolean[] flags = {false};

        Connection conn = mock( Connection.class );
        PooledConnectionReleaseConsumer releaseConsumer = new PooledConnectionReleaseConsumer( pool,
                new AtomicBoolean( false ), PoolSettings.defaultSettings()  /*Does not matter what config for this test*/ )
        {
            @Override
            boolean validConnection( PooledConnection conn )
            {
                return false;
            }
        };

        PooledConnection pooledConnection = new PooledConnection( conn, releaseConsumer, Clock.SYSTEM )
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
        final BlockingQueue<PooledConnection> pool = new LinkedBlockingQueue<>(1);

        final boolean[] flags = {false};

        Connection conn = mock( Connection.class );
        PooledConnectionReleaseConsumer releaseConsumer = new PooledConnectionReleaseConsumer( pool,
                new AtomicBoolean( false ), PoolSettings.defaultSettings()  /*Does not matter what config for this test*/ )
        {
            @Override
            boolean validConnection( PooledConnection conn )
            {
                return true;
            }
        };

        PooledConnection pooledConnection = new PooledConnection( conn, releaseConsumer, Clock.SYSTEM )
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
        assertThat( pool, hasItem(pooledConnection) );
        assertThat( pool.size(), equalTo( 1 ) );
        assertThat( flags[0], equalTo( false ) );
    }

    @Test
    public void shouldDisposeConnectionIfValidConnectionAndIdlePoolIsFull() throws Throwable
    {
        // Given
        final BlockingQueue<PooledConnection> pool = new LinkedBlockingQueue<>(1);

        final boolean[] flags = {false};

        Connection conn = mock( Connection.class );
        PooledConnectionReleaseConsumer releaseConsumer = new PooledConnectionReleaseConsumer( pool,
                new AtomicBoolean( false ), PoolSettings.defaultSettings()  /*Does not matter what config for this test*/ )
        {
            @Override
            boolean validConnection( PooledConnection conn )
            {
                return true;
            }
        };

        PooledConnection pooledConnection = new PooledConnection( conn, releaseConsumer, Clock.SYSTEM );
        PooledConnection shouldBeClosedConnection = new PooledConnection( conn, releaseConsumer, Clock.SYSTEM )
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
        assertThat( pool, hasItem(pooledConnection) );
        assertThat( pool.size(), equalTo( 1 ) );
        assertThat( flags[0], equalTo( true ) );
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
        final BlockingQueue<PooledConnection> pool = new LinkedBlockingQueue<>(1);
        final boolean[] flags = {false};

        Connection conn = mock( Connection.class );
        PooledConnectionReleaseConsumer releaseConsumer = new PooledConnectionReleaseConsumer( pool,
                new AtomicBoolean( true ), PoolSettings.defaultSettings()  /*Does not matter what config for this test*/ );

        PooledConnection pooledConnection = new PooledConnection( conn, releaseConsumer, Clock.SYSTEM )
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
        final AtomicBoolean stopped = new AtomicBoolean( false );
        final BlockingQueue<PooledConnection> pool = new LinkedBlockingQueue<PooledConnection>(1){
            public boolean offer(PooledConnection conn)
            {
                stopped.set( true );
                // some clean work to close all connection in pool
                boolean offer = super.offer( conn );
                assertThat ( this.size(), equalTo( 1 ) );
                // we successfully put the connection back to the pool
                return offer;
            }
        };
        final boolean[] flags = {false};

        Connection conn = mock( Connection.class );

        PooledConnectionReleaseConsumer releaseConsumer = new PooledConnectionReleaseConsumer( pool,
                stopped , PoolSettings.defaultSettings()  /*Does not matter what config for this test*/ );

        PooledConnection pooledConnection = new PooledConnection( conn, releaseConsumer, Clock.SYSTEM )
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
        PooledConnection pooledConnection = new PooledConnection(
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
        PooledConnection pooledConnection = new PooledConnection(
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

        PooledConnection pooledConnection = new PooledConnection(
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
}

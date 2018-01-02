/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.util.Supplier;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;

public class PooledConnectionReleaseConsumerTest
{
    @Test
    public void shouldOfferReusableConnectionsBackToTheConnectionsQueue()
    {
        BlockingPooledConnectionQueue queue = newConnectionQueue();
        PooledConnection connection = acquireConnection( queue );

        PooledConnectionValidator validator = newConnectionValidator( true );
        PooledConnectionReleaseConsumer releaseConsumer = new PooledConnectionReleaseConsumer( queue, validator );

        releaseConsumer.accept( connection );

        // connection should now be idle
        assertEquals( 0, queue.activeConnections() );
        assertEquals( 1, queue.idleConnections() );

        verify( connection ).reset();
        verify( connection ).sync();
    }

    @Test
    public void shouldAskConnectionsQueueToDisposeNotReusableConnections()
    {
        BlockingPooledConnectionQueue queue = newConnectionQueue();
        PooledConnection connection = acquireConnection( queue );

        PooledConnectionValidator validator = newConnectionValidator( false );
        PooledConnectionReleaseConsumer releaseConsumer = new PooledConnectionReleaseConsumer( queue, validator );

        releaseConsumer.accept( connection );

        // connection should've been disposed
        assertEquals( 0, queue.activeConnections() );
        assertEquals( 0, queue.idleConnections() );

        verify( connection ).dispose();
    }

    private static BlockingPooledConnectionQueue newConnectionQueue()
    {
        return new BlockingPooledConnectionQueue( LOCAL_DEFAULT, 5, DEV_NULL_LOGGING );
    }

    @SuppressWarnings( "unchecked" )
    private static PooledConnection acquireConnection( BlockingPooledConnectionQueue queue )
    {
        queue.offer( newConnectionMock() );
        PooledConnection connection = queue.acquire( mock( Supplier.class ) );
        assertEquals( 1, queue.activeConnections() );
        return connection;
    }

    private static PooledConnectionValidator newConnectionValidator( boolean allowsConnections )
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        when( pool.hasAddress( LOCAL_DEFAULT ) ).thenReturn( allowsConnections );
        return new PooledConnectionValidator( pool );
    }

    private static PooledConnection newConnectionMock()
    {
        PooledConnection connection = mock( PooledConnection.class );
        when( connection.boltServerAddress() ).thenReturn( LOCAL_DEFAULT );
        return connection;
    }
}

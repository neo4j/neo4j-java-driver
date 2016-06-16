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
package org.neo4j.driver.internal.pool;

import org.junit.Test;

import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.StreamCollector;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.exceptions.DatabaseException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class PooledConnectionTest
{
    @Test
    public void shouldReturnToPoolIfExceptionDuringReset() throws Throwable
    {
        // Given
        final LinkedList<PooledConnection> returnedToPool = new LinkedList<>();
        Connection conn = mock( Connection.class );

        doThrow( new DatabaseException( "asd", "asd" ) ).when(conn).reset( any( StreamCollector.class) );

        PooledConnection pooledConnection = new PooledConnection( conn, new Consumer<PooledConnection>()
        {
            @Override
            public void accept( PooledConnection pooledConnection )
            {
                returnedToPool.add( pooledConnection );
            }
        }, Clock.SYSTEM );

        // When
        pooledConnection.close();

        // Then
        assertThat( returnedToPool, hasItem(pooledConnection) );
        assertThat( returnedToPool.size(), equalTo( 1 ));
    }


    @Test
    public void shouldCallDisposeToCloseConnectionDirectlyIfIdlePoolIsFull() throws Throwable
    {
        // Given
        final BlockingQueue<PooledConnection> pool = new LinkedBlockingQueue<>(1);

        final boolean[] flags = {false};

        Connection conn = mock( Connection.class );
        PooledConnectionReleaseConsumer releaseConsumer = new PooledConnectionReleaseConsumer( pool,
                new AtomicBoolean( false ), Config.defaultConfig() /*Does not matter what config for this test*/ )
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
    public void shouldCallDisposeToCloseConnectionIfDriverCloseBeforeSessionClose() throws Throwable
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
                new AtomicBoolean( true ), Config.defaultConfig() /*Does not matter what config for this test*/ );

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
}

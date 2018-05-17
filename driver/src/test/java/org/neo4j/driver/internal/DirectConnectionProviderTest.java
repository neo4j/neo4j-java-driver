/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.v1.AccessMode.READ;
import static org.neo4j.driver.v1.AccessMode.WRITE;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class DirectConnectionProviderTest
{
    @Test
    public void acquiresConnectionsFromThePool()
    {
        BoltServerAddress address = BoltServerAddress.LOCAL_DEFAULT;
        Connection connection1 = mock( Connection.class );
        Connection connection2 = mock( Connection.class );

        ConnectionPool pool = poolMock( address, connection1, connection2 );
        DirectConnectionProvider provider = new DirectConnectionProvider( address, pool );

        assertSame( connection1, await( provider.acquireConnection( READ ) ) );
        assertSame( connection2, await( provider.acquireConnection( WRITE ) ) );
    }

    @Test
    public void closesPool()
    {
        BoltServerAddress address = BoltServerAddress.LOCAL_DEFAULT;
        ConnectionPool pool = poolMock( address, mock( Connection.class ) );
        DirectConnectionProvider provider = new DirectConnectionProvider( address, pool );

        provider.close();

        verify( pool ).close();
    }

    @Test
    public void returnsCorrectAddress()
    {
        BoltServerAddress address = new BoltServerAddress( "server-1", 25000 );

        DirectConnectionProvider provider = new DirectConnectionProvider( address, mock( ConnectionPool.class ) );

        assertEquals( address, provider.getAddress() );
    }

    @SuppressWarnings( "unchecked" )
    private static ConnectionPool poolMock( BoltServerAddress address, Connection connection,
            Connection... otherConnections )
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        CompletableFuture<Connection>[] otherConnectionFutures = Stream.of( otherConnections )
                .map( CompletableFuture::completedFuture )
                .toArray( CompletableFuture[]::new );
        when( pool.acquire( address ) ).thenReturn( completedFuture( connection ), otherConnectionFutures );
        return pool;
    }
}

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

import org.junit.Test;

import org.neo4j.driver.internal.async.pool.AsyncConnectionPool;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.PooledConnection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.v1.AccessMode.READ;
import static org.neo4j.driver.v1.AccessMode.WRITE;

public class DirectConnectionProviderTest
{
    @Test
    public void acquiresConnectionsFromThePool()
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        PooledConnection connection1 = mock( PooledConnection.class );
        PooledConnection connection2 = mock( PooledConnection.class );
        when( pool.acquire( any( BoltServerAddress.class ) ) ).thenReturn( connection1, connection1, connection2 );

        DirectConnectionProvider provider = newConnectionProvider( pool );

        assertSame( connection1, provider.acquireConnection( READ ) );
        assertSame( connection2, provider.acquireConnection( WRITE ) );
    }

    @Test
    public void closesPool() throws Exception
    {
        ConnectionPool pool = mock( ConnectionPool.class, RETURNS_MOCKS );
        DirectConnectionProvider provider = newConnectionProvider( pool );

        provider.close();

        verify( pool ).close();
    }

    @Test
    public void returnsCorrectAddress()
    {
        BoltServerAddress address = new BoltServerAddress( "server-1", 25000 );

        DirectConnectionProvider provider = newConnectionProvider( address );

        assertEquals( address, provider.getAddress() );
    }

    @Test
    public void testsConnectivityOnCreation()
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        PooledConnection connection = mock( PooledConnection.class );
        when( pool.acquire( any( BoltServerAddress.class ) ) ).thenReturn( connection );

        assertNotNull( newConnectionProvider( pool ) );

        verify( pool ).acquire( BoltServerAddress.LOCAL_DEFAULT );
        verify( connection ).close();
    }

    @Test
    public void throwsWhenTestConnectionThrows()
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        PooledConnection connection = mock( PooledConnection.class );
        RuntimeException error = new RuntimeException();
        doThrow( error ).when( connection ).close();
        when( pool.acquire( any( BoltServerAddress.class ) ) ).thenReturn( connection );

        try
        {
            newConnectionProvider( pool );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertSame( error, e );
        }
    }

    private static DirectConnectionProvider newConnectionProvider( BoltServerAddress address )
    {
        return new DirectConnectionProvider( address, mock( ConnectionPool.class, RETURNS_MOCKS ),
                mock( AsyncConnectionPool.class ) );
    }

    private static DirectConnectionProvider newConnectionProvider( ConnectionPool pool )
    {
        return new DirectConnectionProvider( BoltServerAddress.LOCAL_DEFAULT, pool, mock( AsyncConnectionPool.class ) );
    }
}

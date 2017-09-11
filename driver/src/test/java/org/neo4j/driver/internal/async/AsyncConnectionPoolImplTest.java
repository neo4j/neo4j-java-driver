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
package org.neo4j.driver.internal.async;

import io.netty.bootstrap.Bootstrap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class AsyncConnectionPoolImplTest
{
    @Rule
    public final TestNeo4j neo4j = new TestNeo4j();

    private AsyncConnectionPoolImpl pool;

    @Before
    public void setUp() throws Exception
    {
        pool = newPool();
    }

    @After
    public void tearDown() throws Exception
    {
        pool.closeAsync();
    }

    @Test
    public void shouldAcquireConnectionWhenPoolIsEmpty() throws Exception
    {
        AsyncConnection connection = await( pool.acquire( neo4j.address() ) );

        assertNotNull( connection );
    }

    @Test
    public void shouldAcquireIdleConnection() throws Exception
    {
        AsyncConnection connection1 = await( pool.acquire( neo4j.address() ) );
        await( connection1.forceRelease() );

        AsyncConnection connection2 = await( pool.acquire( neo4j.address() ) );
        assertNotNull( connection2 );
    }

    @Test
    public void shouldFailToAcquireConnectionToWrongAddress() throws Exception
    {
        try
        {
            await( pool.acquire( new BoltServerAddress( "wrong-localhost" ) ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
            assertThat( e.getMessage(), startsWith( "Unable to connect" ) );
        }
    }

    @Test
    public void shouldFailToAcquireWhenPoolClosed() throws Exception
    {
        AsyncConnection connection = await( pool.acquire( neo4j.address() ) );
        await( connection.forceRelease() );
        await( pool.closeAsync() );

        try
        {
            pool.acquire( neo4j.address() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
            assertThat( e.getMessage(), startsWith( "Pool closed" ) );
        }
    }

    @Test
    public void shouldPurgeAddressWithConnections()
    {
        AsyncConnection connection1 = await( pool.acquire( neo4j.address() ) );
        AsyncConnection connection2 = await( pool.acquire( neo4j.address() ) );
        AsyncConnection connection3 = await( pool.acquire( neo4j.address() ) );

        assertNotNull( connection1 );
        assertNotNull( connection2 );
        assertNotNull( connection3 );

        assertEquals( 3, pool.activeConnections( neo4j.address() ) );

        pool.purge( neo4j.address() );

        assertEquals( 0, pool.activeConnections( neo4j.address() ) );
    }

    @Test
    public void shouldPurgeAddressWithoutConnections()
    {
        assertEquals( 0, pool.activeConnections( neo4j.address() ) );

        pool.purge( neo4j.address() );

        assertEquals( 0, pool.activeConnections( neo4j.address() ) );
    }

    @Test
    public void shouldCheckIfPoolHasAddress()
    {
        assertFalse( pool.hasAddress( neo4j.address() ) );

        await( pool.acquire( neo4j.address() ) );

        assertTrue( pool.hasAddress( neo4j.address() ) );
    }

    @Test
    public void shouldNotCloseWhenClosed()
    {
        assertNull( await( pool.closeAsync() ) );
        assertTrue( pool.closeAsync().isDone() );
    }

    private AsyncConnectionPoolImpl newPool() throws Exception
    {
        FakeClock clock = new FakeClock();
        ConnectionSettings connectionSettings = new ConnectionSettings( neo4j.authToken(), 5000 );
        ActiveChannelTracker poolHandler = new ActiveChannelTracker( DEV_NULL_LOGGING );
        AsyncConnectorImpl connector = new AsyncConnectorImpl( connectionSettings, SecurityPlan.forAllCertificates(),
                poolHandler, clock );
        PoolSettings poolSettings = new PoolSettings( 5, -1, -1, 10, 5000 );
        Bootstrap bootstrap = BootstrapFactory.newBootstrap( 1 );
        return new AsyncConnectionPoolImpl( connector, bootstrap, poolHandler, poolSettings, clock );
    }
}

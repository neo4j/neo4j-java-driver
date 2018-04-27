/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver.internal.async.pool;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.async.BootstrapFactory;
import org.neo4j.driver.internal.async.ChannelConnector;
import org.neo4j.driver.internal.async.ChannelConnectorImpl;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.util.TestNeo4j;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class ConnectionPoolImplTest
{
    private static final BoltServerAddress ADDRESS_1 = new BoltServerAddress( "server:1" );
    private static final BoltServerAddress ADDRESS_2 = new BoltServerAddress( "server:2" );
    private static final BoltServerAddress ADDRESS_3 = new BoltServerAddress( "server:3" );

    @Rule
    public final TestNeo4j neo4j = new TestNeo4j();

    private ConnectionPoolImpl pool;

    @Before
    public void setUp() throws Exception
    {
        pool = newPool();
    }

    @After
    public void tearDown()
    {
        pool.close();
    }

    @Test
    public void shouldAcquireConnectionWhenPoolIsEmpty()
    {
        Connection connection = await( pool.acquire( neo4j.address() ) );

        assertNotNull( connection );
    }

    @Test
    public void shouldAcquireIdleConnection()
    {
        Connection connection1 = await( pool.acquire( neo4j.address() ) );
        await( connection1.release() );

        Connection connection2 = await( pool.acquire( neo4j.address() ) );
        assertNotNull( connection2 );
    }

    @Test
    public void shouldFailToAcquireConnectionToWrongAddress()
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
    public void shouldFailToAcquireWhenPoolClosed()
    {
        Connection connection = await( pool.acquire( neo4j.address() ) );
        await( connection.release() );
        await( pool.close() );

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
    public void shouldNotCloseWhenClosed()
    {
        assertNull( await( pool.close() ) );
        assertTrue( pool.close().toCompletableFuture().isDone() );
    }

    @Test
    public void shouldDoNothingWhenRetainOnEmptyPool()
    {
        ActiveChannelTracker activeChannelTracker = mock( ActiveChannelTracker.class );
        TestConnectionPool pool = new TestConnectionPool( activeChannelTracker );

        pool.retainAll( singleton( LOCAL_DEFAULT ) );

        verifyZeroInteractions( activeChannelTracker );
    }

    @Test
    public void shouldRetainSpecifiedAddresses()
    {
        ActiveChannelTracker activeChannelTracker = mock( ActiveChannelTracker.class );
        TestConnectionPool pool = new TestConnectionPool( activeChannelTracker );

        pool.acquire( ADDRESS_1 );
        pool.acquire( ADDRESS_2 );
        pool.acquire( ADDRESS_3 );

        pool.retainAll( new HashSet<>( asList( ADDRESS_1, ADDRESS_2, ADDRESS_3 ) ) );
        for ( ChannelPool channelPool : pool.channelPoolsByAddress.values() )
        {
            verify( channelPool, never() ).close();
        }
    }

    @Test
    public void shouldClosePoolsWhenRetaining()
    {
        ActiveChannelTracker activeChannelTracker = mock( ActiveChannelTracker.class );
        TestConnectionPool pool = new TestConnectionPool( activeChannelTracker );

        pool.acquire( ADDRESS_1 );
        pool.acquire( ADDRESS_2 );
        pool.acquire( ADDRESS_3 );

        when( activeChannelTracker.activeChannelCount( ADDRESS_1 ) ).thenReturn( 2 );
        when( activeChannelTracker.activeChannelCount( ADDRESS_2 ) ).thenReturn( 0 );
        when( activeChannelTracker.activeChannelCount( ADDRESS_3 ) ).thenReturn( 3 );

        pool.retainAll( new HashSet<>( asList( ADDRESS_1, ADDRESS_3 ) ) );
        verify( pool.getPool( ADDRESS_1 ), never() ).close();
        verify( pool.getPool( ADDRESS_2 ) ).close();
        verify( pool.getPool( ADDRESS_3 ), never() ).close();
    }

    @Test
    public void shouldNotClosePoolsWithActiveConnectionsWhenRetaining()
    {
        ActiveChannelTracker activeChannelTracker = mock( ActiveChannelTracker.class );
        TestConnectionPool pool = new TestConnectionPool( activeChannelTracker );

        pool.acquire( ADDRESS_1 );
        pool.acquire( ADDRESS_2 );
        pool.acquire( ADDRESS_3 );

        when( activeChannelTracker.activeChannelCount( ADDRESS_1 ) ).thenReturn( 1 );
        when( activeChannelTracker.activeChannelCount( ADDRESS_2 ) ).thenReturn( 42 );
        when( activeChannelTracker.activeChannelCount( ADDRESS_3 ) ).thenReturn( 0 );

        pool.retainAll( singleton( ADDRESS_2 ) );
        verify( pool.getPool( ADDRESS_1 ), never() ).close();
        verify( pool.getPool( ADDRESS_2 ), never() ).close();
        verify( pool.getPool( ADDRESS_3 ) ).close();
    }

    private ConnectionPoolImpl newPool() throws Exception
    {
        FakeClock clock = new FakeClock();
        ConnectionSettings connectionSettings = new ConnectionSettings( neo4j.authToken(), 5000 );
        ChannelConnector connector = new ChannelConnectorImpl( connectionSettings, SecurityPlan.forAllCertificates(),
                DEV_NULL_LOGGING, clock );
        PoolSettings poolSettings = newSettings();
        Bootstrap bootstrap = BootstrapFactory.newBootstrap( 1 );
        return new ConnectionPoolImpl( connector, bootstrap, poolSettings, DEV_NULL_LOGGING, clock );
    }

    private static PoolSettings newSettings()
    {
        return new PoolSettings( 10, 5000, -1, -1 );
    }

    private static class TestConnectionPool extends ConnectionPoolImpl
    {
        final Map<BoltServerAddress,ChannelPool> channelPoolsByAddress = new HashMap<>();

        TestConnectionPool( ActiveChannelTracker activeChannelTracker )
        {
            super( mock( ChannelConnector.class ), mock( Bootstrap.class ), activeChannelTracker, newSettings(),
                    DEV_NULL_LOGGING, new FakeClock() );
        }

        ChannelPool getPool( BoltServerAddress address )
        {
            ChannelPool pool = channelPoolsByAddress.get( address );
            assertNotNull( pool );
            return pool;
        }

        @Override
        ChannelPool newPool( BoltServerAddress address )
        {
            ChannelPool channelPool = mock( ChannelPool.class );
            Channel channel = mock( Channel.class );
            doReturn( ImmediateEventExecutor.INSTANCE.newSucceededFuture( channel ) ).when( channelPool ).acquire();
            channelPoolsByAddress.put( address, channelPool );
            return channelPool;
        }
    }
}

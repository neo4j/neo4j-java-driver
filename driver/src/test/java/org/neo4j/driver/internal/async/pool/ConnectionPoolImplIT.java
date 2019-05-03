/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.internal.async.pool;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.async.connection.BootstrapFactory;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.async.connection.ChannelConnectorImpl;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.metrics.InternalAbstractMetrics.DEV_NULL_METRICS;
import static org.neo4j.driver.util.TestUtil.await;

@ParallelizableIT
class ConnectionPoolImplIT
{
    private static final BoltServerAddress ADDRESS_1 = new BoltServerAddress( "server:1" );
    private static final BoltServerAddress ADDRESS_2 = new BoltServerAddress( "server:2" );
    private static final BoltServerAddress ADDRESS_3 = new BoltServerAddress( "server:3" );

    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private ConnectionPoolImpl pool;

    @BeforeEach
    void setUp() throws Exception
    {
        pool = newPool();
    }

    @AfterEach
    void tearDown()
    {
        pool.close();
    }

    @Test
    void shouldAcquireConnectionWhenPoolIsEmpty()
    {
        Connection connection = await( pool.acquire( neo4j.address() ) );

        assertNotNull( connection );
    }

    @Test
    void shouldAcquireIdleConnection()
    {
        Connection connection1 = await( pool.acquire( neo4j.address() ) );
        await( connection1.release() );

        Connection connection2 = await( pool.acquire( neo4j.address() ) );
        assertNotNull( connection2 );
    }

    @Test
    void shouldFailToAcquireConnectionToWrongAddress()
    {
        ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class,
                () -> await( pool.acquire( new BoltServerAddress( "wrong-localhost" ) ) ) );

        assertThat( e.getMessage(), startsWith( "Unable to connect" ) );
    }

    @Test
    void shouldFailToAcquireWhenPoolClosed()
    {
        Connection connection = await( pool.acquire( neo4j.address() ) );
        await( connection.release() );
        await( pool.close() );

        IllegalStateException e = assertThrows( IllegalStateException.class, () -> pool.acquire( neo4j.address() ) );
        assertThat( e.getMessage(), startsWith( "Pool closed" ) );
    }

    @Test
    void shouldNotCloseWhenClosed()
    {
        assertNull( await( pool.close() ) );
        assertTrue( pool.close().toCompletableFuture().isDone() );
    }

    @Test
    void shouldDoNothingWhenRetainOnEmptyPool()
    {
        NettyChannelTracker nettyChannelTracker = mock( NettyChannelTracker.class );
        TestConnectionPool pool = new TestConnectionPool( nettyChannelTracker );

        pool.retainAll( singleton( LOCAL_DEFAULT ) );

        verifyZeroInteractions( nettyChannelTracker );
    }

    @Test
    void shouldRetainSpecifiedAddresses()
    {
        NettyChannelTracker nettyChannelTracker = mock( NettyChannelTracker.class );
        TestConnectionPool pool = new TestConnectionPool( nettyChannelTracker );

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
    void shouldClosePoolsWhenRetaining()
    {
        NettyChannelTracker nettyChannelTracker = mock( NettyChannelTracker.class );
        TestConnectionPool pool = new TestConnectionPool( nettyChannelTracker );

        pool.acquire( ADDRESS_1 );
        pool.acquire( ADDRESS_2 );
        pool.acquire( ADDRESS_3 );

        when( nettyChannelTracker.inUseChannelCount( ADDRESS_1 ) ).thenReturn( 2 );
        when( nettyChannelTracker.inUseChannelCount( ADDRESS_2 ) ).thenReturn( 0 );
        when( nettyChannelTracker.inUseChannelCount( ADDRESS_3 ) ).thenReturn( 3 );

        pool.retainAll( new HashSet<>( asList( ADDRESS_1, ADDRESS_3 ) ) );
        verify( pool.getPool( ADDRESS_1 ), never() ).close();
        verify( pool.getPool( ADDRESS_2 ) ).close();
        verify( pool.getPool( ADDRESS_3 ), never() ).close();
    }

    @Test
    void shouldNotClosePoolsWithActiveConnectionsWhenRetaining()
    {
        NettyChannelTracker nettyChannelTracker = mock( NettyChannelTracker.class );
        TestConnectionPool pool = new TestConnectionPool( nettyChannelTracker );

        pool.acquire( ADDRESS_1 );
        pool.acquire( ADDRESS_2 );
        pool.acquire( ADDRESS_3 );

        when( nettyChannelTracker.inUseChannelCount( ADDRESS_1 ) ).thenReturn( 1 );
        when( nettyChannelTracker.inUseChannelCount( ADDRESS_2 ) ).thenReturn( 42 );
        when( nettyChannelTracker.inUseChannelCount( ADDRESS_3 ) ).thenReturn( 0 );

        pool.retainAll( singleton( ADDRESS_2 ) );
        verify( pool.getPool( ADDRESS_1 ), never() ).close();
        verify( pool.getPool( ADDRESS_2 ), never() ).close();
        verify( pool.getPool( ADDRESS_3 ) ).close();
    }

    private ConnectionPoolImpl newPool() throws Exception
    {
        FakeClock clock = new FakeClock();
        ConnectionSettings connectionSettings = new ConnectionSettings( neo4j.authToken(), 5000 );
        ChannelConnector connector = new ChannelConnectorImpl( connectionSettings, SecurityPlan.forAllCertificates( false ),
                DEV_NULL_LOGGING, clock );
        PoolSettings poolSettings = newSettings();
        Bootstrap bootstrap = BootstrapFactory.newBootstrap( 1 );
        return new ConnectionPoolImpl( connector, bootstrap, poolSettings, DEV_NULL_METRICS, DEV_NULL_LOGGING, clock, true );
    }

    private static PoolSettings newSettings()
    {
        return new PoolSettings( 10, 5000, -1, -1 );
    }

    private static class TestConnectionPool extends ConnectionPoolImpl
    {
        final Map<BoltServerAddress,ChannelPool> channelPoolsByAddress = new HashMap<>();

        TestConnectionPool( NettyChannelTracker nettyChannelTracker )
        {
            super( mock( ChannelConnector.class ), mock( Bootstrap.class ), nettyChannelTracker, newSettings(),
                    DEV_NULL_METRICS, DEV_NULL_LOGGING, new FakeClock(), true );
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

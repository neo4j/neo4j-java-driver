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
package org.neo4j.driver.internal.async.pool;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.util.concurrent.Future;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.async.BootstrapFactory;
import org.neo4j.driver.internal.async.ChannelConnectorImpl;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.AuthenticationException;
import org.neo4j.driver.v1.util.Neo4jRunner;
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.v1.Values.value;

public class NettyChannelPoolTest
{
    @Rule
    public final TestNeo4j neo4j = new TestNeo4j();

    private Bootstrap bootstrap;
    private ChannelPoolHandler poolHandler;
    private NettyChannelPool pool;

    @Before
    public void setUp()
    {
        bootstrap = BootstrapFactory.newBootstrap( 1 );
        poolHandler = mock( ChannelPoolHandler.class );
    }

    @After
    public void tearDown()
    {
        if ( pool != null )
        {
            pool.close();
        }
        if ( bootstrap != null )
        {
            bootstrap.config().group().shutdownGracefully().syncUninterruptibly();
        }
    }

    @Test
    public void shouldAcquireAndReleaseWithCorrectCredentials() throws Exception
    {
        pool = newPool( neo4j.authToken() );

        Future<Channel> acquireFuture = pool.acquire();
        acquireFuture.await( 5, TimeUnit.SECONDS );

        assertTrue( acquireFuture.isSuccess() );
        Channel channel = acquireFuture.getNow();
        assertNotNull( channel );
        verify( poolHandler ).channelCreated( channel );
        verify( poolHandler, never() ).channelReleased( channel );

        Future<Void> releaseFuture = pool.release( channel );
        releaseFuture.await( 5, TimeUnit.SECONDS );

        assertTrue( releaseFuture.isSuccess() );
        verify( poolHandler ).channelReleased( channel );
    }

    @Test
    public void shouldFailToAcquireWithWrongCredentials() throws Exception
    {
        pool = newPool( AuthTokens.basic( "wrong", "wrong" ) );

        Future<Channel> future = pool.acquire();
        future.await( 5, TimeUnit.DAYS );

        assertTrue( future.isDone() );
        assertNotNull( future.cause() );
        assertThat( future.cause(), instanceOf( AuthenticationException.class ) );

        verify( poolHandler, never() ).channelCreated( any() );
        verify( poolHandler, never() ).channelReleased( any() );
    }

    @Test
    public void shouldAllowAcquireAfterFailures() throws Exception
    {
        int maxConnections = 2;

        Map<String,Value> authTokenMap = new HashMap<>();
        authTokenMap.put( "scheme", value( "basic" ) );
        authTokenMap.put( "principal", value( "neo4j" ) );
        authTokenMap.put( "credentials", value( "wrong" ) );
        InternalAuthToken authToken = new InternalAuthToken( authTokenMap );

        pool = newPool( authToken, maxConnections );

        for ( int i = 0; i < maxConnections; i++ )
        {
            try
            {
                pool.acquire().get( 5, TimeUnit.SECONDS );
                fail( "Exception expected" );
            }
            catch ( ExecutionException e )
            {
                assertThat( e.getCause(), instanceOf( AuthenticationException.class ) );
            }
        }

        authTokenMap.put( "credentials", value( Neo4jRunner.PASSWORD ) );

        Channel channel = pool.acquire().get( 5, TimeUnit.SECONDS );
        assertNotNull( channel );
    }

    @Test
    public void shouldLimitNumberOfConcurrentConnections() throws Exception
    {
        int maxConnections = 5;
        pool = newPool( neo4j.authToken(), maxConnections );

        for ( int i = 0; i < maxConnections; i++ )
        {
            Channel channel = pool.acquire().get( 5, TimeUnit.SECONDS );
            assertNotNull( channel );
        }

        try
        {
            pool.acquire().get( 5, TimeUnit.SECONDS );
            fail( "Exception expected" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), instanceOf( TimeoutException.class ) );
            assertEquals( e.getCause().getMessage(), "Acquire operation took longer then configured maximum time" );
        }
    }

    private NettyChannelPool newPool( AuthToken authToken )
    {
        return newPool( authToken, 100 );
    }

    private NettyChannelPool newPool( AuthToken authToken, int maxConnections )
    {
        ConnectionSettings settings = new ConnectionSettings( authToken, 5_000 );
        ChannelConnectorImpl connector = new ChannelConnectorImpl( settings, SecurityPlan.insecure(), DEV_NULL_LOGGING,
                new FakeClock() );
        return new NettyChannelPool( neo4j.address(), connector, bootstrap, poolHandler, ChannelHealthChecker.ACTIVE,
                1_000, maxConnections );
    }
}

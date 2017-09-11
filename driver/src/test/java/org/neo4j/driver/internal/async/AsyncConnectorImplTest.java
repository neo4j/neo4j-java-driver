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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.pool.ChannelPoolHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.exceptions.AuthenticationException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class AsyncConnectorImplTest
{
    @Rule
    public final TestNeo4j neo4j = new TestNeo4j();

    private Bootstrap bootstrap;

    @Before
    public void setUp() throws Exception
    {
        bootstrap = BootstrapFactory.newBootstrap( 1 );
    }

    @After
    public void tearDown() throws Exception
    {
        if ( bootstrap != null )
        {
            bootstrap.config().group().shutdownGracefully();
        }
    }

    @Test
    public void shouldConnect() throws Exception
    {
        AsyncConnectorImpl connector = newConnector( neo4j.authToken() );

        ChannelFuture channelFuture = connector.connect( neo4j.address(), bootstrap );
        assertTrue( channelFuture.await( 10, TimeUnit.SECONDS ) );
        Channel channel = channelFuture.channel();

        assertNull( channelFuture.get() );
        assertTrue( channel.isActive() );
    }

    @Test
    public void shouldFailToConnectToWrongAddress() throws Exception
    {
        AsyncConnectorImpl connector = newConnector( neo4j.authToken() );

        ChannelFuture channelFuture = connector.connect( new BoltServerAddress( "wrong-localhost" ), bootstrap );
        assertTrue( channelFuture.await( 10, TimeUnit.SECONDS ) );
        Channel channel = channelFuture.channel();

        try
        {
            channelFuture.get();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ExecutionException.class ) );
            assertThat( e.getCause(), instanceOf( ServiceUnavailableException.class ) );
            assertThat( e.getCause().getMessage(), startsWith( "Unable to connect" ) );
        }
        assertFalse( channel.isActive() );
    }

    @Test
    public void shouldFailToConnectWithWrongCredentials() throws Exception
    {
        AuthToken authToken = AuthTokens.basic( "neo4j", "wrong-password" );
        AsyncConnectorImpl connector = newConnector( authToken );

        ChannelFuture channelFuture = connector.connect( neo4j.address(), bootstrap );
        assertTrue( channelFuture.await( 10, TimeUnit.SECONDS ) );
        Channel channel = channelFuture.channel();

        try
        {
            channelFuture.get();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ExecutionException.class ) );
            assertThat( e.getCause(), instanceOf( AuthenticationException.class ) );
        }
        assertFalse( channel.isActive() );
    }

    private AsyncConnectorImpl newConnector( AuthToken authToken ) throws Exception
    {
        ConnectionSettings settings = new ConnectionSettings( authToken, 1000 );
        return new AsyncConnectorImpl( settings, SecurityPlan.forAllCertificates(),
                mock( ChannelPoolHandler.class ), new FakeClock() );
    }
}

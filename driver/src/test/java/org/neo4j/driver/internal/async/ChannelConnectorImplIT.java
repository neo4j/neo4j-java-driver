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
package org.neo4j.driver.internal.async;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.async.inbound.ConnectTimeoutHandler;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.exceptions.AuthenticationException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.util.TestNeo4j;

import static java.util.concurrent.CompletableFuture.runAsync;
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

public class ChannelConnectorImplIT
{
    @Rule
    public final TestNeo4j neo4j = new TestNeo4j();

    private Bootstrap bootstrap;

    @Before
    public void setUp()
    {
        bootstrap = BootstrapFactory.newBootstrap( 1 );
    }

    @After
    public void tearDown()
    {
        if ( bootstrap != null )
        {
            bootstrap.config().group().shutdownGracefully().syncUninterruptibly();
        }
    }

    @Test
    public void shouldConnect() throws Exception
    {
        ChannelConnector connector = newConnector( neo4j.authToken() );

        ChannelFuture channelFuture = connector.connect( neo4j.address(), bootstrap );
        assertTrue( channelFuture.await( 10, TimeUnit.SECONDS ) );
        Channel channel = channelFuture.channel();

        assertNull( channelFuture.get() );
        assertTrue( channel.isActive() );
    }

    @Test
    public void shouldSetupHandlers() throws Exception
    {
        ChannelConnector connector = newConnector( neo4j.authToken(), SecurityPlan.forAllCertificates(), 10_000 );

        ChannelFuture channelFuture = connector.connect( neo4j.address(), bootstrap );
        assertTrue( channelFuture.await( 10, TimeUnit.SECONDS ) );

        Channel channel = channelFuture.channel();
        ChannelPipeline pipeline = channel.pipeline();
        assertTrue( channel.isActive() );

        assertNotNull( pipeline.get( SslHandler.class ) );
        assertNull( pipeline.get( ConnectTimeoutHandler.class ) );
    }

    @Test
    public void shouldFailToConnectToWrongAddress() throws Exception
    {
        ChannelConnector connector = newConnector( neo4j.authToken() );

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
        ChannelConnector connector = newConnector( authToken );

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

    @Test
    public void shouldEnforceConnectTimeout() throws Exception
    {
        ChannelConnector connector = newConnector( neo4j.authToken(), 1000 );

        // try connect to a non-routable ip address 10.0.0.0, it will never respond
        ChannelFuture channelFuture = connector.connect( new BoltServerAddress( "10.0.0.0" ), bootstrap );

        try
        {
            await( channelFuture );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
        }
    }

    @Test
    public void shouldFailWhenProtocolNegotiationTakesTooLong() throws Exception
    {
        // run without TLS so that Bolt handshake is the very first operation after connection is established
        testReadTimeoutOnConnect( SecurityPlan.insecure() );
    }

    @Test
    public void shouldFailWhenTLSHandshakeTakesTooLong() throws Exception
    {
        // run with TLS so that TLS handshake is the very first operation after connection is established
        testReadTimeoutOnConnect( SecurityPlan.forAllCertificates() );
    }

    @Test
    public void shouldThrowServiceUnavailableExceptionOnFailureDuringConnect() throws Exception
    {
        ServerSocket server = new ServerSocket( 0 );
        BoltServerAddress address = new BoltServerAddress( "localhost", server.getLocalPort() );

        runAsync( () ->
        {
            try
            {
                // wait for a connection
                Socket socket = server.accept();
                // and terminate it immediately so that client gets a "reset by peer" IOException
                socket.close();
                server.close();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        } );

        ChannelConnector connector = newConnector( neo4j.authToken() );
        ChannelFuture channelFuture = connector.connect( address, bootstrap );

        // connect operation should fail with ServiceUnavailableException
        try
        {
            await( channelFuture );
            fail( "Exception expected" );
        }
        catch ( ServiceUnavailableException ignore )
        {
        }
    }

    private void testReadTimeoutOnConnect( SecurityPlan securityPlan ) throws IOException
    {
        try ( ServerSocket server = new ServerSocket( 0 ) ) // server that accepts connections but does not reply
        {
            int timeoutMillis = 1_000;
            BoltServerAddress address = new BoltServerAddress( "localhost", server.getLocalPort() );
            ChannelConnector connector = newConnector( neo4j.authToken(), securityPlan, timeoutMillis );

            ChannelFuture channelFuture = connector.connect( address, bootstrap );
            try
            {
                await( channelFuture );
                fail( "Exception expected" );
            }
            catch ( ServiceUnavailableException e )
            {
                assertEquals( e.getMessage(), "Unable to establish connection in " + timeoutMillis + "ms" );
            }
        }
    }

    private ChannelConnectorImpl newConnector( AuthToken authToken ) throws Exception
    {
        return newConnector( authToken, Integer.MAX_VALUE );
    }

    private ChannelConnectorImpl newConnector( AuthToken authToken, int connectTimeoutMillis ) throws Exception
    {
        return newConnector( authToken, SecurityPlan.forAllCertificates(), connectTimeoutMillis );
    }

    private ChannelConnectorImpl newConnector( AuthToken authToken, SecurityPlan securityPlan,
            int connectTimeoutMillis )
    {
        ConnectionSettings settings = new ConnectionSettings( authToken, connectTimeoutMillis );
        return new ChannelConnectorImpl( settings, securityPlan, DEV_NULL_LOGGING, new FakeClock() );
    }
}

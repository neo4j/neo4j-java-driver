/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.driver.integration;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.RevocationStrategy;
import org.neo4j.driver.internal.async.connection.BootstrapFactory;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.async.connection.ChannelConnectorImpl;
import org.neo4j.driver.internal.async.inbound.ConnectTimeoutHandler;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.util.TestUtil.await;

@ParallelizableIT
class ChannelConnectorImplIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Bootstrap bootstrap;

    @BeforeEach
    void setUp()
    {
        bootstrap = BootstrapFactory.newBootstrap( 1 );
    }

    @AfterEach
    void tearDown()
    {
        if ( bootstrap != null )
        {
            bootstrap.config().group().shutdownGracefully().syncUninterruptibly();
        }
    }

    @Test
    void shouldConnect() throws Exception
    {
        ChannelConnector connector = newConnector( neo4j.authToken() );

        ChannelFuture channelFuture = connector.connect( neo4j.address(), bootstrap );
        assertTrue( channelFuture.await( 10, TimeUnit.SECONDS ) );
        Channel channel = channelFuture.channel();

        assertNull( channelFuture.get() );
        assertTrue( channel.isActive() );
    }

    @Test
    void shouldSetupHandlers() throws Exception
    {
        ChannelConnector connector = newConnector( neo4j.authToken(), trustAllCertificates(), 10_000 );

        ChannelFuture channelFuture = connector.connect( neo4j.address(), bootstrap );
        assertTrue( channelFuture.await( 10, TimeUnit.SECONDS ) );

        Channel channel = channelFuture.channel();
        ChannelPipeline pipeline = channel.pipeline();
        assertTrue( channel.isActive() );

        assertNotNull( pipeline.get( SslHandler.class ) );
        assertNull( pipeline.get( ConnectTimeoutHandler.class ) );
    }

    @Test
    void shouldFailToConnectToWrongAddress() throws Exception
    {
        ChannelConnector connector = newConnector( neo4j.authToken() );

        ChannelFuture channelFuture = connector.connect( new BoltServerAddress( "wrong-localhost" ), bootstrap );
        assertTrue( channelFuture.await( 10, TimeUnit.SECONDS ) );
        Channel channel = channelFuture.channel();

        ExecutionException e = assertThrows( ExecutionException.class, channelFuture::get );

        assertThat( e.getCause(), instanceOf( ServiceUnavailableException.class ) );
        assertThat( e.getCause().getMessage(), startsWith( "Unable to connect" ) );
        assertFalse( channel.isActive() );
    }

    @Test
    void shouldFailToConnectWithWrongCredentials() throws Exception
    {
        AuthToken authToken = AuthTokens.basic( "neo4j", "wrong-password" );
        ChannelConnector connector = newConnector( authToken );

        ChannelFuture channelFuture = connector.connect( neo4j.address(), bootstrap );
        assertTrue( channelFuture.await( 10, TimeUnit.SECONDS ) );
        Channel channel = channelFuture.channel();

        ExecutionException e = assertThrows( ExecutionException.class, channelFuture::get );
        assertThat( e.getCause(), instanceOf( AuthenticationException.class ) );
        assertFalse( channel.isActive() );
    }

    @Test
    void shouldEnforceConnectTimeout() throws Exception
    {
        ChannelConnector connector = newConnector( neo4j.authToken(), 1000 );

        // try connect to a non-routable ip address 10.0.0.0, it will never respond
        ChannelFuture channelFuture = connector.connect( new BoltServerAddress( "10.0.0.0" ), bootstrap );

        assertThrows( ServiceUnavailableException.class, () -> await( channelFuture ) );
    }

    @Test
    void shouldFailWhenProtocolNegotiationTakesTooLong() throws Exception
    {
        // run without TLS so that Bolt handshake is the very first operation after connection is established
        testReadTimeoutOnConnect( SecurityPlanImpl.insecure() );
    }

    @Test
    void shouldFailWhenTLSHandshakeTakesTooLong() throws Exception
    {
        // run with TLS so that TLS handshake is the very first operation after connection is established
        testReadTimeoutOnConnect( trustAllCertificates() );
    }

    @Test
    void shouldThrowServiceUnavailableExceptionOnFailureDuringConnect() throws Exception
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
        assertThrows( ServiceUnavailableException.class, () -> await( channelFuture ) );
    }

    private void testReadTimeoutOnConnect( SecurityPlan securityPlan ) throws IOException
    {
        try ( ServerSocket server = new ServerSocket( 0 ) ) // server that accepts connections but does not reply
        {
            int timeoutMillis = 1_000;
            BoltServerAddress address = new BoltServerAddress( "localhost", server.getLocalPort() );
            ChannelConnector connector = newConnector( neo4j.authToken(), securityPlan, timeoutMillis );

            ChannelFuture channelFuture = connector.connect( address, bootstrap );

            ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, () -> await( channelFuture ) );
            assertEquals( e.getMessage(), "Unable to establish connection in " + timeoutMillis + "ms" );
        }
    }

    private ChannelConnectorImpl newConnector( AuthToken authToken ) throws Exception
    {
        return newConnector( authToken, Integer.MAX_VALUE );
    }

    private ChannelConnectorImpl newConnector( AuthToken authToken, int connectTimeoutMillis ) throws Exception
    {
        return newConnector( authToken, trustAllCertificates(), connectTimeoutMillis );
    }

    private ChannelConnectorImpl newConnector( AuthToken authToken, SecurityPlan securityPlan,
            int connectTimeoutMillis )
    {
        ConnectionSettings settings = new ConnectionSettings( authToken, "test", connectTimeoutMillis );
        return new ChannelConnectorImpl( settings, securityPlan, DEV_NULL_LOGGING, new FakeClock(), RoutingContext.EMPTY );
    }

    private static SecurityPlan trustAllCertificates() throws GeneralSecurityException
    {
        return SecurityPlanImpl.forAllCertificates( false, RevocationStrategy.NO_CHECKS );
    }
}
